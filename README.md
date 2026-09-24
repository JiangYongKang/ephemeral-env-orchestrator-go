# 本地临时开发环境声明式编排器（Go）

一个**完全本地、无外部依赖**的临时开发环境声明式编排器。它用单个 JSON 状态文件模拟控制面数据（写前日志 + 原子落盘），用内存假后端模拟计算/存储资源，用可注入的时钟驱动 TTL。**不依赖任何真实集群、网络或凭据。**

目标：在进程重启、并发触发、部分失败与短暂抖动后，仍能得到**一致、可解释、无重复资源、无资源泄漏**的终态。

---

## 1. 包结构

| 包 | 职责 |
|---|---|
| `internal/domain` | 核心模型：环境 `Environment`、命令 `Command`、状态 `State`、阶段 `Phase`、分类错误 `Error`、时钟抽象 |
| `internal/store` | 单文件 JSON 全量快照；`临时文件 + fsync + rename + 目录fsync` 原子写 |
| `internal/backend` | 假资源后端；`Ensure*` 按资源 ID 幂等；确定性故障注入；副作用计数器与泄漏探测 |
| `internal/quota` | 纯计算：租户配额与全局容量结算（无 I/O，由编排器持锁调用） |
| `internal/reconciler` | 收敛状态机：每次循环一步后端调用、每步落盘，可中断续跑、自愈、过期注入 |
| `internal/orchestrator` | 应用门面：校验、鉴权、配额原子预留、幂等键、意图持久化、触发收敛、启动恢复 |
| `cmd/eo` | 零外部依赖的端到端演示 CLI |

---

## 2. 声明模型（Declaration Model）

### 2.1 环境规格 `domain.Spec`

| 字段 | 可变性 | 说明 |
|---|---|---|
| `Name` | **可变** | 租户内唯一（仅与存活环境冲突；终止墓碑不占用名字） |
| `Owner` | **不可变** | 创建者；之后任何接口都无法修改 |
| `Requested` | **可变（受配额约束）** | CPU/内存/存储申请量 |
| `TTL` | **可变** | 有效期；0 表示永不过期；更新 TTL 会以“当前时间 + 新TTL”重算过期点 |
| `DesiredRunning` | **可变** | 期望运行态（true=运行，false=停止但保留资源与配额） |

**结构性不可变**：`EnvID`、`Tenant`、`Spec.Owner`、`CreatedAt` 在 API 层没有任何字段可以改写（不存在更新入口），从类型层面杜绝篡改。

### 2.2 命令 `domain.Command`

所有变更都被建模为**先持久化的命令**（write-ahead intent），携带：

- `Seq`：全局单调序号；
- `Type`：`create/update/start/stop/delete/expire/recover`；
- `DedupKey`：幂等键（按租户命名空间）；
- `WantRunning`：start/stop 的意图载荷（放在命令上而非环境上，避免暂存多个冲突意图时相互覆盖）。

### 2.3 完整持久状态 `domain.State`

`Envs`（环境记录）、`Commands`（待收敛命令队列）、`Dedup`（幂等结果表）、`TenantLimits`、`ClusterCapacity`、`LastSeq`、`Version`。配额占用**不从独立计数器读取**，而是由各环境的 `Reserved` 字段实时汇总——这保证重启后配额不会与环境漂移。

---

## 3. 状态流转（State Machine）

```
                         create(Start=true)
   (受理) ──► provisioning ───────────────► starting ──► running
                  │  (Start=false)             ▲          │  │
                  └──────────────► stopped ◄───┘          │  │ stop
                                      ▲   └──── start ─────┘  ▼
                                      └──── stopping ◄────────
                update 可在任意稳态调整 name/resources/ttl/desired，并触发 start/stop
   running/stopped/provisioning/... ──delete/expire──► deleting/expiring
        先 EnsureStopped（有序停止） 再 EnsureDeleted ──► terminated（释放配额）
   任何无法恢复的错误 ──────────────────────────────► failed（记录原因、释放预留）
```

关键阶段：`provisioning / running / stopping / stopped / starting / expiring / deleting / terminated / failed`。

### 3.1 可中断、可续跑

- 收敛器**每次循环只做一次后端调用**，调用前后都把阶段/意图持久化；
- 后端每个操作都是**按资源 ID 幂等**的 `Ensure*`，崩溃后重放同一调用不会产生第二个资源；
- 重启时 `Orchestrator.New` 会立即对状态执行同一个幂等 `Pump`，从最后一个已落盘步骤继续（如 `deleting` 态自动“停止→删除→终止”）；
- `Inspect` 用于异常恢复：若资源在带外消失，`running/stopped` 态会被检测到并精确重建一次，环境与配额不重复。

### 3.2 冲突的确定性优先级

同一环境存在多个待处理命令时，选择规则为 **(优先级升序, 序号升序)**：

| 优先级 | 命令 |
|---|---|
| 0 | `delete` / `expire`（拆除永远最高优先） |
| 1 | `recover` |
| 2 | `create` / `update` |
| 3 | `start` / `stop`（对称意图，按提交序号决胜负） |

一旦某命令把环境带出稳态，就用 `Environment.ActiveSeq` **粘性锁定**该命令直到完成——避免“正在 stop 时，排队的 start 因此刻仍 running 而被误判满足”。因此 **delete 永远胜过并发 start**；先 stop 后 start 会严格按序号 stop→start 收敛，结果与时序无关。

### 3.3 过期回收（TTL）

收敛时若 `now >= ExpiresAt` 且环境仍存活，会注入合成 `expire`：有序停止→删除资源→进入 `terminated`→**原子释放配额**→释放占用的名字。更新 TTL 会续期。过期回收同样可跨重启完成。

---

## 4. 权限规则

- 调用方 `Principal{User, Tenant, Role}`，`user` 必须属于某个租户；`admin` 为全局，可跨租户；
- **读**（Get/List/Usage）：同租户成员或 admin；
- **写**（Update/Start/Stop/Delete）：同租户的 **owner 本人** 或 admin；
- 设置租户配额 / 全局容量：**仅 admin**；
- 跨租户访问返回 `forbidden/cross_tenant`，非属主写返回 `forbidden/not_owner`。

---

## 5. 配额规则

- 两级约束：**租户配额** `TenantLimits[t]` 与 **全局容量** `ClusterCapacity`；任一维度超限即拒绝；
- **受理即预留**：`create`/`update` 在持锁的同一临界区内完成“检查 + 预留”，资源尚未物理创建前就占用配额，因此并发申请不可能超额；
- 占用由环境 `Reserved` 汇总：**stop 不释放配额**（资源保留），只有 **delete / 过期回收** 在进入 `terminated` 的同一次持久化事务里把 `Reserved` 清零；
- 容量/租户限额下调时，若低于当前在用值则拒绝（`capacity_in_use` / `tenant_limit_in_use`），必须先删除/释放；
- 零值限额表示“不限制”。

---

## 6. 幂等与重复提交

- 每个写请求可带 `IdempotencyKey`（按 `tenant|key` 命名空间）；
- 命令**入队时即预占该键**（`DedupRecord{Done:false}`），因此“第一次还在收敛 / 已崩溃”时用同键重试，也只会绑定到**同一个环境**；
- 命令完成后写入终态结果（成功的 envID 或失败的错误分类）；同键重放返回**相同结果且无额外副作用**；
- 拒绝类提交（如超额）也会被记录，重放得到同样的分类错误且不产生环境。

---

## 7. 错误类型（可区分原因）

所有错误均为 `*domain.Error{Kind, Distinct, Msg}`，`Kind`：

| Kind | 典型 Distinct | 含义 |
|---|---|---|
| `validation_error` | `name_required / negative_resources / zero_resources / negative_ttl / ttl_too_long / empty_update / negative_limit` | 非法输入 |
| `unauthorized` | `anonymous / tenant_required / bad_role` | 未认证/主体不合法 |
| `forbidden` | `cross_tenant / not_owner / admin_only` | 越权 |
| `not_found` | `env / already_deleted` | 不存在或已删除 |
| `conflict` | — | 通用冲突 |
| `quota_exceeded` | `tenant_quota / cluster_capacity / capacity_in_use / tenant_limit_in_use` | 租户/全局配额 |
| `immutable_field` | — | 命中不可变字段 |
| `duplicate` | `name_in_use` | 租户内重名 |
| `backend_error` | `transient / missing_resource / not_stopped / persist_failed` | 资源面/持久化（瞬时可重试） |
| `terminal_env` | `phase_terminated / phase_failed` | 对终态环境再操作 |

用 `domain.IsKind(err, kind)` 与 `domain.AsError(err).Distinct` 即可区分“非法 / 越权 / 超额（租户还是全局）/ 终态 / 瞬时”。

---

## 8. 崩溃一致性为什么成立

1. 状态文件写入是 `temp + fsync + rename + fsync(dir)`，崩溃只可能留下**旧文件或新文件**，不会出现撕裂 JSON；
2. 命令先于任何副作用落盘（write-ahead），且幂等键在入队时预占；
3. 每次后端调用都幂等，阶段在调用前后落盘，重启时 `New` 立刻重放 `Pump`；
4. 配额占用即环境 `Reserved` 的聚合，不存在第二个会漂移的计数器；释放与 `terminated` 在同一次落盘内完成；
5. 所有判定在**单把互斥锁**内进行，API 提交与收敛互斥；后端调用本身并发安全。

---

## 9. 本地验证方法

需要 Go 1.25+（本机为 Go 1.26）。**无需任何网络、集群或凭据。**

```bash
# 全部单元测试
go test ./...

# 竞态检测（覆盖并发收敛/并发配额/同键并发）
go test -race ./...

# 详细日志（关键输入 INPUT / 状态迁移 TRANSITION / 判定依据 JUDGE）
go test ./... -v

# 端到端演示（数据写在 /tmp/eo-demo）
go run ./cmd/eo --state /tmp/eo-demo
```

### 测试覆盖矩阵

| 能力 | 测试 |
|---|---|
| CRUD / 状态迁移 / 停止保留配额 / 删除释放 | `TestCRUDHappyPath` |
| 重复提交幂等（无额外环境/副作用） | `TestIdempotentDuplicateCreate`、`TestConcurrentSameIdempotencyKey` |
| 失败结果幂等重放 | `TestIdempotentFailedReplay` |
| 跨租户 / 非属主 / admin | `TestAuthzCrossTenant` |
| 各类非法输入分类 | `TestValidationRejections` |
| 不可变字段 | `TestImmutableFields` |
| 不存在 / 终态 / 重复删除 | `TestNotFoundAndTerminal` |
| 租户重名 / 跨租户复用 | `TestDuplicateName` |
| 租户配额边界（精确占用、停不释放、删释放） | `TestTenantQuotaBoundary` |
| 全局容量区分于租户配额 | `TestClusterCapacityDistinct` |
| 40 并发申请不超额（精确 10 成 30 拒） | `TestConcurrentQuotaNoOvercommit` |
| 容量下调保护 / 更新增量配额原子性 | `TestShrinkCapacityGuarded`、`TestUpdateQuotaDelta` |
| 冲突优先级（delete 胜 start；stop→start 按序） | `TestConflictPriority*` |
| 创建瞬时故障 + 重启续跑 | `TestRestartResumeCreate` |
| 删除中断 + 重启续跑（无泄漏） | `TestRestartInterruptedDelete` |
| 过期回收 + 重启 + 释放名字 | `TestExpiryReclaim` |
| TTL 续期 | `TestTTLUpdateResetsExpiry` |
| 带外资源消失自愈 | `TestSelfHealVanishedResource` |
| 后端幂等 / 瞬时失败不重复 | `internal/backend` 测试 |
| 状态原子读写往返 | `internal/store` 测试 |
