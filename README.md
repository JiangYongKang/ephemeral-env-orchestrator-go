# Ephemeral Environment Orchestrator (local, declarative)

一套**纯本地**的临时开发环境声明式编排器。用一个原子写入的 JSON 文件模拟控制面持久化状态，用可注入故障的内存后端模拟基础设施；**不依赖任何真实集群、外部服务或凭据**，进程重启后能恢复到一致结果。

## 1. 声明模型

一个环境（`internal/model`）由声明（Spec）、期望状态（Desired）和观测状态（Phase）组成：

| 字段 | 含义 | 可变性 |
| --- | --- | --- |
| `id` | 全局唯一、确定性序号（`env-N`） | 不可变 |
| `spec.owner` / `spec.tenantId` | 所有者与租户（身份隔离边界） | **不可变** |
| `spec.name` | 租户内唯一名称，用于创建幂等 | 不可变（同名重放返回冲突） |
| `spec.image` / `spec.replicas` / `spec.resources` | 镜像 / 副本数 / 单副本资源向量 `{cpuMilli, memoryMB, storageGB}` | 可变（Update） |
| `spec.ttlSeconds` | 有效期；`0` 表示不过期 | 可变；过期后更新 TTL 即"续期" |
| `desired` | `Running` / `Stopped` / `Deleted` | 由 Start/Stop/Delete 声明 |
| `generation` | 每次 image/resources/replicas 变化自增 | 系统维护 |
| `allocations[]` | 已物化资源，ID 为确定性的 `env-g<gen>-<i>` | 系统维护 |

校验规则（创建/更新时，原因可区分）：名称/镜像非空、`replicas ∈ [1,100]`、资源各维度为正、`ttlSeconds ≥ 0`；修改 `owner` 直接返回 `ImmutableField`。

## 2. 状态流转

```
            create                         全部物化成功
 Pending ───────────────► Starting ───────────────────────► Running
    │                        │  ▲                              │  ▲
    │ 后端持续失败(≥5次)      │  │ quota恢复 / start重试         │  │ start/续期
    ▼                        ▼  │                              ▼  │
 Failed ◄──────────────────────┘                  Stopping ─► Stopped
    │ start（显式重试，重置计数）                       │ delete      ▲
    └───────────────────────────────────► ...         ▼             │ TTL到期(先Stopping)
                                                  Deleting ─► (tombstone, 查询404)
 Running/Stopped ──TTL到期──► Stopping ──释放完成──► Expired（续期: 更新ttl -> Running）
```

- **可中断续跑**：每个收敛 tick 只推进一小步（建/删一个 allocation），每步成功后立即持久化。创建、启动、停止、删除、过期回收中途被杀进程，重启后从持久化状态继续，不会重复建资源（后端按确定性 allocation ID 幂等），也不会泄漏（删除未知 ID 视为成功）。
- **异常恢复**：瞬时后端故障按 tick 重试；超过 `MaxBackendAttempts=5` 进入 `Failed` 并记录可解释的 `failReason`，保留配额但不占用实际资源；显式 `Start` 或更新声明可重试。控制进程重启会重置瞬时失败计数并重新收敛。
- **过期回收**：收敛循环每 tick 比较注入时钟与 `expiresAt`；过期即释放全部 allocation 与配额，终态 `Expired`；对过期环境直接 Start 返回 `Conflict`，必须先续期。

## 3. 权限与配额规则

**权限（Actor = name + tenantId + role）**
- `owner`：只能访问**本租户且 owner 为自己**的环境；跨租户或非本人 → `Forbidden`；身份字段缺失 → `Unauthorized`。
- `admin`：可访问所有租户的环境；仅 admin 可设置租户配额与集群容量（owner 调用 → `Forbidden`）。
- 未知 ID 一律 `NotFound`（不因存在性泄露信息）。

**配额（保留量模型，`internal/quota`）**
- 环境在 `Desired=Running` 时按**声明值**预留 `resources × replicas`；`Stopped/Expired/Deleted` 预留为 0（停止/删除即原子释放）。
- 租户配额与集群容量两个层级同时校验，任一维度超出即拒绝，错误信息给出 `used/requested/limit` 与具体维度。
- 预留量与实际用量都从持久化状态中的 allocation **派生**（而非自增计数器），崩溃不会造成账目漂移。
- 所有"检查 + 预留"都在编排器单一互斥锁内完成，因此并发申请串行化，**绝不可能超额**；缩容恒通过，扩容按"扣除自身旧预留后的余量"精确判断。
- 管理员可在运行中调小配额/容量：不抢占存量，但继续扩容/重建会被阻塞甚至在持续不满足时落入 `Failed`。

**冲突操作的确定性优先级**（同一环境排队时，数字小者先执行；同优先级按 FIFO `seq`）：
`Delete(0) > Stop(1) > Update(2) > Start(3) > Create(4)`。
因此并发 start/stop/delete 时删除必胜，资源一定被释放，竞态无法绕过权限或多占资源。重复提交相同操作在入队前去重，不产生额外操作或副作用。

## 4. 错误类型

定义于 `internal/errors`，用 `errors.Is(err, errors.KindXxx)` 分类：

| Kind | 触发场景 |
| --- | --- |
| `ValidationError` | 字段非法、副本/资源/TTL 越界 |
| `ImmutableField` | 修改 owner 等不可变字段 |
| `Unauthorized` | 无身份或未知角色 |
| `Forbidden` | 跨租户、非所有者、owner 调用管理接口 |
| `NotFound` | 环境不存在或已删除（墓碑不可见） |
| `QuotaExceeded` | 租户配额或集群容量不足（含具体维度与数值） |
| `Conflict` | 重复创建、对过期环境直接 Start 等 |
| `TransientFailure` | 资源后端瞬时抖动（内部重试，不直接返回用户） |
| `Internal` | 持久化等内部故障 |

终态 `Failed` 的原因写入环境记录的 `failReason`，可通过查询解释。

## 5. 持久化与重启恢复

- 单一状态文档 `State{envs, quotas, capacity, operations, seq}` 写入 `<path>.tmp` → `fsync` → 原子 `rename` → 目录 `fsync`；崩溃时文件要么是旧完整版、要么是新完整版。
- 启动时 `Load` 后将所有持久化 allocation `Reattach` 到新的模拟后端（模拟真实云资源比控制进程长寿），再执行 `recover`：依据 desired + 存活 allocation 修复 `Pending/Starting/Stopping/Deleting/Failed` 等中间态，未完成操作继续按优先级驱动。

## 6. 本地验证方法

需要 Go 1.25+，无外部依赖：

```bash
go build ./...                 # 编译
go vet ./...                   # 静态检查
go test ./... -race -count=1   # 全部测试（含竞态检测）
go test ./... -race -count=5   # 重复跑，验证收敛/恢复稳定性
go run ./cmd/demo              # 端到端演示；再运行一次即"进程重启恢复"
                                 # 状态文件在 $TMPDIR/ephemeral-orchestrator-demo/state.json
```

测试覆盖（`internal/orchestrator/*_test.go`，日志打印关键输入、状态迁移与判定依据）：
- CRUD 主流程、校验分类、创建幂等、不可变字段、权限矩阵；
- 配额边界（恰好打满/超出）、40 并发申请的原子准入、删除释放后复用、集群容量约束、扩缩容、停止后再启动的重新准入；
- 后端前 N 次失败重试、持续失败进入 `Failed` 及显式恢复、删除中断后续跑；
- 冲突操作优先级（Delete 胜过 in-flight Create，无资源泄漏）；
- 运行中重启恢复、创建中途重启、删除中途重启（reattach + 续跑，无重复/泄漏）；
- TTL 过期回收、过期直接 Start 冲突、续期重启、回收配额复用。

## 7. 代码结构

```
internal/model          声明模型、状态机、资源向量与派生用量
internal/errors         可分类错误类型
internal/store          原子文件持久化（JSON + tmp/fsync/rename）
internal/quota          租户配额 / 集群容量的保留量结算
internal/resource       可注入故障的幂等模拟资源后端 + 重启 reattach
internal/orchestrator   CRUD、授权、确定性优先操作队列、可恢复收敛循环
cmd/demo                本地端到端演示
```
