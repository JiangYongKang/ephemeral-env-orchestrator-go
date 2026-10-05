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

## 4. 清单：一次提交一整套环境声明

清单（Manifest，`internal/model/manifest.go`）把一整套环境的期望声明打包提交，先预览、确认后再应用。

**声明格式**

```json
{
  "items": [
    {"name": "web",   "image": "img:1", "replicas": 3,
     "resources": {"cpuMilli": 100, "memoryMB": 128, "storageGB": 1},
     "ttlSeconds": 3600, "desired": "Running"},
    {"name": "cache", "desired": "Stopped", "image": "img:2", "replicas": 1,
     "resources": {"cpuMilli": 100, "memoryMB": 128, "storageGB": 1}},
    {"name": "old",   "action": "Delete"}
  ]
}
```

- 每个条目默认 `action: "Upsert"`、`desired: "Running"`；Upsert 是**全量声明**（image/replicas/resources/ttlSeconds/desired 整体覆盖可变字段），`owner`/`tenantId` 不可变、由系统从现有环境或调用者身份填充。
- `action: "Delete"` 表示该名字的环境必须不存在（删除是幂等的，已不存在则为 NoOp）。清单不做隐式 prune：没出现在清单里的环境不受影响。
- 清单 ID 是内容寻址的 `mf-<hash>`（租户 + 规范化条目集的 SHA-256）：同一份清单重复或并发提交都映射到同一个批次，这是幂等的基石。

**预览语义（`PreviewManifest`，只读）**

- 逐项给出分类：`Create` / `Update` / `Delete` / `NoOp`，附改动前后的关键声明（`before`/`after` spec）、字段级 `diff`（如 `replicas: 2 -> 3`）、该项的资源预留增减 `delta`。
- 汇总整份清单在**租户配额**与**集群容量**两个层面的净变化（`tenantDelta`/`clusterDelta`）以及是否放得下（`admissible` + 原因）。
- 纯函数：不创建/修改/删除任何环境、操作、批次或配额；同一（状态, 清单）对的反复预览结果字节级一致。

**应用语义（`ApplyManifest`，整批原子准入）**

- 应用时的判定逻辑与预览完全共用（`planLocked`），因此实际结果与预览一致。
- **容量按整批净变化结算**：同一批里删除/停止释放的额度可以供养同批的新建/扩容（`quota.CheckNet` 允许负 delta），与条目执行顺序无关。全局操作优先级 `Delete > Stop > Update > Start > Create` 保证执行时也是先释放后占用。
- **整批拒绝**：任一条目越权（`Forbidden`）/声明非法（`ValidationError`），或整批净变化超出租户配额/集群容量（`QuotaExceeded`），则整批拒绝并返回 `*BatchRejection`，其中逐项标注被拒原因与分类；被拒绝时**一个环境都不会被改动**——不建环境、不动 Desired、不延有效期、不留批次记录。
- 准入成功后，批次记录（`Batch`）与操作队列在**同一次原子持久化**中落盘，由收敛循环执行；`GetBatch` 可查询每项的最终结果（`Applied`/`Failed` + 原因），批次仅提交者本人与 admin 可见。

**冲突处理规则（确定且可解释）**

同一清单内针对同一名字的多个条目折叠为一个生效条目：**Delete 永远赢**（与全局操作优先级一致）；没有 Delete 时**文档顺序中最后一条 Upsert 赢**。被压制的条目标记为 NoOp 并在 `note` 中指明被谁压制（如 `superseded by item #2 (Delete wins over Upsert for the same name)`）。冲突折叠发生在配额结算之前，不会借机多占资源；权限校验对每个生效条目独立进行，不会借机越权。

**收敛、幂等与中断恢复**

- 重复应用同一份清单：已满足环境的条目在计划阶段即为 NoOp，相同意图的操作入队前去重，批次记录复用——不产生任何多余动作或后端调用；并发提交同一份清单会折叠到同一个批次。
- 应用可在任意时刻被打断（进程重启、后端抖动）：批次与操作都已持久化，重启后 `recover` 会检查每个未完成条目，操作记录缺失时按条目携带的目标声明重新推导/补发操作，继续向同一结果收敛；后端按确定性 allocation ID 幂等，不会重复建资源或泄漏。
- 对声明未变但处于 `Failed` 的环境再次应用同一清单，是文档化的重试路径（自动补发 Start，重置失败计数）。
- 部分条目最终失败（如后端持续故障）：批次整体标记完成，逐项可解释——哪项 `Applied`、哪项 `Failed` 及具体原因（来自操作的可解释拒绝信息）。

## 5. 错误类型

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

清单整批拒绝时返回 `*orchestrator.BatchRejection`（可用 `errors.As` 取出）：`Kind` 给出批次级分类（任一条目越权/非法时为首个条目错误的分类，净变化超额时为 `QuotaExceeded`），`Items` 逐项携带分类后的原因字符串（前缀即错误分类），可区分越权、声明非法与资源不够三种拒绝。

## 6. 持久化与重启恢复

- 单一状态文档 `State{envs, quotas, capacity, operations, batches, seq}` 写入 `<path>.tmp` → `fsync` → 原子 `rename` → 目录 `fsync`；崩溃时文件要么是旧完整版、要么是新完整版。
- 启动时 `Load` 后将所有持久化 allocation `Reattach` 到新的模拟后端（模拟真实云资源比控制进程长寿），再执行 `recover`：依据 desired + 存活 allocation 修复 `Pending/Starting/Stopping/Deleting/Failed` 等中间态，未完成操作继续按优先级驱动；未完成的清单批次逐项重推导（操作缺失时按条目携带的目标声明补发），继续向同一结果收敛。

## 7. 本地验证方法

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

清单覆盖（`internal/orchestrator/manifest_test.go`，日志逐项打印分类、判定依据与状态走向）：
- 预览只读且确定：反复预览字节一致，环境/操作/批次/预留一概不变，更新项带前后 spec 与字段级 diff；
- 整批净变化结算：删除/停止释放的额度供养同批新建（净零批次在满配额下准入并收敛）；
- 整批拒绝：净超额整批拒绝且零副作用（不建环境、不动 Desired、不延有效期、不留批次）；越权/非法/配额三类原因逐项分类，合法条目不渗漏；
- 幂等与并发：重复应用同清单零新操作零后端调用，8 路并发提交折叠为同一批次、不产生重复环境；
- 冲突确定性：Delete 胜过 Upsert、后者覆盖前者，预览与批次 note 均可解释；
- 中断收敛：应用中途"进程崩溃"（后端阻塞创建）后重启续跑，批次完成、无重复无泄漏；
- 部分失败可解释：单条目后端硬故障 → 批次完成，逐项 Applied/Failed + 原因，批次可见性仅提交者与 admin。

单测命令（日志可见场景、判定依据与状态迁移）：

```bash
go test ./internal/orchestrator/ -run TestManifest -v -count=1
```

## 8. 代码结构

```
internal/model          声明模型、状态机、资源向量与派生用量；清单/批次类型与内容寻址 ID
internal/errors         可分类错误类型
internal/store          原子文件持久化（JSON + tmp/fsync/rename）
internal/quota          租户配额 / 集群容量的保留量结算（含整批净变化 CheckNet）
internal/resource       可注入故障的幂等模拟资源后端 + 重启 reattach
internal/orchestrator   CRUD、授权、确定性优先操作队列、可恢复收敛循环；
                        清单预览/应用（manifest*.go）与批次收敛跟踪
cmd/demo                本地端到端演示
```
