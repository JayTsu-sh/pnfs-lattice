# FDB 可行性分析 — 交付物 8：可行性分析报告（Phase 0–2 收官版，§09 结论页）

> 计划文件：`foundationdb-feasibility-analysis-plan.html`（§09 最终报告结论页模板）
> 基线 commit：`c8aa106`。本文件是 **Phase 0–2（冻结目标 + 现状取证 + 纸面映射）的收官报告**。
> **结论效力声明（计划边界，§01/§05/§08 强制）**：Phase 0–2 产出的全部证据为 **REFERENCE/HYPOTHESIS** 级，无任何 **REAL**（真实目标环境可复现实测）。按硬闸门 **G-GATE-6**（任一关键维度缺真实验证 → 结论只能是"证据不足"），本报告**不构成 Go/No-Go 可行性结论**。它是 Phase 3/4 的完整输入 + 决策方所需的全部纸面资产。

---

## 1. 核心决策问题（§01 原文）

在满足 pNFS/NFSv4 元数据正确性、目标性能、故障恢复与运维约束的前提下，是否值得将当前 RonDB / MySQL NDB 后端替换为 FoundationDB？若值得，应采用何种数据模型、接入方式和迁移策略？

## 2. 推荐决策与适用边界（当前证据下）

**当前可给出的决策：Defer（延后至 Phase 3/4 完成）** —— 不是"不可行"，而是**证据不足**（G-GATE-6）。Phase 0–2 已把 Defer 的成本降到最低：

1. 决策所需的全部纸面前置件已齐（§3 证据清单），Phase 3 可直接进入预验证实现。
2. 纸面分析已**排除/降级**两条路线：双写迁移（C，跨后端原子性不成立）、增量追平迁移（B，changefeed 覆盖 M-2 证伪）——**迁移推荐收敛为 A 停机迁移起步**。
3. 纸面分析已**锁定**三个结构性风险（不实现就无法消除，Phase 3 必须优先验证）：
   - **K-4 rename TOCTOU**（正确性红线，G-GATE-1）：建议路线 A 保底（保留 ns_locks），路线 B（去锁赌 FDB 快照）作独立优化项。
   - **热键冲突墙**（H-01 实测 17x 吞吐差）：所有原子 RMW 语义必须映射 FDB atomic_op，keyspace 已按此设计（`-05` §2.2/2.3）。
   - **每请求 2.3ms 提交延迟**（H-03）：SEQUENCE/drc_slots 热路径（wire 实测占 23.8% op）的 SLO 符合性**取决于尚空的 SLO 登记**——这是决策方必须最先补的输入。
4. 适用边界：若最终走 **Hybrid**（仅协调/恢复关键状态迁 FDB，命名空间权威留 RonDB），Phase 3 范围可大幅缩小（coordination 41 槽 vs authority 71 槽），Defer→Hybrid 的触发条件见 §5。

## 3. 最关键的三项证据（现有证据等级内最强）

| # | 证据 | 等级 | 位置 | 决策含义 |
|---|---|---|---|---|
| E-1 | 热键普通 RMW 370 ops/s 冲突墙 vs atomic ADD 6.5k ops/s 0 冲突（17x 吞吐差、p99 差 50x） | HYPOTHESIS（lab 微基准，非生产） | `-00` §7.1 | FDB 后端热路径**必须** atomic_op，否则扩展性结构性失败；keyspace 已按此设计 |
| E-2 | RonDB 裸事务 ~0.12ms vs FDB 单键写提交 p50 2.3ms（同机/同 lab 口径，17x） | HYPOTHESIS 级（lab A/B 口径，`-01` §1 vs `-00` §7.3） | `-01`/`-00` | 每请求写事务（SEQUENCE 23.8% 占比，`-06`）P50 影响量级已知；SLO 登记后立判 |
| E-3 | 现有错误路径两个真实缺陷：G1（NOSUPPORT→SERVERFAULT 漂移）、G2（重试耗尽→IO 终态而非 DELAY） | REFERENCE（代码确认） | `-02` §A.3 | FDB 后端实现前**必须**修复，否则直接触发 G-GATE-4（错误码漂移/未知提交无法对账） |

**关键缺口（阻塞评分，按权重排序）**：
- SLO/验收线未登记（§01 "必须提前量化"）——权重最高的 3 个维度（性能 15% + 正确性 20% + 一致性 18% 的通过线）无法定义。**Phase 3 后仍缺**（见 §7）。
- 生产工作负载画像缺失（`-06` §3）——所有 lab 数字只能标"预验证"。**Phase 3 后仍缺**（Phase 3 用 lab 集群产出的是 **lab-REAL**，非生产 REAL）。
- ~~FDB 后端代码为零~~ ——**已消减**：Phase 3 落地 6 槽垂直切片（`catalogue_fdb.c`，`ENABLE_FDB` 开关），node 44 真实 3 节点集群 harness 28/28（见 §7）。authority 71 槽中的 6 槽已 REAL 化，其余槽仍为切片外（G1/NOSUPPORT）。

## 4. 残余风险与责任人（`-03` §4 决策点 4 指定裁决人后激活）

| 风险 | 等级 | 责任人 | 消减路径 |
|---|---|---|---|
| K-4 rename TOCTOU（G-GATE-1） | 极高 | 待指定（架构） | Phase 3 路线 A/B 交错 + 故障注入（`-02` §B.1 I-3） |
| K-3 SEQUENCE 热路径 SLO | 高 | 待指定（性能） | 先登记 SLO；Phase 3 提交策略专项（`-05` §2.6） |
| G1/G2 错误语义缺陷 | 高 | 待指定（开发） | FDB 后端 PR 的前置 fix（`-02` §A.3） |
| 迁移不可逆点管理（反向同步） | 高 | 待指定（SRE） | 路线 A 彩排 + 回滚演练（`-07` §3） |
| 生产等价性 | 高 | 待指定（SRE） | §01 等价性验收（`-06` §2 外推说明） |
| 权重未冻结（治理） | 中 | 决策委员会 | 签署 `-03`（`-03` §6） |
| 无 FDB 值守能力（技能差距） | 中 | 待指定（SRE） | Phase 4 runbook 演练（维度 7） |

## 5. 下一阶段与退出条件

**下一阶段 = Phase 3 预验证实现**（`-03` 签署 + SLO 登记后即可启动，无需等待决策方做 Go/No-Go）：
1. 前置 fix：G1/G2（`-02` §A.3）——小改动，可独立合入。
2. 最小垂直切片（维度 10 要求）：FDB 后端实现 **核心 ns_* 6 槽**（ns_create/ns_remove/ns_lookup/ns_getattr/ns_setattr/ns_readdir，覆盖 I-1..I-5 不变量）+ atomic_op 热路径 + keyspace 按 `-05` §1。
3. 不变量校验器 `validate_invariants`（`-02` §B.2，RonDB 侧先行作参照）。
4. 9 个注入测试（`-04` §3）+ K-1..K-9 逐条实测（`-05` §4）。
5. 结果按 `-02` §B.3 manifest 归档，证据等级标注。

**Defer 退出条件（复评触发，任一满足即回决策委员会）**：
- Phase 3 全部 REAL 数据齐 + Phase 4 真实环境 A/B 完成 → 正常进 §08 评分。
- 任一硬闸门在 Phase 3 即被触达（如 K-4 路线 A 也保不住 I-3）→ 提前 No-Go，省 Phase 4 投入。
- **Hybrid 触发**：若 Phase 3 显示 authority 71 槽的 FDB 化成本/风险显著高于 coordination 41 槽（如 K-4 无法在合理预算内关闭），建议转 Hybrid 决策（仅 coordination/恢复状态迁 FDB，利用其高可用 + 无锁恢复语义），命名空间权威留 RonDB。
- 反向触发（维持 Defer 升级 No-Go 的红旗）：FDB 集群降级时错误映射无法稳定（G-GATE-5 灾备项）在生产拓扑等价性验收下无法消减。

## 6. 交付物索引（本分析 Phase 0–2 全量产出）

| 文件 | 内容 | 计划对应 |
|---|---|---|
| `foundationdb-feasibility-analysis-plan.html` | 执行框架 + 进度登记区 + 启动清单（12 项：4 勾 / 2 部分 / 1 待签 / 5 待 Phase3+） | §05–09 |
| `docs/fdb-feasibility-00-interface-inventory.md` | 114 槽 vtable 清单 + 127 包装函数调用点 + 36 表 + H-01/02/03 微基准 | D1 现状基线（接口面） |
| `docs/fdb-feasibility-01-rondb-baseline.md` | RonDB 实验室性能基线（4 组 bench，多轮方差）+ 恢复事件 | D1 现状基线（性能面） |
| `docs/fdb-feasibility-02-error-retry-invariants.md` | 错误映射规范 + 两层重试架构 + G1/G2 缺陷 + I-1..I-9 不变量 + 校验器/归档 spec | D2/D6 前置 |
| `docs/fdb-feasibility-03-scoring-decision-proposal.md` | 权重/硬闸门/证据等级/争议裁决（待签） | §08 |
| `docs/fdb-feasibility-04-idempotency-reconciliation.md` | 未知提交/部分失败/毒任务设计 + 9 注入测试 | D6 前置 |
| `docs/fdb-feasibility-05-keyspace-mapping.md` | 36 表 keyspace 映射 + K-1..K-9 + 事务预算表 | D2 目标设计（草案） |
| `docs/fdb-feasibility-06-workload-profile.md` | wire-level 操作混合 + conformance 基线 + 压测配方 | D1 工作负载画像（lab 半） |
| `docs/fdb-feasibility-07-migration-design.md` | 四路线评估 + M-2 证伪 + 回滚设计 | D6 迁移 |
| `tests/empirical/fdb_bench.c` | FDB 7.4 C API 微基准（micro/create/unlink/counter） | D3 验证资产（部分） |
| `docs/fdb-feasibility-09-phase3-engineering.md` | Phase 3 工程实现：6 槽切片 + atomic_op 热路径 + validator + 9 注入测试 + K-1..K-9 实测（lab-REAL） | D3 验证资产（Phase 3 收官） |
| `src/catalogue/catalogue_fdb.c` + `tests/empirical/fdb_{slice_harness,validate_invariants,injection_tests,hotdir_bench}.c` | FDB 后端 6 槽实现 + 可复现实测 harness/validator/注入/压测 | D3 真实验证资产（lab-REAL） |
| `docs/fdb-feasibility-10-backend-comparison.md` | RonDB vs FDB 结构级优缺点比较：12 维总表（逐行证据等级）+ 三个结构性事实 + 5 项比较盲点显式登记；不代判评分（G-GATE-6） | §08 决策输入（结构解释载体） |
| `tests/empirical/results/fdb/` | Phase 3 结果归档（`-02` §B.3 manifest 约定：manifest.json + README） | D3 归档纪律 |

**D4 性能容量报告 / D5 可靠性运维**：Phase 4 产物，当前为空（如实登记，不代填）。
D3 真实验证资产已在 Phase 3 落地（见上行 + §7），但**全部为 lab-REAL**（node 44 实验室集群），
生产 REAL 仍待 §01 等价性验收 + Phase 4 真实环境 A/B。

---

## 7. Phase 3 工程实现增量（2026-10-15，lab-REAL 证据）

> 证据等级：**lab-REAL**（node 44 真实 3 节点 FDB 7.4.8 集群，可复现命令见 `-09`）。
> 非生产 REAL——G-GATE-6 仍要求生产等价性验收（§01）+ Phase 4 真实环境 A/B，
> **本增量不改变"Defer"决策效力，只把 Phase 3 退出条件的第 1-5 步证据补齐**。
> 完整工程细节（键布局、11 个真 bug、逐 K 判定）见 `docs/fdb-feasibility-09-phase3-engineering.md`。

**落地范围（§5 的 5 步全收口）**：
1. G1/G2 前置 fix：done（NOSUPPORT→NFS4ERR 映射 + 重试耗尽→DELAY）。
2. 6 槽垂直切片：`catalogue_fdb.c`（~1100 行，`ENABLE_FDB` 默认 OFF，本地 66 项 ctest 不受影响），
   node 44 harness **28/28 两轮**（create/lookup/getattr/setattr/remove/readdir+分页/EXISTS 预检/
   200 连发/二次 open/4×25 并发 S-11/文件删除父 nlink S-7d）。
3. `validate_invariants`（I-1/2/4/5/9，只读）：**首战即抓真 bug B-4**（remove 文件误减父 NL，
   与 create 的 +0 不对称，对照 RonDB 925-926 修）；负向注入精确触发 I-1/I-5/I-9；正向 ALL PASS。
4. 9 注入测试（`-04` §3 切片子集）：J-01/J-03/J-06 实测 **9/9 两轮**；6 项 SKIPPED 注明原因。
   **J-03 揪出 1021 盲重放缺口并修**：库把 commit_unknown_result 当可重试（文档一致），
   create/remove 遇 1021 绝不重放→DELAY；claim 可重放（最多浪费 1024 号窗，不重发号）。
5. K-1..K-9 逐条实测（`fdb_hotdir_bench.c`，真实 ns_create 路径，热父目录）：
   - **K-1 通过**：热父 1→16 线程 324→3162 ops/s 近线性 9.8x，**无 H-01 的 374→369 平顶冲突墙**，
     父 CHG 精确 +6400（无丢失更新）。真实 create 吞吐锚点从"冲突 bound"上移到
     **提交延迟 bound**；约为 RonDB shared 16T（23k）的 14%，差距=17x 提交延迟（H-03），与冲突无关。
   - **K-8 通过待 SLO**：p99 3.7→8.55ms 亚线性、无 H-01 的 122-175ms 墙、无悬崖；
     p999 16T 33.6ms（良性批认领尾）。合规性待 SLO 登记后按 p99 8.5ms 判。
   - **K-5 量化关闭**：0x21 双写 ~28B/op（~8% 写字节），FDB 无索引为有序性的代价，可忽略。
   - **K-7 确认关闭**：285B inode value = 0.28% 告警阈值；trace 零值大小告警零 Warning，6400 连发稳定。
   - **K-2/K-3/K-4/K-9 DEFERRED**（切片外子系统，非失败）：队列分片/sessions 每请求提交/
     rename TOCTOU/跨 MDS 2PC 对账——各需切片外子系统落地才能实测，转入 Phase 4 / 决策项。
     **K-4 仍是唯一未触碰的正确性红线（G-GATE-1）**，路线 A/B 未拍板（`-05` §6 建议 A 保底）。

**对 §4 残余风险表的影响**：
- "热键冲突墙"（E-1/H-01）→ **lab 实测消减**：atomic_op 计数器设计在真实 create 路径上
  16 线程无冲突墙（K-1）。
- "G1/G2 错误语义缺陷" → **已修**（E-3 闭环）。
- "K-3 SEQUENCE SLO" → 仍待 SLO 登记；K-8 的 p99 8.55ms 提供了第一个 lab 锚点。
- "K-4 rename TOCTOU" → **未消减**（切片外，仍为最高风险，需路线拍板 + Phase 4 故障注入）。
- "迁移不可逆点 / 生产等价性 / 权重未冻结 / 无 FDB 值守" → 无变化（Phase 4 / 治理项）。

**下一步（决策方输入）**：
1. 登记 SLO → 把 K-8 的 p99 8.55ms / 提交延迟 2.3ms 落到合规判定（§3 E-2 解锁）。
2. 拍板 K-4 rename 路线（A 保底 vs B 激进）——当前唯一未消减的结构风险。
3. 生产工作负载画像（§01 等价性验收）→ 把 lab-REAL 升级为生产 REAL。
4. 权重冻结（`-03` 签署）→ 进 §08 评分。
以上任一触发 Hybrid / No-Go 的退出条件见 §5。

---

## 8. Phase 4 C-12 增量（2026-10-15 实现 / 2026-10-16 lab-REAL 已跑通）

> 证据等级：**lab-REAL 已实测（2026-10-16，node 44 真实 3 节点 FDB 集群）**——S-12..S-15 61/61、I-6/I-6f/I-7 ALL PASS、mixed_load_bench 1→16T RC=0（详见下"验证状态"）。
> **不改变"Defer"决策效力**——把 C-12 盲点（`-10` §4）从"槽未实现"推进到"槽已实现 + lab-REAL 实测通过"（仍缺生产等价性 + Phase 4 A/B）。

**落地范围（C-12 盲点的代码侧收口）**：
1. **FDB 后端补 3 族槽位**（`catalogue_fdb.c`，~1500 行，`ENABLE_FDB` 默认 OFF）：
   - **0x30/0x31 stripe map 族**：多 stripe 文件 LAYOUTGET 的布局数据。头+N 明细单事务写入（同 NDB 形状）；
     单 stripe 文件走 inline（MDS_IFLAG_INLINE_STRIPE，wire 携带）不建键——与 NDB 的
     "无布局行=单 stripe"约定一致。
   - **0x50-0x54 layout state 族**：pNFS layout grant/lookup/scan。主键 0x50 + 三个二级
     （0x51 by_client / 0x52 by_file / 0x53 by_ds）+ **0x54 by_stateid 反向索引**
     （NDB `ix_layout_state_stateid` 等价，`-05` 前缀表漏列已补登记），五索引单事务维护。
   - **0x70-0x73 协议态族**：sessions / session_by_client / drc_slots（NDB PK=(clientid,slot_id)，
     `-05` §2.6 误写 session_id 已更正）/ clients。
2. **read-before-write RMW（本增量揪出的真实缺口，2026-10-16 细化）**：FDB 冲突检测只对
   **同事务内读过**的键生效——`fdb_slot_put` / `fdb_layout_grant` 原为纯写事务，并发同键写会
   **静默丢更新**。首版给两者写前加 `fdb_txn_get` 同键探测；**细化发现该 get 与写分属两个事务**
   （catalogue API 的 slot_get→slot_put 是跨事务 RMW）：put 事务的读前探测注册的是**新**读版本，
   而写入的值按**早先 get** 算出——中间任何并发提交都被**静默丢失**（TOCTOU 缺口：读前探测只挡
   "同事务内"的写，挡不住跨事务）。真正的 no-lost-update 单元是**单事务 RMW**（读+写同一 FDB
   事务，冲突重试序列化同键写）。故 **S-15 改为单事务 RMW 风暴**（raw FDB，读+写同事务，终值精确
   断言）验证该性质，mixed_load_bench 则沿用跨事务 API 对（对齐内核 drc_slot 续期真实路径）把
   TOCTOU 丢失量作为**数据**上报（量化"内核续期必须单事务 RMW"，见下 §5）。
3. **功能断言**（`fdb_slice_harness.c`）：S-12 stripe map round-trip（单/多 stripe + inline 不建键）、
   S-13 layout grant/lookup/scan（五索引一致）、S-14 协议态 round-trip（session+slot+client）、
   S-15 并发 slot/layout 写风暴（**单事务 RMW**：4 线程同键、读+写同 FDB 事务，终值精确 = 线程数×轮数，零丢更新）。
4. **不变量校验**（`validate_invariants.c`）：I-6（stripe 头↔明细闭环 + layout 四索引交叉验证）、
   I-6f（0x51/0x52 by-client/by-file 索引行↔0x50 主行闭环，无孤儿索引，2026-10-16 新增）、
   I-7（session↔client 索引闭环 + 值 well-formed）从 SKIPPED 升级为真校验。
5. **混合负载压测**（`tests/empirical/mixed_load_bench.c`，新建）：按 `-06` §1.2.4 配方
   （LAYOUTGET 45% / SEQUENCE 25% / PUTFH+GETATTR+ACCESS 20% / other 10%），1→16 线程 × 15s 每档，
   seeded keyspace 共享，per-class p50/p95/p99 延迟采样 + SEQUENCE 更新核算（1T 档无并发→硬断言
   精确，不符即真 bug 退码 3；N>1T 档上报跨事务 API 对 TOCTOU 丢失量作为**数据**，不作失败判据）。

**验证状态（本地，2026-10-16 细化后复核）**：
- `cmake -DENABLE_FDB=ON` 全量构建 RC=0（本会话重配复核）；`-DENABLE_FDB=OFF` 树不受影响
  （catalogue_fdb.c 不参与 OFF 编译，改动全在 FDB-gated 源集内）。
- 四个经验二进制（catalogue_fdb.c / fdb_slice_harness.c / validate_invariants.c / mixed_load_bench.c）
  `gcc -O1 -Wall` 独立编译零警告（本会话复核 RC=0；本机 KVM CPU 无 AVX，FDB 链接二进制只能 type-check+link，
  运行留 node 44）。
- 细化另修 `catalogue_fdb.c` 两处真 bug（REAL，代码侧）：
  (a) 0x50 layout 行值缓冲原仅 36B 定长头——多 stripe 文件行尾带 ds_ids 尾（4B×ds_count，最多 256），
  读回被静默截断；扩到 `LS_FIXED_VAL + 4*256` 且 `rc != LS_FIXED_VAL` → `rc < LS_FIXED_VAL`；
  (b) `fdb_layout_scan_for_file` 的 `fdb_future_get_keyvalue_array` 首参原传 NULL——客户端经 out 指针
  写数组基址，NULL 即 segfault；改 `&kvs`。
- **lab-REAL 已跑通（2026-10-16，node 44 真实 3 节点集群）**：此前"node 44 ssh 被拒"是**测错用户**
  的误判——FDB 节点 44/45/46 用 `root@`（非 `pnfs@`，后者仅存于 MDS/DS 164/165），`root@10.131.9.44`
  用 fdbenv 实测可登，集群 3/3 triple-redundancy Healthy。据此完成 lab 实测（执行清单 = `-09` 附录 C；
  raw 输出归档 `tests/empirical/results/fdb/phase4-c12-{slice-harness,validate-invariants,mixed-load-bench}-2026-10-16.log`，commit 57ba769）：
  - `fdb_slice_harness`（S-12..S-15）：**61/61 PASS，RC=0**。S-15 **单事务 RMW** slot 风暴终值
    seq=100（want 100）、renewal 风暴终值 seqid=40（want 40）——**零丢更新**。
  - `validate_invariants`（I-6 含 I-6f 子校验 / I-7）：**ALL PASS，RC=0**（扫 3 layout 行 / 6 layout-idx /
    2 ds / 3 rev-idx / 1 session / 1 client，stripe+layout+协议态索引闭环全通过）。
  - `mixed_load_bench`（1→16T 配方 `-06` §1.2.4）：**RC=0**。1T 档 SEQUENCE 精确 sum=1725 ok=1725
    （无并发零丢）；吞吐近线性 1T=461 → 2T=911 → 4T=1705 → 8T=2940 → 16T=4634 ops/s；**N>1T 跨事务
    API 对 TOCTOU 丢失 690/39909 SEQ ops（1.73%）**——量化"内核 drc_slot 续期必须单事务 RMW"（S-15
    已 lab 实证单事务 RMW 零丢失），丢失量为数据非失败判据。

**对 G-GATE 的影响**：C-12 盲点从"槽未实现"推进到**"槽已实现 + lab-REAL 实测通过"**（G-GATE-6 的
lab 前提已满足）。仍缺生产等价性验收（§01）+ Phase 4 真实环境 A/B——故**不改变"Defer"决策效力**，
只是把 C-12 的 lab 证据补齐。

**C-12 细化（2026-10-16，lab-REAL 已通过）**：
首版把"写前加同键探测"当作 no-lost-update 的修复，设计 S-15 时发现 catalogue API 的
slot_get→slot_put 是**跨事务**的（get 一个事务、put 另一个）：put 事务的读前探测注册的是其**新**
读版本，写入值却按早先 get 算出——并发下中间提交仍被静默丢失（TOCTOU 缺口，读前探测只挡"同事务内"
的写，挡不住跨事务）。据此三处改动（均已 lab-REAL 验证）：
- **S-15 改为单事务 RMW**（raw FDB，读+写同一 FDB 事务，冲突重试序列化同键写）：lab 实测 slot/renewal
  风暴终值精确、**零丢更新**，验证"同键并发写零丢更新"这一性质本身。
- **mixed_load_bench 沿用跨事务 API 对**（对齐内核 drc_slot 续期真实调用形状）：1T 档精确
  sum=1725 ok=1725 PASS；N>1T 档上报 TOCTOU 丢失 690/39909（1.73%）作为**数据**——量化"内核续期
  路径必须单事务 RMW"这一对内核侧的硬要求（单事务 RMW 的零丢失已由 S-15 lab 实证）。
- validate_invariants 增 **I-6f**（0x51/0x52 by-client/by-file 索引行↔0x50 主行闭环，无孤儿索引）：
  作为 I-6 的子校验随 ALL PASS 通过。
**不改变 Defer 决策效力**：C-12 现为"槽已实现 + lab-REAL 实测通过"，仍缺生产等价性 + Phase 4 A/B，
G-GATE-6 不变。

---

## 9. Phase 4 C-5 增量（2026-10-16 FDB HA/恢复故障注入 lab-REAL）

> 证据等级：**lab-REAL（node 44，真实 3 节点 triple FDB 集群，PVE 真实停/启节点）**。填 C-5 的
> "FDB 侧零实测"盲点（`-10` 行 C-5 / `-09` §4）——G-GATE-6 的 D5（可靠性）输入。

**实验**：保持 601(44) 为存活 fdbcli 客户端，对 602(45/fdb-node2) 做 `qm stop` → 观察降级 →
`qm start` → 验证恢复（one-at-a-time，绝不并发停第二节点）。triple 提交 quorum=2/3，基线
Fault Tolerance=1 machine without data loss。（603 节点对称复测未做——与 602 对称，一次完整
故障+恢复周期即 C-5 代表性结果。）

**结果**（raw 归档 `results/fdb/phase4_c5_ha_20261016/c5_ha_fault_injection.log`）：
- **基线（3/3）**：triple、3 进程、FT=1 machine、Replication Healthy、3 MB / 637 MB disk。
- **停 1/3 节点（602）**：coordinator 显示 44/46 reachable、45 unreachable（"will remain
  operational as long as we can connect to a quorum"）；但 `status minimal` → **unavailable**，
  `status json` → **`recovery_state=recruiting_transaction_servers`、`available=false`、
  `seconds_since_last_recovered=-1`（>3 min 从未恢复）**。角色已重选（46=controller、
  44=master+data_distributor+ratekeeper+consistency_scan），但**事务日志组无法在 2/3 重组**。
  交叉验证：46 上 `status minimal` 同样 unavailable（集群级、非 44 客户端问题）；44/46 fdbserver
  均运行 + LISTENING。
- **恢复节点（602 start）**：+75s `status minimal` → **available**；3/3 进程、FT 恢复、
  Replication Healthy、**数据无损**（3 MB / 637 MB 不变）。
- **数据完整性**：validate_invariants 真实不变量 I-1/I-2/I-4/I-5/I-6/I-9 全 PASS（数据尺寸前后
  一致）；I-7 FAIL 系 **mixed_load_bench 种子缺口**（seed 建 16 个 0x70 session 未建对应
  0x73 client——`session_put` 无 `client_put`、且先 `catalogue_fdb_wipe`），**与节点故障无关、
  非数据丢失**，属 C-12 bench 种子待修项（follow-up）。

**含义（解释；机制=2/3 下 TX-server 招募卡住）**：极简 3 节点 FDB 集群**非 2/3 持续可用**——丢 1
节点会卡在"招募事务服务器"、不可用（观测窗口 >3 min 未恢复）。生产 FDB HA 需**更多节点/多区域**，
让事务日志组能在缺节点时重组。这与 RonDB 侧 lab 实测（ndbd 卡死、人工重启）**对称**：极简 3 节点下
两侧都非"故障期间持续可用"，生产 HA 都需更大规模。

**对 G-GATE 的影响**：C-5 FDB 侧从"零实测"推进到 **lab-REAL**（D5 可靠性有真实故障注入证据），
不改变 Defer 决策效力；剩余 = 603 对称复测 + K-9 journal 对账 + 确认更长窗口是否终会 2/3 恢复。
