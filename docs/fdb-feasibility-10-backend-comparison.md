# FDB 可行性分析 — 交付物 10：RonDB vs FDB 结构级优缺点比较（Phase 3 收官版）

日期: 2026-10-15
证据等级标注: **lab-REAL**（Phase 3，node 44 真实 3 节点 FDB 7.4.8 集群实测）/ **lab**（Phase 3 前 lab 微基准，两侧口径一致）/ REFERENCE（代码/文献/官方文档）/ HYPOTHESIS（外推/推断）

> **边界声明（计划 §01/§08 纪律）**：
> 1. 本文是**结构级**比较——每行回答"差异是什么 + 为什么结构上如此 + 证据等级"。
>    **不构成 §08 评分，不代判 Go/No-Go**（G-GATE-6：生产等价性验收 + Phase 4 真实环境
>    A/B 仍缺，全部 lab 证据非生产 REAL）。
> 2. 生产 op 混合 top-2（LAYOUTGET 47.5% / SEQUENCE 23.8%，`-06` §2）在 **FDB 侧无实测
>    覆盖**；涉及这两类的行一律标 HYPOTHESIS 或 lab 口径，不代填。
> 3. 比较口径：**仅 ns_create 热父目录是真实同口径 A/B**（两侧同机、同并发、同 op）；
>    其余行为"同口径锚点对照"（两侧口径一致但各自独立 run），§4 盲点清单显式登记。

## 1. 比较总表（结构级，12 维）

| # | 维度 | RonDB (NDB) | FDB | 差异根因（结构性） | 证据 | 位置 |
|---|---|---|---|---|---|---|
| C-1 | 每事务提交延迟 | 裸事务 0.11-0.13ms | 单键写提交 p50 2.3ms / p99 3.3ms（**17x**） | NDB 提交=集群内协调；FDB 提交=3 节点 2PC + 复制。结构性，应用侧不可消除，只能批量/合并缓解 | lab（两侧同机同口径） | `-01` §1 / `-00` §7.3 |
| C-2 | 热父目录扩展 | shared 16T 23.1k（spread 37.8k → 热父慢 1.6x，**16T 扩展墙**） | 普通 RMW 374→369 **硬墙**（p99 122-175ms）；atomic_op 后 16T 3162 ops/s 近线性 9.8x、p99 8.55ms、CHG 精确 | FDB 冲突=键级读写集 → 任何 RMW 必须 atomic_op；NDB 行级 RMW 冲突成本低（因提交快）。**两侧各有一道结构性墙，消除手段不同** | lab-REAL（FDB 全路径 K-1）+ lab（两侧微基准） | `-09` §9 / `-01` §3 / `-00` §7.1 |
| C-3 | rename / TOCTOU 正确性模型 | 单节点事务 + 8 类显式 ns_locks（生产在用，成熟） | 快照隔离 + 无原生锁：路线 A（保 ns_locks）/ B（去锁赌快照）**未拍板** | 最能区分两设计的维度；FDB 侧零实测，是 G-GATE-1 正确性红线 | REFERENCE（代码）+ HYPOTHESIS（路线未定） | `-00` §4 / `-05` §6 / `-09` §9（K-4 DEFERRED） |
| C-4 | 错误语义 / 未知提交 | 错误码明确（无 unknown-commit 域；4009 集群降级可诊断） | 1021 commit_unknown_result：库当可重试（与官方文档一致），**防双应用是应用层责任**；G1/G2 映射缺陷已修 | FDB 2PC 提交存在"未知"态 → 新增对账维度（journal 面三分对账待实施） | lab-REAL（J-03 实测 + G1/G2 fix） | `-09` §8 / `-02` §A |
| C-5 | 恢复 / HA | ndbd checkpoint + 集群状态机；lab 实测 ndbd 卡死、两次人工重启（可用性事件，一天两次） | lab-REAL（2026-10-16 node 44，3 节点 triple 停 1 节点）：**停 1/3 → 卡 "recruiting_transaction_servers"、available=false、>3min 未恢复**；恢复节点 → 3/3 可用、数据无损。极简 3 节点**非 2/3 持续可用**（生产需更多节点/多区域） | 对称盲已填 FDB 侧：两侧都有实测恢复事件（RonDB ndbd 卡死 / FDB TX-server 招募卡住）——共同点=极简 3 节点下**两侧都非"故障期间持续可用"**，生产 HA 都需更大规模 | lab（RonDB 事件）+ **lab-REAL（FDB 故障注入 2026-10-16）** | `-01` §4 / `-08` §8 / results/fdb/phase4_c5_ha_20261016/ |
| C-6 | 二级索引 | 引擎内原生索引 | 无原生索引 → 0x21 键空间双写 ~28B/op（~8% 写字节） | 字节等价（NDB 同数据量），但索引一致性变为应用层责任（I-9 双写集合可机检） | lab-REAL（K-5） | `-09` §9 / `-05` §2.2 |
| C-7 | 值大小 / schema | 285B wire | 285B **直接复用**（rondb_schema.h:501 "extends FDB format"）；=100KB 告警阈值的 0.28%，trace 零告警 | wire 格式设计上已面向 FDB，数据模型转换成本低于预期 | lab-REAL（K-7） | `-09` §9 / `-05` §2.1 |
| C-8 | 队列 / GC 族（K-2） | 表 + CAS claim（gc_seq FIFO、owner 分区 drain） | 键前缀分片 + TTL claim + dead-letter（0x41-0x43 **未实现**；dead-letter 前缀**仍未定**） | FDB 侧纯设计，零实测；评审缺口已登记（`-04` §4） | HYPOTHESIS（设计）+ REFERENCE | `-05` §2.4 / `-04` §4 / `-09` §9（K-2 DEFERRED） |
| C-9 | 跨 MDS 2PC / in-doubt（K-9） | 启动扫描对账（rename_2pc.c:791，生产在用） | 三分对账（commit/abort/unknown）设计定、**journal 面待实施** | FDB 无锁恢复使 in-doubt 窗口从"节点故障"移到"提交未知"；潜在新增运维项（周期扫描未定） | REFERENCE（代码+设计） | `-04` §1.1 / `-09` §8（J-02/J-04 SKIPPED） |
| C-10 | 运维能力 | mgm/ndbd/mgmctl 团队现有能力（本 lab 恢复操作即手工完成） | fdbmonitor/fdbcli + 新增对账项；**技能差距已登记** | `-08` 风险表"无 FDB 值守能力"（中） | REFERENCE | `-08` §4 / `-03` 维度 7 |
| C-11 | 迁移成本 | —（基线） | 路线 A 停机迁移推荐起步（完全可逆至切流后第一次写）；B 增量追平被 M-2 证伪、C 双写不成立；**反向同步必须实现+演练** | 一次性成本 + 不可逆点管理，属 SRE 决策项 | REFERENCE（设计） | `-07` |
| C-12 | 工作负载匹配（meta） | 基线建在真实流量画像上（`-06`） | top-2 高频 op 的 FDB 槽位**已实现**（2026-10-15 C-12 增量：0x30/0x31 stripe map + 0x50-0x54 layout state + 0x70-0x73 协议态，含**单事务 RMW 冲突保护**——2026-10-16 细化：catalogue API 跨事务 get→put 对存在 TOCTOU 缺口，S-15 改单事务 RMW 验证"同键并发写零丢更新"、mixed_load_bench 上报 TOCTOU 丢失量作数据），本地双树编译通过；**lab-REAL 已跑通（2026-10-16，node 44 真实 3 节点）**——S-12..S-15 61/61 PASS（S-15 单事务 RMW 零丢更新）、validator I-6/I-6f/I-7 ALL PASS、mixed_load_bench 1T 精确 sum=1725 ok=1725 + 16T 4634 ops/s + N>1T 跨事务 API 对 TOCTOU 丢失 690/39909（1.73%）；此前"node 44 被拒"系测错用户（FDB 节点用 `root@`）的误判 | 本行已升级 **lab-REAL**（面内真实，2026-10-16 实测）；面外推断仍 HYPOTHESIS | lab-REAL（面内真实）+ HYPOTHESIS（面外推断，待 Phase 4 A/B） | `-06` §2 vs `-09` §9 |

## 2. 双方结构优缺点小结（逐条带证据）

### RonDB 优点（有证据）
1. 提交延迟 17x 占优（C-1）——每请求写路径（SEQUENCE 23.8% op）的主导成本项在我方
2. 热父目录绝对值 ≈7x（23.1k vs 3.2k @16T，C-2）——虽有墙，绝对量级仍高
3. rename 单节点事务 + 显式锁成熟（C-3）——正确性红线有生产记录
4. 恢复/运维现有能力（C-5/C-10）——无新增技能投入

### RonDB 缺点（有证据）
1. 热父目录扩展墙（spread 37.8k vs shared 23.1k @16T，C-2）——结构性，不改设计无法消除
2. ndbd 卡死可用性事件（`-01` §4；10-13 本 lab 一天两次，需人工 systemctl 重启 + MDS 重启）
3. 无无锁恢复语义——恢复依赖集群状态机（mgmd/ndbmtd 状态 + checkpoint）
4. 36 表 + 8 类应用层锁的复杂度是存量维护成本（FDB 路线 A 下 8 类锁保留，路线 B 下消除但未验证）

### FDB 优点（有证据）
1. 热键冲突墙可**结构性消除**（atomic_op；K-1 lab-REAL：16 线程近线性、CHG 精确 +6400 零丢失更新）
2. 无锁 / 快照恢复 + HA（REFERENCE，FDB 核心卖点——**实测待 Phase 4**，目前是最强的"无 REAL 的优点行"）
3. 错误归一 + 未知提交行为实测明确（J-03），不变量可机检（validator 首战抓 B-4 真 bug）
4. 285B wire 直接复用（K-7 lab-REAL）——数据模型转换成本低于纸面预期

### FDB 缺点（有证据）
1. 提交延迟 17x（C-1）——结构性（3 节点 2PC），不可消除只能缓解；SLO 未登记前"是否可接受"无答案
2. unknown-commit 新维度（C-4）——对账/运维新增面
3. 无原生索引（C-6）——字节代价可忽略，一致性责任上移到应用层
4. 高频 op 未测（C-12）——比较覆盖面偏斜，这是当前最大的"未知"而非"缺点"

## 3. 三个结构性事实（不随路线 A/B、不随 SLO 改变）

1. **FDB 热键的任何 RMW 必须映射 atomic_op**（否则 370 ops/s 墙，H-01 lab 实测）。
   keyspace 已按此设计（`-05` §2.2/2.3），但**每个新槽实现时必须逐槽复核**——
   Phase 3 的 11 个真 bug 里有多个正出在"看似简单"的键/值/选择器编码。
2. **17x 提交延迟是固定税**（C-1）。SLO 决定"是否可接受"，任何设计决定不了"是否可消除"。
   批量/合并提交（`-05` §2.6 方向）只能改系数，不改量级。
3. **1021 unknown commit 必须应用层对账**（C-4，J-03 实测库行为=可重试）。
   这是 FDB 相对 NDB 的新运维面，没有 NDB 侧对应物，任何路线都要保留。

## 4. 比较盲点（显式登记，不隐藏）

| 盲点 | 对比较的影响 | 填法 |
|---|---|---|
| LAYOUTGET（47.5%）/ SEQUENCE（23.8%）FDB 侧未测 | 真实流量最高频两行只能用 H-03 微基准外推（C-12） | 槽实现**已完成**（2026-10-15：stripe map + layout state + 协议态 + 单事务 RMW——read-before-write 跨事务对 TOCTOU 缺口→S-15 改单事务 RMW 验证、mixed_load_bench 上报 TOCTOU 丢失量 + S-12..S-15/I-6/I-6f/I-7，本地编译通过；2026-10-16 另修 catalogue_fdb.c 两处真 bug：0x50 行 ds_ids 尾截断 + scan 的 kvs out 指针 NULL segfault）；**lab-REAL 已跑通（2026-10-16，node 44 真实 3 节点）**——S-12..S-15 61/61 PASS（S-15 单事务 RMW 零丢更新）+ I-6/I-6f/I-7 ALL PASS + mixed_load_bench 1T 精确 1725/1725、16T 4634 ops/s、N>1T 跨事务 API 对 TOCTOU 丢失 690/39909（1.73%），raw 归档 `results/fdb/phase4-c12-*-2026-10-16.log`；此前"node 44 被拒"系测错用户（FDB 节点用 `root@`）的误判 → 盲点已填，仅剩面外 HYPOTHESIS 待 Phase 4 真实环境 A/B |
| 无同流 A/B（现为同口径锚点对照） | op 混合的端到端 P50/p99 无法比较 | Phase 4 真实环境同流 A/B |
| K-4 rename 未测 | 正确性红线行（C-3）无 REAL 证据 | 路线 A/B 拍板 + 故障注入（J-02/J-04） |
| FDB HA/恢复已 lab-REAL（2026-10-16 node 44） | C-5 FDB 侧已 lab-REAL：停 1/3 节点→卡 recruiting_transaction_servers 不可用、恢复节点→3/3 无损 | 剩余：603 节点对称复测 + K-9 journal 对账 + 确认是否更长窗口终会 2/3 恢复（本次 >3min 未恢复） |
| 生产等价性未验收 | 全部 lab-REAL 不可升生产 REAL（G-GATE-6） | §01 等价性验收 |

## 5. 对决策的含义（不是判断）

- 最可能改变结论的两行：**C-3（K-4 路线）定正确性侧，C-1+C-12（SLO + 高频 op 实测）
  定性能侧**。其余行在合理范围内不翻转。
- 若 C-12 补测显示"layout/协议态槽 + 每请求 2.3ms 提交"超出预算，
  **Hybrid 触发条件**（`-08` §5：authority 71 槽成本/风险显著高于 coordination 41 槽）
  更可能被触及——命名空间权威留 RonDB、协调/恢复状态迁 FDB。
- Defer 决策不变。本文是给决策方的"差距结构性解释"载体：每行差距都能回答
  "为什么 + 能否消除 + 消除代价"，评分与判定仍待 §08（生产 REAL + 权重签署）。

## 6. 证据索引（复核命令）

```
sed -n '345,375p' docs/fdb-feasibility-00-interface-inventory.md   # H-01/H-03 表（C-1/C-2 FDB 侧）
sed -n '15,55p'  docs/fdb-feasibility-01-rondb-baseline.md         # RonDB 裸事务/16T/恢复事件（C-1/C-2/C-5）
sed -n '/## 9\./,/## 10\./p' docs/fdb-feasibility-09-phase3-engineering.md  # K-1..K-9 lab-REAL 表
cat tests/empirical/results/fdb/phase3_slice_hotdir_20261015/manifest.json  # 归档 manifest
```

代码演进（G1/G2 → 切片 → atomic → 校验器 → 注入 → K 压测）：
`1c48d21 → c668831 → 3bb2d63 → e7d3cc8 → f70f860 → 63f07b6`
