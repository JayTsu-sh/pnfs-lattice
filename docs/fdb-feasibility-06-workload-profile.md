# FDB 可行性分析 — 交付物 6：实验室工作负载画像（启动清单第 13 项收尾）

> 计划文件：`foundationdb-feasibility-analysis-plan.html`
> 启动清单第 13 项 = "提取 RonDB 现状基线（✅ `fdb-feasibility-01`）及**真实生产/实验室工作负载画像**"。本文补后半段。
> 基线 commit：`bbd7183`。数据源：`tests/empirical/results/0007/`（2026-06-08 wire-level 实测 + pynfs conformance）、`fdb-feasibility-01`（2026-10-13 实验室）、MDS 9090 metrics（2026-10-13 读取）。
> 证据等级：REFERENCE（实验室画像）。**生产工作负载画像不可得**（生产规模/流量数据不在本环境可达范围）——本文只覆盖实验室部分，生产部分保持"待补"，评分时对应维度按"预验证"处理（§01 环境定义）。

---

## 1. 操作混合（wire-level 实测，0007 全量 17 项 smoke 通过的那套负载）

来源：phase A/C 的 pcap 解码（`*decode.txt`，COMPOUND 内 `Opcode: X (N)` 精确计数）+ phase D pynfs JSON（179 用例通过/2 已知 divergence/81 omit）。

### 1.1 实测 op 分布（capture 内）
| op | 次数 | 占比 | 后端语义（keyspace 影响） |
|---|---|---|---|
| LAYOUTGET | 572 | 47.5% | 0x30/0x31 或 inode inline 读；layout_state 关联读 |
| SEQUENCE | 286 | 23.8% | **0x72 drc_slots 每请求 1 键写**（H-03 热路径） |
| PUTFH | 286 | 23.8% | 句柄→inode 点读（0x10） |
| OPEN | 24 | 2.0% | 0x60 open_state 写 + 0x73 clients |
| GETFH | 12 | 1.0% | 句柄点读 |
| ACCESS | 12 | 1.0% | 纯权限检查（inode 读） |
| GETATTR | 12 | 1.0% | 0x10 点读 |
| GRAND | 1204 | 100% | |

### 1.2 画像结论（Phase 3/4 实验设计输入）
1. **pNFS 数据面主导**：LAYOUTGET 近半——FDB 后端布局路径（0x30/0x31 + inline）是最高频读，但**全读无写**，冲突范围零，不是风险点。
2. **SEQUENCE 是真正的每请求写**：23.8% 的 op 都是 SEQUENCE，且每个 SEQUENCE = drc_slots 1 键写事务。capture 里 LAYOUTGET:SEQUENCE ≈ 2:1（每次布局操作一个 session 请求），**生产中若客户端更激进取布局，SEQUENCE 占比会更高**——H-03 的 2.3ms 提交延迟影响面比 capture 表面更大。
3. **create/rename/remove 在 conformance 负载里占比极低**（OPEN 2%，无 RENAME/CREATE 主体）——但 0007 是**协议正确性**负载，不是**吞吐**负载。吞吐画像以 `fdb-feasibility-01` 的 bench 族为准（create 8k / remove 2.3k / 16T spread 37.8k ops/s）。
4. **混合负载比例建议（Phase 3 压测配方）**：
   - 读路径：LAYOUTGET 45% / PUTFH+GETATTR+ACCESS 20% / SEQUENCE 25% / 其它 10%
   - 写路径（create storm 变体）：CREATE 60% / OPEN 20% / LAYOUTGET 15% / SETATTR 5%
   - 删除变体：REMOVE 50% / GC drain 后台 / LAYOUTGET 30% / 其它 20%
5. **conformance 基线**：phase D 179 通过、2 divergence（stStaleRename/COMP5，pmds 行为差异，`phase_d_pynfs_classify.txt`）——**FDB 后端的 Phase 4 验收必须复现同一套 179 通过 + divergence ≤ 2 且无 unexpected**（0007 是"RonDB 行为基线"，不是"RFC 完美基线"）。

## 2. 环境容量画像（2026-10-13 读取）

| 项 | 值 | 来源 |
|---|---|---|
| MDS 9090 计数器（重启后 40min） | getattr 19 / open 3 / readdir 1 / layoutget 1 / layoutcommit 1；commits_ok=0（metrics 口径不含直连路径）；transient_retries=0 | curl 9090 |
| DS 文件 | 256,033 个，全部 0 字节（bench 残留），/export/ds 23M | find -printf |
| prealloc 池 | 499,237 槽（recover_pool 输出） | bench 日志 |
| RonDB data dir | 2.3G（LCP 数据），盘 19G 用 49% | du/df |
| ndbmtd 内存 | ~7.2G RSS（DataMemory 4G + TransactionMemory 2G 配置内） | /proc/status |
| catalogue 表行数 | **未取到**（无独立 mysqld、admin 9800 未启用；skill rondb-ops 已记录） | — |
| FDB 集群 | triple / ssd-2 / 3 进程 3 机器 / coordinators 3 / Healthy / Fault Tolerance=0 machines (1 without data loss) | fdbcli status details |
| FDB 环境 | 每节点 4C/8G/40G，lab 基准拓扑非生产 | 计划登记区 |

**外推说明**：lab catalogue 规模是 bench 累计残留量级（25 万 0 字节文件 + 50 万 prealloc 槽），**不代表生产规模**；生产规模未知（无生产数据可达），Phase 4 大容量实验前必须按 §01 "数据与流量真实" 补生产画像或等价性验收。

## 3. 对后续实验的直接输入

- Phase 3 预验证：按 §1.2 三配方压测，FDB vs RonDB 同配方 A/B。
- Phase 4 SLO 登记（计划 §01 "必须提前量化的验收线"）现在有参照系：SEQUENCE 热路径 P50 预算必须对 H-03 的 2.3ms 明确"接受/优化"；create 吞吐对标 8k 单线程 / 37.8k 16T-spread；remove 对标 2.3k。
- conformance 回归集固定为 0007 那 17 项 + pynfs 179 用例（2 divergence 白名单）。
