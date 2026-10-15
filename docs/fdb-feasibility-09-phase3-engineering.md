# FDB 可行性分析 Phase 3 — 工程实现与垂直切片实测

日期: 2026-10-15（Phase 3 收官，含收尾登记）
证据等级标注: **REAL**（本实验室实测）/ REFERENCE（文献/官方文档）/ HYPOTHESIS（推断）

## 1. 范围

Phase 3 五步中的第 1 步：FDB 6 槽垂直切片（ns_create / ns_remove /
ns_lookup / ns_getattr / ns_setattr / ns_readdir），目标是在真实 3 节点
FDB 集群（7.4.8, node 44-46）上跑通并验证 -05 键空间设计的不变量。

前置：G1（NOSUPPORT→NOTSUPP 映射）与 G2（5 站 -2→DELAY）已修复并提交
（commit 1c48d21）。

## 2. 实现

- `src/catalogue/catalogue_fdb.c`（新，~940 行）：6 槽实现
  - 键空间严格按 -05：`0x00` 计数器 / `0x10|fileid(8)` inode（285B wire
    原样复用 rondb_inode_serialize）/ `0x20|parent(8)|name` dirent
  - **H-01 落实**：父目录 nlink/change 用 `0x00|NL|parent(8)` /
    `0x00|CHG|parent(8)` 独立全宽 8B LE 计数器键 + FDB atomic ADD
    （全宽 operand，无偏移技巧）；新建目录在 create 事务内播种自身
    计数器（nlink=2, change=1）
  - 冲突重试：`fdb_transaction_on_error` 退避 + reset + 重放，50 次上限
    → MDS_ERR_DELAY（G2 规范），绝不映射为终态 MDS_ERR_IO
  - FID 计数器暂为普通 RMW（冷键；atomic_op 专项任务再改）
  - 未实现槽（rename/link/mkdir/rmdir/...）= NULL → NOSUPPORT → NOTSUPP
    （G1 路径，由 test_mds_status_to_nfs4_mapping 守护）
  - FDB 网络是进程级单例：setup 幂等、close 不停网络（重开/二次 open
    场景实测通过）
- `include/pnfs_mds.h`：`MDS_BACKEND_FDB = 1`
- `src/common/config.c`：`catalogue_backend=fdb` 解析（HAVE_FDB guard）
- `include/mds_catalogue.h`：`catalogue_fdb_open` / `catalogue_fdb_wipe`
  声明
- `src/catalogue/catalogue_factory.c`：HAVE_FDB 分支
- `CMakeLists.txt` / `src/mds/CMakeLists.txt`：`ENABLE_FDB` option +
  FDB_ROOT include/link + `HAVE_FDB=1`
- 本地回归：ENABLE_FDB=OFF 全量 ctest 64/64 两轮通过；
  ENABLE_FDB=ON catalogue_fdb.c 对象零警告零错误（本地 KVM 无 AVX2，
  libfdb_c 的 AVX 指令会 SIGILL，故运行只能在 node 44）

## 3. 实测环境

- FDB 7.4.8（client == server 版本一致），3 节点 10.131.9.44-46，
  triple 冗余 ssd-2，fdbcli status 健康
- 运行机 = node 44（QEMU 透传 AVX2）
- 测试程序：`tests/empirical/fdb_slice_harness.c`（10 组 S 断言，wipe
  模式可重复跑）+ `tests/empirical/fdb_raw_range_test.c`（零共享代码
  最小 range 探测）

## 4. 结果（REAL）

### 4.1 切片功能：23/23 通过（2026-10-14 两轮复跑一致）

| 断言 | 结果 | 说明 |
|------|------|------|
| S-1 root inode (nlink=2 计数器) | PASS | |
| S-2 create dir + lookup/getattr 往返 | PASS | fid 从计数器分配 |
| S-3 create file（父 nlink 不变, change+1, 原子 ADD） | PASS | |
| S-4 重复 create → EXISTS | PASS | 原子预检 |
| S-5a/S-5b readdir + 分页 | PASS | 修 KEYSEL 后全绿，见 §4.3 |
| S-6 setattr 持久化（mode/size/uid/gid, change+1） | PASS | |
| S-7 remove file（dirent 消失 + inode 键真删 + NOTFOUND） | PASS | |
| S-8 remove dir → NOSUPPORT | PASS | G1 边界路径 |
| S-9 200 顺序 create 全提交（无跨键冲突） | PASS | 冲突域收敛到 dirent 键 |
| S-10 二次 open 状态完整 | PASS | 网络单例修复后 |

### 4.2 切片开发中修掉的 6 个真 bug（全部 REAL，均在本仓库代码侧）

1. **inode 键长 10→9 字节**：0x10|fileid 是 9B 键，第 10 字节为栈垃圾
   → 写/读键不一致，产生非确定性 NOTFOUND/IO（最严重根因）
2. **fdb_txn_get 返回语义**：改为返回真实 value 长度；ns_lookup 内
   遗留 `rc < 9` 判断因此在"命中"路径从恒真 bug 变为正确校验
3. **readdir 上界键**：`prefix||0x00` 小于该 parent 所有真实条目
   （name 首字节非零）→ range 恒空；改为 `0x20|(parent+1)` 排他上界
   + UINT64_MAX 回绕守护
4. **ns_remove inode clear 10→9**（同 bug 1 的 clear 侧）
5. **FDB 网络生命周期**：close 调 fdb_stop_network 导致二次 open 必败
   （FDB 网络是进程级单例）；改为 setup 幂等、close 不停网络
6. **FID 计数器种子**：ensure_root 未播种计数器 → 前两次分配 fid=1/2
   撞 root 键；播种为 2（0/1 保留，2=root）

### 4.3 B-1 已解决（2026-10-14 反转）：根因是 C API KEYSEL 编码误用，集群无异常

**初判（已被推翻）**：零共享代码 + 系统头编译的最小 range 测试复现
"range 上下界整体左移一个键位"，一度登记为集群级异常。

**反转证据**：同机同库同集群，fdbcli（独立二进制）`getrange` 对同样的
键返回**完全正确**的结果（[A,B)={A01,A02} 等）→ 集群健康、库健康，
问题在我的 C 调用。

**根因（REAL，代码侧）**：`fdb_transaction_get_range` 的 KeySelector
参数 (or_equal, offset) 语义按头文件 KEYSEL 宏：

```c
FDB_KEYSEL_LAST_LESS_OR_EQUAL(k, l)     = k, l, 1, 0
FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(k, l) = k, l, 0, 1
```

readdir 的 begin 我写成了 (1,0) = LAST_LESS_OR_EQUAL —— 即"begin 的
**前一个**键"（begin 本身不在 DB 时取全局前驱）。这精确解释了全部
观测：[A,B) 返回 {X, A}（X = A 前一个全局键）、S-5a 返回 slice_subdir
（0x20|3 前缀的全局前驱）、reverse 同样偏移、边界键不存在时
or_equal 0/1 无差别。第二层同样错误：end 用 (0,0) 排他会把该 parent
**最后一个** dirent 静默丢弃（S-5a count=1 少一个）。第三层：cookie
skip 比较 NUL 计长不一致（key 的 name 段含 NUL，strlen 不含）。

**修复**（catalogue_fdb.c ns_readdir，已在集群上验证）：
begin = (0,1) FIRST_GREATER_OR_EQUAL，end = (0,1) FIRST_GREATER_OR_EQUAL
(next parent prefix)，cookie skip 用 nlen-1。修复后 **23/23 两轮全绿**。

**教训**：FDB C 的 KeySelector 四个组合语义反直觉（or_equal 与 offset
共同决定），调用时直接对照头文件 KEYSEL 宏编码，不要凭参数名猜；
range 类 API 必须配一个"键值已知的小范围黄金用例"做黄金断言
（本次 fdbcli getrange 对照 + raw_range_test 黄金用例即此用途）。

## 5. atomic_op 热路径（Phase 3 第 2 步，2026-10-14 完成并实测）

按 -05 §2.2/§2.3 落地了两件事，均在 node 44 真实集群验证：

1. **FID 全局计数器 ADD 批认领**（H-01 生产化）：从 create 事务内的
   普通 RMW 改为**事务外** atomic ADD 批认领 + 线程本地 1024 批池
   （对齐 catalogue_rondb.c:431-471 的 per-thread pool 模型）。
   FID 键彻底退出 create 事务写集——create 的冲突范围收敛到
   dirent/inode/父计数器键本身。批认领的"ADD 后同事务回读"会因
   读集重叠互相冲突，属 BENIGN（事务整体回滚、无双重认领），
   走标准 on_error 退避重试（50 上限→DELAY，G2 路径）。
2. **0x21 dirents_child 二级索引**（-05 §2.2 双向写）：
   create 双写 `0x21|parent(8)|child_fileid(8) -> name(NUL)`，
   remove 双清；wipe 扩到 0x21 段。承载 FDB 无原生索引的
   child-ordered 有序性（READDIR-by-fid / name-reuse 探测）。

### 5.1 本步揪出的两个真 bug（REAL，代码侧）

**B-2：`fdb_txn_get` use-after-free（最重）**
原实现在 `fdb_future_destroy(f)` **之后**才 `memcpy(buf, val, vlen)`——
`fdb_future_get_value()` 返回的指针只在 future 销毁前有效，销毁后读
的是客户端内部已释放/池化缓冲。纯读事务里窗口极短所以此前 23/23
侥幸全绿；一旦事务内携带 mutation（批认领的 atomic ADD），缓冲复用
确定性发生，回读稳定得到全零（诊断：`rc=8 buf=00 00 00 00 00 00 00 00`）。
症状链：newval=0 → fid 批基址 2^64-1023（负 fid）→ 并发下 4 线程
拿到同一负窗口。修复：先 memcpy 再 destroy。**教训：FDB C 的
future 值指针生命周期 = future 生命周期，所有 get_* 取值必须
在 destroy 前拷出**（与 KEYSEL 教训同级，写进 skill）。

**B-3：claim 批基址 clamp 的无符号比较 no-op**
`base = newval - count + 1; if (base < 3) ...` —— newval 小于
count 时 base 无符号回绕成巨大值，`base < 3` 恒假，clamp 失效
（fid=-1023 直接流出）。修复：改比 newval 本身
（`if (newval >= count + 2)`），另加空批守护。

**取证方法**（值得复用）：① 最小探针 probe_claim.c（set→点读→
tx 内 ADD+回读→提交→点读）证明集群/库/.so 全对；② 同机用
fdbcli 直看 0x00 段真实值（CHG=101=FID 各批 ADD 均正确落库）
证明后端写路径正确；③ 加 `FDB_CLAIM_DEBUG` 打印 rc+字节，
一步钉死"值在 destroy 后才被读"。三层隔离（探针 vs fdbcli vs
带诊断源码）排除了全部环境嫌疑。

### 5.2 并发验证 S-11（harness 新增，REAL 测量）

4 线程 × 25 同父并发 create：fid 四批窗口互不重叠
（1027-2050 / 2051-3074 / 3075-4098 / 4099-5122）、100/100 唯一
零重复、父 CHG 精确 1→101（100 次 committed ADD 无丢失）、
readdir=100。**harness 27/27 两轮全绿**（node 44，系统头编译）。
这是 K-1 热点目录压测的功能侧前置：atomic 父计数器 + ADD 批
FID 在并发下无丢失更新、无冲突丢失。

### 5.3 回归

本地 ENABLE_FDB=OFF ctest 全过（test_compound 在全量并发跑中偶发
glibc malloc abort，单跑稳定通过，与本步改动无关——本步仅触及
ENABLE_FDB=ON 才编译的 catalogue_fdb.c + 不在 ctest 里的 harness）。

## 6. 与 K 指标的对应

切片 23/23 + 热路径 27/27 全绿后无环境阻塞：K-1（create 吞吐）
可直接压测（S-11 已证功能侧无丢失更新）；K-2（lookup）等单键类
指标可直接采样；readdir 类指标（依赖 range 扫描）在 B-1 根因
（KEYSEL 误用）修复后同样可测；K-4（rename，未实现）仍待后续槽位。

## 7. validate_invariants 首战（Phase 3 第 3 步，2026-10-14）

只读校验器 `tests/empirical/validate_invariants.c`（-02 §B spec）落地
并上真实集群：I-1/I-2（dirent→inode 闭环）、I-4（NL == 2+目录子项，
逐父核对）、I-5（fileid 唯一 + ≤ FID 计数器单调）、I-9（无孤儿 +
0x21/0x20 双写集合排序后逐对相等）；I-3/I-6/I-7/I-8 按切片边界
SKIPPED（rename 未实现 / stripe / 协议态 / 队列不在切片）。

**首战即抓到后端真 bug（I-4 FAIL，REAL）**：`0x00NL|parent3 = 1`
（期望 2）。根因 = ns_remove 最终 unlink 对**所有**子类型无条件父
NL -1，而 create 对文件子项是 +0 —— 删一个文件就把父目录 nlink
减 1（POSIX 目录 nlink = 2 + 子目录数，文件删除不应动父 nlink）。
对照 RonDB 权威实现 catalogue_rondb.c:925-926
（`parent_nlink_delta = (type==DIR) ? -1 : 0`）确认语义后修复；
harness 加 S-7d 回归断言（文件删除后父 nlink 不变）。此 bug 在
27/27 全绿时 S-7 未查父 nlink 而漏网——校验器首战即兑现价值。
（同期修了校验器自身一个坑：C 字符串字面量 `"\x00FID"` 被预处理
器贪婪解析成 0x0F'ID'，改用显式字节数组。）

**负向测试（validator 不是空转）**：注入一个孤儿 inode 键
（fid=0xDEADBEEF12345678，value 1B 畸形）→ 校验器同时报
I-1（value len 1≠285）/ I-5（超计数器 5122）/ I-9（孤儿）三条
FAIL，样本精确点名 fid，rc=1；clear 注入键后回到 ALL PASS。
正向：harness 终态上 I-1/I-4/I-5/I-9 全 PASS（104 inodes /
103 dirents / 103 index rows，counter=5122）。

**当前集群终态**：harness 28/28 两轮全绿（含 S-7d），
validate_invariants ALL PASS。

## 8. 注入测试（Phase 3 第 4 步，2026-10-15）

`tests/empirical/fdb_injection_tests.c` 按 -04 §3 的 9 故障点清单落地
切片可测子集（3 项实测 + 6 项 SKIPPED 并注明原因）。结果
**9/9 PASS，两轮稳定**（node 44 真实集群）。

**J-01 未知提交后重放（3.1 断网/kill 的切片可测核心 = 重试幂等性）**：
recovery 重发已提交的 create → EXISTS、fileid 不变、父 CHG/NL 零
副作用（重复 create 走只读存在预检，不 stage 写事务）、dirent 仍
映射原 fileid。4/4 PASS——"fileid 不重复分配、nlink 不双减"在
后端契约层成立。

**J-03 commit_unknown_result(1021)**：实测 FDB 7.4.8 库行为——
`on_error(1021)` **成功返回并施加 backoff**，即官方文档明确写的
"库把 1021 当可重试，防双应用是应用层的责任"。断言与文档一致
PASS。同时揪出切片真缺口：create/remove/claim 三个提交循环对
**任何** commit 错误都 backoff+重放 ≤50 次 → 1021 会被盲重放。
修复（本次入库）：新增 `fdb_commit_maybe_applied()`，create/remove
遇 1021 **绝不重放**，立即返回 DELAY（调用方重发时存在预检保证
安全）；claim 路径保留重试——1021 时重放 claim 最多浪费一个
1024 号窗（计数器单调，永不重发号），不会双重分配。-04 的三分对账
（commit/abort/unknown）属 MDS journal 面，留 journal 面实施。

**J-06 读版本过旧中止（3.2）**：读集固定于旧版本的事务在并发写
后提交 → 1020 abort，库保留并发写者值，无半成品写入。2/2 PASS。
（FDB 7.4 无 set_read_snapshot；事务内旧读即固定读集版本，等价
于陈旧快照机制。）

**SKIPPED（子系统不在切片）**：J-02 kill-MDS（需 MDS 进程 +
journal 扫描）、J-04 2PC rename 中途崩溃（rename 未实现，
G1/NOSUPPORT，K-4 路线未拍板）、J-05 DS 通知副作用（DS 面）、
J-07/J-08/J-09 毒 GC 行（0x41/0x42/0x43 keyspace + claim/TTL
未实现）。6 项均输出 SKIPPED 行并注明原因，不假装通过。

1021 修复回归：harness 28/28 两轮全绿、注入 9/9 两轮、
validator ALL PASS。另修 validator 回调一处 UB（声明 int 却
`return;` 无值——FDB 忽略回调返回值所以未爆，显式 `return 0`）。

## 9. K-1..K-9 实测（Phase 3 第 5 步，2026-10-15）

`tests/empirical/fdb_hotdir_bench.c`：N 线程在**同一热父目录**并发
真实 ns_create（0x20 dirent + 0x21 索引 + 0x10 inode + 2 个 atomic
ADD 计数器 + 存在预检读，非 H-01 独立微基准），记录逐 op 延迟，
事后校验父 CHG 精确增量 + FID 单调。node 44 集群，400 ops/线程：

| 线程 | 聚合 ops/s | p50 | p99 | p999 | 父 CHG delta | 错误 |
|---|---|---|---|---|---|---|
| 1 | 324 | 3.05ms | 3.74ms | 4.05ms | 400 精确 | 0 |
| 2 | 625 | 3.14ms | 4.14ms | 6.00ms | 800 精确 | 0 |
| 4 | 1202 | 3.27ms | 4.02ms | 9.48ms | 1600 精确 | 0 |
| 8 | 2109 | 3.68ms | 4.71ms | 17.71ms | 3200 精确 | 0 |
| 16 | 3162 | 4.88ms | 8.55ms | 33.64ms | 6400 精确 | 0 |

**K-1（热父目录 atomic_op，高/结构性）：通过。** 聚合 1→16 线程
324→3162 ops/s（9.8x，近线性），对照 H-01 普通 RMW 热键 4→8 线程
374→369 **平顶墙**（-00 §7.1）：冲突墙被 atomic_op 计数器设计
消除。单线程 3.05ms/op ≈ H-03 提交延迟 2.3ms + 5-6 键读写集——
真实 create 的吞吐锚点已从"冲突 bound（370 墙）"上移到
**"提交延迟 bound"（K-3/H-03 域）**；比 H-01 原子 ADD 微基准
6474@16T 慢 ~2x 完全由额外键解释，无冲突贡献。CHG 在 16 线程
风暴下精确 +6400：atomic ADD 无丢失更新（S-11 的 4 线程版铁证的
16 线程外推）。

**K-8（并发尾延迟，高）：通过（SLO 对照待登记值）。** p99 3.7→
8.55ms（2.3x，亚线性增长），无 H-01 RMW 的 122-175ms 墙，无
悬崖；p999 16T 33.6ms（FID 批认领冲突重试的良性尾，B-2 修复后
无静默错误）。RonDB 对照锚点（-01 §3）：shared 16T 23k ops/s、
spread 37.8k——FDB 热父 3.2k ops/s 约为 RonDB shared 的 14%，
差距全部来自提交延迟（2.3ms vs 0.12ms ≈ 17x，H-03/E-2），
与冲突无关。**SLO 登记后按 p99 8.5ms 判合规性**（-03 锚点，
当前 SLO 为空，不代判）。

**K-5（二级索引写放大，中）：量化关闭。** 每次 create 事务写
= 0x20 键(9+name+1)值9B + 0x21 键17值(name+1) + 0x10 键9值285B
+ NL/CHG 两个 8B ADD ≈ 330-360B（8B 名）+ 1 次预检读。0x21 双写
贡献 ~28B/op（≈8% 写字节）——FDB 无原生索引为 child-ordered
有序性付出的代价，量级可忽略；NDB 侧同数据量（索引在引擎内，
FDB 在应用键空间，字节等价）。

**K-7（285B inode value，低）：确认关闭。** 285B = FDB 100KB
大值告警阈值的 0.28%；集群侧证据：K-bench 终态 5MB KV / ~7 万行
（inode 行 294B 为主），trace 日志 **零值大小告警、零 Warning**，
6400 连发稳定无退化。

**K-2 / K-3 / K-4 / K-9：切片外，如实登记 DEFERRED**（非失败）：
K-2 队列分片（0x41/0x42 未实现）、K-3 sessions/drc_slots 每请求
提交（协议态未实现，SLO 尚空）、K-4 rename TOCTOU（rename 未
实现，G-GATE-1 正确性红线，路线 A/B 未拍板——-05 §6 建议 A
保底）、K-9 跨 MDS 2PC 对账（journal 面）。全部需要切片外子系统
落地后才能实测，转入 Phase 4 / 决策项，不代测。

K-1..K-9 收口状态：5 实测关闭（K-1/K-5/K-7 通过+K-8 通过待
SLO 对照，K-6 大目录分页由 S-5 分页断言部分覆盖）+ 4 项
DEFERRED 注明依赖。

## 10. 待办

- [x] B-1 已解决（§4.3）：根因是 KeySelector 编码误用（begin=(1,0)
      应为 (0,1)），非集群异常；fdbcli 对照 + 黄金用例证实，23/23 全绿
- [x] S-5 复绿：23/23 两轮一致（node 44 真实集群）
- [x] atomic_op 热路径（§5）：FID ADD 批认领 + 0x21 二级索引，
      揪出 B-2（use-after-free）/ B-3（clamp 无符号 no-op）两个
      真 bug，27/27 两轮全绿（含 S-11 并发）
- [x] validate_invariants（§7）：首战抓到 B-4（remove 文件误减父
      NL，与 create 的 +0 不对称；对照 RonDB 925-926 修复，S-7d
      回归断言），负向测试注入孤儿 inode 精确触发 I-1/I-5/I-9；
      harness 28/28 + validator ALL PASS
- [x] 注入测试（§8）：J-01/J-03/J-06 实测 9/9 两轮稳定；J-03
      揪出 1021 盲重放缺口并修复（create/remove 遇 1021 绝不
      重放→DELAY；claim 可重放，只浪费号窗不重发号）
- [x] K-1..K-9 实测（§9）：K-1 通过（热父 16T 3162 ops/s 无冲突
      墙，CHG 精确）、K-8 通过待 SLO 对照（p99 8.5ms 亚线性）、
      K-5 量化关闭（0x21 双写 ~8% 写字节）、K-7 确认关闭（零告警）；
      K-2/K-3/K-4/K-9 DEFERRED（切片外子系统）
- [x] 更新 -08 报告的 Phase 3 章节（2026-10-15，commit 93bdd1f：§3
      "FDB 后端代码为零" 标记已消减 + 新增 §7 Phase 3 增量（lab-REAL），
      Defer 决策效力不变；已 push）
- [x] 收尾登记（2026-10-15）：`-05` §5 事务预算表验收线按 lab-REAL 填写
      （ns_create 原草案线"ops/s ≥ RonDB shared"**聚合未达标**（≈14%），
      差距全为 17x 提交延迟 → 结构 PASS + 聚合待 SLO；其余 5 行切片外
      标注 DEFERRED + 锚点）；`-04` §4 决策点状态更新（1021 库行为实测
      关闭；dead-letter 前缀仍未定 = keyspace 评审缺口——`-05` §1 前缀表
      无 dead-letter 前缀，0x43 是 prealloc_pool）；`-02` §B.3 归档目录
      落地（`tests/empirical/results/fdb/`：manifest.json + README）
