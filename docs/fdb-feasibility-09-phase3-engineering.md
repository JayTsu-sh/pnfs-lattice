# FDB 可行性分析 Phase 3 — 工程实现与垂直切片实测

日期: 2026-10-14 (更新中)
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
validate_invariants ALL PASS。Phase 3 剩：9 注入测试、K-1..K-9 实测。

## 8. 待办

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
- [ ] 更新 -08 报告的 Phase 3 章节
- [ ] 9 注入测试
- [ ] 9 注入测试
- [ ] K-1..K-9 实测
