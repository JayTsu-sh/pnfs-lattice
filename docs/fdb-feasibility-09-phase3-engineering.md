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

## 5. 与 K 指标的对应

切片 23/23 全绿后无环境阻塞：K-1（create 吞吐）/ K-2（lookup）等
单键类指标可直接采样；readdir 类指标（依赖 range 扫描）在 B-1 根因
（KEYSEL 误用）修复后同样可测；K-4（rename，未实现）仍待后续槽位。

## 6. 待办

- [x] B-1 已解决（§4.3）：根因是 KeySelector 编码误用（begin=(1,0)
      应为 (0,1)），非集群异常；fdbcli 对照 + 黄金用例证实，23/23 全绿
- [x] S-5 复绿：23/23 两轮一致（node 44 真实集群）
- [ ] 提交 slice 收尾 + 更新 -08 报告的 Phase 3 章节
- [ ] atomic_op 热路径（FID 计数器 ADD 化 + 0x21 二级索引）
- [ ] validate_invariants I-1..I-9
- [ ] 9 注入测试
- [ ] K-1..K-9 实测
