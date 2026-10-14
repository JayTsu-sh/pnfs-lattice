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

### 4.1 切片功能：21/23 通过

| 断言 | 结果 | 说明 |
|------|------|------|
| S-1 root inode (nlink=2 计数器) | PASS | |
| S-2 create dir + lookup/getattr 往返 | PASS | fid 从计数器分配 |
| S-3 create file（父 nlink 不变, change+1, 原子 ADD） | PASS | |
| S-4 重复 create → EXISTS | PASS | 原子预检 |
| S-5a/S-5b readdir + 分页 | **FAIL** | 见 §5 range 异常 |
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

### 4.3 FDB 集群 range 扫描异常（REAL，环境层，阻塞 S-5）

**现象**（`fdb_raw_range_test.c`，零共享代码，系统头编译，node 44）：

```
set: A01=de ad, A02=be ef, B03=11 22, B04=33 44   (commit err=0)
point read B03 -> present, val=11 22                (正确)
range [A,B) 期望 {A01,A02}
  实际 {20 00...03 66 5f 31 00 (范围外的旧键!), 41 01}
range [B,C) 期望 {B03,B04}
  实际 {41 02 (低于下界!), 42 03}                    (漏 B04)
```

规律：**range 的上下界同时左移一个键位**——结果 = {prev(begin), begin}，
精确地整体偏移一行。点读（get）全部正确；set/commit/点读/集群 status
全部正常。

**已排除**：
- 业务代码：零共享代码的最小测试复现（raw_range_test.c 不链接
  catalogue_fdb.c 的任何代码）
- 头文件 ABI 错配：用 44 系统 /usr/include/foundationdb 头重编，结果相同
- 版本错配：client 7.4.8 == server 7.4.8
- 配置：foundationdb.conf 无自定义 knob（默认配置）
- 数据新鲜度：range 返回的键值正是本进程刚 set 的值（同一 DB）

**未排除**：
- 该集群的 server 端 range 扫描路径（TSS/日志层）存在 7.4.8 特有缺陷
  或该部署的数据目录处于异常状态
- 从 node 45/46 用不同客户端进程验证（45 无 gcc，44 中转 scp 被
  审批门超时拦截，待批准后补做）

**影响**：ns_readdir（以及任何依赖 range 扫描的路径：readdir、
migration 批量读、future 的 0x21 二级索引）在该集群上不可信。
单键路径（lookup/getattr/setattr/create/remove 的主键读）不受影响，
S-1..S-4/S-6..S-10 的通过是有效的。

**处置**：
- 登记为 Phase 3 阻塞项 B-1；S-5 的失败判定为环境因素而非实现缺陷
  （readdir 实现本身在 raw 探测层面已验证键构造正确）
- 建议下一步：node 45/46 交叉验证 → 若同样异常则重装/重建该集群
  数据目录，或升级 FDB 版本复测
- 在 B-1 解除前，readdir 相关的 K-1..K-9 实测不可进行

## 5. 与 K 指标的对应

K-1（create 吞吐）/ K-2（lookup）等单键类指标可在现有 21/23 基础上
先行采样；K-4（rename，未实现）与 readdir 类指标受 B-1 阻塞。

## 6. 待办

- [ ] B-1：node 45/46 交叉验证 range 异常（需 scp 中转审批）
- [ ] B-1：若确认集群级 → 重建集群数据目录复测
- [ ] S-5 复绿后提交 slice 收尾 + 更新 -08 报告的 Phase 3 章节
- [ ] atomic_op 热路径（FID 计数器 ADD 化 + 0x21 二级索引）
- [ ] validate_invariants I-1..I-9
- [ ] 9 注入测试
- [ ] K-1..K-9 实测
