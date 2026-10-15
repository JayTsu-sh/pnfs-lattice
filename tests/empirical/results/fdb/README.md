# FDB 侧实验结果归档（fdb-feasibility-02 §B.3）

约定：每次 A/B 实验一个子目录 `<run_id>_<backend>_<variant>/`，目录内放
`manifest.json`，最少字段见 `-02` §B.3：`commit_sha` / `backend` / `env` /
`slo_anchor` / `evidence_class` / `raw_paths` / `operator` / `timestamp`。

纪律（对齐计划 §06 "原始结果、日志、trace、配置、脚本与 commit SHA 一并归档"）：
- 只存**原始**产物 + manifest；摘要/结论进 `docs/fdb-feasibility-*` 并回链 manifest 路径。
- `evidence_class` 强制三档（REAL / REFERENCE / HYPOTHESIS）；Phase 3 产物为 **lab-REAL**
  （node 44 实验室 3 节点集群，非生产拓扑）。
- `validate_invariants` 的 FAIL 样本落 `<run_id>/invariants/`。Phase 3 运行 ALL PASS
  无 FAIL 样本；负向注入的 FAIL 样本（孤儿 inode fid=0xDEADBEEF12345678）为瞬态诊断产物，
  clear 后未留存——复现步骤见 `-09` §7。

说明：Phase 3（2026-10-14..15）各 run 的原始终端转录在 Hermes 会话记录（node 44
10.131.9.44 上执行）；verbatim 原始结果表已登记进
`docs/fdb-feasibility-09-phase3-engineering.md`（§4 harness 28/28 / §7 校验器 /
§8 注入 9/9 / §9 hotdir bench 逐线程表），manifest 的 `raw_paths` 指向对应文档节。
既有 RonDB 侧基线目录（`../0003`…`0007`、`../baseline_lattice_main`）早于本规范建立，
未补 manifest（如需 A/B 回链可后续补，不阻塞）。
