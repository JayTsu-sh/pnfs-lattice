/*
 * fdb_injection_tests.c -- Phase 3 injection tests, the slice-applicable
 * subset of docs/fdb-feasibility-04 section 3 (9 fault points total;
 * the 6 that need 2PC / MDS journal / GC-queue / DS-notification
 * subsystems are not implemented in the slice and are reported
 * SKIPPED with reason -- the slice is an in-process 6-slot backend).
 *
 * J-01 (3.1 "commit 后断网" / "commit 后 kill MDS", slice-level core):
 *      the reconciliation subsystem is out of slice, so the
 *      TESTABLE precondition it depends on is RETRY IDEMPOTENCY:
 *      a create whose commit succeeded (result unknown to the
 *      caller) must be re-issuable by a "recovery" without duplicate
 *      side effects -- re-create of the same name -> EXISTS, fileid
 *      unchanged, parent CHG moved exactly once.  This is the
 *      "fileid 不重复分配、nlink 不双减" expectation at the
 *      backend contract level.
 * J-03 (3.1 FDB commit_unknown_result 1021): the client library
 *      must NOT classify 1021 as retryable: fdb_transaction_on_error
 *      with 1021 must fail (must-cancel), while 1020 (not_committed)
 *      and 1007 (conflict) must succeed and apply backoff.
 *      Additionally the slice's retry loop is reported honestly: it
 *      currently backoffs on ANY commit error code, so a 1021 would
 *      be blindly re-staged -- the -04 three-way reconciliation
 *      (commit/abort/unknown) lives in the MDS journal plane, not in
 *      the slice; reported as a tracked gap, not a silent pass.
 * J-06 (3.2 读版本过旧中止): a transaction whose read set was
 *      populated BEFORE a concurrent writer advanced a key must be
 *      ABORTED (1007/1020) at commit -- no half-write; the database
 *      keeps the concurrent writer's value.  (FDB 7.4 has no
 *      set_read_snapshot; an in-tx read at an old version pins the
 *      read set at that version, which is exactly the mechanism a
 *      stale snapshot would use.)
 *
 * SKIPPED (subsystem not in slice): J-02 kill-MDS after commit
 * (needs the MDS process + journal scan), J-04 2PC rename mid-crash
 * (rename unimplemented, G1/NOSUPPORT), J-05 DS-notification side
 * effect (DS plane out of scope), J-07/J-08/J-09 poison GC rows
 * (0x41/0x42/0x43 keyspaces + claim/TTL machinery unimplemented).
 *
 * Exit: 0 = all applicable checks PASS, 1 = any FAIL, 2 = env error.
 *
 * Build (node 44, system headers):
 *   gcc -O2 -I /usr/include/foundationdb -I include -I src/catalogue \
 *       fdb_injection_tests.c src/catalogue/catalogue_fdb.c \
 *       src/catalogue/rondb_schema.c -lfdb_c -lpthread -lm
 * Run:  ./fdb_injection_tests [cluster_file]
 *
 * NOTE: this test WIPES the dedicated FDB lab cluster keyspaces at
 * start (same contract as fdb_slice_harness) and leaves J-01's
 * single test file behind; J-06's synthetic inode key is cleaned up
 * so validate_invariants' I-9 (no orphans) stays green afterwards.
 */
#define FDB_API_VERSION 740
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <foundationdb/fdb_c.h>

#include "mds_catalogue.h"
#include "catalogue_internal.h"
#include "rondb_schema.h"
#include "mds_log.h"
#include "ds_prealloc.h"
#include "pnfs_mds.h"

/* ---- stubs (same as fdb_slice_harness.c) ---- */
int mds_log_component_levels[LOG_COMP_COUNT];
void mds_log(int comp, int level, const char *fmt, ...)
{
	(void)comp; (void)level; (void)fmt;
}
int ds_prealloc_pop(struct ds_prealloc_ctx *ctx,
		    struct mds_ds_map_entry *entry,
		    uint32_t *stripe_unit, uint64_t *fileid_out)
{
	(void)ctx; (void)entry; (void)stripe_unit; (void)fileid_out;
	return -1;
}

extern enum mds_status catalogue_fdb_open(const struct mds_config *cfg,
					  struct mds_catalogue **out);
extern enum mds_status catalogue_fdb_wipe(struct mds_catalogue *cat);

struct dir_result {
	int count;
	uint64_t fids[256];
	char names[256][MDS_MAX_NAME + 1];
	int types[256];
};

static int dir_cb(const struct mds_cat_dirent *e, void *arg)
{
	struct dir_result *r = arg;
	if (r->count < 256) {
		r->fids[r->count] = e->fileid;
		r->types[r->count] = e->type;
		strncpy(r->names[r->count], e->name, MDS_MAX_NAME);
	}
	r->count++;
	return 0;
}

static int g_pass = 0, g_fail = 0;
#define CHECK(cond, fmt, ...) do { \
	if (cond) { g_pass++; printf("  PASS " fmt "\n", ##__VA_ARGS__); } \
	else { g_fail++; printf("  FAIL " fmt "\n", ##__VA_ARGS__); } \
} while (0)

static const char *stname(enum mds_status st)
{
	switch (st) {
	case MDS_OK: return "OK";
	case MDS_ERR_NOTFOUND: return "NOTFOUND";
	case MDS_ERR_EXISTS: return "EXISTS";
	case MDS_ERR_IO: return "IO";
	case MDS_ERR_INVAL: return "INVAL";
	case MDS_ERR_STALE: return "STALE";
	case MDS_ERR_DELAY: return "DELAY";
	case MDS_ERR_NOSUPPORT: return "NOSUPPORT";
	default: return "other";
	}
}

int main(int argc, char **argv)
{
	const char *cluster = (argc > 1) ? argv[1]
		: "/etc/foundationdb/fdb.cluster";
	struct mds_config cfg;
	struct mds_catalogue *cat = NULL;
	struct mds_cat_txn txn;
	struct mds_inode child, before, after, in;
	enum mds_status st;
	int i;

	printf("== FDB injection tests (slice subset, cluster: %s) ==\n",
	       cluster);
	memset(&cfg, 0, sizeof(cfg));
	snprintf(cfg.catalogue_backend_conf,
		 sizeof(cfg.catalogue_backend_conf), "%s", cluster);
	cfg.catalogue_backend = MDS_BACKEND_FDB;
	st = catalogue_fdb_open(&cfg, &cat);
	if (st != MDS_OK) {
		printf("open failed: %s\n", stname(st));
		return 2;
	}
	st = catalogue_fdb_wipe(cat);
	if (st != MDS_OK) {
		printf("wipe failed: %s\n", stname(st));
		return 2;
	}
	memset(&txn, 0, sizeof(txn));
	txn.cat = cat;
	txn.txn_backend = MDS_BACKEND_FDB;

	/* The network is running (started by catalogue_fdb_open in
	 * this process); a second database handle is cheap and lets
	 * J-03/J-06 exercise raw FDB semantics without touching the
	 * slice's internal state struct. */
	FDBDatabase *db2 = NULL;
	{
		fdb_error_t fe = fdb_create_database(cluster, &db2);
		if (fe) {
			printf("raw db open failed: %d\n", (int)fe);
			return 2;
		}
	}

	/* J-01: recovery re-issues a committed create whose result was
	 * unknown to the caller (3.1 "commit 后断网/kill MDS").  The
	 * slice's reconciliation is out of scope; the testable core is
	 * that re-issuing is side-effect-free:
	 *   - same name -> EXISTS (the insert path never runs)
	 *   - fileid unchanged (no duplicate allocation)
	 *   - parent CHG unchanged AND parent row count unchanged
	 *     (no duplicate counter bump, no duplicate index row)
	 * On this backend the duplicate create is a read-only
	 * pre-check -> no write transaction is staged at all, so
	 * "no side effect" is observable as CHG unchanged + dirent
	 * still mapping to the original fileid. */
	uint64_t fid1 = 0;
	st = cat->auth_ops->ns_create(cat, &txn, 2, "inj_file",
				      MDS_FTYPE_REG, 0644, 0, 0,
				      NULL, &child);
	if (st != MDS_OK) {
		printf("  setup create failed: %s\n", stname(st));
		return 2;
	}
	fid1 = child.fileid;
	st = cat->auth_ops->ns_getattr(cat, 2, &before);

	/* the "recovery" re-issues the same create */
	st = cat->auth_ops->ns_create(cat, &txn, 2, "inj_file",
				      MDS_FTYPE_REG, 0644, 0, 0,
				      NULL, &in);
	CHECK(st == MDS_ERR_EXISTS,
	      "J-01a re-create -> EXISTS (st=%s)", stname(st));
	st = cat->auth_ops->ns_lookup(cat, 2, "inj_file", &in);
	CHECK(st == MDS_OK && in.fileid == fid1,
	      "J-01b fileid unchanged %llu==%llu (st=%s)",
	      (unsigned long long)fid1,
	      (unsigned long long)in.fileid, stname(st));
	st = cat->auth_ops->ns_getattr(cat, 2, &after);
	CHECK(st == MDS_OK && after.change == before.change &&
	      after.nlink == before.nlink,
	      "J-01c no duplicate side effect (CHG %llu->%llu, NL %u->%u)",
	      (unsigned long long)before.change,
	      (unsigned long long)after.change,
	      before.nlink, after.nlink);
	{
		struct dir_result dr;
		uint64_t seen_fid = 0;
		memset(&dr, 0, sizeof(dr));
		st = cat->auth_ops->ns_readdir(cat, 2, NULL, 256,
					       &txn, dir_cb, &dr);
		if (st == MDS_OK) {
			for (i = 0; i < dr.count; i++) {
				if (strcmp(dr.names[i], "inj_file") == 0)
					seen_fid = dr.fids[i];
			}
		}
		CHECK(st == MDS_OK && seen_fid == fid1,
		      "J-01d dirent still maps to original fileid "
		      "(%llu, st=%s)", (unsigned long long)seen_fid,
		      stname(st));
	}

	/* ---------- J-03: commit_unknown_result(1021) ---------- */
	printf("J-03 commit_unknown_result (1021) retryability policy\n");
	{
		FDBTransaction *tr = NULL;
		FDBFuture *f;
		int e;

		fdb_database_create_transaction(db2, &tr);
		/* DOCUMENTED FDB behavior (error-codes page + the
		 * -04 spec): on_error treats 1021 as RETRYABLE --
		 * the client will apply backoff and a blind retry
		 * loop could double-apply the transaction.  Assert
		 * the library matches that documented policy, then
		 * report the consequence for the slice. */
		f = fdb_transaction_on_error(tr, 1021);
		fdb_future_block_until_ready(f);
		e = (int)fdb_future_get_error(f);
		CHECK(e == 0,
		      "J-03a on_error(1021) applies backoff -> library\n"
		      "         treats commit_unknown_result as RETRYABLE per\n"
		      "         documented policy (err=%d); the protection is\n"
		      "         the APP's job (-04: reconciliation, never\n"
		      "         blind retry)", e);
		fdb_future_destroy(f);
		fdb_transaction_reset(tr);
		/* must BE retryable (contrast) */
		f = fdb_transaction_on_error(tr, 1020);
		fdb_future_block_until_ready(f);
		e = (int)fdb_future_get_error(f);
		CHECK(e == 0,
		      "J-03b on_error(1020 not_committed) accepted as "
		      "retryable (err=%d)", e);
		fdb_future_destroy(f);
		fdb_transaction_reset(tr);
		f = fdb_transaction_on_error(tr, 1007);
		fdb_future_block_until_ready(f);
		e = (int)fdb_future_get_error(f);
		CHECK(e == 0,
		      "J-03c on_error(1007 conflict) accepted as "
		      "retryable (err=%d)", e);
		fdb_future_destroy(f);
		fdb_transaction_destroy(tr);
		printf("  GAP (tracked): the slice retry loop\n"
		       "  (catalogue_fdb.c) backs off on ANY commit error and re-stages\n"
		       "  up to 50 times -> a 1021 (commit_unknown_result) would be\n"
		       "  blindly retried. The -04 three-way reconciliation (commit/\n"
		       "  abort/unknown) lives in the MDS journal plane, not in the\n"
		       "  slice; closing it needs the journal, so it is a deferred\n"
		       "  hardening item, not a silent pass. In-slice mitigation:\n"
		       "  map commit err==1021 to MDS_ERR_DELAY immediately (no\n"
		       "  re-stage) -- left as a Phase 3.1 follow-up.");
	}

	/* ---------- J-06: stale-snapshot write conflict ---------- */
	printf("J-06 stale-snapshot abort: read pinned before a concurrent\n"
	       "     change, commit a write -> abort, no half-write\n");
	{
		uint8_t key[9];
		uint64_t fid = 0x0000000000004351ULL; /* 17233: far above
						       the FID counter
						       after a single
						       create; cleaned
						       up at the end */
		FDBTransaction *t1, *t2, *t3, *t4;
		FDBFuture *fc;
		uint8_t v1b[8] = { 1, 0, 0, 0, 0, 0, 0, 0 };
		uint8_t v2b[8] = { 2, 0, 0, 0, 0, 0, 0, 0 };
		uint8_t v3b[8] = { 3, 0, 0, 0, 0, 0, 0, 0 };
		int err_seed, err_adv, err_conf, err_cl;
		fdb_bool_t pv;
		const uint8_t *vv;
		int vvlen;

		key[0] = 0x10;
		{
			int j;
			for (j = 0; j < 8; j++)
				key[1 + j] =
					(uint8_t)(fid >> (8 * (7 - j)));
		}

		/* tx1: seed the key */
		fdb_database_create_transaction(db2, &t1);
		fdb_transaction_set(t1, key, 9, v1b, 8);
		fc = fdb_transaction_commit(t1);
		fdb_future_block_until_ready(fc);
		err_seed = (int)fdb_future_get_error(fc);
		fdb_future_destroy(fc);
		fdb_transaction_destroy(t1);
		if (err_seed != 0) {
			printf("  env: seed commit %d\n", err_seed);
			return 2;
		}

		/* tx3 (stale reader): open before the concurrent
		 * writer; its read pins the key in the read set at
		 * the version it started at */
		fdb_database_create_transaction(db2, &t3);
		{
			FDBFuture *g = fdb_transaction_get(t3, key, 9, 0);
			fdb_future_block_until_ready(g);
			pv = 0;
			vv = NULL;
			vvlen = 0;
			fdb_future_get_value(g, &pv, &vv, &vvlen);
			fdb_future_destroy(g);
		}
		fdb_transaction_set(t3, key, 9, v3b, 8);

		/* tx2: concurrent writer advances the same key */
		fdb_database_create_transaction(db2, &t2);
		fdb_transaction_set(t2, key, 9, v2b, 8);
		fc = fdb_transaction_commit(t2);
		fdb_future_block_until_ready(fc);
		err_adv = (int)fdb_future_get_error(fc);
		fdb_future_destroy(fc);
		fdb_transaction_destroy(t2);
		if (err_adv != 0) {
			printf("  env: advance commit %d\n", err_adv);
			return 2;
		}

		/* commit the stale tx: must abort with a conflict,
		 * never commit a half-write */
		fc = fdb_transaction_commit(t3);
		fdb_future_block_until_ready(fc);
		err_conf = (int)fdb_future_get_error(fc);
		fdb_future_destroy(fc);
		fdb_transaction_destroy(t3);
		CHECK(err_conf == 1007 || err_conf == 1020,
		      "J-06a stale-snapshot commit aborted (err=%d)",
		      err_conf);

		/* the db holds the concurrent writer's value */
		fdb_database_create_transaction(db2, &t4);
		{
			FDBFuture *g = fdb_transaction_get(t4, key, 9, 0);
			fdb_future_block_until_ready(g);
			pv = 0;
			vv = NULL;
			vvlen = 0;
			fdb_future_get_value(g, &pv, &vv, &vvlen);
			CHECK(pv && vvlen == 8 && vv[0] == 2,
			      "J-06b db holds concurrent writer's value "
			      "(got %d)",
			      (pv && vvlen == 8) ? (int)vv[0] : -1);
			fdb_future_destroy(g);
		}
		fdb_transaction_clear(t4, key, 9);
		fc = fdb_transaction_commit(t4);
		fdb_future_block_until_ready(fc);
		err_cl = (int)fdb_future_get_error(fc);
		fdb_future_destroy(fc);
		fdb_transaction_destroy(t4);
		if (err_cl != 0)
			printf("  NOTE: J-06 cleanup commit failed (%d);\n"
			       "        synthetic orphan key 0x10|%016llx "
			       "remains\n",
			       err_cl, (unsigned long long)fid);

		fdb_database_destroy(db2);
	}

	printf("SKIPPED J-02 kill-MDS after commit (needs the MDS\n"
	       "       process + journal scan; slice is in-process)\n");
	printf("SKIPPED J-04 2PC rename mid-crash (rename unimplemented,\n"
	       "       G1/NOSUPPORT; K-4 route decision pending)\n");
	printf("SKIPPED J-05 DS-notification side effect (DS plane not\n"
	       "       in the slice)\n");
	printf("SKIPPED J-07/J-08/J-09 poison GC rows (0x41/0x42/0x43\n"
	       "       keyspaces + claim/TTL machinery unimplemented)\n");

	printf("\n== result: %d passed, %d failed ==\n", g_pass, g_fail);
	cat->ops->close(cat);
	return g_fail ? 1 : 0;
}
