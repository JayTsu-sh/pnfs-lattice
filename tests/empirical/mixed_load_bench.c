/*
 * mixed_load_bench.c -- C-12 / K-3 measured closure: the FDB backend
 * under the REAL workload mix, catalogue-API level.  Closes the
 * "LAYOUTGET/SEQUENCE FDB 侧未测" blind spot (fdb-feasibility-10 §4):
 * Phase 3 (H-01/H-03/K-1) measured micro-ops and the create storm
 * only; the production-shaped MIX has never been run against FDB.
 *
 * Recipe: fdb-feasibility-06 §1.2.4 read-path mix (wire-level
 * capture 0007): LAYOUTGET 45% / PUTFH+GETATTR+ACCESS 20% /
 * SEQUENCE 25% / other 10%.  Stage sweep 1,2,4,8,16 threads,
 * fixed wall duration per stage (default 15 s), one seeded key
 * space shared by all stages (seed once, compare across stages).
 *
 * Op modeling at catalogue API level (documented, not hidden):
 *   LAYOUTGET   = layout_get_by_stateid (0x54+0x50) +
 *                 stripe_map_get (0x30/0x31)  -- the real read shape
 *   SEQUENCE    = slot_get + slot_put RMW on (session, slot)
 *                 (0x72) -- THE per-request write (H-03 hot path)
 *   PUTFH/GETATTR/ACCESS = ns_getattr point read (0x10) -- all three
 *                 are one inode read at catalogue level
 *   other (10%) = CREATE+REMOVE churn pair (namespace write path;
 *                 the capture's residual is OPEN 2%/GETFH 1%/...,
 *                 open_state 0x60 is outside this increment's vtable
 *                 scope, so namespace churn stands in for the write
 *                 residual)
 *
 * Post-run verification (C-12 SEQUENCE claim):
 *   - 1T stage SEQUENCE exactness: sum of slot seq_id == total
 *     successful SEQUENCE ops EXACTLY (no concurrency, no TOCTOU
 *     window; a mismatch is a real bug -> exit 3)
 *   - final update accounting: the API-pair (slot_get->slot_put,
 *     cross-transaction) RMW has a TOCTOU gap, so N>1T stages LOSE
 *     updates by construction (S-15 proves single-transaction RMW is
 *     exact).  The lost delta is reported as data -- it quantifies
 *     why kernel renewal paths must RMW in one FDB transaction.
 *
 * Usage: mixed_load_bench [cluster_file] [stage_seconds]
 *   defaults: /etc/foundationdb/fdb.cluster 15
 *
 * NOTE: WIPES the dedicated FDB lab cluster keyspaces at start
 * (same contract as fdb_slice_harness / fdb_hotdir_bench).
 *
 * Build (node 44, system headers):
 *   gcc -O2 -I /usr/include/foundationdb -I include -I src/catalogue \
 *       mixed_load_bench.c src/catalogue/catalogue_fdb.c \
 *       src/catalogue/rondb_schema.c -lfdb_c -lpthread -lm
 */
#define FDB_API_VERSION 740
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include <stdbool.h>

#include <foundationdb/fdb_c.h>

#include "mds_catalogue.h"
#include "catalogue_internal.h"
#include "rondb_schema.h"
#include "mds_log.h"
#include "ds_prealloc.h"
#include "pnfs_mds.h"
#include "open_state.h"

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

/* ---- workload shape ---- */
#define NSF 64          /* seeded files: stripe map + layout grant  */
#define NS  16          /* seeded sessions (clientid 5000+i)        */
#define SLOTS 64        /* slots per session                        */
#define MAXT 16
#define RING 65536      /* per-thread per-class latency ring        */
#define STAGE_T { 1, 2, 4, 8, 16 }

/* op classes (recipe: -06 §1.2.4) */
#define CL_LG     0   /* LAYOUTGET   45% */
#define CL_SEQ    1   /* SEQUENCE    25% */
#define CL_READ   2   /* PUTFH/GETATTR/ACCESS 20% */
#define CL_OTHER  3   /* CREATE+REMOVE churn 10% */
#define NCL       4
static const char *cl_name[NCL] = { "LAYOUTGET", "SEQUENCE",
				    "READ(fh/attr/access)", "OTHER(churn)" };

struct seeded {
	uint64_t fid;
	uint8_t sid_other[12];
};

/* ---- seeded state (file scope: workers read it) ---- */
static struct seeded g_seed[NSF];
static uint8_t g_sess_id_arr[NS][16];

static const uint8_t *g_sess_id(uint32_t i)
{
	return g_sess_id_arr[i];
}

struct ml_worker {
	int tid;
	struct mds_catalogue *cat;
	struct mds_cat_txn txn;
	uint64_t parent;
	volatile int *stop;
	int opidx;
	/* per-class results */
	uint64_t count[NCL];
	uint64_t errors[NCL];
	uint64_t delays[NCL];
	int ridx[NCL];
	double lat_us[NCL][RING];
	/* churn bookkeeping */
	uint64_t churn_n;
};

static volatile int g_stop = 0;
static uint64_t g_seq_ok = 0;   /* successful SEQUENCE ops (global) */

static uint64_t now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void rec(struct ml_worker *w, int cl, double us)
{
	w->count[cl]++;
	w->lat_us[cl][w->ridx[cl]] = us;
	w->ridx[cl] = (w->ridx[cl] + 1) % RING;
}

static void *ml_worker(void *arg)
{
	struct ml_worker *w = arg;
	uint64_t i;

	while (!*w->stop) {
		enum mds_status s;
		uint64_t t0, t1;
		int m = (int)(i = (uint64_t)w->opidx) % 100;

		w->opidx++;
		if (m < 45) {
			/* LAYOUTGET: stateid lookup + stripe map */
			const struct seeded *sd = &g_seed[i % NSF];
			uint64_t cid, fid, off, len;
			uint32_t mode, seq, sc, su, mc;
			struct mds_ds_map_entry *ents = NULL;

			t0 = now_ns();
			s = w->cat->coord_ops->layout_get_by_stateid(
					w->cat, sd->sid_other, &cid, &fid,
					&mode, &off, &len, &seq);
			if (s == MDS_OK)
				s = w->cat->auth_ops->stripe_map_get(
						w->cat, sd->fid, &sc, &su,
						&mc, &ents);
			t1 = now_ns();
			rec(w, CL_LG, (double)(t1 - t0) / 1000.0);
			if (ents != NULL)
				free(ents);
			if (s != MDS_OK) {
				w->errors[CL_LG]++;
				if (s == MDS_ERR_DELAY)
					w->delays[CL_LG]++;
			}
		} else if (m < 70) {
			/* SEQUENCE: slot RMW on (session, slot) */
			uint8_t sess[16];
			uint32_t sl = (uint32_t)((i * 17) % SLOTS);
			struct mds_coord_drc_slot_row row;
			uint32_t base;

			memcpy(sess, g_sess_id((uint32_t)((i * 31) % NS)),
			       16);
			t0 = now_ns();
			memset(&row, 0, sizeof(row));
			s = w->cat->coord_ops->slot_get(w->cat, sess, sl,
							&row);
			if (s != MDS_OK && s != MDS_ERR_NOTFOUND)
				goto seq_err;
			if (row.cached_reply != NULL)
				free(row.cached_reply);
			base = row.seq_id;
			s = w->cat->coord_ops->slot_put(w->cat, sess, sl,
							base + 1, "r", 1);
			t1 = now_ns();
			rec(w, CL_SEQ, (double)(t1 - t0) / 1000.0);
			if (s != MDS_OK) {
				w->errors[CL_SEQ]++;
				if (s == MDS_ERR_DELAY)
					w->delays[CL_SEQ]++;
			} else {
				__sync_fetch_and_add(&g_seq_ok, 1);
			}
			continue;
seq_err:
			t1 = now_ns();
			rec(w, CL_SEQ, (double)(t1 - t0) / 1000.0);
			w->errors[CL_SEQ]++;
			if (s == MDS_ERR_DELAY)
				w->delays[CL_SEQ]++;
		} else if (m < 90) {
			/* PUTFH/GETATTR/ACCESS: one inode point read */
			uint64_t f = (i * (7 + (uint64_t)(m - 45) % 3)) % NSF;
			struct mds_inode in;

			t0 = now_ns();
			s = w->cat->auth_ops->ns_getattr(w->cat,
							 g_seed[f].fid, &in);
			t1 = now_ns();
			rec(w, CL_READ, (double)(t1 - t0) / 1000.0);
			if (s != MDS_OK) {
				w->errors[CL_READ]++;
				if (s == MDS_ERR_DELAY)
					w->delays[CL_READ]++;
			}
		} else {
			/* other: CREATE+REMOVE churn pair = one op */
			char nm[48];
			struct mds_inode out;

			snprintf(nm, sizeof(nm), "churn_t%d_%06llu",
				 w->tid, (unsigned long long)w->churn_n);
			w->churn_n++;
			t0 = now_ns();
			s = w->cat->auth_ops->ns_create(w->cat, &w->txn,
					w->parent, nm, MDS_FTYPE_REG, 0644,
					0, 0, NULL, &out);
			if (s == MDS_OK)
				s = w->cat->auth_ops->ns_remove(w->cat,
						&w->txn, w->parent, nm);
			t1 = now_ns();
			rec(w, CL_OTHER, (double)(t1 - t0) / 1000.0);
			if (s != MDS_OK) {
				w->errors[CL_OTHER]++;
				if (s == MDS_ERR_DELAY)
					w->delays[CL_OTHER]++;
			}
		}
	}
	return NULL;
}

static int cmp_dbl(const void *a, const void *b)
{
	double x = *(const double *)a, y = *(const double *)b;
	return (x > y) - (x < y);
}

static double percentile(double *v, int n, double p)
{
	int idx = (int)((p / 100.0) * (n - 1));
	qsort(v, (size_t)n, sizeof(double), cmp_dbl);
	return v[idx];
}

int main(int argc, char **argv)
{
	const char *cluster = (argc > 1) ? argv[1]
		: "/etc/foundationdb/fdb.cluster";
	int stage_sec = (argc > 2) ? atoi(argv[2]) : 15;
	struct mds_config cfg;
	struct mds_catalogue *cat = NULL;
	struct mds_cat_txn txn;
	struct mds_inode in, child;
	enum mds_status st;
	int i, j, t, f;
	pthread_t th[MAXT];
	static struct ml_worker w[MAXT];
	static const int stages[] = STAGE_T;
	int nstages = sizeof(stages) / sizeof(stages[0]);

	printf("== FDB mixed-load bench (C-12/K-3, cluster: %s) ==\n",
	       cluster);
	printf("recipe: fdb-feasibility-06 §1.2.4 (LAYOUTGET 45%% / "
	       "SEQUENCE 25%% / READ 20%% / other 10%%)\n");
	printf("seed: %d files (stripe+layout) + %d sessions x %d slots; "
	       "stages 1/2/4/8/16T x %ds\n\n", NSF, NS, SLOTS, stage_sec);

	memset(&cfg, 0, sizeof(cfg));
	snprintf(cfg.catalogue_backend_conf,
		 sizeof(cfg.catalogue_backend_conf), "%s", cluster);
	cfg.catalogue_backend = MDS_BACKEND_FDB;
	st = catalogue_fdb_open(&cfg, &cat);
	if (st != MDS_OK) {
		printf("open failed st=%d\n", (int)st);
		return 2;
	}
	st = catalogue_fdb_wipe(cat);
	if (st != MDS_OK) {
		printf("wipe failed st=%d\n", (int)st);
		return 2;
	}
	memset(&txn, 0, sizeof(txn));
	txn.cat = cat;
	txn.txn_backend = MDS_BACKEND_FDB;

	/* ---- seed: dir + NSF files (stripe map + layout grant)
	 * + NS sessions ---- */
	st = cat->auth_ops->ns_create(cat, &txn, 2, "mixed",
				      MDS_FTYPE_DIR, 0755, 0, 0, NULL,
				      &child);
	if (st != MDS_OK) {
		printf("seed dir create failed st=%d\n", (int)st);
		return 2;
	}
	for (f = 0; f < NSF; f++) {
		struct mds_ds_map_entry ents[4];
		struct nfs4_stateid sid;
		uint32_t ds_ids[2] = { 1, 2 };
		int e;

		{
			char nm[32];
			snprintf(nm, sizeof(nm), "seed_%02d", f);
			st = cat->auth_ops->ns_create(cat, &txn,
					child.fileid, nm, MDS_FTYPE_REG,
					0644, 0, 0, NULL, &in);
		}
		if (st != MDS_OK) {
			printf("seed file %d create failed st=%d\n", f,
			       (int)st);
			return 2;
		}
		g_seed[f].fid = in.fileid;
		for (e = 0; e < 4; e++) {
			ents[e].ds_id = 1 + e;
			ents[e].nfs_fh_len = 16;
			memset(ents[e].nfs_fh, 0, sizeof(ents[e].nfs_fh));
			ents[e].nfs_fh[0] = (uint8_t)(0x40 + f);
			ents[e].nfs_fh[15] = (uint8_t)(e + 1);
		}
		st = cat->auth_ops->stripe_map_put(cat, &txn, g_seed[f].fid,
				4, 1048576, 1, ents);
		if (st != MDS_OK) {
			printf("seed file %d stripe put failed st=%d\n", f,
			       (int)st);
			return 2;
		}
		memset(&sid, 0, sizeof(sid));
		sid.seqid = 1;
		for (j = 0; j < 8; j++)
			sid.other[j] = (uint8_t)(g_seed[f].fid >> (8 * j));
		sid.other[8] = (uint8_t)0xAB;
		sid.other[9] = (uint8_t)(f >> 8);
		sid.other[10] = (uint8_t)0xCD;
		sid.other[11] = (uint8_t)0xEF;
		st = cat->coord_ops->layout_grant(cat, &txn,
				(uint64_t)(1000 + f), g_seed[f].fid, 2, 0,
				1 << 20, &sid, ds_ids, 2);
		if (st != MDS_OK) {
			printf("seed file %d layout grant failed st=%d\n",
			       f, (int)st);
			return 2;
		}
		memcpy(g_seed[f].sid_other, sid.other, 12);
	}
	for (t = 0; t < NS; t++) {
		struct mds_coord_client_row cl;
		struct mds_coord_session_row ss;

		/* client first (NFSv4 SETCLIENTID precedes CREATESESSION) so
		 * every seeded 0x70 session has a 0x73 client row -- keeps the
		 * seeded keyspace I-7-consistent (validate_invariants would
		 * otherwise flag 16 sessions referencing missing clients). */
		memset(&cl, 0, sizeof(cl));
		cl.clientid = 5000 + t;
		cl.co_ownerid_len = 4;
		memcpy(cl.co_ownerid, "test", 4);
		memset(cl.verifier, (uint8_t)(5000 + t), 8);
		cl.confirmed = true;
		st = cat->coord_ops->client_put(cat, &cl);
		if (st != MDS_OK) {
			printf("seed client %d put failed st=%d\n", t,
			       (int)st);
			return 2;
		}

		memset(&ss, 0, sizeof(ss));
		for (j = 0; j < 8; j++)
			ss.session_id[j] = (uint8_t)(
				(uint64_t)(5000 + t) >> (8 * j));
		ss.clientid = 5000 + t;
		ss.num_slots = SLOTS;
		ss.cb_prog = 1;
		ss.cb_sec_flavor = 3;
		st = cat->coord_ops->session_put(cat, &ss);
		if (st != MDS_OK) {
			printf("seed session %d put failed st=%d\n", t,
			       (int)st);
			return 2;
		}
		memcpy(g_sess_id_arr[t], ss.session_id, 16);
	}
	printf("seeded: %d files, %d sessions, %d clients\n\n",
	       NSF, NS, NS);

	/* ---- stage sweep ---- */
	for (t = 0; t < nstages; t++) {
		int n = stages[t];
		uint64_t t0, t1, wall_us;
		uint64_t tot = 0, terr = 0, tdel = 0;
		uint64_t cls_count[NCL] = { 0, 0, 0, 0 };
		uint64_t cls_err[NCL] = { 0, 0, 0, 0 };
		uint64_t cls_del[NCL] = { 0, 0, 0, 0 };
		int c, nsamp[NCL] = { 0, 0, 0, 0 };

		for (i = 0; i < n; i++) {
			memset(&w[i], 0, sizeof(w[i]));
			w[i].tid = i;
			w[i].cat = cat;
			w[i].txn.cat = cat;
			w[i].txn.txn_backend = MDS_BACKEND_FDB;
			w[i].parent = child.fileid;
			w[i].stop = &g_stop;
		}
		g_stop = 0;
		t0 = now_ns();
		for (i = 0; i < n; i++)
			pthread_create(&th[i], NULL, ml_worker, &w[i]);
		{
			struct timespec stop_at = { stage_sec, 0 };
			nanosleep(&stop_at, NULL);
		}
		g_stop = 1;
		for (i = 0; i < n; i++)
			pthread_join(th[i], NULL);
		t1 = now_ns();
		wall_us = (t1 - t0) / 1000ULL;

		/* 1T stage: no concurrency -> no TOCTOU window -> the
		 * API-pair RMW MUST be exact.  A mismatch here is a
		 * real bug (not a race artifact), so hard-fail. */
		if (t == 0) {
			uint64_t s1t = 0;
			int sj, si;

			for (sj = 0; sj < NS; sj++)
				for (si = 0; si < SLOTS; si++) {
					struct mds_coord_drc_slot_row row;

					memset(&row, 0, sizeof(row));
					if (cat->coord_ops->slot_get(
						       cat,
						       g_sess_id_arr[sj],
						       (uint32_t)si,
						       &row) == MDS_OK)
						s1t += row.seq_id;
					if (row.cached_reply != NULL)
						free(row.cached_reply);
				}
			printf("1T exactness: sum=%llu ok=%llu %s\n",
			       (unsigned long long)s1t,
			       (unsigned long long)g_seq_ok,
			       s1t == g_seq_ok ? "PASS" : "FAIL");
			if (s1t != g_seq_ok) {
				cat->ops->close(cat);
				return 3;
			}
		}

		for (i = 0; i < n; i++)
			for (c = 0; c < NCL; c++) {
				cls_count[c] += w[i].count[c];
				cls_err[c] += w[i].errors[c];
				cls_del[c] += w[i].delays[c];
			}

		/* collect latency samples (ring-truncated) */
		for (c = 0; c < NCL; c++) {
			nsamp[c] = 0;
			for (i = 0; i < n; i++) {
				int k = w[i].count[c] > (uint64_t)RING ?
					RING : (int)w[i].count[c];
				nsamp[c] += k;
			}
		}
		{
			long total_samp = 0;

			for (c = 0; c < NCL; c++)
				total_samp += nsamp[c];
			/* the copy pass below writes ALL classes back-to-back
			 * (cur runs across classes), so the buffer must hold
			 * the cross-class SUM, not a per-class max */
			double *samp = malloc((size_t)(total_samp > 1 ?
						    total_samp : 1) * sizeof(double));
			int base[NCL] = { 0, 0, 0, 0 };
			int cur = 0;
			for (c = 0; c < NCL; c++) {
				base[c] = cur;
				for (i = 0; i < n; i++) {
					int k = w[i].count[c] > (uint64_t)RING ?
						RING : (int)w[i].count[c];
					memcpy(samp + cur,
					       w[i].lat_us[c],
					       (size_t)k * sizeof(double));
					cur += k;
				}
			}
			printf("== stage %dT (%.2f s) ==\n", n,
			       (double)wall_us / 1e6);
			for (c = 0; c < NCL; c++) {
				tot += cls_count[c];
				terr += cls_err[c];
				tdel += cls_del[c];
			}
			printf("  total ops: %llu  ops/s: %.0f  "
			       "errors: %llu  delays: %llu\n",
			       (unsigned long long)tot,
			       tot ? (double)tot * 1e6 / (double)wall_us
				  : 0.0,
			       (unsigned long long)terr,
			       (unsigned long long)tdel);
			for (c = 0; c < NCL; c++) {
				if (cls_count[c] == 0)
					continue;
				printf("  %-20s ops=%-8llu p50=%9.1fus "
				       "p95=%9.1fus p99=%9.1fus err=%llu "
				       "del=%llu\n",
				       cl_name[c],
				       (unsigned long long)cls_count[c],
				       nsamp[c] ? percentile(
					       samp + base[c], nsamp[c],
					       50.0) : 0.0,
				       nsamp[c] ? percentile(
					       samp + base[c], nsamp[c],
					       95.0) : 0.0,
				       nsamp[c] ? percentile(
					       samp + base[c], nsamp[c],
					       99.0) : 0.0,
				       (unsigned long long)cls_err[c],
				       (unsigned long long)cls_del[c]);
			}
			free(samp);
		}
	}

	/* ---- final SEQUENCE update-accounting report:
	 * sum(slot seq over NS x SLOTS) vs total successful SEQ ops.
	 * The workload uses the cross-transaction slot_get->slot_put
	 * API pair, which has a TOCTOU gap (S-15 proof: single-tx RMW
	 * is exact, API-pair RMW loses updates under concurrency).
	 * The delta is the MEASUREMENT (kernel renewal paths must RMW
	 * in one transaction); it is reported, not a bench failure.
	 * 1T exactness is asserted separately (hard, above). ---- */
	{
		uint64_t sum = 0;
		uint64_t lost;

		for (t = 0; t < NS; t++)
			for (i = 0; i < SLOTS; i++) {
				struct mds_coord_drc_slot_row row;
				memset(&row, 0, sizeof(row));
				st = cat->coord_ops->slot_get(cat,
						g_sess_id_arr[t],
						(uint32_t)i, &row);
				if (st == MDS_OK)
					sum += row.seq_id;
				if (row.cached_reply != NULL)
					free(row.cached_reply);
			}
		lost = g_seq_ok > sum ? g_seq_ok - sum : 0;
		printf("\n== SEQUENCE update accounting ==\n");
		printf("successful SEQ ops : %llu\n",
		       (unsigned long long)g_seq_ok);
		printf("sum of slot seq_id : %llu\n",
		       (unsigned long long)sum);
		printf("lost (API-pair TOCTOU, expected under N>1T): %llu "
		       "-- kernel RMW must be single-transaction (S-15)\n",
		       (unsigned long long)lost);
		cat->ops->close(cat);
		return 0;
	}
}
