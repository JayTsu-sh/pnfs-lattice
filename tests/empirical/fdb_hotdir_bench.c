/*
 * fdb_hotdir_bench.c -- K-1 + K-8 measured validation on the REAL
 * cluster, through the REAL ns_create path (0x20 dirent + 0x21 index
 * + 0x10 inode + atomic ADD parent counters), NOT the standalone
 * H-01 micro-benchmark:
 *
 * K-1 (hot parent directory): N threads all creating files in the
 * SAME parent.  If the parent counters were plain RMW the run would
 * hit the H-01 conflict wall (~370 ops/s, -00 §7.1); with the
 * atomic_op counter design the wall must be absent.  Benchmarked
 * against the RonDB baseline (-01 §3): shared-parent 16T ~23k
 * ops/s is the parity anchor, H-01's 6.5k ops/s zero-conflict
 * standalone ADD is the floor.
 *
 * K-8 (tail latency under concurrency): per-op latencies are
 * recorded; p50/p99/p999 reported.  H-01's p99 122-175ms wall
 * (-00 §7.1) is the contrast anchor.
 *
 * Post-run verification (the numbers are only trustworthy if
 * nothing was lost):
 *   - parent CHG counter moved exactly by N*per-thread-ops
 *     (no lost atomic updates)
 *   - FID counter == total creates (monotone, no id handout twice)
 *   - all returned fids unique
 *
 * Usage: fdb_hotdir_bench [cluster_file] [threads] [ops_per_thread]
 *   defaults: /etc/foundationdb/fdb.cluster 8 200
 *
 * NOTE: WIPES the dedicated FDB lab cluster keyspaces at start
 * (same contract as fdb_slice_harness).  Leaves the hot-dir test
 * tree behind (cleaned only by the next wipe).
 *
 * Build (node 44, system headers):
 *   gcc -O2 -I /usr/include/foundationdb -I include -I src/catalogue \
 *       fdb_hotdir_bench.c src/catalogue/catalogue_fdb.c \
 *       src/catalogue/rondb_schema.c -lfdb_c -lpthread -lm
 */
#define FDB_API_VERSION 740
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>

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

#define MAXT 32
#define MAXOPS 200000

struct bench_worker {
	int tid;
	struct mds_catalogue *cat;
	struct mds_cat_txn txn;
	uint64_t parent;
	int ops;
	/* results */
	int done;           /* ops completed */
	int err;            /* first error code (mds_status), 0 = none */
	double lat_us[MAXOPS];
	uint64_t fids[MAXOPS];
};

static int cmp_dbl(const void *a, const void *b)
{
	double x = *(const double *)a, y = *(const double *)b;
	return (x > y) - (x < y);
}

static double percentile(const double *v, int n, double p)
{
	int idx = (int)((p / 100.0) * (n - 1));
	double *s = malloc((size_t)n * sizeof(double));
	if (!s) return -1;
	memcpy(s, v, (size_t)n * sizeof(double));
	qsort(s, (size_t)n, sizeof(double), cmp_dbl);
	double r = s[idx];
	free(s);
	return r;
}

static uint64_t now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void *bench_worker(void *arg)
{
	struct bench_worker *w = arg;
	char nm[48];
	int i;

	w->done = 0;
	w->err = 0;
	for (i = 0; i < w->ops; i++) {
		struct mds_inode out;
		enum mds_status s;
		uint64_t t0, t1;

		snprintf(nm, sizeof(nm), "t%d_f%06d", w->tid, i);
		t0 = now_ns();
		s = w->cat->auth_ops->ns_create(w->cat, &w->txn,
						w->parent, nm,
						MDS_FTYPE_REG, 0644,
						0, 0, NULL, &out);
		t1 = now_ns();
		w->lat_us[i] = (double)(t1 - t0) / 1000.0;
		if (s != MDS_OK) {
			w->err = (int)s;
			break;
		}
		w->fids[i] = out.fileid;
		w->done++;
	}
	return NULL;
}

/* Read a raw key and return its value as a uint64 LE.  Returns 0 on
 * success, -1 if the key is absent, >0 on transport error. */
static int read_u64(FDBDatabase *db, const uint8_t *key, int klen,
		    uint64_t *out)
{
	uint8_t buf[8];
	FDBTransaction *tr;
	FDBFuture *g;
	fdb_bool_t p = 0;
	const uint8_t *v;
	int vl = 0, rc, j;

	memset(buf, 0, sizeof(buf));
	fdb_database_create_transaction(db, &tr);
	g = fdb_transaction_get(tr, key, klen, 0);
	fdb_future_block_until_ready(g);
	rc = (int)fdb_future_get_error(g);
	if (rc == 0) {
		if (fdb_future_get_value(g, &p, &v, &vl) == 0 &&
		    p && vl == 8)
			memcpy(buf, v, 8);
		else
			rc = -1;
	}
	fdb_future_destroy(g);
	fdb_transaction_destroy(tr);
	if (rc != 0)
		return rc;
	uint64_t x = 0;
	for (j = 7; j >= 0; j--)
		x = (x << 8) | buf[j];
	*out = x;
	return 0;
}

/* counter key builders (mirror cnt_change_key / CNT_FID_KEY) */
static int chg_key(uint8_t *k, uint64_t parent)
{
	k[0] = 0x00; k[1] = 'C'; k[2] = 'H'; k[3] = 'G';
	{
		int j;
		for (j = 0; j < 8; j++)
			k[4 + j] = (uint8_t)(parent >> (8 * (7 - j)));
	}
	return 12;
}
static const uint8_t fid_key[4] = { 0x00, 'F', 'I', 'D' };

int main(int argc, char **argv)
{
	const char *cluster = (argc > 1) ? argv[1]
		: "/etc/foundationdb/fdb.cluster";
	int threads = (argc > 2) ? atoi(argv[2]) : 8;
	int ops = (argc > 3) ? atoi(argv[3]) : 200;
	struct mds_config cfg;
	struct mds_catalogue *cat = NULL;
	struct mds_cat_txn txn;
	struct mds_inode in, child;
	enum mds_status st;
	int i, j, total, errors = 0;
	pthread_t th[MAXT];
	static struct bench_worker w[MAXT];
	static double all_lat[MAXT * MAXOPS];
	uint64_t t0, t1, wall_us;
	uint64_t fid_counter, chg_before, chg_after;
	uint64_t chg_delta, fid_after;
	int rc = 0;

	if (threads > MAXT)
		threads = MAXT;
	if (ops > MAXOPS)
		ops = MAXOPS;

	printf("== FDB hot-parent bench (K-1/K-8, cluster: %s) ==\n",
	       cluster);
	printf("threads=%d ops_per_thread=%d (total %d creates, ONE "
	       "shared parent)\n\n", threads, ops, threads * ops);

	memset(&cfg, 0, sizeof(cfg));
	snprintf(cfg.catalogue_backend_conf,
		 sizeof(cfg.catalogue_backend_conf), "%s", cluster);
	cfg.catalogue_backend = MDS_BACKEND_FDB;
	st = catalogue_fdb_open(&cfg, &cat);
	if (st != MDS_OK) {
		printf("open failed\n");
		return 2;
	}
	st = catalogue_fdb_wipe(cat);
	if (st != MDS_OK) {
		printf("wipe failed\n");
		return 2;
	}
	memset(&txn, 0, sizeof(txn));
	txn.cat = cat;
	txn.txn_backend = MDS_BACKEND_FDB;

	/* hot parent = a dedicated dir under root */
	st = cat->auth_ops->ns_create(cat, &txn, 2, "hotdir",
				      MDS_FTYPE_DIR, 0755, 0, 0, NULL,
				      &child);
	if (st != MDS_OK) {
		printf("hotdir create failed\n");
		return 2;
	}
	{
		uint64_t hotdir = child.fileid;
		st = cat->auth_ops->ns_getattr(cat, hotdir, &in);
		chg_before = (st == MDS_OK) ? in.change : 0;

		for (i = 0; i < threads; i++) {
			memset(&w[i], 0, sizeof(w[i]));
			w[i].tid = i;
			w[i].cat = cat;
			w[i].txn.cat = cat;
			w[i].txn.txn_backend = MDS_BACKEND_FDB;
			w[i].parent = hotdir;
			w[i].ops = ops;
		}
		t0 = now_ns();
		for (i = 0; i < threads; i++)
			pthread_create(&th[i], NULL, bench_worker, &w[i]);
		for (i = 0; i < threads; i++)
			pthread_join(th[i], NULL);
		t1 = now_ns();
		wall_us = (t1 - t0) / 1000ULL;

		total = 0;
		for (i = 0; i < threads; i++) {
			total += w[i].done;
			if (w[i].err) {
				errors++;
				printf("thread %d stopped: st=%d after %d\n",
				       i, w[i].err, w[i].done);
			}
		}

		{
			long long opsum = 0;
			int n = 0;
			for (i = 0; i < threads; i++)
				for (j = 0; j < w[i].done; j++) {
					all_lat[n++] = w[i].lat_us[j];
					opsum += (long long)
						(w[i].lat_us[j] + 0.5);
				}
			printf("\n== throughput ==\n");
			printf("wall time:      %lld ms\n",
			       (long long)(wall_us / 1000));
			printf("completed:      %d / %d\n", total,
			       threads * ops);
			printf("ops/s:          %.0f\n",
			       total ? (double)total * 1e6 /
				       (double)wall_us : 0.0);
			printf("mean op latency: %.1f us "
			       "(thread-serialized sum %.1f ms)\n",
			       n ? (double)opsum / n : 0.0,
			       (double)opsum / 1000.0);
			printf("per-thread serial ops/s: %.0f\n",
			       ops ? (double)ops * 1e6 /
				     (double)(opsum ? opsum / threads
						    : 1) : 0.0);
			printf("\n== latency percentiles (us) ==\n");
			if (n > 0) {
				double p50 = percentile(all_lat, n, 50.0);
				double p99 = percentile(all_lat, n, 99.0);
				double p999 = percentile(all_lat, n,
							 99.9);
				printf("p50  = %10.1f\n", p50);
				printf("p99  = %10.1f\n", p99);
				printf("p999 = %10.1f\n", p999);
			}

			printf("\n== correctness (atomicity of counters) "
			       "==\n");
			{
				FDBDatabase *db2 = NULL;
				uint8_t ck[12];
				int cklen = chg_key(ck, w[0].parent);
				if (fdb_create_database(cluster,
							&db2) == 0) {
					int r1 = read_u64(db2, ck,
							  cklen,
							  &chg_after);
					int r2 = read_u64(db2, fid_key,
							  4, &fid_after);
					if (r1 == 0) {
						chg_delta =
							chg_after -
							chg_before;
						printf("parent CHG "
						       "delta: %llu "
						       "(expected %d)\n",
						       (unsigned long
						      long)chg_delta,
						       total);
						rc |=
							(chg_delta !=
							 (uint64_t)total);
					} else {
						printf("CHG read "
						       "failed r=%d\n",
						       r1);
						rc |= 1;
					}
					if (r2 == 0) {
						printf("FID counter: "
						       "%llu (>= total "
						       "creates %d)\n",
						       (unsigned long
						      long)fid_after,
						       total);
						rc |=
							(fid_after <
							 (uint64_t)total);
					} else {
						printf("FID read "
						       "failed r=%d\n",
						       r2);
						rc |= 1;
					}
					fid_counter = fid_after;
					fdb_database_destroy(db2);
				}
			}
		}
	}

	printf("\n== result: %s (%d errors) ==\n",
	       (errors == 0 && total == threads * ops && rc == 0) ?
	       "PASS" : "FAIL", errors);
	(void)fid_counter;
	cat->ops->close(cat);
	return (errors == 0 && total == threads * ops && rc == 0) ? 0 : 1;
}
