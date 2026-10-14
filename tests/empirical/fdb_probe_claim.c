/* probe_claim.c -- isolate the FID claim read-back behavior.
 *
 * Q1: is a seeded counter key visible to a plain point read?
 * Q2: in ONE transaction, after atomic ADD on a key, what does a
 *     read-your-writes get on the SAME key return? (post-ADD value,
 *     pre-ADD value, or absent?)
 * Q3: does the ADD actually commit?
 * Q4: alternate pattern (get FIRST, then ADD): does the pre-read
 *     make the read-back see the op?
 *
 * Build (node 44): gcc -O2 -I/usr/include/foundationdb probe_claim.c
 *                   -lfdb_c -lpthread -lm -o probe_claim
 */
#define FDB_API_VERSION 740
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

static void *net_thread(void *a) { (void)a; fdb_run_network(); return NULL; }

static uint8_t K[] = {0x00, 'F', 'I', 'D'}; /* same key as the backend */

static void do_set(FDBDatabase *db, int64_t val)
{
	FDBTransaction *tr = NULL;
	fdb_database_create_transaction(db, &tr);
	uint8_t b[8], i;
	for (i = 0; i < 8; i++) b[i] = (uint8_t)((uint64_t)val >> (8*i));
	fdb_transaction_set(tr, K, 4, b, 8);
	FDBFuture *f = fdb_transaction_commit(tr);
	fdb_future_block_until_ready(f);
	printf("  set FID=%lld commit_err=%d\n", (long long)val,
	       (int)fdb_future_get_error(f));
	fdb_future_destroy(f);
	fdb_transaction_destroy(tr);
}

static void point_read(FDBDatabase *db, const char *label)
{
	FDBTransaction *tr = NULL;
	fdb_database_create_transaction(db, &tr);
	FDBFuture *f = fdb_transaction_get(tr, K, 4, 0);
	fdb_future_block_until_ready(f);
	int ge = (int)fdb_future_get_error(f);
	printf("  [%s] err=%d", label, ge);
	if (ge == 0) {
		fdb_bool_t p = 0;
		const uint8_t *v = NULL;
		int vl = 0;
		if (fdb_future_get_value(f, &p, &v, &vl) == 0 && p && vl > 0) {
			printf(" val=0x");
			int i; for (i = vl-1; i >= 0; i--) printf("%02x", v[i]);
			printf(" (%d)", vl);
		} else {
			printf(" ABSENT");
		}
	}
	printf("\n");
	fdb_future_destroy(f);
	fdb_transaction_destroy(tr);
}

/* mode 1: ADD then read-back (current claim pattern)
 * mode 2: read then ADD then read-back (standard counter pattern) */
static void claim_tx(FDBDatabase *db, const char *label, int read_first)
{
	uint8_t op[8] = { 0x00, 0x04, 0, 0, 0, 0, 0, 0 }; /* +1024 LE */
	FDBTransaction *tr = NULL;
	fdb_database_create_transaction(db, &tr);
	fdb_bool_t pre = 0; const uint8_t *pv = NULL; int plv = 0;
	int64_t preval = -1;

	if (read_first) {
		FDBFuture *g = fdb_transaction_get(tr, K, 4, 0);
		fdb_future_block_until_ready(g);
		if (fdb_future_get_error(g) == 0)
			fdb_future_get_value(g, &pre, &pv, &plv);
		if (pre && plv == 8) {
			preval = 0;
			int i;
			for (i = 0; i < 8; i++)
				preval |= (int64_t)pv[i] << (8*i);
		}
		fdb_future_destroy(g);
	}
	fdb_transaction_atomic_op(tr, K, 4, op, 8, FDB_MUTATION_TYPE_ADD);
	FDBFuture *f = fdb_transaction_get(tr, K, 4, 0);
	fdb_future_block_until_ready(f);
	int ge = (int)fdb_future_get_error(f);
	printf("  [%s] readback err=%d", label, ge);
	if (ge == 0) {
		fdb_bool_t p = 0;
		const uint8_t *v = NULL;
		int vl = 0;
		if (fdb_future_get_value(f, &p, &v, &vl) == 0 && p && vl > 0) {
			int64_t x = 0;
			int i;
			for (i = 0; i < vl; i++) x |= (int64_t)v[i] << (8*i);
			printf(" val=%lld", (long long)x);
			if (read_first)
				printf(" (pre=%lld expected=%lld)",
				       (long long)preval,
				       (long long)(preval + 1024));
		} else {
			printf(" ABSENT");
		}
	}
	FDBFuture *c = fdb_transaction_commit(tr);
	fdb_future_block_until_ready(c);
	printf(" commit_err=%d\n", (int)fdb_future_get_error(c));
	fdb_future_destroy(f);
	fdb_future_destroy(c);
	fdb_transaction_destroy(tr);
}

int main(int argc, char **argv)
{
	const char *cf = (argc > 1) ? argv[1] : "/etc/foundationdb/fdb.cluster";
	fdb_error_t e;
	FDBDatabase *db = NULL;

	e = fdb_select_api_version(FDB_API_VERSION);
	if (e) { printf("select %d\n", (int)e); return 1; }
	e = fdb_setup_network();
	if (e) { printf("setup %d\n", (int)e); return 1; }
	pthread_t th;
	pthread_create(&th, NULL, net_thread, NULL);
	e = fdb_create_database(cf, &db);
	if (e) { printf("create_db %d: %s\n", (int)e, fdb_get_error(e));
		 return 1; }

	printf("A. seed + verify\n");
	do_set(db, 2);
	point_read(db, "A.1 point read after seed (want 2)");

	printf("B. tx: ADD 1024 then read-back (current claim pattern)\n");
	claim_tx(db, "B.1", 0);
	point_read(db, "B.2 committed value (want 1026)");

	printf("C. tx: read FIRST, ADD, read-back (standard pattern)\n");
	claim_tx(db, "C.1", 1);
	point_read(db, "C.2 committed value (want 2050)");

	printf("D. repeat B pattern (stability)\n");
	claim_tx(db, "D.1", 0);
	point_read(db, "D.2 committed (want 3074)");

	return 0;
}
