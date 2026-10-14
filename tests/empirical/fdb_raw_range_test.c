/* raw_range_test.c -- zero shared code.  Minimal raw FDB range check.
 *
 * Sets 4 keys in 2 one-byte bands with a fresh transaction, commits,
 * then reads the two band ranges and prints every returned key in hex.
 * If the ranges come back exactly right, the lib/cluster range path is
 * sound and the anomaly lives in the harness context.  If they come
 * back shifted, the environment itself is broken (document as REAL).
 */
#define FDB_API_VERSION 740
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

static void *net_thread(void *a) { (void)a; fdb_run_network(); return NULL; }

static void hexdump(const char *label, const uint8_t *p, int n)
{
	int i;
	printf("  %s: ", label);
	for (i = 0; i < n; i++) printf("%02x", p[i]);
	printf("\n");
}

static void check_range(FDBDatabase *db, const char *label,
			const uint8_t *b, int blen,
			const uint8_t *e, int elen)
{
	FDBTransaction *tr = NULL;
	fdb_database_create_transaction(db, &tr);
	FDBFuture *f = fdb_transaction_get_range(tr, b, blen, 1, 0,
						e, elen, 0, 0,
						0, 0, -2, 0, 0, 0);
	fdb_future_block_until_ready(f);
	int ge = (int)fdb_future_get_error(f);
	printf("  [%s] get_err=%d", label, ge);
	if (ge == 0) {
		FDBKeyValue const *kvs = NULL;
		int count = 0, more = 0, i;
		if (fdb_future_get_keyvalue_array(f, &kvs, &count, &more) == 0) {
			printf(" count=%d more=%d", count, more);
			for (i = 0; i < count; i++) {
				printf("\n    key(%d)", kvs[i].key_length);
				int j;
				for (j = 0; j < kvs[i].key_length; j++)
					printf(" %02x", kvs[i].key[j]);
				printf("  val(%d)", kvs[i].value_length);
				for (j = 0; j < kvs[i].value_length; j++)
					printf(" %02x", kvs[i].value[j]);
			}
		}
	}
	printf("\n");
	fdb_future_destroy(f);
	fdb_transaction_destroy(tr);
}

int main(int argc, char **argv)
{
	const char *cf = (argc > 1) ? argv[1] : "/etc/foundationdb/fdb.cluster";
	fdb_error_t e;
	FDBDatabase *db = NULL;

	e = fdb_select_api_version(FDB_API_VERSION);
	if (e) { printf("select_api_version %d\n", (int)e); return 1; }
	e = fdb_setup_network();
	if (e) { printf("setup_network %d\n", (int)e); return 1; }
	pthread_t th;
	pthread_create(&th, NULL, net_thread, NULL);
	e = fdb_create_database(cf, &db);
	if (e) { printf("create_database %d: %s\n", (int)e, fdb_get_error(e));
		 return 1; }

	/* band 'A': A01, A02 ; band 'B': B03, B04 */
	uint8_t kA1[] = {'A', 1}, kA2[] = {'A', 2};
	uint8_t kB1[] = {'B', 3}, kB2[] = {'B', 4};
	uint8_t bA[] = {'A'}, bB[] = {'B'}, bC[] = {'C'};
	uint8_t v1[] = {0xDE, 0xAD}, v2[] = {0xBE, 0xEF};
	uint8_t v3[] = {0x11, 0x22}, v4[] = {0x33, 0x44};

	FDBTransaction *tr = NULL;
	fdb_database_create_transaction(db, &tr);
	/* clear old test keys so the test is repeatable */
	fdb_transaction_clear_range(tr, bA, 1, bC, 1);
	fdb_transaction_set(tr, kA1, 2, v1, 2);
	fdb_transaction_set(tr, kA2, 2, v2, 2);
	fdb_transaction_set(tr, kB1, 2, v3, 2);
	fdb_transaction_set(tr, kB2, 2, v4, 2);
	FDBFuture *cfut = fdb_transaction_commit(tr);
	fdb_future_block_until_ready(cfut);
	int ce = (int)fdb_future_get_error(cfut);
	printf("setup commit err=%d\n", ce);
	fdb_future_destroy(cfut);
	fdb_transaction_destroy(tr);
	if (ce) return 1;

	/* point reads as a control */
	tr = NULL;
	fdb_database_create_transaction(db, &tr);
	FDBFuture *pf = fdb_transaction_get(tr, kB1, 2, 0);
	fdb_future_block_until_ready(pf);
	fdb_bool_t present = 0; const uint8_t *pv = NULL; int pl = 0;
	fdb_future_get_value(pf, &present, &pv, &pl);
	printf("point read B03: present=%d len=%d\n", (int)present, pl);
	fdb_future_destroy(pf);
	fdb_transaction_destroy(tr);

	check_range(db, "range [A, B) expect A01 A02", bA, 1, bB, 1);
	check_range(db, "range [B, C) expect B03 B04", bB, 1, bC, 1);

	return 0;
}
