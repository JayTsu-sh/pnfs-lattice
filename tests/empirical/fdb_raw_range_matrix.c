/* raw_range_test3.c -- characterize the B-1 range shift precisely.
 * Same 4 keys (A01,A02,B03,B04).  Probes:
 *  P1  limit=1      [A,B)   -> 1 key: which one?
 *  P2  full range   [A,C)   -> 4 keys expected; how many, which?
 *  P3  reverse      [A,C)   -> order check
 *  P4  [B,C) limit=1         -> 1 key: which one?
 *  P5  [A,C) WANT_ALL again  -> repeat for determinism
 * Prints every key so the shift pattern is unambiguous.
 */
#define FDB_API_VERSION 740
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

static void *net_thread(void *a) { (void)a; fdb_run_network(); return NULL; }

static void probe(FDBDatabase *db, const char *label,
		  const uint8_t *b, int blen,
		  const uint8_t *e, int elen,
		  int limit, int reverse)
{
	FDBTransaction *tr = NULL;
	fdb_database_create_transaction(db, &tr);
	FDBFuture *f = fdb_transaction_get_range(tr, b, blen, 1, 0,
						e, elen, 0, 0,
						limit, 0, -2, 0, 0,
						(fdb_bool_t)reverse);
	fdb_future_block_until_ready(f);
	int ge = (int)fdb_future_get_error(f);
	printf("  [%s] get_err=%d", label, ge);
	if (ge == 0) {
		FDBKeyValue const *kvs = NULL;
		int count = 0, more = 0, i;
		if (fdb_future_get_keyvalue_array(f, &kvs, &count, &more) == 0) {
			printf(" count=%d more=%d keys:", count, more);
			for (i = 0; i < count; i++) {
				printf(" [");
				int j;
				for (j = 0; j < kvs[i].key_length; j++)
					printf("%02x", kvs[i].key[j]);
				printf("]");
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

	uint8_t kA1[] = {'A', 1}, kA2[] = {'A', 2};
	uint8_t kB1[] = {'B', 3}, kB2[] = {'B', 4};
	uint8_t bA[] = {'A'}, bB[] = {'B'}, bC[] = {'C'};
	uint8_t v1[] = {0xDE, 0xAD}, v2[] = {0xBE, 0xEF};
	uint8_t v3[] = {0x11, 0x22}, v4[] = {0x33, 0x44};

	FDBTransaction *tr = NULL;
	fdb_database_create_transaction(db, &tr);
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

	probe(db, "P1 [A,B) limit=1",     bA, 1, bB, 1, 1, 0);
	probe(db, "P2 [A,C) limit=0",     bA, 1, bC, 1, 0, 0);
	probe(db, "P3 [A,C) reverse",     bA, 1, bC, 1, 0, 1);
	probe(db, "P4 [B,C) limit=1",     bB, 1, bC, 1, 1, 0);
	probe(db, "P5 [A,C) repeat",      bA, 1, bC, 1, 0, 0);

	return 0;
}
