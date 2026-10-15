/* inject_violation.c -- negative-test helper for validate_invariants.
 * Sets the NL counter for parent 2 to 42 (expected 3 in the harness
 * end state: 2 + 1 subdir).  Run AFTER the harness, BEFORE the
 * validator; expect I-4 to FAIL with the sample naming parent 2.
 */
#define FDB_API_VERSION 740
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <pthread.h>

static void *net_thread(void *a) { (void)a; fdb_run_network(); return NULL; }

int main(int argc, char **argv)
{
	const char *cf = (argc > 1) ? argv[1] : "/etc/foundationdb/fdb.cluster";
	uint8_t key[11] = { 0x00, 'N', 'L',
			       0,0,0,0, 0,0,0, 2 };
	uint8_t val[8] = { 42,0,0,0, 0,0,0,0 };
	fdb_error_t e;
	FDBDatabase *db = NULL;
	e = fdb_select_api_version(FDB_API_VERSION);
	if (e) return 1;
	e = fdb_setup_network();
	if (e) return 1;
	pthread_t th;
	pthread_create(&th, NULL, net_thread, NULL);
	e = fdb_create_database(cf, &db);
	if (e) return 1;
	FDBTransaction *tr = NULL;
	fdb_database_create_transaction(db, &tr);
	fdb_transaction_set(tr, key, 11, val, 8);
	FDBFuture *f = fdb_transaction_commit(tr);
	fdb_future_block_until_ready(f);
	int err = (int)fdb_future_get_error(f);
	printf("inject NL[2]=42 commit_err=%d\n", err);
	fdb_future_destroy(f);
	fdb_transaction_destroy(tr);
	fdb_database_destroy(db);
	return err ? 1 : 0;
}
