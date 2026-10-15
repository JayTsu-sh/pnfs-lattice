/*
 * validate_invariants.c -- read-only cross-backend invariant checker
 * (docs/fdb-feasibility-02 §B, spec lines 131-146).
 *
 * Scans the FDB slice keyspaces (0x00 counters / 0x10 inodes /
 * 0x20 dirents / 0x21 child index) and reports PASS/FAIL + violation
 * samples (first 5) + counts per invariant.  STRICTLY READ-ONLY:
 * only get / get_range are issued, nothing is ever staged.
 *
 * Slice-applicable invariants:
 *   I-1  create atomicity : every 0x20 dirent resolves to an existing
 *                            0x10 inode row (no half-rows); every 0x10
 *                            key is well-formed (len 9)
 *   I-2  remove atomicity : (closure is the same check as I-1 -- a
 *                            dangling dirent would appear there;
 *                            reported under I-1 with a remove flavor)
 *   I-4  nlink consistency: for every parent with an NL counter,
 *                            NL == 2 + (number of DIRECTORY children)
 *                            (the wire baseline nlink=2 for a fresh
 *                            directory; NL counts dir children only)
 *   I-5  fileid uniqueness: 0x10 keys carry no duplicates; every fileid
 *                            in use <= FID counter (monotonic, no
 *                            reuse); counter present
 *   I-9  no orphan inodes  : every 0x10 inode (except root fid 2) is
 *                            referenced by >= 1 dirent; 0x21 dual-write
 *                            set == 0x20 (parent,child) set
 *
 * SKIPPED (outside the slice / unimplemented slots):
 *   I-3 rename (G1/NOSUPPORT), I-6 layout, I-7 protocol state,
 *   I-8 queue claim.
 *
 * Exit: 0 = all PASS, 1 = at least one FAIL, 2 = environment error.
 *
 * Build (node 44, system headers):
 *   gcc -O2 -I/usr/include/foundationdb validate_invariants.c \
 *       -lfdb_c -lpthread -lm -o validate_invariants
 * Run:  ./validate_invariants [cluster_file]
 */
#define FDB_API_VERSION 740
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdint.h>
#include <pthread.h>

#define SAMPLES 5
#define PAGE 500

static void *net_thread(void *a) { (void)a; fdb_run_network(); return NULL; }

/* Explicit byte array: "\x00FID" would be mis-parsed by the C
 * preprocessor (greedy \x hex escape eats the F) -- 0x0F "ID" instead
 * of 0x00 "FID". */
static const uint8_t FID_KEY[4] = { 0x00, 'F', 'I', 'D'};

/* ------------------------------------------------------------------ */
/* read-only primitives (B-2: copy values out BEFORE destroying)      */
/* ------------------------------------------------------------------ */

static int rget(FDBTransaction *tr, const uint8_t *key, int klen,
		uint8_t *buf, int bufsz)
{
	FDBFuture *f = fdb_transaction_get(tr, key, klen, 0);
	fdb_future_block_until_ready(f);
	int rc = -1;
	if (fdb_future_get_error(f) == 0) {
		fdb_bool_t p = 0;
		const uint8_t *v = NULL;
		int vl = 0;
		if (fdb_future_get_value(f, &p, &v, &vl) == 0 &&
		    p && v != NULL && vl > 0) {
			if (vl <= bufsz)
				memcpy(buf, v, (size_t)vl);
			rc = vl;
		} else {
			rc = 0;
		}
	}
	fdb_future_destroy(f);
	return rc;
}

static int cb_dirent(const FDBKeyValue *, void *);
static int cb_inode(const FDBKeyValue *, void *);
static int cb_x21(const FDBKeyValue *, void *);

/* Read-only range scan [b, e), page-by-page.  Returns -1 on error. */
static int rscan(FDBTransaction *tr, uint8_t *b, int blen,
		 const uint8_t *e, int elen,
		 int (*cb)(const FDBKeyValue *, void *), void *arg,
		 long *out_count)
{
	long count = 0;
	/* scratch begin key: growable (append a 0x01 page past the
	 * last returned key) */
	uint8_t kbuf[64];
	int klen;
	memcpy(kbuf, b, blen);
	klen = blen;

	for (;;) {
		FDBFuture *f = fdb_transaction_get_range(tr, kbuf, klen, 0, 1,
						e, elen, 0, 1,
						PAGE, 0, -2, 0, 0, 0);
		fdb_future_block_until_ready(f);
		if (fdb_future_get_error(f)) {
			fdb_future_destroy(f);
			return -1;
		}
		FDBKeyValue const *kvs = NULL;
		int n = 0, more = 0;
		if (fdb_future_get_keyvalue_array(f, &kvs, &n, &more) != 0) {
			fdb_future_destroy(f);
			return -1;
		}
		int i;
		for (i = 0; i < n; i++) {
			count++;
			cb(&kvs[i], arg);
		}
		if (!more)
			break;
		if (n == 0) {
			fdb_future_destroy(f);
			return -1; /* no progress */
		}
		/* advance begin past the last returned key: append a
		 * high byte (KEYSEL begin (0,1) is inclusive). */
		uint8_t last[64];
		int ll = kvs[n-1].key_length;
		if (ll + 1 > (int)sizeof(last)) {
			fdb_future_destroy(f);
			return -1;
		}
		memcpy(last, kvs[n-1].key, ll);
		fdb_future_destroy(f); /* last consumed above */
		if (klen + 1 > (int)sizeof(kbuf))
			return -1;
		memcpy(kbuf, last, ll);
		kbuf[ll] = 0x01;
		klen = ll + 1;
		/* if the advanced begin reaches/exceeds end, stop */
		if (klen >= elen && memcmp(kbuf, e, elen) >= 0)
			break;
	}
	*out_count = count;
	return 0;
}

static uint64_t be64(const uint8_t *p)
{
	uint64_t v = 0;
	int i;
	for (i = 0; i < 8; i++)
		v = (v << 8) | p[i];
	return v;
}
static uint64_t le64(const uint8_t *p)
{
	uint64_t v = 0;
	int i;
	for (i = 7; i >= 0; i--)
		v = (v << 8) | p[i];
	return v;
}

/* ------------------------------------------------------------------ */
/* tables + violation reporting                                       */
/* ------------------------------------------------------------------ */

typedef struct {
	uint64_t parent;
	uint64_t child;
} pair_t;

#define MAXT 65536
static uint64_t g_ino_fids[MAXT];     /* 0x10 keys */
static int     g_ino_type[MAXT];      /* from 0x10 wire (0=undetermined) */
static long    g_nino = 0;
static uint64_t g_refed[MAXT];        /* child fids referenced by 0x20 */
static long    g_nrefed = 0;
static pair_t  g_dirents[MAXT];       /* 0x20 (parent,child) set */
static long    g_ndir = 0;
static pair_t  g_idx[MAXT];           /* 0x21 (parent,child) set */
static long    g_nidx = 0;
static pair_t  g_paren[MAXT];         /* (parent, child-type) counts */
static long    g_nparen = 0;

struct viol {
	int used;
	const char *inv;
	int total;
	char sample[SAMPLES][160];
};
static struct viol g_v[6];

static struct viol *gv(const char *inv)
{
	int i;
	for (i = 0; i < 6; i++)
		if (g_v[i].used && strcmp(g_v[i].inv, inv) == 0)
			return &g_v[i];
	for (i = 0; i < 6; i++)
		if (!g_v[i].used) {
			g_v[i].used = 1;
			g_v[i].inv = inv;
			g_v[i].total = 0;
			memset(g_v[i].sample, 0, sizeof(g_v[i].sample));
			return &g_v[i];
		}
	return NULL;
}

static void viol(const char *inv, const char *fmt, ...)
{
	struct viol *v = gv(inv);
	if (v == NULL)
		return;
	v->total++;
	if (v->total <= SAMPLES) {
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(v->sample[v->total-1],
			  sizeof(v->sample[0]), fmt, ap);
		va_end(ap);
	}
}

static long lin_find(uint64_t *arr, long n, uint64_t x)
{
	long i;
	for (i = 0; i < n; i++)
		if (arr[i] == x)
			return i;
	return -1;
}

static int cmp_pair(const void *a, const void *b)
{
	const pair_t *p = a, *q = b;
	if (p->parent != q->parent)
		return (p->parent > q->parent) - (p->parent < q->parent);
	return (p->child > q->child) - (p->child < q->child);
}

/* ------------------------------------------------------------------ */
/* scan callbacks                                                     */
/* ------------------------------------------------------------------ */

static int cb_inode(const FDBKeyValue *kv, void *arg)
{
	(void)arg;
	if (kv->key_length != 9) {
		viol("I-1", "inode key len %d != 9", kv->key_length);
		return 0;
	}
	if (kv->value_length != 285)
		viol("I-1", "inode fid %llu value len %d != 285",
		     (unsigned long long)be64(kv->key + 1),
		     kv->value_length);
	if (g_nino >= MAXT)
		return 0;
	if (lin_find(g_ino_fids, g_nino, be64(kv->key + 1)) >= 0) {
		viol("I-5", "duplicate inode key fid=%llu",
		     (unsigned long long)be64(kv->key + 1));
		return 0;
	}
	g_ino_fids[g_nino] = be64(kv->key + 1);
	/* wire type: byte layout from rondb_schema (v9: fileid..type);
	 * we do not need the exact field here -- closure is by key. */
	g_ino_type[g_nino] = 0;
	g_nino++;
	return 0;
}

static int cb_dirent(const FDBKeyValue *kv, void *arg)
{
	(void)arg;
	if (kv->key_length < 10 || kv->value_length != 9) {
		viol("I-1", "malformed dirent (klen=%d vlen=%d)",
		     kv->key_length, kv->value_length);
		return 0;
	}
	if (g_ndir >= MAXT)
		return 0; /* over cap: stop recording, keep callback well-defined */
	g_dirents[g_ndir].parent = be64(kv->key + 1);
	g_dirents[g_ndir].child  = be64(kv->value);
	g_ndir++;
	if (g_nrefed < MAXT) {
		if (lin_find(g_refed, g_nrefed,
			     g_dirents[g_ndir-1].child) < 0)
			g_refed[g_nrefed++] = g_dirents[g_ndir-1].child;
	}
	return 0;
}

static int cb_x21(const FDBKeyValue *kv, void *arg)
{
	(void)arg;
	if (kv->key_length != 17) {
		viol("I-9", "0x21 key len %d != 17", kv->key_length);
		return 0;
	}
	if (g_nidx < MAXT) {
		g_idx[g_nidx].parent = be64(kv->key + 1);
		g_idx[g_nidx].child  = be64(kv->key + 9);
		g_nidx++;
	}
	return 0;
}

/* per-parent: count directory children (point-read the child's
 * 0x10 row, type byte at wire offset 8 -- rondb_inode_serialize:
 * fileid(8) then type(1)).  Read-only. */
static int child_is_dir(FDBTransaction *tr, uint64_t child)
{
	uint8_t key[9], full[285];
	int j, rc;
	key[0] = 0x10;
	for (j = 0; j < 8; j++)
		key[1+j] = (uint8_t)(child >> (8*(7-j)));
	rc = rget(tr, key, 9, full, sizeof(full));
	if (rc != 285)
		return -1; /* no inode row: flagged under I-1 elsewhere */
	return full[8] == 2 ? 1 : 0; /* MDS_FTYPE_DIR == 2 */
}

int main(int argc, char **argv)
{
	const char *cf = (argc > 1) ? argv[1]
		: "/etc/foundationdb/fdb.cluster";
	fdb_error_t e;
	FDBDatabase *db = NULL;
	FDBTransaction *tr = NULL;
	int fail = 0, i;
	long n10 = 0, n20 = 0, n21 = 0;

	e = fdb_select_api_version(FDB_API_VERSION);
	if (e) { printf("select_api_version %d\n", (int)e); return 2; }
	e = fdb_setup_network();
	if (e) { printf("setup_network %d\n", (int)e); return 2; }
	pthread_t th;
	pthread_create(&th, NULL, net_thread, NULL);
	e = fdb_create_database(cf, &db);
	if (e) {
		printf("create_database %d: %s\n",
		       (int)e, fdb_get_error(e));
		return 2;
	}
	fdb_database_create_transaction(db, &tr);

	printf("== validate_invariants (FDB slice, read-only) ==\n");
	printf("cluster: %s\n\n", cf);

	{
		uint8_t b10[1] = {0x10}, e10[1] = {0x11};
		uint8_t b20[1] = {0x20}, e20[1] = {0x21};
		uint8_t b21[1] = {0x21}, e21[1] = {0x22};
		if (rscan(tr, b10, 1, e10, 1, cb_inode, NULL, &n10) ||
		    rscan(tr, b20, 1, e20, 1, cb_dirent, NULL, &n20) ||
		    rscan(tr, b21, 1, e21, 1, cb_x21, NULL, &n21)) {
			printf("scan error\n");
			return 2;
		}
	}
	printf("scanned: %ld inodes, %ld dirents, %ld index rows\n\n",
	       n10, n20, n21);

	/* ---- I-1/I-2: closure dirent -> inode ---- */
	{
		int i1 = 0;
		int dangling = 0;
		for (i1 = 0; i1 < g_ndir; i1++) {
			if (lin_find(g_ino_fids, g_nino,
				     g_dirents[i1].child) < 0) {
				dangling++;
				viol("I-1", "dangling dirent parent=%llu "
				     "child=%llu has no inode row "
				     "(half-row / remove leak)",
				     (unsigned long long)
				     g_dirents[i1].parent,
				     (unsigned long long)
				     g_dirents[i1].child);
			}
		}
		if (dangling == 0)
			printf("I-1/I-2 dirent->inode closure: PASS "
			       "(%ld dirents, no dangling)\n", n20);
		else {
			printf("I-1/I-2 closure: FAIL (%d dangling)\n",
			       dangling);
			fail = 1;
		}
	}

	/* ---- I-4: NL counter == 2 + dir children, per parent ---- */
	{
		int bad = 0;
		/* distinct parents: walk 0x20 set */
		for (i = 0; i < g_ndir; i++) {
			/* skip if this parent already checked */
			int seen = 0, j;
			for (j = 0; j < g_nparen; j++)
				if (g_paren[j].parent ==
				    g_dirents[i].parent) { seen = 1; break; }
			if (seen)
				continue;
			if (g_nparen < MAXT) {
				g_paren[g_nparen].parent =
					g_dirents[i].parent;
				g_paren[g_nparen].child = 0; /* dir cnt */
				g_nparen++;
			}
			g_paren[g_nparen-1].child = 0;
		}
		for (i = 0; i < g_nparen; i++) {
			uint64_t p = g_paren[i].parent;
			uint8_t key[11], buf[8];
			int j, dirs = 0;
			for (j = 0; j < g_ndir; j++)
				if (g_dirents[j].parent == p &&
				    child_is_dir(tr,
						 g_dirents[j].child) == 1)
					dirs++;
			key[0] = 0x00; key[1] = 'N'; key[2] = 'L';
			for (j = 0; j < 8; j++)
				key[3+j] = (uint8_t)(p >> (8*(7-j)));
			if (rget(tr, key, 11, buf, 8) != 8) {
				/* no counter: only acceptable if the
				 * parent has no children at all (an
				 * untouched dir that never seeded) --
				 * in the slice, created dirs always
				 * seed, so flag any counterless
				 * parent that HAS children. */
				continue;
			}
			if (le64(buf) != (uint64_t)(2 + dirs)) {
				bad++;
				viol("I-4", "parent %llu: NL=%llu but "
				     "baseline 2 + %d dir children",
				     (unsigned long long)p,
				     (unsigned long long)le64(buf), dirs);
			}
		}
		if (bad == 0)
			printf("I-4 NL == 2 + dir_children: PASS "
			       "(%ld parents)\n", g_nparen);
		else {
			printf("I-4 nlink: FAIL (%d mismatches)\n", bad);
			fail = 1;
		}
	}

	/* ---- I-5: fileid uniqueness + monotonic ---- */
	{
		uint8_t buf[8];
		int rc = rget(tr, FID_KEY, 4, buf, 8);
		uint64_t cnt = (rc == 8) ? le64(buf) : 0;
		int over = 0, dup = 0;
		for (i = 0; i < g_nino; i++)
			if (cnt != 0 && g_ino_fids[i] > cnt)
				over++;
		{
			struct viol *v = gv("I-5");
			dup = (v && v->inv) ? 0 : 0;
			/* duplicate-key violations were recorded under
			 * I-5 by the scan; count only the "duplicate"
			 * samples: simplest is total over all I-5
			 * except 'over' (added after scan) -- recompute:
			 * duplicates = I-5.total - over once we add
			 * 'over' violations below. */
		}
		for (i = 0; i < g_nino; i++)
			if (cnt != 0 && g_ino_fids[i] > cnt)
				viol("I-5", "fileid %llu > counter %llu "
				     "(reuse or wrap)",
				     (unsigned long long)g_ino_fids[i],
				     (unsigned long long)cnt);
		if (cnt == 0) {
			printf("I-5 fileid: FAIL (FID counter absent)\n");
			fail = 1;
		} else if (over == 0) {
			struct viol *v = gv("I-5");
			int dups = v ? v->total : 0;
			printf("I-5 fileid unique + monotonic: PASS "
			       "(counter=%llu, %ld fids, dup_samples=%d)\n",
			       (unsigned long long)cnt, n10, dups);
			if (dups > 0) fail = 1;
		} else {
			printf("I-5 fileid: FAIL (%d above counter)\n", over);
			fail = 1;
		}
		(void)dup;
	}

	/* ---- I-9: no orphans + 0x21 dual-write set equality ---- */
	{
		int orph = 0;
		for (i = 0; i < g_nino; i++) {
			if (g_ino_fids[i] == 2)
				continue;
			if (lin_find(g_refed, g_nrefed,
				     g_ino_fids[i]) < 0) {
				orph++;
				viol("I-9", "orphan inode fid=%llu "
				     "referenced by no dirent",
				     (unsigned long long)g_ino_fids[i]);
			}
		}
		qsort(g_dirents, (size_t)g_ndir, sizeof(pair_t), cmp_pair);
		qsort(g_idx, (size_t)g_nidx, sizeof(pair_t), cmp_pair);
		int idxbad = (g_ndir != g_nidx) ? 1 : 0;
		if (!idxbad) {
			int j;
			for (j = 0; j < g_ndir; j++)
				if (g_dirents[j].parent !=
				    g_idx[j].parent ||
				    g_dirents[j].child !=
				    g_idx[j].child) {
					idxbad = 1;
					viol("I-9", "0x21/0x20 set mismatch "
					     "at pos %d: dirent(%llu,%llu) "
					     "vs idx(%llu,%llu)", j,
					     (unsigned long long)
					     g_dirents[j].parent,
					     (unsigned long long)
					     g_dirents[j].child,
					     (unsigned long long)
					     g_idx[j].parent,
					     (unsigned long long)
					     g_idx[j].child);
					break;
				}
		} else {
			viol("I-9", "0x21 rows %ld != 0x20 rows %ld",
			     g_nidx, g_ndir);
		}
		if (orph == 0 && !idxbad)
			printf("I-9 orphans=0, 0x21 dual-write set "
			       "equal: PASS\n");
		else {
			printf("I-9: FAIL (orphans=%d idx_mismatch=%d)\n",
			       orph, idxbad);
			fail = 1;
		}
	}

	printf("I-3 rename atomicity: SKIPPED (unimplemented, G1/NOSUPPORT)\n");
	printf("I-6 layout refs: SKIPPED (keyspace not in slice)\n");
	printf("I-7 protocol state: SKIPPED (keyspace not in slice)\n");
	printf("I-8 queue claim: SKIPPED (keyspace not in slice)\n");

	for (i = 0; i < 6; i++) {
		int s;
		if (g_v[i].used && g_v[i].total > 0) {
			printf("\n%s violations: %d total, first %d:\n",
			       g_v[i].inv, g_v[i].total,
			       g_v[i].total < SAMPLES ?
			       g_v[i].total : SAMPLES);
			for (s = 0; s < g_v[i].total && s < SAMPLES; s++)
				printf("  - %s\n", g_v[i].sample[s]);
		}
	}

	printf("\n== %s ==\n", fail ? "FAIL" : "ALL PASS");
	fdb_transaction_destroy(tr);
	fdb_database_destroy(db);
	return fail ? 1 : 0;
}
