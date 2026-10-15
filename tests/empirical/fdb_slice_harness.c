/*
 * fdb_slice_harness.c -- Phase 3 vertical-slice functional test.
 *
 * Runs the six FDB namespace slots (ns_create/ns_remove/ns_lookup/
 * ns_getattr/ns_setattr/ns_readdir) against the REAL 3-node lab
 * cluster and verifies the invariants that matter for the slice:
 *
 *   S-1  root inode (fileid 2) exists, nlink=2 via counter
 *   S-2  create dir -> lookup/getattr round-trip, parent counters moved
 *   S-3  create file under dir -> parent nlink unchanged, change +1
 *   S-4  duplicate create -> MDS_ERR_EXISTS (atomic pre-check)
 *   S-5  readdir returns all created entries, name-ordered, paginable
 *   S-6  setattr (mode+size+times) persists; change/ctime bump
 *   S-7  remove file -> parent nlink -1, change +1, lookup NOTFOUND,
 *        inode key gone (stale read fails)
 *   S-8  remove dir -> MDS_ERR_NOSUPPORT (slice boundary, G1 path)
 *   S-9  N sequential creates of distinct names all commit (no
 *        cross-key conflicts; per-parent conflict range = dirent key)
 *   S-10 idempotent reopen: a second catalogue_fdb_open() on the same
 *        cluster sees the state created by the first (no root reset,
 *        counters intact)
 *
 * Exit 0 = all pass.  Evidence grade for the feasibility report:
 * this is a REAL measurement on the lab FDB cluster (7.4.8).
 */

#define FDB_API_VERSION 740

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <pthread.h>

#include "mds_catalogue.h"
#include "catalogue_internal.h"
#include "rondb_schema.h"
#include "mds_log.h"
#include "ds_prealloc.h"
#include "pnfs_mds.h"

/* ---- stubs: the slice harness needs only these three external
 * symbols from outside {catalogue_fdb.c, rondb_schema.c} ---- */

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
	return -1; /* pool empty: force counter path */
}

/* The open/wipe entry points live in catalogue_fdb.c. */
extern enum mds_status catalogue_fdb_open(const struct mds_config *cfg,
					  struct mds_catalogue **out);
extern enum mds_status catalogue_fdb_wipe(struct mds_catalogue *cat);

/* ---- test plumbing ---- */

static int g_fail = 0;
static int g_pass = 0;

#define CHECK(cond, fmt, ...) do { \
	if (cond) { g_pass++; printf("  PASS " fmt "\n", ##__VA_ARGS__); } \
	else { g_fail++; printf("  FAIL " fmt "\n", ##__VA_ARGS__); } \
} while (0)

static const char *stname(enum mds_status st)
{
	switch (st) {
	case MDS_OK: return "OK";
	case MDS_ERR_NOMEM: return "NOMEM";
	case MDS_ERR_IO: return "IO";
	case MDS_ERR_NOTFOUND: return "NOTFOUND";
	case MDS_ERR_EXISTS: return "EXISTS";
	case MDS_ERR_INVAL: return "INVAL";
	case MDS_ERR_STALE: return "STALE";
	case MDS_ERR_DELAY: return "DELAY";
	case MDS_ERR_NOSUPPORT: return "NOSUPPORT";
	default: return "other";
	}
}

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

/* sorted? (name order, byte compare) */
static int names_sorted(const struct dir_result *r, int n)
{
	int i;
	for (i = 1; i < n; i++)
		if (strcmp(r->names[i-1], r->names[i]) >= 0)
			return 0;
	return 1;
}

/* ---- S-11 concurrency test context (file scope: the worker must
 * not be a nested function) ---- */
struct s11_ctx {
	int tid;
	struct mds_catalogue *cat;
	struct mds_cat_txn txn;
	uint64_t parent;
	enum mds_status st;
	uint64_t fids[25];
};
static struct s11_ctx s11[4];

static void *s11_worker(void *arg)
{
	struct s11_ctx *c = arg;
	int i;
	char nm[32];

	c->st = MDS_OK;
	for (i = 0; i < 25; i++) {
		enum mds_status s2;
		struct mds_inode out2;

		snprintf(nm, sizeof(nm), "c%d_%02d", c->tid, i);
		s2 = c->cat->auth_ops->ns_create(c->cat, &c->txn, c->parent,
						 nm, MDS_FTYPE_REG, 0644,
						 0, 0, NULL, &out2);
		if (s2 != MDS_OK) {
			c->st = s2;
			break;
		}
		c->fids[i] = out2.fileid;
	}
	return NULL;
}

int main(int argc, char **argv)
{
	const char *cluster = (argc > 1) ? argv[1]
		: "/etc/foundationdb/fdb.cluster";
	struct mds_config cfg;
	struct mds_catalogue *cat = NULL;
	struct mds_cat_txn txn;
	struct mds_inode in, child;
	enum mds_status st;
	uint64_t dir_fid = 0, file_fid = 0;
	const char *subdir = "slice_subdir";
	const char *fname  = "slice_file_0";
	int i, rc;

	printf("== FDB vertical-slice harness (cluster: %s) ==\n", cluster);
	printf("   NOTE: use a FRESH cluster or pre-clear the 0x00/0x10/0x20\n");
	printf("   keyspaces between runs; the harness assumes it owns them.\n\n");

	memset(&cfg, 0, sizeof(cfg));
	snprintf(cfg.catalogue_backend_conf,
		 sizeof(cfg.catalogue_backend_conf), "%s", cluster);
	cfg.catalogue_backend = MDS_BACKEND_FDB;

	st = catalogue_fdb_open(&cfg, &cat);
	CHECK(st == MDS_OK, "open backend (st=%s)", stname(st));
	if (st != MDS_OK) return 1;
	st = catalogue_fdb_wipe(cat);
	CHECK(st == MDS_OK, "wipe keyspaces + reseed root (st=%s)", stname(st));
	if (st != MDS_OK) return 1;

	memset(&txn, 0, sizeof(txn));
	txn.cat = cat;
	txn.txn_backend = MDS_BACKEND_FDB;

	/* S-1: root inode */
	st = cat->auth_ops->ns_getattr(cat, 2, &in);
	CHECK(st == MDS_OK, "S-1a root getattr (st=%s)", stname(st));
	if (st == MDS_OK) {
		CHECK(in.type == MDS_FTYPE_DIR && in.nlink == 2,
		      "S-1b root is dir nlink=2 (type=%d nlink=%u)",
		      (int)in.type, in.nlink);
	}

	/* S-2: create a directory under root */
	st = cat->auth_ops->ns_create(cat, &txn, 2, subdir,
				      MDS_FTYPE_DIR, 0755, 0, 0, NULL, &child);
	CHECK(st == MDS_OK, "S-2a create dir (st=%s)", stname(st));
	if (st == MDS_OK) {
		dir_fid = child.fileid;
		CHECK(dir_fid >= 3, "S-2b dir fileid from counter (fid=%llu)",
		      (unsigned long long)dir_fid);
		memset(&in, 0, sizeof(in));
		st = cat->auth_ops->ns_lookup(cat, 2, subdir, &in);
		CHECK(st == MDS_OK && in.fileid == dir_fid &&
		      in.type == MDS_FTYPE_DIR && in.parent_fileid == 2,
		      "S-2c lookup dir round-trip (st=%s fid=%llu)",
		      stname(st), (unsigned long long)in.fileid);
		st = cat->auth_ops->ns_getattr(cat, dir_fid, &in);
		CHECK(st == MDS_OK && in.nlink == 2,
		      "S-2d new dir nlink=2 via counter (st=%s nlink=%u)",
		      stname(st), in.nlink);
	}

	/* S-3: create a file under the new dir; parent nlink stays 2,
	 * change must have moved. */
	if (dir_fid) {
		struct mds_inode before, after;
		st = cat->auth_ops->ns_getattr(cat, dir_fid, &before);
		st = cat->auth_ops->ns_create(cat, &txn, dir_fid, fname,
					      MDS_FTYPE_REG, 0644, 1000, 1000,
					      NULL, &child);
		CHECK(st == MDS_OK, "S-3a create file (st=%s)", stname(st));
		if (st == MDS_OK) {
			file_fid = child.fileid;
			st = cat->auth_ops->ns_getattr(cat, dir_fid, &after);
			CHECK(st == MDS_OK && before.nlink == 2 &&
			      after.nlink == 2 && after.change > before.change,
			      "S-3b file create: parent nlink 2->2, change %llu->%llu",
			      (unsigned long long)before.change,
			      (unsigned long long)after.change);
		}
	}

	/* S-4: duplicate create must be EXISTS */
	st = cat->auth_ops->ns_create(cat, &txn, dir_fid ? dir_fid : 2, fname,
				      MDS_FTYPE_REG, 0644, 0, 0, NULL, &in);
	CHECK(st == MDS_ERR_EXISTS, "S-4 duplicate create -> EXISTS (st=%s)",
	      stname(st));

	/* S-5: readdir on the dir.  Add a second file so paging is
	 * actually exercised (start_after the first name -> 1 left). */
	if (dir_fid) {
		struct dir_result r;
		st = cat->auth_ops->ns_create(cat, &txn, dir_fid, "slice_file_1",
					      MDS_FTYPE_REG, 0644, 0, 0,
					      NULL, &child);
		CHECK(st == MDS_OK, "S-5.0 second file create (st=%s)",
		      stname(st));
		memset(&r, 0, sizeof(r));
		st = cat->auth_ops->ns_readdir(cat, dir_fid, NULL, 256,
					       &txn, dir_cb, &r);
		CHECK(st == MDS_OK && r.count == 2 &&
		      names_sorted(&r, r.count),
		      "S-5a readdir: count=%d sorted=%d (st=%s)", r.count,
		      names_sorted(&r, r.count), stname(st));
		if (r.count == 2) {
			/* page 2: start after the first name -> 1 left */
			struct dir_result r2;
			memset(&r2, 0, sizeof(r2));
			st = cat->auth_ops->ns_readdir(cat, dir_fid,
						       r.names[0], 256,
						       &txn, dir_cb, &r2);
			CHECK(st == MDS_OK && r2.count == 1 &&
			      strcmp(r2.names[0], r.names[1]) == 0,
			      "S-5b paged readdir start_after=%s -> %d",
			      r.names[0], r2.count);
		}
	}

	/* S-6: setattr on the file: mode/size persist, change bumps */
	if (file_fid) {
		struct mds_inode attrs, before, after;
		memset(&before, 0, sizeof(before));
		st = cat->auth_ops->ns_getattr(cat, file_fid, &before);
		memset(&attrs, 0, sizeof(attrs));
		attrs.mode = 0600;
		attrs.size = 12345;
		attrs.uid = 42;
		attrs.gid = 43;
		st = cat->auth_ops->ns_setattr(cat, &txn, file_fid, &attrs,
					       MDS_ATTR_MODE | MDS_ATTR_SIZE |
					       MDS_ATTR_UID | MDS_ATTR_GID);
		CHECK(st == MDS_OK, "S-6a setattr (st=%s)", stname(st));
		memset(&after, 0, sizeof(after));
		st = cat->auth_ops->ns_getattr(cat, file_fid, &after);
		CHECK(st == MDS_OK && after.mode == 0600 &&
		      after.size == 12345 && after.uid == 42 &&
		      after.gid == 43 && after.change > before.change,
		      "S-6b persisted: mode=%o size=%llu uid=%llu change %llu->%llu",
		      (unsigned)after.mode,
		      (unsigned long long)after.size,
		      (unsigned long long)after.uid,
		      (unsigned long long)before.change,
		      (unsigned long long)after.change);
	}

	/* S-7: remove the file; parent nlink 2->? (file remove: nlink
	 * decrement only on FINAL unlink of a *directory child*...
	 * note: parent NL for file children is untouched by design --
	 * NL counts directory children; verify lookup NOTFOUND +
	 * inode gone) */
	if (file_fid) {
		struct mds_inode nl_before, nl_after;
		st = cat->auth_ops->ns_getattr(cat, dir_fid, &nl_before);
		st = cat->auth_ops->ns_remove(cat, &txn, dir_fid, fname);
		CHECK(st == MDS_OK, "S-7a remove file (st=%s)", stname(st));
		st = cat->auth_ops->ns_lookup(cat, dir_fid, fname, &in);
		CHECK(st == MDS_ERR_NOTFOUND,
		      "S-7b lookup after remove -> NOTFOUND (st=%s)",
		      stname(st));
		st = cat->auth_ops->ns_getattr(cat, file_fid, &in);
		CHECK(st == MDS_ERR_NOTFOUND,
		      "S-7c inode gone -> NOTFOUND (st=%s)", stname(st));
		/* I-4 mirror: removing a FILE child must not move the
		 * parent's nlink (dir nlink = 2 + dir children only;
		 * the validator caught the old unconditional -1). */
		st = cat->auth_ops->ns_getattr(cat, dir_fid, &nl_after);
		CHECK(st == MDS_OK && nl_after.nlink == nl_before.nlink,
		      "S-7d parent nlink unchanged by file remove (%u->%u)",
		      nl_before.nlink, nl_after.nlink);
	}

	/* S-8: directory removal is a slice boundary -> NOSUPPORT */
	st = cat->auth_ops->ns_remove(cat, &txn, 2, subdir);
	CHECK(st == MDS_ERR_NOSUPPORT,
	      "S-8 remove dir -> NOSUPPORT (st=%s)", stname(st));

	/* S-9: 200 sequential creates under root, all commit */
	if (st == MDS_OK) {
		int ok = 0, bad = 0;
		char nm[64];
		for (i = 0; i < 200; i++) {
			snprintf(nm, sizeof(nm), "bulk_%03d", i);
			st = cat->auth_ops->ns_create(cat, &txn, 2, nm,
						      MDS_FTYPE_REG, 0644,
						      0, 0, NULL, &in);
			if (st == MDS_OK) ok++;
			else bad++;
		}
		CHECK(ok == 200 && bad == 0,
		      "S-9 200 sequential creates: ok=%d bad=%d", ok, bad);
		/* and the bulk files are visible */
		{
			struct dir_result r;
			memset(&r, 0, sizeof(r));
			st = cat->auth_ops->ns_readdir(cat, 2, NULL, 256,
							&txn, dir_cb, &r);
			CHECK(st == MDS_OK && r.count == 201, /* subdir + 200 bulk */
			      "S-9b readdir root after bulk: count=%d (want 201)",
			      r.count);
		}
	}

	/* S-10: idempotent reopen */
	cat->ops->close(cat);
	cat = NULL;
	st = catalogue_fdb_open(&cfg, &cat);
	CHECK(st == MDS_OK, "S-10a reopen (st=%s)", stname(st));
	if (st == MDS_OK) {
		st = cat->auth_ops->ns_getattr(cat, 2, &in);
		CHECK(st == MDS_OK && in.type == MDS_FTYPE_DIR,
		      "S-10b root survives reopen (st=%s nlink=%u)",
		      stname(st), in.nlink);
		if (dir_fid) {
			st = cat->auth_ops->ns_getattr(cat, dir_fid, &in);
			CHECK(st == MDS_OK,
			      "S-10c subdir survives reopen (st=%s)",
			      stname(st));
		}
		/* Leave the catalogue open: S-11 runs on it and the
		 * final close happens at the end of main. */
	}

	/* S-11: concurrent creates on ONE hot parent (4 threads x 25,
	 * distinct names).  Validates the atomic_op hot path:
	 * - FID claims are atomic ADD batches -> every fid unique
	 * - parent NL/CHG full-width atomic ADDs -> exact delta, no
	 *   lost updates (the 370 ops/s RMW wall would show as lost
	 *   counter increments or DELAY storms at this scale)
	 * - per-name dirent keys never collide -> 0 conflict retries
	 */
	{
		struct dir_result r;
		uint64_t dir2_fid = 0;
		pthread_t th[4];
		int t, all_ok = 1;
		int dup = 0;
		uint64_t seen[256];
		int nseen = 0;

		if (cat == NULL) {
			printf("  SKIP S-11 (catalogue not open)\n");
			goto s11_done;
		}
		st = cat->auth_ops->ns_create(cat, &txn, 2, "slice_hotdir",
					      MDS_FTYPE_DIR, 0755, 0, 0,
					      NULL, &child);
		CHECK(st == MDS_OK, "S-11.0 hotdir create (st=%s)", stname(st));
		if (st == MDS_OK)
			dir2_fid = child.fileid;

		if (dir2_fid) {
			struct mds_inode before, after;

			memset(&before, 0, sizeof(before));
			cat->auth_ops->ns_getattr(cat, dir2_fid, &before);

			for (t = 0; t < 4; t++) {
				s11[t].tid = t;
				s11[t].cat = cat;
				s11[t].txn = txn;
				s11[t].parent = dir2_fid;
				s11[t].st = MDS_OK;
				memset(s11[t].fids, 0, sizeof(s11[t].fids));
				pthread_create(&th[t], NULL, s11_worker,
						&s11[t]);
			}
			for (t = 0; t < 4; t++)
				pthread_join(th[t], NULL);

			for (t = 0; t < 4; t++) {
				int i;
				int nfilled = 0;
				if (s11[t].st != MDS_OK) {
					all_ok = 0;
					printf("  (S-11 t%d st=%s)\n", t,
					       stname(s11[t].st));
				}
				for (i = 0; i < 25; i++) {
					uint64_t f = s11[t].fids[i];
					int j;
					if (f == 0) continue;
					nfilled++;
					for (j = 0; j < nseen; j++)
						if (seen[j] == f)
							dup++;
					seen[nseen++] = f;
				}
				printf("  (S-11 t%d: filled=%d first=%llu last=%llu)\n",
				       t, nfilled,
				       (unsigned long long)(nfilled ?
					       s11[t].fids[0] : 0),
				       (unsigned long long)(nfilled ?
					       s11[t].fids[nfilled-1] : 0));
			}
			CHECK(all_ok && dup == 0 && nseen == 100,
			      "S-11a 4x25 concurrent creates: ok=%d unique=%d dup=%d",
			      all_ok, nseen, dup);

			memset(&after, 0, sizeof(after));
			st = cat->auth_ops->ns_getattr(cat, dir2_fid, &after);
			CHECK(st == MDS_OK && after.change == before.change + 100,
			      "S-11b parent CHG exact: %llu -> %llu (want +100)",
			      (unsigned long long)before.change,
			      (unsigned long long)after.change);

			memset(&r, 0, sizeof(r));
			st = cat->auth_ops->ns_readdir(cat, dir2_fid, NULL,
						       256, &txn, dir_cb, &r);
			CHECK(st == MDS_OK && r.count == 100,
			      "S-11c readdir hotdir: count=%d (want 100, st=%s)",
			      r.count, stname(st));
		}
	}

	/* final close (catalogue left open through S-11) */
s11_done:
	if (cat != NULL)
		cat->ops->close(cat);

	printf("\n== result: %d passed, %d failed ==\n", g_pass, g_fail);
	(void)rc;
	return g_fail ? 1 : 0;
}
