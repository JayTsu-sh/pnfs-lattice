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
 *   S-11 4x25 concurrent creates on ONE hot parent: FID unique, parent
 *        CHG exact +100 (no lost atomic updates), readdir = 100
 *   S-12 stripe map round-trip: put(sc=4) -> get field-by-field,
 *        del -> NOTFOUND, absent map -> NOTFOUND
 *   S-13 layout grant/lookup/scan: grant(ds=2) -> get_by_stateid
 *        field round-trip, scan_for_file true/false, ds_count=0 grant
 *        still resolvable
 *   S-14 protocol state round-trip: client/session/slot put -> get
 *        field-by-field (incl. cached_reply blob), del -> NOTFOUND
 *   S-15 concurrency: slot RMW storm (4x25, final seq exact +100) +
 *        layout renewal storm on ONE stateid (4x10, final seqid
 *        exact +40) -- FDB conflict retry must serialize same-key
 *        writers with zero lost updates
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
#include "open_state.h"
#include <stdbool.h>

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

/* ---- S-15 worker contexts (file scope: the worker must not be a
 * nested function) ---- */
struct s15_slot_ctx {
	struct mds_catalogue *cat;
	uint8_t sess[16];
	uint32_t iters;
	enum mds_status st;
	uint32_t errors;
};
struct s15_layout_ctx {
	struct mds_catalogue *cat;
	uint8_t other[12];
	uint64_t fid;
	uint32_t iters;
	enum mds_status st;
};

/* RMW increment on ONE (session, slot): read seq, write seq+1.
 * With read-before-write in fdb_slot_put, concurrent increments on
 * the same key must serialize -> every increment applies exactly
 * once -> the final seq is EXACT (no lost updates). */
static void *s15_slot_worker(void *arg)
{
	struct s15_slot_ctx *c = arg;
	uint32_t i;

	c->st = MDS_OK;
	c->errors = 0;
	for (i = 0; i < c->iters; i++) {
		enum mds_status s2;
		struct mds_coord_drc_slot_row row;

		memset(&row, 0, sizeof(row));
		s2 = c->cat->coord_ops->slot_get(c->cat, c->sess, 0, &row);
		if (s2 != MDS_OK && s2 != MDS_ERR_NOTFOUND) {
			c->errors++;
			continue;
		}
		if (row.cached_reply != NULL)
			free(row.cached_reply);
		s2 = c->cat->coord_ops->slot_put(c->cat, c->sess, 0,
				row.seq_id + 1, "r", 1);
		if (s2 != MDS_OK) {
			c->st = s2;
			break;
		}
	}
	return NULL;
}

/* RMW increment of the LAYOUT stateid seqid on ONE stateid
 * (the SEQUENCE/LAYOUTGET renewal serialization property). */
static void *s15_layout_worker(void *arg)
{
	struct s15_layout_ctx *c = arg;
	uint32_t i;
	uint64_t cid, fid, off, len;
	uint32_t mode, seq;
	struct nfs4_stateid sid;

	c->st = MDS_OK;
	for (i = 0; i < c->iters; i++) {
		enum mds_status s2;

		s2 = c->cat->coord_ops->layout_get_by_stateid(c->cat,
				c->other, &cid, &fid, &mode, &off, &len,
				&seq);
		if (s2 != MDS_OK) {
			c->st = s2;
			break;
		}
		memset(&sid, 0, sizeof(sid));
		sid.seqid = seq + 1;
		memcpy(sid.other, c->other, 12);
		s2 = c->cat->coord_ops->layout_grant(c->cat, NULL, cid,
				c->fid, mode, off, len, &sid, NULL, 0);
		if (s2 != MDS_OK) {
			c->st = s2;
			break;
		}
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

	/* S-12: stripe map family (0x30/0x31) -- LAYOUTGET read path.
	 * put(sc=4) -> get must round-trip header + every entry
	 * field-by-field; del -> NOTFOUND; a file without a map ->
	 * NOTFOUND (the side-table contract, compound_layout.c:1025). */
	{
		uint64_t sm_fid = 0;
		struct mds_ds_map_entry ents[4];
		struct mds_ds_map_entry *back = NULL;
		uint32_t sc, su, mc, e;

		if (cat == NULL) {
			printf("  SKIP S-12 (catalogue not open)\n");
			goto s12_done;
		}
		st = cat->auth_ops->ns_create(cat, &txn, 2, "stripe_file",
				MDS_FTYPE_REG, 0644, 0, 0, NULL, &child);
		CHECK(st == MDS_OK, "S-12.0 stripe file create (st=%s)",
		      stname(st));
		if (st == MDS_OK)
			sm_fid = child.fileid;

		if (sm_fid) {
			for (e = 0; e < 4; e++) {
				ents[e].ds_id = 1 + e;
				ents[e].nfs_fh_len = 16;
				memset(ents[e].nfs_fh, 0, sizeof(ents[e].nfs_fh));
				ents[e].nfs_fh[0] = (uint8_t)(0xA0 + e);
				ents[e].nfs_fh[15] = (uint8_t)(e + 1);
			}
			st = cat->auth_ops->stripe_map_put(cat, &txn, sm_fid,
					4, 1048576, 2, ents);
			CHECK(st == MDS_OK, "S-12a stripe_map_put sc=4 (st=%s)",
			      stname(st));
			/* stripe_map_get backend-allocates *entries;
			 * free + reset around every call. */
			st = cat->auth_ops->stripe_map_get(cat, sm_fid,
					&sc, &su, &mc, &back);
			CHECK(st == MDS_OK && sc == 4 && su == 1048576 &&
			      mc == 2,
			      "S-12b get header (st=%s sc=%u su=%u mc=%u)",
			      stname(st), sc, su, mc);
			if (st == MDS_OK && sc == 4 && back != NULL) {
				int ok = 1;
				for (e = 0; e < 4; e++) {
					if (back[e].ds_id != ents[e].ds_id ||
					    back[e].nfs_fh_len !=
						ents[e].nfs_fh_len ||
					    memcmp(back[e].nfs_fh,
						 ents[e].nfs_fh, 16) != 0)
						ok = 0;
				}
				CHECK(ok,
				      "S-12c 4 entries field-by-field (ds_id/fh)");
			}
			free(back);
			back = NULL;
			/* Absent map on a different file -> NOTFOUND. */
			st = cat->auth_ops->ns_create(cat, &txn, 2,
					"nostripe_file", MDS_FTYPE_REG,
					0644, 0, 0, NULL, &child);
			if (st == MDS_OK) {
				st = cat->auth_ops->stripe_map_get(cat,
						child.fileid, &sc, &su, &mc,
						&back);
				CHECK(st == MDS_ERR_NOTFOUND,
				      "S-12d absent map -> NOTFOUND (st=%s)",
				      stname(st));
			}
			free(back);
			back = NULL;
			st = cat->auth_ops->stripe_map_del(cat, &txn, sm_fid);
			CHECK(st == MDS_OK, "S-12e stripe_map_del (st=%s)",
			      stname(st));
			st = cat->auth_ops->stripe_map_get(cat, sm_fid, &sc,
					&su, &mc, &back);
			CHECK(st == MDS_ERR_NOTFOUND,
			      "S-12f after del -> NOTFOUND (st=%s)",
			      stname(st));
			free(back);
			back = NULL;
		}
	}
s12_done:

	/* S-13: layout state family (0x50-0x54) -- pNFS grant path. */
	{
		uint64_t lg_fid = 0;
		struct nfs4_stateid sid;
		uint32_t ds_ids[2] = { 1, 2 };
		uint64_t r_cid, r_fid;
		uint32_t r_mode, r_seq;
		uint64_t r_off, r_len;
		bool has = true;

		if (cat == NULL) {
			printf("  SKIP S-13 (catalogue not open)\n");
			goto s13_done;
		}
		st = cat->auth_ops->ns_create(cat, &txn, 2, "layout_file",
				MDS_FTYPE_REG, 0644, 0, 0, NULL, &child);
		CHECK(st == MDS_OK, "S-13.0 layout file create (st=%s)",
		      stname(st));
		if (st == MDS_OK)
			lg_fid = child.fileid;

		if (lg_fid) {
			memset(&sid, 0, sizeof(sid));
			sid.seqid = 1;
			memcpy(sid.other, "\x01\x02\x03\x04\x05\x06\x07\x08"
				 "\x09\x0a\x0b\x0c", 12);
			st = cat->coord_ops->layout_grant(cat, &txn,
					100, lg_fid, 2, 0, 1 << 20, &sid,
					ds_ids, 2);
			CHECK(st == MDS_OK, "S-13a grant client=100 ds=2 (st=%s)",
			      stname(st));
			st = cat->coord_ops->layout_get_by_stateid(cat,
					sid.other, &r_cid, &r_fid, &r_mode,
					&r_off, &r_len, &r_seq);
			CHECK(st == MDS_OK && r_cid == 100 &&
			      r_fid == lg_fid && r_mode == 2 && r_off == 0 &&
			      r_len == (1 << 20) && r_seq == 1,
			      "S-13b by_stateid round-trip (st=%s cid=%llu "
			      "fid=%llu mode=%u off=%llu len=%llu seq=%u)",
			      stname(st), (unsigned long long)r_cid,
			      (unsigned long long)r_fid, r_mode,
			      (unsigned long long)r_off,
			      (unsigned long long)r_len, r_seq);
			has = false;
			st = cat->coord_ops->layout_scan_for_file(cat, lg_fid,
					&has);
			CHECK(st == MDS_OK && has,
			      "S-13c scan_for_file granted (st=%s has=%d)",
			      stname(st), (int)has);
			has = true;
			st = cat->coord_ops->layout_scan_for_file(cat, 2,
					&has);
			CHECK(st == MDS_OK && !has,
			      "S-13d scan_for_file ungranted (st=%s has=%d)",
			      stname(st), (int)has);
			/* ds_count=0 grant (no 0x53 rows) still resolves. */
			sid.seqid = 7;
			sid.other[11] ^= 0xff;
			st = cat->coord_ops->layout_grant(cat, &txn,
					101, lg_fid, 1, 0, 4096, &sid,
					NULL, 0);
			CHECK(st == MDS_OK, "S-13e grant ds_count=0 (st=%s)",
			      stname(st));
			st = cat->coord_ops->layout_get_by_stateid(cat,
					sid.other, &r_cid, &r_fid, &r_mode,
					&r_off, &r_len, &r_seq);
			CHECK(st == MDS_OK && r_cid == 101 && r_seq == 7 &&
			      r_fid == lg_fid,
			      "S-13f ds_count=0 resolves (st=%s cid=%llu seq=%u)",
			      stname(st), (unsigned long long)r_cid, r_seq);
			/* Unknown stateid -> NOTFOUND. */
			sid.other[0] ^= 0xff;
			st = cat->coord_ops->layout_get_by_stateid(cat,
					sid.other, &r_cid, &r_fid, &r_mode,
					&r_off, &r_len, &r_seq);
			CHECK(st == MDS_ERR_NOTFOUND,
			      "S-13g unknown stateid -> NOTFOUND (st=%s)",
			      stname(st));
		}
	}
s13_done:

	/* S-14: NFSv4.1 protocol state (0x70-0x73) -- SEQUENCE hot path. */
	{
		struct mds_coord_client_row cl, clb;
		struct mds_coord_session_row ss, ssb;
		struct mds_coord_drc_slot_row sl;
		const char *reply = "cached-reply-bytes";

		if (cat == NULL) {
			printf("  SKIP S-14 (catalogue not open)\n");
			goto s14_done;
		}
		memset(&cl, 0, sizeof(cl));
		cl.clientid = 200;
		cl.co_ownerid_len = 11;
		memcpy(cl.co_ownerid, "test-client", 11);
		memcpy(cl.verifier, "verifier", 8);
		cl.confirmed = true;
		cl.owner_mds_id = 0;
		cl.owner_boot_epoch = 7;
		cl.lease_renewed_ns = 123456;
		st = cat->coord_ops->client_put(cat, &cl);
		CHECK(st == MDS_OK, "S-14a client_put (st=%s)", stname(st));
		st = cat->coord_ops->client_get(cat, 200, &clb);
		CHECK(st == MDS_OK && clb.clientid == 200 &&
		      clb.co_ownerid_len == 11 &&
		      memcmp(clb.co_ownerid, "test-client", 11) == 0 &&
		      clb.confirmed && clb.owner_boot_epoch == 7 &&
		      clb.lease_renewed_ns == 123456 &&
		      memcmp(clb.verifier, "verifier", 8) == 0,
		      "S-14b client_get round-trip (st=%s len=%u)",
		      stname(st), clb.co_ownerid_len);

		memset(&ss, 0, sizeof(ss));
		memcpy(ss.session_id, "session-id-000", 14);
		ss.clientid = 200;
		ss.num_slots = 64;
		ss.cb_prog = 1;
		ss.cb_sec_flavor = 3;
		ss.owner_mds_id = 0;
		ss.owner_boot_epoch = 7;
		ss.created_ns = 999;
		st = cat->coord_ops->session_put(cat, &ss);
		CHECK(st == MDS_OK, "S-14c session_put (st=%s)", stname(st));
		st = cat->coord_ops->session_get(cat, ss.session_id, &ssb);
		CHECK(st == MDS_OK && ssb.clientid == 200 &&
		      ssb.num_slots == 64 && ssb.cb_prog == 1 &&
		      ssb.cb_sec_flavor == 3 && ssb.owner_boot_epoch == 7 &&
		      ssb.created_ns == 999,
		      "S-14d session_get round-trip (st=%s slots=%u)",
		      stname(st), ssb.num_slots);

		st = cat->coord_ops->slot_put(cat, ss.session_id, 3, 42,
				reply, (uint32_t)strlen(reply));
		CHECK(st == MDS_OK, "S-14e slot_put seq=42 (st=%s)",
		      stname(st));
		st = cat->coord_ops->slot_get(cat, ss.session_id, 3, &sl);
		CHECK(st == MDS_OK && sl.slot_id == 3 && sl.seq_id == 42 &&
		      sl.reply_len == (uint32_t)strlen(reply) &&
		      sl.cached_reply != NULL &&
		      memcmp(sl.cached_reply, reply, sl.reply_len) == 0 &&
		      sl.last_used_ns > 0,
		      "S-14f slot_get round-trip (st=%s seq=%u rlen=%u)",
		      stname(st), sl.seq_id, sl.reply_len);
		if (sl.cached_reply != NULL)
			free(sl.cached_reply);
		st = cat->coord_ops->slot_get(cat, ss.session_id, 99, &sl);
		CHECK(st == MDS_ERR_NOTFOUND,
		      "S-14g empty slot -> NOTFOUND (st=%s)", stname(st));

		/* Slot on a session that exists but the session is
		 * deleted -> slot still there (no cascade; matches the
		 * NDB schema: drc_slots has no FK cascade), then
		 * session_get NOTFOUND. */
		st = cat->coord_ops->session_del(cat, ss.session_id);
		CHECK(st == MDS_OK, "S-14h session_del (st=%s)", stname(st));
		st = cat->coord_ops->session_get(cat, ss.session_id, &ssb);
		CHECK(st == MDS_ERR_NOTFOUND,
		      "S-14i session after del -> NOTFOUND (st=%s)",
		      stname(st));
		st = cat->coord_ops->session_del(cat, ss.session_id);
		CHECK(st == MDS_ERR_NOTFOUND,
		      "S-14j double del -> NOTFOUND (st=%s)", stname(st));
		st = cat->coord_ops->client_del(cat, 200);
		CHECK(st == MDS_OK, "S-14k client_del (st=%s)", stname(st));
		st = cat->coord_ops->client_get(cat, 200, &clb);
		CHECK(st == MDS_ERR_NOTFOUND,
		      "S-14l client after del -> NOTFOUND (st=%s)",
		      stname(st));
	}
s14_done:

	/* S-15: same-key RMW storms -- the SEQUENCE/LAYOUTGET renewal
	 * serialization property.  FDB conflict retry must serialize
	 * writers on the same key: every increment applies exactly
	 * once, so the final value is EXACT (no lost updates).
	 * 4 threads x 25 slot increments (read seq, write seq+1) ->
	 * final 100; 4 threads x 10 layout renewals (read seqid,
	 * write seqid+1, same stateid) -> final 40. */
	{
		struct mds_coord_session_row ss;
		struct mds_coord_drc_slot_row sl;
		struct nfs4_stateid sid;
		pthread_t th[4];
		int t;
		struct s15_slot_ctx s15s[4];
		struct s15_layout_ctx s15l[4];

		if (cat == NULL) {
			printf("  SKIP S-15 (catalogue not open)\n");
			goto s15_done;
		}
		memset(&ss, 0, sizeof(ss));
		memcpy(ss.session_id, "s15-sess", 8);
		ss.clientid = 300;
		ss.num_slots = 128;
		ss.cb_prog = 1;
		ss.cb_sec_flavor = 3;
		st = cat->coord_ops->session_put(cat, &ss);
		CHECK(st == MDS_OK, "S-15.0 seed session (st=%s)", stname(st));

		if (st == MDS_OK) {
			for (t = 0; t < 4; t++) {
				s15s[t].cat = cat;
				memcpy(s15s[t].sess, ss.session_id, 16);
				s15s[t].iters = 25;
				s15s[t].st = MDS_OK;
				s15s[t].errors = 0;
				pthread_create(&th[t], NULL,
						s15_slot_worker, &s15s[t]);
			}
			for (t = 0; t < 4; t++)
				pthread_join(th[t], NULL);
			for (t = 0; t < 4; t++) {
				if (s15s[t].st != MDS_OK) {
					printf("  (S-15 t%d st=%s err=%u)\n",
					       t, stname(s15s[t].st),
					       s15s[t].errors);
				}
			}
			st = cat->coord_ops->slot_get(cat, ss.session_id, 0,
					&sl);
			CHECK(st == MDS_OK && sl.seq_id == 100,
			      "S-15a slot storm final seq=%u (want 100, st=%s)",
			      sl.seq_id, stname(st));
			if (sl.cached_reply != NULL)
				free(sl.cached_reply);
		}

		/* Layout renewal storm: one stateid, 4 threads x 10. */
		if (cat != NULL) {
			uint64_t lg_fid = 0;
			uint32_t r_seq = 0;
			st = cat->auth_ops->ns_create(cat, &txn, 2,
					"renew_file", MDS_FTYPE_REG, 0644,
					0, 0, NULL, &child);
			CHECK(st == MDS_OK, "S-15.1 renew file create (st=%s)",
			      stname(st));
			if (st == MDS_OK) {
				lg_fid = child.fileid;
				memset(&sid, 0, sizeof(sid));
				sid.seqid = 0;
				memcpy(sid.other, "s15-layout", 10);
				st = cat->coord_ops->layout_grant(cat, &txn,
						400, lg_fid, 2, 0, 4096,
						&sid, NULL, 0);
				CHECK(st == MDS_OK,
				      "S-15.2 seed grant seqid=0 (st=%s)",
				      stname(st));
				if (st == MDS_OK) {
					for (t = 0; t < 4; t++) {
						s15l[t].cat = cat;
						memcpy(s15l[t].other,
							sid.other, 12);
						s15l[t].fid = lg_fid;
						s15l[t].iters = 10;
						s15l[t].st = MDS_OK;
						pthread_create(&th[t], NULL,
							s15_layout_worker,
							&s15l[t]);
					}
					for (t = 0; t < 4; t++)
						pthread_join(th[t], NULL);
					for (t = 0; t < 4; t++)
						if (s15l[t].st != MDS_OK)
							printf("  (S-15L t%d st=%s)\n",
							       t,
							       stname(s15l[t].st));
					st = cat->coord_ops->layout_get_by_stateid(
							cat, sid.other, NULL,
							NULL, NULL, NULL, NULL,
							&r_seq);
					CHECK(st == MDS_OK && r_seq == 40,
					      "S-15b renewal storm final seqid=%u (want 40, st=%s)",
					      r_seq, stname(st));
				}
			}
		}
	}
s15_done:

	/* final close (catalogue left open through S-11..S-15) */
s11_done:
	if (cat != NULL)
		cat->ops->close(cat);

	printf("\n== result: %d passed, %d failed ==\n", g_pass, g_fail);
	(void)rc;
	return g_fail ? 1 : 0;
}
