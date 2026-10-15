/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_fdb.c -- FoundationDB catalogue backend (Phase 3 vertical
 * slice).
 *
 * Implements the six core namespace slots of mds_authority_ops on top of
 * the FDB C API (7.4), with the keyspace layout from
 * docs/fdb-feasibility-05-keyspace-mapping.md:
 *
 *   0x00  meta counters
 *         "\x00FID"          8B LE u64  global fileid counter
 *         "NL" | parent(8)   8B LE u64  parent dir nlink counter (hot)
 *         "CHG" | parent(8)  8B LE u64  parent dir change counter (hot)
 *   0x10  inodes           key = 0x10 | fileid(8)  value = 285B wire
 *   0x20  dirents          key = 0x20 | parent(8) | name(NUL)
 *                          value = fileid(8,BE) | type(1)
 *   0x21  dirents_child    key = 0x21 | parent(8) | child_fileid(8)
 *                          value = name(NUL)  (secondary index, -05 §2.2:
 *                          READDIR-by-fid + name-reuse detection; dual-write
 *                          on create, dual-clear on remove)
 *
 * Design decisions carried over from the paper analysis (-05):
 *   - A directory's nlink and change live in INDEPENDENT full-width
 *     8-byte little-endian counter keys in the 0x00 band, updated with
 *     FDB atomic ADD (full width, no operand-offset acrobatics).  This
 *     is the H-01 mitigation: the hot parent key is never a plain
 *     read-modify-write (RMW on a hot parent key measured at 370 ops/s
 *     wall; atomic ADD at 6.5k ops/s, 0 conflicts).
 *   - The inode wire image (rondb_schema.h) is used verbatim as the
 *     FDB value -- no reserialisation.  A directory's wire nlink is
 *     only its own cold value at create time; the authoritative
 *     running count for the parent is the NL counter.
 *   - Transient FDB errors (conflicts, node busy) retry with
 *     fdb_transaction_on_error backoff + reset + re-stage, bounded;
 *     exhaustion maps to MDS_ERR_DELAY per the G2 spec
 *     (fdb-feasibility-02 A.3/A.4), never a terminal MDS_ERR_IO.
 *   - All other authority slots are left NULL: the dispatch layer
 *     turns them into MDS_ERR_NOSUPPORT, which the G1 fix in
 *     mds_status_to_nfs4 maps to a stable NFS4ERR_NOTSUPP instead of
 *     NFS4ERR_SERVERFAULT.
 *
 * NOT production-ready: rename/link/xattr/queues/stateful-NFS paths are
 * intentionally unimplemented in the slice; the lab config that enables
 * this backend must not run real workloads beyond the six slots.
 */

#define FDB_API_VERSION 740

#include <foundationdb/fdb_c.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "mds_catalogue.h"
#include "catalogue_internal.h"
#include "rondb_schema.h"
#include "mds_log.h"
#include "ds_prealloc.h"

#define FDB_RETRY_MAX 50

/* Meta-counter key tags (0x00 band). */
static const uint8_t CNT_FID_KEY[4]  = {0x00, 'F', 'I', 'D'};
#define CNT_FID_LEN 4
static const uint8_t CNT_NL_TAG[3]   = {0x00, 'N', 'L'};
static const uint8_t CNT_CHG_TAG[4]  = {0x00, 'C', 'H', 'G'};
#define CNT_NL_LEN  (3 + 8)
#define CNT_CHG_LEN (4 + 8)

struct fdb_state {
	FDBDatabase *db;
	pthread_t   net_thread;
};

static const struct mds_authority_ops fdb_authority_ops;

/* -----------------------------------------------------------------------
 * Byte helpers (inode wire is big-endian; FDB counters are little-endian)
 * ----------------------------------------------------------------------- */

static void fdb_put_u64_be(uint8_t *p, uint64_t v)
{
	p[0] = (uint8_t)(v >> 56); p[1] = (uint8_t)(v >> 48);
	p[2] = (uint8_t)(v >> 40); p[3] = (uint8_t)(v >> 32);
	p[4] = (uint8_t)(v >> 24); p[5] = (uint8_t)(v >> 16);
	p[6] = (uint8_t)(v >> 8);  p[7] = (uint8_t)v;
}

static uint64_t fdb_get_u64_be(const uint8_t *p)
{
	return ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
	       ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
	       ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
	       ((uint64_t)p[6] << 8)  | (uint64_t)p[7];
}

static void fdb_put_u64_le(uint8_t *p, uint64_t v)
{
	uint32_t i;
	for (i = 0; i < 8; i++)
		p[i] = (uint8_t)(v >> (8 * i));
}

static uint64_t fdb_get_u64_le(const uint8_t *p)
{
	uint64_t v = 0;
	uint32_t i;
	for (i = 0; i < 8; i++)
		v |= (uint64_t)p[i] << (8 * i);
	return v;
}

/* Full-width 8-byte little-endian ADD operand (signed delta).  FDB ADD
 * on an 8-byte value is LE integer arithmetic; a signed delta wraps
 * into the two's-complement representation, which is exactly what a
 * signed decrement needs. */
static void fdb_add_op(uint8_t op[8], int64_t delta)
{
	fdb_put_u64_le(op, (uint64_t)delta);
}

/* -----------------------------------------------------------------------
 * FDB primitives (single network thread; blocking futures from any
 * thread are safe with fdb_future_block_until_ready)
 * ----------------------------------------------------------------------- */

static struct fdb_state *fdb_state(struct mds_catalogue *cat)
{
	return (struct fdb_state *)cat->backend_private;
}

static FDBTransaction *fdb_begin(FDBDatabase *db)
{
	FDBTransaction *tr = NULL;
	fdb_database_create_transaction(db, &tr);
	return tr;
}

/* read one key; returns value length (>0) if present, 0 if absent,
 * -1 on FDB error.  buf receives min(vlen, bufsz) bytes.
 *
 * API contract: the value pointer from fdb_future_get_value() is
 * only valid until the future is destroyed -- copy it out FIRST.
 * (Copying after destroy reads client-internal freed/pooled
 * memory; it is a latent use-after-free that deterministically
 * clobbers values in transactions that also carry mutations.) */
static int fdb_txn_get(FDBTransaction *tr, const uint8_t *key,
		       uint32_t klen, uint8_t *buf, uint32_t bufsz)
{
	FDBFuture *f = fdb_transaction_get(tr, key, (int)klen, 0);
	fdb_future_block_until_ready(f);
	fdb_error_t err = fdb_future_get_error(f);
	int rc = -1;
	if (!err) {
		fdb_bool_t present = 0;
		const uint8_t *val = NULL;
		int vlen = 0;
		if (fdb_future_get_value(f, &present, &val, &vlen) == 0 &&
		    present && val != NULL && vlen > 0) {
			if ((uint32_t)vlen <= bufsz)
				memcpy(buf, val, (size_t)vlen);
			rc = vlen;
		} else {
			rc = 0;
		}
	}
	fdb_future_destroy(f);
	return rc;
}

static fdb_error_t fdb_txn_commit_block(FDBTransaction *tr)
{
	FDBFuture *f = fdb_transaction_commit(tr);
	fdb_future_block_until_ready(f);
	fdb_error_t err = fdb_future_get_error(f);
	fdb_future_destroy(f);
	return err;
}

static void fdb_conflict_backoff(FDBTransaction *tr, fdb_error_t err)
{
	FDBFuture *f = fdb_transaction_on_error(tr, err);
	if (f != NULL) {
		fdb_future_block_until_ready(f);
		fdb_future_destroy(f);
	}
}

/* -----------------------------------------------------------------------
 * Counter keys
 * ----------------------------------------------------------------------- */

static void cnt_nlink_key(uint8_t *k, uint64_t parent)
{
	memcpy(k, CNT_NL_TAG, 3);
	fdb_put_u64_be(k + 3, parent);
}

static void cnt_change_key(uint8_t *k, uint64_t parent)
{
	memcpy(k, CNT_CHG_TAG, 4);
	fdb_put_u64_be(k + 4, parent);
}

/* -----------------------------------------------------------------------
 * Inode / counter merge helpers
 * ----------------------------------------------------------------------- */

static enum mds_status fdb_read_inode(FDBDatabase *db, uint64_t fileid,
				      struct mds_inode *inode)
{
	uint8_t key[9];
	uint8_t buf[RONDB_INODE_FIXED_SIZE];
	int rc;

	key[0] = 0x10;
	fdb_put_u64_be(key + 1, fileid);
	rc = fdb_txn_get(fdb_begin(db), key, 9, buf, sizeof(buf));
	if (rc < 0) return MDS_ERR_IO;
	if (rc == 0) return MDS_ERR_NOTFOUND;
	if (rondb_inode_deserialize(buf, RONDB_INODE_FIXED_SIZE, inode,
				    NULL) != 0)
		return MDS_ERR_IO;
	return MDS_OK;
}

/* Overlay the authoritative NL/CHG counters on a directory's wire
 * image (best-effort: a missing counter falls back to the wire value,
 * which root is pre-seeded with).  Cold reads only. */
static void fdb_merge_dir_counters(struct mds_catalogue *cat,
				   struct mds_inode *in)
{
	struct fdb_state *s = fdb_state(cat);
	uint8_t key[12];
	uint8_t buf[8];
	int rc;

	if (s == NULL || in->type != MDS_FTYPE_DIR)
		return;
	cnt_nlink_key(key, in->fileid);
	rc = fdb_txn_get(fdb_begin(s->db), key, CNT_NL_LEN, buf, sizeof(buf));
	if (rc > 0)
		in->nlink = (uint32_t)(fdb_get_u64_le(buf) & 0xFFFFFFFF);
	cnt_change_key(key, in->fileid);
	rc = fdb_txn_get(fdb_begin(s->db), key, CNT_CHG_LEN, buf, sizeof(buf));
	if (rc > 0)
		in->change = fdb_get_u64_le(buf);
}

/* -----------------------------------------------------------------------
 * ns_getattr / ns_lookup
 * ----------------------------------------------------------------------- */

static enum mds_status fdb_ns_getattr(struct mds_catalogue *cat,
				      uint64_t fileid, struct mds_inode *inode)
{
	struct fdb_state *s = fdb_state(cat);
	enum mds_status st;

	if (s == NULL || inode == NULL) return MDS_ERR_INVAL;
	st = fdb_read_inode(s->db, fileid, inode);
	if (st != MDS_OK)
		return st;
	fdb_merge_dir_counters(cat, inode);
	return MDS_OK;
}

static enum mds_status fdb_ns_lookup(struct mds_catalogue *cat,
				     uint64_t parent, const char *name,
				     struct mds_inode *child)
{
	struct fdb_state *s = fdb_state(cat);
	size_t nlen = strlen(name);
	uint8_t key[2 + 8 + MDS_MAX_NAME + 1];
	uint8_t dval[9];
	uint64_t fileid;
	int rc;

	if (s == NULL || child == NULL || nlen > MDS_MAX_NAME)
		return MDS_ERR_INVAL;
	key[0] = 0x20;
	fdb_put_u64_be(key + 1, parent);
	memcpy(key + 9, name, nlen + 1);

	rc = fdb_txn_get(fdb_begin(s->db), key, 9 + (uint32_t)nlen + 1,
			 dval, sizeof(dval));
	if (rc < 0) return MDS_ERR_IO;
	if (rc == 0) return MDS_ERR_NOTFOUND;
	if (rc < 9)  return MDS_ERR_IO;

	fileid = fdb_get_u64_be(dval);
	{
		enum mds_status st = fdb_read_inode(s->db, fileid, child);
		if (st != MDS_OK)
			return st;
		child->type = (enum mds_file_type)dval[8];
		child->parent_fileid = parent;
		fdb_merge_dir_counters(cat, child);
	}
	return MDS_OK;
}

/* -----------------------------------------------------------------------
 * ns_create
 * ----------------------------------------------------------------------- */

/* NOTE: the global FID counter is claimed OUTSIDE the create
 * transaction as an atomic ADD batch (H-01, mirrors the RonDB
 * per-thread 1024-batch pool in catalogue_rondb.c:431-471):
 * one small claim transaction (1 atomic op) per ~1024 creates
 * per thread, and the FID key never appears in any create tx's
 * write set -- the create tx's conflict range is exactly its own
 * dirent/inode/counter keys.  Plain in-transaction RMW was the
 * 370 ops/s wall measured in fdb-feasibility-01. */
static _Thread_local uint64_t tl_fid_base      = 0;
static _Thread_local uint32_t tl_fid_remaining = 0;

/* Claim a batch of fileids from the global counter: atomic ADD of
 * MDS_FILEID_BATCH on the FID key, read back the new counter value
 * (the mutation is visible in-transaction), commit.  Returns
 * [base, base+count) via out_base/out_count.  The counter is the
 * last-issued id, so the batch is (new-value - count, new-value].
 * The counter is seeded at 2 by fdb_ensure_root (0/1 reserved,
 * 2 = root); if it was ever missing, ADD treats it as 0 and the
 * clamping below skips the reserved ids.
 *
 * The read-back is read-your-writes (it must include this tx's own
 * ADD), so the FID key lands in the read set and concurrent claims
 * conflict with each other on it.  That conflict is BENIGN: a
 * conflicted tx commits nothing (no double-claim), and the retry
 * re-carves a fresh batch.  Claims are rare (one per 1024 creates
 * per thread), so this never touches the hot create path. */
static enum mds_status fdb_claim_fileids(FDBDatabase *db,
						   uint64_t *out_base,
						   uint32_t *out_count)
{
	uint8_t op[8];
	uint64_t newval = 0, base;
	uint32_t count = MDS_FILEID_BATCH;
	uint32_t attempt;

	fdb_put_u64_le(op, count);

	for (attempt = 0; attempt <= FDB_RETRY_MAX; attempt++) {
		FDBTransaction *tr = fdb_begin(db);
		fdb_error_t err;
		int rc;
		uint8_t buf[8];

		memset(buf, 0, sizeof(buf));
		fdb_transaction_atomic_op(tr, CNT_FID_KEY, CNT_FID_LEN, op, 8,
					  FDB_MUTATION_TYPE_ADD);
		rc = fdb_txn_get(tr, CNT_FID_KEY, CNT_FID_LEN, buf,
				 sizeof(buf));
		if (rc < 0) {
			fdb_transaction_destroy(tr);
			return MDS_ERR_IO;
		}
		newval = fdb_get_u64_le(buf);
		err = fdb_txn_commit_block(tr);
		if (err == 0) {
			fdb_transaction_destroy(tr);
			break;
		}
		/* Conflicted on the counter read: nothing committed,
		 * apply the recommended backoff, then re-claim. */
		fdb_conflict_backoff(tr, err);
		fdb_transaction_destroy(tr);
		if (attempt == FDB_RETRY_MAX)
			return MDS_ERR_DELAY;
	}

	/* Map the committed counter (newval = last-issued id) to the
	 * claimed batch (newval-count+1 .. newval], clamping the
	 * reserved ids 0/1/2.  NB: the clamp must compare NEWVAL, not
	 * the wrapped base -- uint64 underflow makes base a huge
	 * value for which `base < 3` is never true. */
	if (newval >= (uint64_t)count + 2) {
		base = newval - count + 1;
	} else {
		base = 3;
		count = (newval >= 2) ? (uint32_t)(newval - 2) : 0;
	}
	*out_base = base;
	*out_count = count;
	return MDS_OK;
}

/* Next fileid from this thread's batch pool; refills transparently. */
static enum mds_status fdb_alloc_fileid(FDBDatabase *db, uint64_t *out)
{
	if (tl_fid_remaining == 0) {
		enum mds_status st = fdb_claim_fileids(db, &tl_fid_base,
						       &tl_fid_remaining);
		if (st != MDS_OK)
			return st;
		if (tl_fid_remaining == 0)
			return MDS_ERR_IO; /* empty claim: nothing to give */
	}
	*out = tl_fid_base;
	tl_fid_base++;
	tl_fid_remaining--;
	return MDS_OK;
}

static enum mds_status fdb_ns_create(struct mds_catalogue *cat,
				     struct mds_cat_txn *txn, uint64_t parent,
				     const char *name, enum mds_file_type type,
				     uint32_t mode, uint64_t uid, uint64_t gid,
				     struct ds_prealloc_ctx *prealloc,
				     struct mds_inode *out)
{
	struct fdb_state *s = fdb_state(cat);
	size_t nlen = strlen(name);
	uint8_t dkey[2 + 8 + MDS_MAX_NAME + 1];
	uint8_t dval[9];
	uint8_t ibuf[RONDB_INODE_FIXED_SIZE];
	uint8_t nop[8], cop[8];
	uint8_t nkey[CNT_NL_LEN], ckey[CNT_CHG_LEN];
	struct mds_inode child;
	struct timespec now;
	uint64_t fid = 0;
	uint32_t attempt;

	(void)txn;
	if (s == NULL || out == NULL || nlen > MDS_MAX_NAME)
		return MDS_ERR_INVAL;
	if (type != MDS_FTYPE_REG && type != MDS_FTYPE_DIR)
		return MDS_ERR_INVAL;

	dkey[0] = 0x20;
	fdb_put_u64_be(dkey + 1, parent);
	memcpy(dkey + 9, name, nlen + 1);
	cnt_nlink_key(nkey, parent);
	cnt_change_key(ckey, parent);
	fdb_add_op(nop, (type == MDS_FTYPE_DIR) ? 1 : 0);
	fdb_add_op(cop, 1);

	if (type == MDS_FTYPE_REG && prealloc != NULL) {
		struct mds_ds_map_entry ds_entry;
		uint32_t stripe_unit = 0;
		uint64_t prealloc_fid = 0;
		if (ds_prealloc_pop(prealloc, &ds_entry, &stripe_unit,
				    &prealloc_fid) == 0 &&
		    prealloc_fid != 0) {
			fid = prealloc_fid;
		}
	}

	clock_gettime(CLOCK_REALTIME, &now);

	/* FID claim happens OUTSIDE the retry loop: the thread's batch
	 * pool hands out ids without touching FDB, and a retried
	 * attempt reuses the same fid (the pre-check re-runs inside). */
	if (fid == 0) {
		enum mds_status st = fdb_alloc_fileid(s->db, &fid);
		if (st != MDS_OK)
			return st;
	}

	for (attempt = 0; attempt <= FDB_RETRY_MAX; attempt++) {
		FDBTransaction *tr = fdb_begin(s->db);
		fdb_error_t err;
		uint8_t fbuf[8];
		uint8_t pkey[9];
		uint8_t ikey[17];
		int rc;

		fdb_put_u64_be(fbuf, fid);

		/* Existence pre-check (the insert itself is the
		 * authoritative check in a production backend). */
		rc = fdb_txn_get(tr, dkey, 9 + (uint32_t)nlen + 1,
				 dval, sizeof(dval));
		if (rc < 0) { fdb_transaction_destroy(tr); return MDS_ERR_IO; }
		if (rc > 0) { fdb_transaction_destroy(tr);
			       return MDS_ERR_EXISTS; }

		/* Parent hot counters: full-width atomic ADDs (H-01). */
		fdb_transaction_atomic_op(tr, nkey, CNT_NL_LEN, nop, 8,
					  FDB_MUTATION_TYPE_ADD);
		fdb_transaction_atomic_op(tr, ckey, CNT_CHG_LEN, cop, 8,
					  FDB_MUTATION_TYPE_ADD);

		/* New directory: seed its own NL/CHG counters from the
		 * wire baseline (nlink=2, change=1) so the counters are
		 * authoritative from birth and every later delta is a
		 * pure ADD.  Files carry no counters -- their wire
		 * image stays authoritative (cold keys). */
		if (type == MDS_FTYPE_DIR) {
			uint8_t cnkey[CNT_NL_LEN], cckey[CNT_CHG_LEN];
			uint8_t cval[8];
			cnt_nlink_key(cnkey, fid);
			cnt_change_key(cckey, fid);
			fdb_put_u64_le(cval, 2);
			fdb_transaction_set(tr, cnkey, CNT_NL_LEN, cval, 8);
			fdb_put_u64_le(cval, 1);
			fdb_transaction_set(tr, cckey, CNT_CHG_LEN, cval, 8);
		}

		/* Child inode (its own nlink: fresh, cold). */
		memset(&child, 0, sizeof(child));
		child.fileid = fid;
		child.type = type;
		child.mode = mode;
		child.uid = uid;
		child.gid = gid;
		child.nlink = (type == MDS_FTYPE_DIR) ? 2 : 1;
		child.atime = now;
		child.mtime = now;
		child.ctime = now;
		child.change = 1;
		child.generation = 1;
		child.parent_fileid = parent;
		if (rondb_inode_serialize(&child, 0, ibuf, sizeof(ibuf)) < 0) {
			fdb_transaction_destroy(tr);
			return MDS_ERR_IO;
		}
		pkey[0] = 0x10;
		fdb_put_u64_be(pkey + 1, fid);
		fdb_transaction_set(tr, pkey, 9, ibuf,
				    RONDB_INODE_FIXED_SIZE);

		/* Dirent: fileid(8,BE) | type(1). */
		memcpy(dval, fbuf, 8);
		dval[8] = (uint8_t)type;
		fdb_transaction_set(tr, dkey, 9 + (uint32_t)nlen + 1,
				    dval, 9);

		/* Secondary index 0x21 (dual-write, -05 §2.2):
		 * 0x21 | parent(8) | child_fileid(8) -> name(NUL).
		 * Carries the child-ordered ordering FDB cannot
		 * index natively (READDIR-by-fid, name-reuse probe). */
		ikey[0] = 0x21;
		fdb_put_u64_be(ikey + 1, parent);
		fdb_put_u64_be(ikey + 9, fid);
		fdb_transaction_set(tr, ikey, 17,
				    (const uint8_t *)name, nlen + 1);

		err = fdb_txn_commit_block(tr);
		if (err == 0) {
			fdb_transaction_destroy(tr);
			break;
		}
		fdb_conflict_backoff(tr, err);
		fdb_transaction_reset(tr);
		fdb_transaction_destroy(tr);
		if (attempt == FDB_RETRY_MAX)
			return MDS_ERR_DELAY; /* G2 spec */
	}

	{
		enum mds_status st = fdb_read_inode(s->db, fid, out);
		if (st != MDS_OK)
			return st;
	}
	catalog_stat_inc(&cat->stats.authority_writes);
	return MDS_OK;
}

/* -----------------------------------------------------------------------
 * ns_remove (non-directories; directory removal is out of the slice)
 * ----------------------------------------------------------------------- */

static enum mds_status fdb_ns_remove(struct mds_catalogue *cat,
				     struct mds_cat_txn *txn, uint64_t parent,
				     const char *name)
{
	struct fdb_state *s = fdb_state(cat);
	size_t nlen = strlen(name);
	uint8_t dkey[2 + 8 + MDS_MAX_NAME + 1];
	uint8_t dval[9];
	uint8_t ikey[9];
	uint8_t ibuf[RONDB_INODE_FIXED_SIZE];
	uint8_t nop[8], cop[8];
	uint8_t nkey[CNT_NL_LEN], ckey[CNT_CHG_LEN];
	struct mds_inode child;
	uint32_t attempt;

	(void)txn;
	if (s == NULL || nlen > MDS_MAX_NAME)
		return MDS_ERR_INVAL;

	dkey[0] = 0x20;
	fdb_put_u64_be(dkey + 1, parent);
	memcpy(dkey + 9, name, nlen + 1);
	cnt_nlink_key(nkey, parent);
	cnt_change_key(ckey, parent);
	fdb_add_op(cop, 1);

	for (attempt = 0; attempt <= FDB_RETRY_MAX; attempt++) {
		FDBTransaction *tr = fdb_begin(s->db);
		fdb_error_t err;
		int rc;
		uint64_t fid;
		int final;

		rc = fdb_txn_get(tr, dkey, 9 + (uint32_t)nlen + 1, dval, 9);
		if (rc < 0) { fdb_transaction_destroy(tr); return MDS_ERR_IO; }
		if (rc == 0) { fdb_transaction_destroy(tr);
			       return MDS_ERR_NOTFOUND; }
		fid = fdb_get_u64_be(dval);

		ikey[0] = 0x10;
		fdb_put_u64_be(ikey + 1, fid);
		rc = fdb_txn_get(tr, ikey, 9, ibuf, sizeof(ibuf));
		if (rc <= 0) { fdb_transaction_destroy(tr);
			       return (rc == 0) ? MDS_ERR_STALE : MDS_ERR_IO; }
		/* Secondary index key for the child (dual-clear). */
		{
			uint8_t xkey[17];
			xkey[0] = 0x21;
			fdb_put_u64_be(xkey + 1, parent);
			fdb_put_u64_be(xkey + 9, fid);
			fdb_transaction_clear(tr, xkey, 17);
		}
		if (rondb_inode_deserialize(ibuf, RONDB_INODE_FIXED_SIZE,
					    &child, NULL) != 0) {
			fdb_transaction_destroy(tr);
			return MDS_ERR_IO;
		}
		if (child.type == MDS_FTYPE_DIR) {
			fdb_transaction_destroy(tr);
			return MDS_ERR_NOSUPPORT; /* slice: no rmdir */
		}
		final = (child.nlink <= 1);
		if (final) {
			/* Last link: drop the child inode.  The parent
			 * NL counter moves ONLY for directory children
			 * (dir nlink = 2 + dir-children; file removal
			 * never touches it) -- mirrors
			 * catalogue_rondb.c:925-926
			 * (parent_nlink_delta = (type == DIR) ? -1 : 0). */
			fdb_transaction_clear(tr, ikey, 9);
			fdb_add_op(nop, (child.type == MDS_FTYPE_DIR)
					? -1 : 0);
		} else {
			/* Hardlink: decrement child nlink in its own
			 * (cold) inode; parent NL is unchanged. */
			child.nlink--;
			child.change++;
			{
				struct timespec now;
				clock_gettime(CLOCK_REALTIME, &now);
				child.ctime = now;
			}
			if (rondb_inode_serialize(&child, 0, ibuf,
						  sizeof(ibuf)) < 0) {
				fdb_transaction_destroy(tr);
				return MDS_ERR_IO;
			}
			fdb_transaction_set(tr, ikey, 9, ibuf,
					    RONDB_INODE_FIXED_SIZE);
			fdb_add_op(nop, 0);
		}
		fdb_transaction_atomic_op(tr, nkey, CNT_NL_LEN, nop, 8,
					  FDB_MUTATION_TYPE_ADD);
		fdb_transaction_atomic_op(tr, ckey, CNT_CHG_LEN, cop, 8,
					  FDB_MUTATION_TYPE_ADD);
		fdb_transaction_clear(tr, dkey, 9 + (uint32_t)nlen + 1);

		err = fdb_txn_commit_block(tr);
		if (err == 0) {
			fdb_transaction_destroy(tr);
			catalog_stat_inc(&cat->stats.authority_writes);
			return MDS_OK;
		}
		fdb_conflict_backoff(tr, err);
		fdb_transaction_destroy(tr);
		if (attempt == FDB_RETRY_MAX)
			return MDS_ERR_DELAY; /* G2 spec */
	}
	return MDS_ERR_DELAY;
}

/* -----------------------------------------------------------------------
 * ns_setattr (masked RMW on the inode wire image)
 * ----------------------------------------------------------------------- */

static enum mds_status fdb_ns_setattr(struct mds_catalogue *cat,
				      struct mds_cat_txn *txn, uint64_t fileid,
				      const struct mds_inode *attrs,
				      uint32_t mask)
{
	struct fdb_state *s = fdb_state(cat);
	uint8_t key[9];
	uint8_t buf[RONDB_INODE_FIXED_SIZE];
	struct mds_inode cur;
	struct timespec now;
	uint32_t attempt;

	(void)txn;
	if (s == NULL || attrs == NULL) return MDS_ERR_INVAL;

	key[0] = 0x10;
	fdb_put_u64_be(key + 1, fileid);

	for (attempt = 0; attempt <= FDB_RETRY_MAX; attempt++) {
		FDBTransaction *tr = fdb_begin(s->db);
		fdb_error_t err;
		int rc;

		rc = fdb_txn_get(tr, key, 9, buf, sizeof(buf));
		if (rc < 0) { fdb_transaction_destroy(tr); return MDS_ERR_IO; }
		if (rc == 0) { fdb_transaction_destroy(tr);
			       return MDS_ERR_NOTFOUND; }
		if (rondb_inode_deserialize(buf, RONDB_INODE_FIXED_SIZE,
					    &cur, NULL) != 0) {
			fdb_transaction_destroy(tr);
			return MDS_ERR_IO;
		}
		clock_gettime(CLOCK_REALTIME, &now);
		if (mask & MDS_ATTR_MODE)  cur.mode  = attrs->mode;
		if (mask & MDS_ATTR_UID)   cur.uid   = attrs->uid;
		if (mask & MDS_ATTR_GID)   cur.gid   = attrs->gid;
		if (mask & MDS_ATTR_SIZE)  cur.size  = attrs->size;
		if (mask & MDS_ATTR_SIZE_EXTEND &&
		    attrs->size > cur.size) cur.size = attrs->size;
		if (mask & MDS_ATTR_ATIME) cur.atime = attrs->atime;
		if (mask & MDS_ATTR_MTIME) cur.mtime = attrs->mtime;
		if (mask & MDS_ATTR_ATIME_NOW) cur.atime = now;
		if (mask & MDS_ATTR_MTIME_NOW) cur.mtime = now;
		if (mask & MDS_ATTR_FLAGS) cur.flags = attrs->flags;
		cur.change++;
		cur.ctime = now;
		if (rondb_inode_serialize(&cur, 0, buf, sizeof(buf)) < 0) {
			fdb_transaction_destroy(tr);
			return MDS_ERR_IO;
		}
		fdb_transaction_set(tr, key, 9, buf,
				    RONDB_INODE_FIXED_SIZE);

		err = fdb_txn_commit_block(tr);
		fdb_transaction_destroy(tr);
		if (err == 0) {
			catalog_stat_inc(&cat->stats.authority_writes);
			return MDS_OK;
		}
		fdb_conflict_backoff(tr, err);
		if (attempt == FDB_RETRY_MAX)
			return MDS_ERR_DELAY; /* G2 spec */
	}
	return MDS_ERR_DELAY;
}

/* -----------------------------------------------------------------------
 * ns_readdir
 *
 * start_after: name of the last entry of the previous page; NULL or ""
 * means start of the directory.  Entries are delivered in name order
 * (the dirent key space is parent | name).  The cookie model here is
 * NAME-based; the 0x21 child-fileid index (cookie-by-fileid) is part
 * of the hot-path task, not this slice.
 * ----------------------------------------------------------------------- */

static enum mds_status fdb_ns_readdir(struct mds_catalogue *cat,
				      uint64_t parent,
				      const char *start_after,
				      uint32_t max_entries,
				      struct mds_cat_txn *txn,
				      mds_readdir_cb cb, void *ctx)
{
	struct fdb_state *s = fdb_state(cat);
	uint8_t begin[10];
	uint8_t end[10];
	uint32_t delivered = 0;
	int i;

	(void)txn;
	if (s == NULL || cb == NULL) return MDS_ERR_INVAL;

	begin[0] = 0x20;
	fdb_put_u64_be(begin + 1, parent);
	/* Exclusive upper bound: 0x20 | (parent+1).  All of this
	 * parent's dirents are 0x20 | parent | name (name first byte
	 * non-zero, NUL-terminated), so the next parent's prefix is
	 * strictly greater than every key in this range.  Wrap guard:
	 * at max parent the bound is the next key band (0x21). */
	if (parent == UINT64_MAX) {
		end[0] = 0x21;
	} else {
		end[0] = 0x20;
		fdb_put_u64_be(end + 1, parent + 1);
	}

	/* Range-read the whole parent prefix, then page in memory.
	 * Slice scale: directories fit in one range fetch. */
	{
		FDBTransaction *tr = fdb_begin(s->db);
		FDBFuture *f;
		FDBKeyValue const *kvs = NULL;
		int count = 0;
		fdb_bool_t more = 0;

		/* Selectors per fdb_c.h KEYSEL macros (verified
		 * empirically on the lab cluster):
		 * begin = FIRST_GREATER_OR_EQUAL(0x20|parent)  -> (0,1)
		 * end   = FIRST_GREATER_OR_EQUAL(0x20|parent+1) -> (0,1)
		 * (exclusive end at the next parent's prefix).
		 * NB: LAST_LESS_THAN (0,0) as end would silently drop
		 * the last dirent of this parent. */
		f = fdb_transaction_get_range(tr, begin, 9, 0, 1,
					      end, 9, 0, 1,
					      0, 0, FDB_STREAMING_MODE_WANT_ALL,
					      0, 0, 0);
		fdb_future_block_until_ready(f);
		if (fdb_future_get_error(f) != 0) {
			fdb_future_destroy(f);
			fdb_transaction_destroy(tr);
			return MDS_ERR_IO;
		}
		if (fdb_future_get_keyvalue_array(f, &kvs, &count, &more) != 0) {
			fdb_future_destroy(f);
			fdb_transaction_destroy(tr);
			return MDS_ERR_IO;
		}
		/* kvs[] is valid until the future is destroyed. */
		for (i = 0; i < count; i++) {
			const uint8_t *k = kvs[i].key;
			const uint8_t *v = kvs[i].value;
			uint32_t klen = (uint32_t)kvs[i].key_length;
			uint32_t vlen = (uint32_t)kvs[i].value_length;
			const char *nm = (const char *)(k + 9);
			size_t nlen = (size_t)klen - 9;
			struct mds_cat_dirent entry;
			int stop;

			if (nlen == 0 || nlen > MDS_MAX_NAME || vlen < 9)
				continue; /* malformed: skip defensively */
			if (nlen == 1 && nm[0] == '.')
				continue; /* '.'/'..' sentinels (not stored) */
			/* name segment in the key is NUL-terminated, so nlen
			 * includes the NUL; the cookie compares the name
			 * WITHOUT it (nlen-1). */
			if (start_after != NULL && start_after[0] != '\0' &&
			    (nlen - 1) == strlen(start_after) &&
			    memcmp(nm, start_after, nlen - 1) == 0)
				continue; /* resume strictly after cookie */
			memset(&entry, 0, sizeof(entry));
			entry.fileid = fdb_get_u64_be(v);
			entry.type = v[8];
			memcpy(entry.name, nm, nlen);
			entry.name[nlen] = '\0';
			if (delivered >= max_entries) {
				stop = 1;
			} else {
				delivered++;
				stop = cb(&entry, ctx);
			}
			if (stop)
				break;
		}
		fdb_future_destroy(f);
		fdb_transaction_destroy(tr);
	}
	return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

static void *fdb_net_thread(void *arg)
{
	(void)arg;
	fdb_run_network();
	return NULL;
}

/* The FDB network is a process-wide singleton: setup/stop must each
 * happen at most once per process, so the backend tracks it in a
 * static rather than per-catalogue state.  close() therefore never
 * stops the network -- a second catalogue (or a harness reopen) must
 * be able to open on the same running network.  The network lives
 * until process exit, which is the MDS lifecycle anyway. */
static int g_fdb_net_started = 0;

static void fdb_close_backend(struct mds_catalogue *cat)
{
	struct fdb_state *s = fdb_state(cat);

	if (s == NULL) return;
	if (s->db != NULL)
		fdb_database_destroy(s->db);
	free(s);
	cat->backend_private = NULL;
}

static enum mds_status fdb_probe_backend(struct mds_catalogue *cat)
{
	struct fdb_state *s = fdb_state(cat);
	uint8_t buf[8];
	int rc;

	if (s == NULL || s->db == NULL) return MDS_ERR_IO;
	rc = fdb_txn_get(fdb_begin(s->db), CNT_FID_KEY, CNT_FID_LEN,
			 buf, sizeof(buf));
	return (rc >= 0) ? MDS_OK : MDS_ERR_IO;
}

static const struct mds_catalogue_ops fdb_catalogue_ops = {
	.close = fdb_close_backend,
	.probe = fdb_probe_backend,
};

/* Only the six vertical-slice slots are filled.  Everything else stays
 * NULL -> MDS_ERR_NOSUPPORT -> NFS4ERR_NOTSUPP (G1). */
static const struct mds_authority_ops fdb_authority_ops = {
	.ns_create = fdb_ns_create,
	.ns_remove = fdb_ns_remove,
	.ns_lookup = fdb_ns_lookup,
	.ns_getattr = fdb_ns_getattr,
	.ns_setattr = fdb_ns_setattr,
	.ns_readdir = fdb_ns_readdir,
};

/* Ensure the root inode (fileid 2) + its NL/CHG counters exist;
 * idempotent. */
static int fdb_ensure_root(FDBDatabase *db)
{
	uint8_t ikey[9];
	uint8_t buf[RONDB_INODE_FIXED_SIZE];
	uint8_t cbuf[8];
	uint8_t nkey[CNT_NL_LEN], ckey[CNT_CHG_LEN];
	struct mds_inode root;
	struct timespec now;
	FDBTransaction *tr;
	fdb_error_t e;
	int rc;

	ikey[0] = 0x10;
	fdb_put_u64_be(ikey + 1, 2);
	rc = fdb_txn_get(fdb_begin(db), ikey, 9, buf, sizeof(buf));
	if (rc > 0) return 0; /* already there */
	if (rc < 0) return -1;

	clock_gettime(CLOCK_REALTIME, &now);
	memset(&root, 0, sizeof(root));
	root.fileid = 2;
	root.type = MDS_FTYPE_DIR;
	root.mode = 0755;
	root.nlink = 2;
	root.atime = now;
	root.mtime = now;
	root.ctime = now;
	root.change = 1;
	root.generation = 1;
	root.parent_fileid = 2;
	if (rondb_inode_serialize(&root, 0, buf, sizeof(buf)) < 0)
		return -1;

	cnt_nlink_key(nkey, 2);
	cnt_change_key(ckey, 2);
	tr = fdb_begin(db);
	fdb_transaction_set(tr, ikey, 9, buf, RONDB_INODE_FIXED_SIZE);
	fdb_put_u64_le(cbuf, 2); /* root nlink = 2 */
	fdb_transaction_set(tr, nkey, CNT_NL_LEN, cbuf, 8);
	fdb_put_u64_le(cbuf, 1);
	fdb_transaction_set(tr, ckey, CNT_CHG_LEN, cbuf, 8);
	/* Seed the fileid counter at 2 so the first allocation is 3
	 * (0 and 1 are reserved; 2 is root).  Without this the first two
	 * allocations would hand out 1 and 2 -- colliding with root's
	 * inode key on the second create. */
	fdb_put_u64_le(cbuf, 2);
	fdb_transaction_set(tr, CNT_FID_KEY, CNT_FID_LEN, cbuf, 8);
	e = fdb_txn_commit_block(tr);
	fdb_transaction_destroy(tr);
	return (e == 0) ? 0 : -1;
}

/* Test-only: clear the three slice keyspaces so a harness run starts
 * from a pristine state.  Never call from the MDS proper. */
enum mds_status catalogue_fdb_wipe(struct mds_catalogue *cat)
{
	struct fdb_state *s = fdb_state(cat);
	uint8_t b0[1] = {0x00}, e0[1] = {0x01};
	uint8_t b1[1] = {0x10}, e1[1] = {0x11};
	uint8_t b2[1] = {0x20}, e2[1] = {0x21};
	uint8_t b3[1] = {0x21}, e3[1] = {0x22};
	FDBTransaction *tr;
	fdb_error_t e;

	if (s == NULL || s->db == NULL) return MDS_ERR_INVAL;
	tr = fdb_begin(s->db);
	fdb_transaction_clear_range(tr, b0, 1, e0, 1);
	fdb_transaction_clear_range(tr, b1, 1, e1, 1);
	fdb_transaction_clear_range(tr, b2, 1, e2, 1);
	fdb_transaction_clear_range(tr, b3, 1, e3, 1);
	e = fdb_txn_commit_block(tr);
	fdb_transaction_destroy(tr);
	if (e != 0) return MDS_ERR_IO;
	/* Re-seed root + counters so the caller sees a valid empty FS. */
	return fdb_ensure_root(s->db) == 0 ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_fdb_open(const struct mds_config *cfg,
				   struct mds_catalogue **out)
{
	struct mds_catalogue *cat;
	struct fdb_state *s;
	fdb_error_t e;
	const char *cluster_file;
	static int g_fdb_api_selected = 0;

	if (cfg == NULL || out == NULL) return MDS_ERR_INVAL;
	cluster_file = (cfg->catalogue_backend_conf[0] != '\0')
		? cfg->catalogue_backend_conf : "/etc/foundationdb/fdb.cluster";

	s = calloc(1, sizeof(*s));
	cat = calloc(1, sizeof(*cat));
	if (s == NULL || cat == NULL) {
		free(s);
		free(cat);
		return MDS_ERR_NOMEM;
	}

	if (!g_fdb_api_selected) {
		e = fdb_select_api_version(FDB_API_VERSION);
		if (e != 0) {
			MDS_LOG_ERROR(LOG_COMP_CAT,
				      "fdb_select_api_version: %s",
				      fdb_get_error(e));
			goto fail;
		}
		g_fdb_api_selected = 1;
	}
	if (!g_fdb_net_started) {
		e = fdb_setup_network();
		if (e != 0) {
			MDS_LOG_ERROR(LOG_COMP_CAT, "fdb_setup_network: %s",
				      fdb_get_error(e));
			goto fail;
		}
		if (pthread_create(&s->net_thread, NULL, fdb_net_thread,
				   NULL) != 0) {
			/* Leave the network up (singleton); a later
			 * open may still succeed. */
			goto fail;
		}
		g_fdb_net_started = 1;
	}

	e = fdb_create_database(cluster_file, &s->db);
	if (e != 0) {
		MDS_LOG_ERROR(LOG_COMP_CAT, "fdb_create_database(%s): %s",
			      cluster_file, fdb_get_error(e));
		goto fail;
	}

	if (fdb_ensure_root(s->db) != 0) {
		MDS_LOG_ERROR(LOG_COMP_CAT,
			      "fdb: failed to create root inode");
		fdb_database_destroy(s->db);
		goto fail;
	}

	cat->backend = MDS_BACKEND_FDB;
	cat->auth_ops = &fdb_authority_ops;
	cat->coord_ops = NULL; /* coordination slots: NOSUPPORT in slice */
	cat->ops = &fdb_catalogue_ops;
	cat->backend_private = s;
	*out = cat;
	MDS_LOG_INFO(LOG_COMP_CAT,
		     "FDB catalogue backend open (vertical slice), cluster=%s",
		     cluster_file);
	return MDS_OK;

fail:
	free(s);
	free(cat);
	return MDS_ERR_IO;
}
