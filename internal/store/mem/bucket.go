// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package mem

import (
	"bytes"
	"fmt"

	"blockwatch.cc/knoxdb/internal/store"
)

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the store.Bucket interface.
type bucket struct {
	tx  *transaction
	id  [bucketIdLen]byte
	key []byte
	seq store.Sequence
}

// Enforce bucket implements the store.Bucket interface.
var _ store.Bucket = (*bucket)(nil)

// bucketizedKey returns the actual key to use for storing and retrieving a key
// for the provided bucket ID.  This is required because bucketizing is handled
// through the use of a unique prefix per bucket.
func bucketizedKey(bucketID [bucketIdLen]byte, key []byte) []byte {
	// The serialized block index key format is:
	//   <bucketid><key>
	bKey := make([]byte, bucketIdLen+len(key))
	copy(bKey, bucketID[:])
	copy(bKey[bucketIdLen:], key)
	return bKey
}

// Bucket retrieves a nested bucket with the given key.  Returns nil if
// the bucket does not exist.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Bucket(key []byte) store.Bucket {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// Attempt to fetch the ID for the child bucket.  The bucket does not
	// exist if the bucket index entry does not exist.
	childKey := bucketizedKey(b.id, key)
	childID, ok := b.tx.db.bucketIds[string(childKey)]
	if !ok {
		return nil
	}

	return &bucket{tx: b.tx, id: childID, key: childKey}
}

// CreateBucket creates and returns a new nested bucket with the given key.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketExists if the bucket already exists
//   - ErrBucketNameRequired if the key is empty
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//   - ErrTxConflict if the next bucket sequence overflows
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) CreateBucket(key []byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "create bucket requires a writable database transaction"
		return nil, makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		str := "create bucket requires a key"
		return nil, makeDbErr(store.ErrBucketNameRequired, str, nil)
	}

	// Ensure bucket does not already exist.
	childKey := bucketizedKey(b.id, key)
	if _, ok := b.tx.db.bucketIds[string(childKey)]; ok {
		str := "bucket already exists"
		return nil, makeDbErr(store.ErrBucketExists, str, nil)
	}

	// Find the appropriate next bucket ID to use for the new bucket.
	var err error
	childID, err := b.tx.nextBucketID()
	if err != nil {
		return nil, err
	}

	// Add the new bucket to the bucket index.
	log.Debugf("Creating bucket %q with id 0x%x", string(key), childID)
	b.tx.db.bucketIds[string(childKey)] = childID

	return &bucket{tx: b.tx, id: childID, key: childKey}, nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with the
// given key if it does not already exist.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketNameRequired if the key is empty
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//   - ErrTxConflict if the next bucket sequence overflows
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) CreateBucketIfNotExists(key []byte) (store.Bucket, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "create bucket requires a writable database transaction"
		return nil, makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Return existing bucket if it already exists, otherwise create it.
	if bucket := b.Bucket(key); bucket != nil {
		return bucket, nil
	}
	return b.CreateBucket(key)
}

// DeleteBucket removes a nested bucket with the given key.
//
// Returns the following errors as required by the interface contract:
//   - ErrBucketNotFound if the specified bucket does not exist
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) DeleteBucket(key []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "delete bucket requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Attempt to fetch the ID for the child bucket.  The bucket does not
	// exist if the bucket index entry does not exist.  In the case of the
	// special internal block index, keep the fixed ID.
	childKey := bucketizedKey(b.id, key)
	childID, ok := b.tx.db.bucketIds[string(childKey)]
	if !ok {
		str := fmt.Sprintf("bucket %q does not exist", key)
		return makeDbErr(store.ErrBucketNotFound, str, nil)
	}

	log.Debugf("Deleting bucket %q with id 0x%x", string(key), childID)

	// Remove all nested buckets and their keys.
	childIDs := [][bucketIdLen]byte{childID}
	for len(childIDs) > 0 {
		childID = childIDs[len(childIDs)-1]
		childIDs = childIDs[:len(childIDs)-1]

		log.Debugf("Deleting nested bucket id 0x%x", childID)

		// Mark all non-bucket keys for deletion
		rng := store.BytesPrefix(childID[:])
		b.tx.db.store.AscendRange(Item{rng.Start, nil}, Item{rng.Limit, nil}, func(t Item) bool {
			b.tx.deletes[string(t.Key)] = struct{}{}
			return true
		})

		// Iterate through all nested buckets.
		for n, v := range b.tx.db.bucketIds {
			if !bytes.HasPrefix([]byte(n), childID[:]) {
				continue
			}

			// Push the id of the nested bucket onto the stack for
			// the next iteration.
			childIDs = append(childIDs, v)

			// Remove the nested bucket from the bucket index.
			delete(b.tx.db.bucketIds, n)
		}
	}

	// Remove the nested bucket from the bucket index. Any buckets nested
	// under it were already removed above.
	delete(b.tx.db.bucketIds, string(childKey))

	return nil
}

// Cursor returns a new cursor, allowing for iteration over the bucket's
// key/value pairs in forward or backward order.
//
// You must seek to a position using the First, Last, or Seek functions before
// calling the Next, Prev, Key, or Value functions.  Failure to do so will
// result in the same return values as an exhausted cursor, which is false for
// the Prev and Next functions and nil for Key and Value functions.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Cursor(_ ...store.CursorOptions) store.Cursor {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	// Create the cursor. The user must ensure to close all cursors
	// before tx commit or rollback.
	return newCursor(b, b.id[:])
}

// Range returns a new ranged cursor, allowing for iteration over the
// bucket's key/value pairs (and nested buckets) that satisfy the prefix
// condition in forward or backward order.
//
// This cursor automatically seeks to the first key that satisfies prefix
// stops when the next key does not match the prefix. Its sufficient to
// only use Next, but you can reset the cursor with First, Last and Seek,
// however, calls to these functions consider the original prefix.
func (b *bucket) Range(prefix []byte, _ ...store.CursorOptions) store.Cursor {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}

	// Create the cursor with custom prefix. User must close this cursor
	// before tx commit/rollback.
	return newCursor(b, append(b.id[:], prefix...))
}

// ForEach invokes the passed function with every key/value pair in the bucket.
// This does not include nested buckets or the key/value pairs within those
// nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys and/or values.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Invoke the callback for each cursor item.  Return the error returned
	// from the callback when it is non-nil.
	c := newCursor(b, b.id[:])
	defer c.Close()
	for ok := c.First(); ok; ok = c.Next() {
		err := fn(c.Key(), c.Value())
		if err != nil {
			return err
		}
	}

	return nil
}

// ForEachBucket invokes the passed function with every nested bucket in the
// current bucket. This does not include any nested buckets within those
// nested buckets.
//
// WARNING: It is not safe to mutate data while iterating with this method.
// Doing so may cause the underlying cursor to be invalidated and return
// unexpected keys.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// NOTE: The values returned by this function are only valid during a
// transaction.  Attempting to access them after a transaction has ended will
// likely result in an access violation.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) ForEachBucket(fn func(k []byte, b store.Bucket) error) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Invoke the callback for each child bucket.  Return the error returned
	// from the callback when it is non-nil.
	for n, v := range b.tx.db.bucketIds {
		childKey := []byte(n)
		if !bytes.HasPrefix(childKey, b.id[:]) {
			continue
		}
		key := copySlice(childKey[bucketIdLen:])
		bucket := &bucket{tx: b.tx, id: v, key: key}
		err := fn(key, bucket)
		if err != nil {
			return err
		}
	}

	return nil
}

// Writable returns whether or not the bucket is writable.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Writable() bool {
	return b.tx.writable
}

// Put saves the specified key/value pair to the bucket.  Keys that do not
// already exist are added and keys that already exist are overwritten.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Put(key, value []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "setting a key requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Ensure a key was provided.
	if len(key) == 0 {
		str := "put requires a key"
		return makeDbErr(store.ErrKeyRequired, str, nil)
	}

	// allow user-defined overrides
	if b.tx.db.opts.PutCallback != nil {
		var err error
		key, value, err = b.tx.db.opts.PutCallback(key, value)
		if err != nil {
			return err
		}
	}

	effectiveKey := bucketizedKey(b.id, key)
	effectiveKeyString := string(effectiveKey)

	// store a copy of key/value for insert/update
	b.tx.updates[effectiveKeyString] = copySlice(value)

	// if key was previously deleted in the same tx, remove from deletes map
	delete(b.tx.deletes, effectiveKeyString)

	return nil
}

// Get returns the value for the given key.  Returns nil if the key does not
// exist in this bucket.  An empty slice is returned for keys that exist but
// have no value assigned.
//
// NOTE: The value returned by this function is only valid during a transaction.
// Attempting to access it after a transaction has ended results in undefined
// behavior.  Additionally, the value must NOT be modified by the caller.
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Get(key []byte) []byte {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}

	// Nothing to return if there is no key.
	if len(key) == 0 {
		return nil
	}

	effectiveKey := bucketizedKey(b.id, key)
	effectiveKeyString := string(effectiveKey)

	// first attempt to find the key in the tx updates list
	val, ok := b.tx.updates[effectiveKeyString]
	if ok {
		if b.tx.db.opts.GetCallback != nil {
			return b.tx.db.opts.GetCallback(key, val)
		}
		return val
	}

	// check if key was deleted
	if _, ok := b.tx.deletes[effectiveKeyString]; ok {
		return nil
	}

	// lookup in btree
	item, ok := b.tx.db.store.Get(Item{effectiveKey, nil})
	if !ok || item.Key == nil {
		return nil
	}

	if b.tx.db.opts.GetCallback != nil {
		return b.tx.db.opts.GetCallback(key, item.Val)
	}
	return item.Val
}

// Delete removes the specified key from the bucket.  Deleting a key that does
// not exist does not return an error.
//
// Returns the following errors as required by the interface contract:
//   - ErrKeyRequired if the key is empty
//   - ErrIncompatibleValue if the key is the same as an existing bucket
//   - ErrTxNotWritable if attempted against a read-only transaction
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the store.Bucket interface implementation.
func (b *bucket) Delete(key []byte) error {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "deleting a value requires a writable database transaction"
		return makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Nothing to do if there is no key.
	if len(key) == 0 {
		return makeDbErr(store.ErrKeyRequired, "missing key", nil)
	}

	// allow user-defined overrides
	if b.tx.db.opts.DeleteCallback != nil {
		var err error
		key, err = b.tx.db.opts.DeleteCallback(key)
		if err != nil {
			return err
		}
	}

	effectiveKey := bucketizedKey(b.id, key)
	effectiveKeyString := string(effectiveKey)

	// remove key from pending updates
	delete(b.tx.updates, effectiveKeyString)

	// add key to pending deletes
	b.tx.deletes[effectiveKeyString] = struct{}{}

	return nil
}

func (b *bucket) NextSequence() (uint64, error) {
	// Ensure transaction state is valid.
	if err := b.tx.checkClosed(); err != nil {
		return 0, err
	}

	// Ensure the transaction is writable.
	if !b.tx.writable {
		str := "deleting a value requires a writable database transaction"
		return 0, makeDbErr(store.ErrTxNotWritable, str, nil)
	}

	// Create new bucket-specific sequence
	if b.seq == nil {
		var err error
		b.seq, err = b.tx.db.Sequence(b.key, 1)
		if err != nil {
			return 0, err
		}
	}
	return b.seq.Next()
}

func (_ *bucket) FillPercent(_ float64) {
	// unsupported
}

func (b *bucket) Stats() store.BucketStats {
	stats := store.BucketStats{
		BucketN: 1,
	}
	if err := b.tx.checkClosed(); err != nil {
		return stats
	}

	if err := b.ForEachBucket(func(_ []byte, _ store.Bucket) error {
		stats.BucketN++
		return nil
	}); err != nil {
		return stats
	}

	// counting keys and data size is too expensive
	return stats
}
