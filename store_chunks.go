package main

import (
	"bytes"
	"encoding/binary"
	"io"

	bolt "go.etcd.io/bbolt"
)

// A history item up to itemChunkBytes is one value in `items`, the record
// format every store has always used. A larger item is ordered chunks in
// `itemchunks` keyed hash|index (all but the last exactly itemChunkBytes),
// each written in a bounded write: one bbolt Put of a 51 MiB value made
// bbolt allocate about 3x its size in heap at commit. Its `itemrefs` record
// is written with the last chunk, so a reader never sees a partial item.
const itemChunkBytes = 1 << 20

// storeWriteBatch bounds the payload one staging write transaction carries.
const storeWriteBatch = 4 << 20

var bucketItemChunks = []byte("itemchunks")

func chunkKey(hash []byte, i int64) []byte {
	k := make([]byte, 36)
	copy(k, hash)
	binary.BigEndian.PutUint32(k[32:], uint32(i))
	return k
}

// storedItemSize reports a stored item's payload length in either format.
func storedItemSize(tx *bolt.Tx, hash []byte) (int64, error) {
	if v := tx.Bucket(bucketItems).Get(hash); v != nil {
		return int64(len(v)), nil
	}
	r := tx.Bucket(bucketItemRefs).Get(hash)
	if len(r) != 16 || tx.Bucket(bucketItemChunks).Get(chunkKey(hash, 0)) == nil {
		return 0, errMissingItem
	}
	return number(r[:8]), nil
}

// readStoredItem copies item bytes from off into dst inside tx; it returns
// at most one chunk per call, never zero bytes before the end.
func readStoredItem(tx *bolt.Tx, hash []byte, off int64, dst []byte) (int, error) {
	if v := tx.Bucket(bucketItems).Get(hash); v != nil {
		if off > int64(len(v)) {
			return 0, errMissingItem
		}
		return copy(dst, v[off:]), nil
	}
	c := tx.Bucket(bucketItemChunks).Get(chunkKey(hash, off/itemChunkBytes))
	in := off % itemChunkBytes
	if c == nil || in >= int64(len(c)) {
		return 0, errMissingItem
	}
	return copy(dst, c[in:]), nil
}

// loadStoredItem copies a whole item out of tx (test and debug paths only).
func loadStoredItem(tx *bolt.Tx, hash []byte) ([]byte, error) {
	size, err := storedItemSize(tx, hash)
	if err != nil {
		return nil, err
	}
	out := make([]byte, size)
	for off := int64(0); off < size; {
		n, err := readStoredItem(tx, hash, off, out[off:])
		if err != nil {
			return nil, err
		}
		off += int64(n)
	}
	return out, nil
}

// deleteStoredItem removes an item's payload in either format, including
// the chunks of a partial write.
func deleteStoredItem(tx *bolt.Tx, hash []byte) error {
	if err := tx.Bucket(bucketItems).Delete(hash); err != nil {
		return err
	}
	chunks := tx.Bucket(bucketItemChunks)
	var keys [][]byte
	c := chunks.Cursor()
	for k, _ := c.Seek(hash); k != nil && bytes.HasPrefix(k, hash); k, _ = c.Next() {
		keys = append(keys, append([]byte(nil), k...))
	}
	for _, k := range keys {
		if err := chunks.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// itemStream yields the next history item as n bytes read from r; r is
// valid until the next call. It returns io.EOF when drained.
type itemStream func() (n int64, r io.Reader, err error)
type itemStreamFactory func() itemStream

// streamItems adapts a whole-item source to the streaming interface.
func streamItems(source itemFactory) itemStreamFactory {
	return func() itemStream {
		next := source()
		return func() (int64, io.Reader, error) {
			item, err := next(nil)
			if err != nil {
				return 0, nil, err
			}
			return int64(len(item)), bytes.NewReader(item), nil
		}
	}
}
