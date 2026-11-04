package main

import bolt "go.etcd.io/bbolt"

// Count before allocating so every newly parallel snapshot hash vector is
// charged, including append's old/new capacity overlap.
func (s *store) snapshotHashes(tx *bolt.Tx, tip [32]byte, owner [32]byte) ([][32]byte, func(), error) {
	count := 0
	var zero [32]byte
	for node := tip; node != zero; {
		record := tx.Bucket(bucketNodes).Get(node[:])
		if len(record) != 104 || string(record[64:96]) != string(owner[:]) {
			return nil, nil, errMissingItem
		}
		count++
		copy(node[:], record[:32])
	}
	release := func() {}
	if s.inputWork != nil {
		var err error
		release, err = s.inputWork.reserveInput(int64(count)*96 + 128)
		if err != nil {
			return nil, nil, err
		}
	}
	hashes, err := s.historyHashes(tx, tip, owner)
	if err != nil {
		release()
		return nil, nil, err
	}
	return hashes, release, nil
}
