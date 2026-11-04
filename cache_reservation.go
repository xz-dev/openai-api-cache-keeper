package main

// The serialized store writer reserves its complete resulting live-byte
// ceiling, not old live bytes plus a duplicate full-history increment.
func (k *keeper) reserveHistoryCache(encoded int64) (int64, func()) {
	if k.cfg.maxStoreBytes <= 0 {
		return -1, func() {}
	}
	k.cacheMu.Lock()
	_, history := k.store.stats()
	limit := min(max(int64(0), k.cfg.maxStoreBytes-k.eventBytes-k.cachePending), history+encoded)
	k.cacheHistoryReserve = limit
	k.cacheMu.Unlock()
	return limit, func() {
		k.cacheMu.Lock()
		k.cacheHistoryReserve = 0
		k.cacheMu.Unlock()
	}
}
