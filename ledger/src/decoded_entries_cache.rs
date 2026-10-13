//! An in-memory, best-effort cache of already-decoded completed data sets.
//!
//! `window_service::run_insert` (in `solana-core`) keeps the raw shred payloads
//! it has just inserted into the blockstore in memory for a short while (see
//! its `ShredPayloadCache`). When a data set completes and every one of its
//! shreds is still in that cache, it decodes the entries right there and
//! [`insert`]s them here directly. `Blockstore::get_slot_components_with_shred_info_fast`
//! (used by `confirm_slot` in `blockstore_processor.rs`) checks this cache
//! before falling back to its normal RocksDB-backed read, letting ReplayStage
//! skip re-reading and re-deshredding shred payloads it, through
//! window_service, already had in memory a moment earlier.
//!
//! This is purely a latency optimization, never a correctness dependency:
//! every lookup that misses (a `BlockMarker` range, a cache eviction, replay
//! falling behind window_service, or no decoded entry ever produced) falls
//! back to the exact, unchanged RocksDB-backed path. See
//! `docs/perf/shred-to-geyser-prometheus-plan.md`, "Phase 3, hypothesis 5".
//!
//! Eviction is capacity-based (see [`MAX_CACHED_DATA_SETS`]) and entirely
//! self-contained -- it is deliberately NOT tied to `window_service`'s own
//! `ShredPayloadCache` recency window. An earlier version of this cache made
//! that mistake: `ShredPayloadCache` only needs to hold raw shred payloads
//! from "not yet complete" to "decoded the instant the last shred arrives"
//! (a short window, hence its small 16-slot cap), but THIS cache needs to
//! hold already-decoded entries from "decoded" until replay actually calls
//! `confirm_slot` for that slot -- which can lag much further behind,
//! especially while catching up. Coupling the two silently shrank this
//! cache's effective window and measurably hurt the hit rate in production
//! (~45% -> ~21% at steady state) before being caught and reverted; see the
//! plan doc's "hypothesis 5" section for the numbers.
//!
//! A second bug, caught right after fixing the first one, kept the hit rate
//! flat at ~22% even once eviction was decoupled: `BoundedCache::insert`'s
//! `insertion_order` queue grew on every call, including ones that only
//! overwrote an already-present key (the same `(slot, start_index)` decoded
//! and inserted more than once before replay consumed it). That inflated
//! `order.len()` past the `DashMap`'s real distinct-key count, so the
//! eviction loop fired far more often than the `cap` intended -- including
//! evicting entries that had just been refreshed. Fixed by only pushing to
//! `insertion_order` when the key is genuinely new (`DashMap::insert`
//! returned `None`).

use {
    dashmap::DashMap,
    solana_clock::Slot,
    solana_entry::entry::Entry,
    std::{
        collections::VecDeque,
        ops::Range,
        sync::{LazyLock, Mutex},
    },
};

/// Hard cap on the number of cached data sets, so a replay stall (or a bug)
/// can't grow this cache unboundedly. Comfortably above normal in-flight
/// counts; see the module docs for why this is sized independently of
/// `window_service`'s own `ShredPayloadCache`.
const MAX_CACHED_DATA_SETS: usize = 4096;

/// A `DashMap` of decoded data sets plus an insertion-order record of its
/// keys, so a full-cache eviction can pop the oldest key in O(1) instead of
/// sorting the whole keyset. The order queue is written only by `insert` --
/// in production, only ever called from the single
/// `window_service::run_insert` writer thread -- and never read by
/// `take_if_fully_cached`, so its `Mutex` is never contended by readers and
/// is effectively uncontended on the writer side too.
struct BoundedCache {
    cache: DashMap<(Slot, u32), Vec<Entry>>,
    insertion_order: Mutex<VecDeque<(Slot, u32)>>,
    cap: usize,
}

impl BoundedCache {
    fn new(cap: usize) -> Self {
        Self {
            cache: DashMap::new(),
            insertion_order: Mutex::new(VecDeque::with_capacity(cap)),
            cap,
        }
    }

    fn insert(&self, key: (Slot, u32), entries: Vec<Entry>) {
        // Only a genuinely new key grows `insertion_order`. Without this
        // check, re-inserting an already-present key (overwriting its value)
        // would push a second order entry for the same key, inflating
        // `order.len()` past the `DashMap`'s actual distinct-key count and
        // making the eviction loop below evict far more aggressively than
        // the `cap` intends -- including entries that were just refreshed.
        // This is exactly what happened in production: see the module docs.
        let is_new_key = self.cache.insert(key, entries).is_none();
        if !is_new_key {
            return;
        }
        let mut order = self.insertion_order.lock().unwrap();
        order.push_back(key);
        while order.len() > self.cap {
            if let Some(oldest) = order.pop_front() {
                self.cache.remove(&oldest);
            }
        }
    }

    fn take_if_fully_cached(&self, slot: Slot, ranges: &[Range<u32>]) -> Option<Vec<Vec<Entry>>> {
        let hit = ranges.iter().all(|r| self.cache.contains_key(&(slot, r.start)));
        solana_metrics::pipeline_metrics::DECODED_ENTRIES_CACHE_TOTAL
            .with_label_values(&[if hit { "hit" } else { "miss" }])
            .inc();
        if !hit {
            return None;
        }
        // Re-checked via `remove`, not just the `contains_key` above: in the
        // rare case of two concurrent lookups for the same slot racing on
        // completion, one of these removes can still come back empty even
        // though the `contains_key` scan above saw it. Falling back to
        // `None` (and losing whichever entries this call *did* remove along
        // the way) is safe -- RocksDB remains the source of truth -- just
        // not maximally efficient for that vanishingly rare race.
        ranges
            .iter()
            .map(|r| self.cache.remove(&(slot, r.start)).map(|(_, entries)| entries))
            .collect()
    }
}

static CACHE: LazyLock<BoundedCache> = LazyLock::new(|| BoundedCache::new(MAX_CACHED_DATA_SETS));

/// Called by `window_service::run_insert` right after it decodes a
/// just-completed data set from its in-memory shred payload cache.
/// `indices` matches the `indices` field of the `CompletedDataSetInfo` this
/// was derived from; only its `start` is used as the cache key, matching
/// `take_if_fully_cached`.
pub fn insert(slot: Slot, indices: Range<u32>, entries: Vec<Entry>) {
    CACHE.insert((slot, indices.start), entries);
}

/// Checks whether EVERY range in `ranges` (for `slot`) is present, and if so,
/// removes and returns them in the same order as `ranges`. On any miss,
/// returns `None`; ranges that *were* present are left in place (a lookup
/// never discards an entry it can't use).
///
/// Callers must treat this as all-or-nothing for the whole `ranges` slice:
/// `Blockstore::get_slot_components_in_block`'s RocksDB batching assumes the
/// ranges it's given are contiguous (one `multi_get` spanning the first
/// range's start to the last range's end, walked sequentially), so splicing
/// a partial hit into an otherwise-RocksDB-backed read isn't safe -- a miss
/// on any single range means the whole call falls back to RocksDB.
pub(crate) fn take_if_fully_cached(slot: Slot, ranges: &[Range<u32>]) -> Option<Vec<Vec<Entry>>> {
    CACHE.take_if_fully_cached(slot, ranges)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(num: u64) -> Entry {
        Entry {
            num_hashes: num,
            hash: solana_hash::Hash::default(),
            transactions: vec![],
        }
    }

    // Each test uses its own slot range to avoid cross-test interference on
    // the process-global cache.
    #[test]
    fn hit_when_fully_cached() {
        let slot = 10_001;
        insert(slot, 0..4, vec![entry(1)]);
        insert(slot, 4..7, vec![entry(2), entry(3)]);

        let result = take_if_fully_cached(slot, &[0..4, 4..7]).unwrap();
        assert_eq!(result, vec![vec![entry(1)], vec![entry(2), entry(3)]]);

        // entries are removed once taken
        assert!(take_if_fully_cached(slot, &[0..4]).is_none());
    }

    #[test]
    fn miss_when_partially_cached_leaves_cache_untouched() {
        let slot = 10_002;
        insert(slot, 0..4, vec![entry(1)]);

        assert!(take_if_fully_cached(slot, &[0..4, 4..7]).is_none());
        // the range that *was* cached must still be there for a later retry
        assert_eq!(
            take_if_fully_cached(slot, &[0..4]),
            Some(vec![vec![entry(1)]])
        );
    }

    #[test]
    fn miss_when_not_cached_at_all() {
        let slot = 10_003;
        assert!(take_if_fully_cached(slot, &[0..4]).is_none());
    }

    #[test]
    fn eviction_is_independent_of_shred_payload_cache_window() {
        // Regression test for the bug described in the module docs: this
        // cache must retain entries well past any short, slot-count-based
        // window like `ShredPayloadCache`'s -- its own cap is the only thing
        // that should ever evict an unconsumed entry. Uses its own isolated
        // `BoundedCache` (not the process-global `CACHE`) so a tiny cap here
        // can't evict other tests' entries out of the shared cache.
        let base_slot = 20_000;
        let cap = 4;
        let cache = BoundedCache::new(cap);
        for i in 0..cap {
            cache.insert((base_slot + i as u64, 0), vec![entry(i as u64)]);
        }
        // Inserting one more, beyond the cap, evicts only the oldest.
        cache.insert((base_slot + cap as u64, 0), vec![entry(99)]);

        assert!(cache.take_if_fully_cached(base_slot, &[0..1]).is_none());
        for i in 1..=cap {
            assert!(
                cache
                    .take_if_fully_cached(base_slot + i as u64, &[0..1])
                    .is_some()
            );
        }
    }

    #[test]
    fn reinserting_same_key_does_not_inflate_eviction_accounting() {
        // Regression test for the second bug described in the module docs:
        // re-inserting an already-present key (e.g. a completed data set
        // decoded and cached more than once before replay consumes it) must
        // not count as a second entry toward `cap`, or genuinely distinct,
        // still-unconsumed entries get evicted early. Uses its own isolated
        // `BoundedCache`, same reasoning as the test above.
        let base_slot = 30_000;
        let cap = 4;
        let cache = BoundedCache::new(cap);
        for i in 0..cap {
            cache.insert((base_slot + i as u64, 0), vec![entry(i as u64)]);
        }
        // Re-insert the very first key again. If this wrongly grew the
        // order queue, the next genuinely-new insert below would evict two
        // entries instead of one.
        cache.insert((base_slot, 0), vec![entry(100)]);
        cache.insert((base_slot + cap as u64, 0), vec![entry(99)]);

        // The second-oldest key must still be present -- only the oldest
        // (`base_slot`, now refreshed) should have been evicted, not it too.
        assert!(
            cache
                .take_if_fully_cached(base_slot + 1, &[0..1])
                .is_some()
        );
    }
}
