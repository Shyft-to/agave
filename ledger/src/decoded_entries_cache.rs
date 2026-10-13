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
//! There is no dedicated eviction policy or size cap here: `window_service`
//! is the single writer, and it already keeps its own bounded, FIFO-by-slot
//! recency window for `ShredPayloadCache` (see its docs); [`evict_slot`] is
//! called from that same eviction point, so this cache's lifetime always
//! tracks `ShredPayloadCache`'s window rather than needing a second,
//! independent bound.

use {
    dashmap::DashMap,
    solana_clock::Slot,
    solana_entry::entry::Entry,
    std::{ops::Range, sync::LazyLock},
};

static CACHE: LazyLock<DashMap<(Slot, u32), Vec<Entry>>> = LazyLock::new(DashMap::new);

/// Called by `window_service::run_insert` right after it decodes a
/// just-completed data set from its in-memory shred payload cache.
/// `indices` matches the `indices` field of the `CompletedDataSetInfo` this
/// was derived from; only its `start` is used as the cache key, matching
/// `take_if_fully_cached`.
pub fn insert(slot: Slot, indices: Range<u32>, entries: Vec<Entry>) {
    CACHE.insert((slot, indices.start), entries);
}

/// Called by `window_service::run_insert` when it evicts `slot` from its own
/// `ShredPayloadCache` recency window, so this cache never outlives that one.
pub fn evict_slot(slot: Slot) {
    CACHE.retain(|&(s, _), _| s != slot);
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
    let hit = ranges.iter().all(|r| CACHE.contains_key(&(slot, r.start)));
    solana_metrics::pipeline_metrics::DECODED_ENTRIES_CACHE_TOTAL
        .with_label_values(&[if hit { "hit" } else { "miss" }])
        .inc();
    if !hit {
        return None;
    }
    // Re-checked via `remove`, not just the `contains_key` above: in the rare
    // case of two concurrent lookups for the same slot racing on completion,
    // one of these removes can still come back empty even though the
    // `contains_key` scan above saw it. Falling back to `None` (and losing
    // whichever entries this call *did* remove along the way) is safe --
    // RocksDB remains the source of truth -- just not maximally efficient
    // for that vanishingly rare race.
    ranges
        .iter()
        .map(|r| CACHE.remove(&(slot, r.start)).map(|(_, entries)| entries))
        .collect()
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
    fn evict_slot_removes_only_that_slot() {
        let slot_a = 10_004;
        let slot_b = 10_005;
        insert(slot_a, 0..4, vec![entry(1)]);
        insert(slot_b, 0..4, vec![entry(2)]);

        evict_slot(slot_a);

        assert!(take_if_fully_cached(slot_a, &[0..4]).is_none());
        assert_eq!(
            take_if_fully_cached(slot_b, &[0..4]),
            Some(vec![vec![entry(2)]])
        );
    }
}
