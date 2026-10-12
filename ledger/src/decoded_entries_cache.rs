//! An in-memory, best-effort cache of already-decoded completed data sets.
//!
//! `window_service::run_insert` (in `solana-core`) keeps the raw shred payloads
//! it has just inserted into the blockstore in memory for a short while (see
//! its `ShredPayloadCache`). When a data set completes and every one of its
//! shreds is still in that cache, it decodes the entries right there and sends
//! them here, over [`sender`]. `Blockstore::get_slot_components_with_shred_info_fast`
//! (used by `confirm_slot` in `blockstore_processor.rs`) checks this cache
//! before falling back to its normal RocksDB-backed read, letting ReplayStage
//! skip re-reading and re-deshredding shred payloads it, through
//! window_service, already had in memory a moment earlier.
//!
//! This is purely a latency optimization, never a correctness dependency:
//! every lookup that misses (a `BlockMarker` range, a cache eviction, replay
//! falling behind window_service, or no Geyser^Wno sender registered at all)
//! falls back to the exact, unchanged RocksDB-backed path. See
//! `docs/perf/shred-to-geyser-prometheus-plan.md`, "Phase 3, hypothesis 5".

use {
    crossbeam_channel::{Receiver, Sender, unbounded},
    solana_clock::Slot,
    solana_entry::entry::Entry,
    std::{
        collections::HashMap,
        ops::Range,
        sync::{LazyLock, Mutex},
    },
};

/// A completed data set's shreds, already decoded into entries.
pub struct DecodedCompletedDataSet {
    pub slot: Slot,
    /// Data shred indices this data set spans, matching the `indices` field of
    /// `blockstore::CompletedDataSetInfo` that `window_service` derived this from.
    pub indices: Range<u32>,
    pub entries: Vec<Entry>,
}

/// Hard cap on the number of cached data sets, so a replay stall (or a bug)
/// can't grow this cache unboundedly. Comfortably above normal in-flight
/// counts -- window_service's own per-slot shred-payload cache is already
/// bounded to a small number of slots, so this would only be reached if
/// replay fell far behind for a sustained period.
const MAX_CACHED_DATA_SETS: usize = 4096;

struct Shared {
    sender: Sender<DecodedCompletedDataSet>,
    receiver: Receiver<DecodedCompletedDataSet>,
    cache: Mutex<HashMap<(Slot, u32), Vec<Entry>>>,
}

static SHARED: LazyLock<Shared> = LazyLock::new(|| {
    let (sender, receiver) = unbounded();
    Shared {
        sender,
        receiver,
        cache: Mutex::new(HashMap::new()),
    }
});

/// Returns a cloned sender for `window_service::run_insert` to send decoded
/// data sets on. Cheap to call (clones a crossbeam `Sender`); callers should
/// still hoist it out of their hot loop for clarity.
pub fn sender() -> Sender<DecodedCompletedDataSet> {
    SHARED.sender.clone()
}

/// Drains any newly-arrived decoded data sets into the cache, then checks
/// whether EVERY range in `ranges` (for `slot`) is present. On a full hit,
/// removes and returns them in the same order as `ranges`. On any miss,
/// leaves the cache untouched and returns `None`.
///
/// Callers must treat this as all-or-nothing for the whole `ranges` slice:
/// `Blockstore::get_slot_components_in_block`'s RocksDB batching assumes the
/// ranges it's given are contiguous (one `multi_get` spanning the first
/// range's start to the last range's end, walked sequentially), so splicing
/// a partial hit into an otherwise-RocksDB-backed read isn't safe -- a miss
/// on any single range means the whole call falls back to RocksDB.
pub(crate) fn take_if_fully_cached(slot: Slot, ranges: &[Range<u32>]) -> Option<Vec<Vec<Entry>>> {
    let mut cache = SHARED.cache.lock().unwrap();
    for decoded in SHARED.receiver.try_iter() {
        cache.insert((decoded.slot, decoded.indices.start), decoded.entries);
    }
    if cache.len() > MAX_CACHED_DATA_SETS {
        let mut keys: Vec<_> = cache.keys().copied().collect();
        keys.sort_unstable();
        for key in keys.into_iter().take(cache.len() - MAX_CACHED_DATA_SETS) {
            cache.remove(&key);
        }
    }

    let hit = ranges.iter().all(|r| cache.contains_key(&(slot, r.start)));
    solana_metrics::pipeline_metrics::DECODED_ENTRIES_CACHE_TOTAL
        .with_label_values(&[if hit { "hit" } else { "miss" }])
        .inc();
    if !hit {
        return None;
    }
    Some(
        ranges
            .iter()
            .map(|r| cache.remove(&(slot, r.start)).unwrap())
            .collect(),
    )
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
        let sender = sender();
        sender
            .send(DecodedCompletedDataSet {
                slot,
                indices: 0..4,
                entries: vec![entry(1)],
            })
            .unwrap();
        sender
            .send(DecodedCompletedDataSet {
                slot,
                indices: 4..7,
                entries: vec![entry(2), entry(3)],
            })
            .unwrap();

        let result = take_if_fully_cached(slot, &[0..4, 4..7]).unwrap();
        assert_eq!(result, vec![vec![entry(1)], vec![entry(2), entry(3)]]);

        // entries are removed once taken
        assert!(take_if_fully_cached(slot, &[0..4]).is_none());
    }

    #[test]
    fn miss_when_partially_cached_leaves_cache_untouched() {
        let slot = 10_002;
        sender()
            .send(DecodedCompletedDataSet {
                slot,
                indices: 0..4,
                entries: vec![entry(1)],
            })
            .unwrap();

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
}
