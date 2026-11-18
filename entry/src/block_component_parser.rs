use {
    crate::{
        block_component::{BlockComponent, ParsedBlockComponent, VersionedBlockMarker},
        entry::{Entry, EntryView, MAX_DATA_SHREDS_SIZE},
    },
    agave_transaction_view::{
        result::TransactionViewError, transaction_view::UnsanitizedTransactionView,
    },
    bytes::Bytes,
    solana_hash::Hash,
    solana_transaction::versioned::VersionedTransaction,
    std::mem::size_of,
    wincode::{SchemaRead, config::DefaultConfig, io::Reader},
};

/// Parses a [`ParsedBlockComponent`] from `bytes`, using the same wire format
/// as [`BlockComponent`].
///
/// Built from [`parse_component_header`], [`parse_entry_header_at`] and
/// [`parse_transaction_at`], which incremental decoders of partially received
/// data sets also use, so both read exactly the same entries.
pub fn parse<B: Into<Bytes>>(bytes: B) -> Result<ParsedBlockComponent, ParseError> {
    let bytes: Bytes = bytes.into();

    let (entry_count, mut offset) = match parse_component_header(&bytes)? {
        ComponentHeader::BlockMarker => {
            let remaining = &bytes[BlockComponent::ENTRY_COUNT_SIZE..];
            let marker = wincode::deserialize::<VersionedBlockMarker>(remaining)
                .map_err(ParseError::BlockMarker)?;
            return Ok(ParsedBlockComponent::BlockMarker(marker));
        }
        ComponentHeader::EntryBatch {
            entry_count,
            entries_offset,
        } => (entry_count, entries_offset),
    };

    // Safe to pre-allocate: entry_count already passed the preallocation-size check in
    // `parse_component_header`.
    let mut entry_views = Vec::with_capacity(entry_count);

    for entry_index in 0..entry_count {
        let (header, transactions_offset) = parse_entry_header_at(&bytes, offset, entry_index)?;
        offset = transactions_offset;

        // Safe to pre-allocate: tx_count already passed the preallocation-size check in
        // `parse_entry_header_at`.
        let mut transactions = Vec::with_capacity(header.tx_count);

        for tx_index in 0..header.tx_count {
            let (view, next_offset) = parse_transaction_at(&bytes, offset, entry_index, tx_index)?;
            offset = next_offset;
            transactions.push(view);
        }

        entry_views.push(EntryView {
            num_hashes: header.num_hashes,
            hash: header.hash,
            transactions,
        });
    }

    // Trailing bytes after the last entry are intentionally ignored, matching
    // legacy `wincode::deserialize` behavior.
    Ok(ParsedBlockComponent::EntryBatch(entry_views))
}

/// What a serialized component's leading entry count encodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComponentHeader {
    /// An entry count of zero: a [`VersionedBlockMarker`] follows.
    BlockMarker,
    /// An entry batch of `entry_count` entries, the first one at byte
    /// `entries_offset`.
    EntryBatch {
        entry_count: usize,
        entries_offset: usize,
    },
}

/// The fixed-size fields that start every entry of an entry batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryHeader {
    pub num_hashes: u64,
    pub hash: Hash,
    pub tx_count: usize,
}

/// Reads and validates a component's entry count, exactly as [`parse`] does.
pub fn parse_component_header(bytes: &[u8]) -> Result<ComponentHeader, ParseError> {
    let mut header: &[u8] = bytes;
    let entry_count =
        <u64 as SchemaRead<DefaultConfig>>::get(header.by_ref()).map_err(ParseError::EntryCount)?;

    // entry count of zero encodes a block marker
    let entry_count_encodes_block_marker = entry_count == 0;
    if entry_count_encodes_block_marker {
        return Ok(ComponentHeader::BlockMarker);
    }

    let entry_count = usize::try_from(entry_count)
        .map_err(|_| ParseError::EntryCountOverflow { count: entry_count })?;
    if entry_count >= BlockComponent::MAX_ENTRIES {
        return Err(ParseError::TooManyEntries {
            count: entry_count,
            max: BlockComponent::MAX_ENTRIES,
        });
    }

    // Since the bytes are wincode serialization of [`BlockComponent::EntryBatch`] which
    // contains regular [`Entry`]s, bound by Entry's size to match wincode's prealloc check
    // behavior for deserialization.
    check_prealloc_limit::<Entry>(entry_count).map_err(|needed| {
        ParseError::EntryCountPreallocLimit {
            count: entry_count,
            needed,
            limit: MAX_DATA_SHREDS_SIZE,
        }
    })?;

    Ok(ComponentHeader::EntryBatch {
        entry_count,
        entries_offset: BlockComponent::ENTRY_COUNT_SIZE,
    })
}

/// Reads and validates the header of entry `entry_index`, which starts at
/// byte `offset`, exactly as [`parse`] does. Returns the header and the offset
/// of the entry's first transaction. An `offset` past the end of `bytes` reads
/// as no bytes and fails like a truncated header.
pub fn parse_entry_header_at(
    bytes: &[u8],
    offset: usize,
    entry_index: usize,
) -> Result<(EntryHeader, usize), ParseError> {
    let mut cursor: &[u8] = bytes.get(offset..).unwrap_or_default();

    let num_hashes =
        <u64 as SchemaRead<DefaultConfig>>::get(cursor.by_ref()).map_err(|source| {
            ParseError::EntryHeader {
                entry_index,
                field: "num_hashes",
                source,
            }
        })?;
    let hash = <Hash as SchemaRead<DefaultConfig>>::get(cursor.by_ref()).map_err(|source| {
        ParseError::EntryHeader {
            entry_index,
            field: "hash",
            source,
        }
    })?;
    let tx_count = <u64 as SchemaRead<DefaultConfig>>::get(cursor.by_ref()).map_err(|source| {
        ParseError::EntryHeader {
            entry_index,
            field: "tx_count",
            source,
        }
    })?;

    let transactions_offset = bytes.len() - cursor.len();

    let tx_count = usize::try_from(tx_count).map_err(|_| ParseError::TransactionCountOverflow {
        entry_index,
        count: tx_count,
    })?;
    // Since the bytes are wincode serialization of [`BlockComponent::EntryBatch`] whose
    // entries contain regular [`VersionedTransaction`]s, bound by VersionedTransaction's
    // size to match wincode's prealloc check behavior for deserialization.
    check_prealloc_limit::<VersionedTransaction>(tx_count).map_err(|needed| {
        ParseError::TransactionCountPreallocLimit {
            entry_index,
            count: tx_count,
            needed,
            limit: MAX_DATA_SHREDS_SIZE,
        }
    })?;

    Ok((
        EntryHeader {
            num_hashes,
            hash,
            tx_count,
        },
        transactions_offset,
    ))
}

/// Parses transaction `tx_index` of entry `entry_index`, which starts at byte
/// `offset`, exactly as [`parse`] does. Returns the transaction view and the
/// offset just past it. An `offset` past the end of `bytes` reads as no bytes
/// and fails like a truncated transaction.
pub fn parse_transaction_at(
    bytes: &Bytes,
    offset: usize,
    entry_index: usize,
    tx_index: usize,
) -> Result<(UnsanitizedTransactionView<Bytes>, usize), ParseError> {
    let remaining_bytes = bytes.slice(offset.min(bytes.len())..);
    let (view, consumed_len) = UnsanitizedTransactionView::try_new_unsanitized_from_prefix(
        remaining_bytes,
    )
    .map_err(|error| ParseError::Transaction {
        entry_index,
        tx_index,
        error,
    })?;
    Ok((view, offset + consumed_len))
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("failed to read entry count: {0}")]
    EntryCount(wincode::ReadError),

    #[error("entry count {count} does not fit in usize")]
    EntryCountOverflow { count: u64 },

    #[error("entry count {count} exceeds max {max}")]
    TooManyEntries { count: usize, max: usize },

    #[error("entry count {count} would need {needed} bytes, exceeding preallocation limit {limit}")]
    EntryCountPreallocLimit {
        count: usize,
        needed: usize,
        limit: usize,
    },

    #[error("failed to read {field} for entry {entry_index}: {source}")]
    EntryHeader {
        entry_index: usize,
        field: &'static str,
        source: wincode::ReadError,
    },

    #[error("transaction count {count} for entry {entry_index} does not fit in usize")]
    TransactionCountOverflow { entry_index: usize, count: u64 },

    #[error(
        "transaction count {count} for entry {entry_index} would need {needed} bytes, exceeding \
         preallocation limit {limit}"
    )]
    TransactionCountPreallocLimit {
        entry_index: usize,
        count: usize,
        needed: usize,
        limit: usize,
    },

    #[error("failed to parse transaction {tx_index} of entry {entry_index}: {error:?}")]
    Transaction {
        entry_index: usize,
        tx_index: usize,
        error: TransactionViewError,
    },

    #[error("failed to deserialize block marker: {0}")]
    BlockMarker(wincode::ReadError),
}

/// Checks that pre-allocating `count` elements of `T` would stay within
/// [`MAX_DATA_SHREDS_SIZE`] bytes (`count * size_of::<T>()`), returning the bytes needed as
/// the error otherwise.
fn check_prealloc_limit<T>(count: usize) -> Result<(), usize> {
    // max(1) to count zero sized types
    let size_of_t = size_of::<T>().max(1);
    let needed = count.saturating_mul(size_of_t);

    if needed > MAX_DATA_SHREDS_SIZE {
        Err(needed)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            block_component::{
                BlockFooterV1, BlockHeaderV1, GenesisCertBlockMarker, UpdateParentV1,
            },
            entry::Entry,
        },
        solana_bls_signatures::Keypair as BlsKeypair,
        solana_keypair::Keypair,
        solana_pubkey::Pubkey,
        solana_transaction::versioned::VersionedTransaction,
        std::{assert_matches, iter::repeat_n},
        test_case::test_case,
    };

    fn tick_entries(n: usize) -> Vec<Entry> {
        repeat_n(Entry::default(), n).collect()
    }

    fn entry_with_transactions(num_hashes: u64, transactions: Vec<VersionedTransaction>) -> Entry {
        Entry {
            num_hashes,
            hash: Hash::new_unique(),
            transactions,
        }
    }

    fn transfer_transaction(amount: u64) -> VersionedTransaction {
        solana_system_transaction::transfer(
            &Keypair::new(),
            &Pubkey::new_unique(),
            amount,
            Hash::default(),
        )
        .into()
    }

    fn sample_footer() -> BlockFooterV1 {
        BlockFooterV1 {
            bank_hash: Hash::new_unique(),
            block_producer_time_nanos: 1234567890,
            block_user_agent: b"test-agent".to_vec(),
            block_final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        }
    }

    /// Serializes `entries` as a `BlockComponent` entry batch and parses it back.
    fn parse_entry_batch(entries: Vec<Entry>) -> Vec<EntryView<Bytes>> {
        let component = BlockComponent::new_entry_batch(entries).unwrap();
        let bytes = wincode::serialize(&component).unwrap();
        let ParsedBlockComponent::EntryBatch(views) = parse(bytes).unwrap() else {
            panic!("expected EntryBatch");
        };
        views
    }

    #[test]
    fn parsed_tick_entry_view_has_same_num_hashes_and_hash_as_source_entry() {
        let source_tick_entry = Entry {
            num_hashes: 7,
            hash: Hash::new_unique(),
            transactions: vec![],
        };

        let parsed_entry_views = parse_entry_batch(vec![source_tick_entry.clone()]);

        assert_eq!(parsed_entry_views.len(), 1);
        assert_eq!(
            parsed_entry_views[0].num_hashes,
            source_tick_entry.num_hashes
        );
        assert_eq!(parsed_entry_views[0].hash, source_tick_entry.hash);
        assert!(parsed_entry_views[0].transactions.is_empty());
    }

    #[test]
    fn parsed_entry_views_preserve_source_entry_order_across_multiple_entries() {
        let source_entries_with_distinct_num_hashes: Vec<Entry> = (0..5)
            .map(|num_hashes| Entry {
                num_hashes,
                hash: Hash::new_unique(),
                transactions: vec![],
            })
            .collect();

        let parsed_entry_views = parse_entry_batch(source_entries_with_distinct_num_hashes.clone());

        assert_eq!(
            parsed_entry_views.len(),
            source_entries_with_distinct_num_hashes.len()
        );
        for (parsed_entry_view, source_entry) in parsed_entry_views
            .iter()
            .zip(source_entries_with_distinct_num_hashes.iter())
        {
            assert_eq!(parsed_entry_view.num_hashes, source_entry.num_hashes);
            assert_eq!(parsed_entry_view.hash, source_entry.hash);
        }
    }

    #[test]
    fn parsed_transaction_view_exposes_same_signature_as_source_transaction() {
        let source_transaction = transfer_transaction(1);
        let source_entry = entry_with_transactions(1, vec![source_transaction.clone()]);

        let parsed_entry_views = parse_entry_batch(vec![source_entry]);

        assert_eq!(parsed_entry_views.len(), 1);
        assert_eq!(parsed_entry_views[0].transactions.len(), 1);
        assert_eq!(
            parsed_entry_views[0].transactions[0].signatures(),
            source_transaction.signatures.as_slice()
        );
    }

    #[test]
    fn parsed_transaction_views_preserve_source_transaction_order_within_entry() {
        let first_source_transaction = transfer_transaction(1);
        let second_source_transaction = transfer_transaction(2);
        let source_entry = entry_with_transactions(
            1,
            vec![
                first_source_transaction.clone(),
                second_source_transaction.clone(),
            ],
        );

        let parsed_entry_views = parse_entry_batch(vec![source_entry]);

        let parsed_transaction_views = &parsed_entry_views[0].transactions;
        assert_eq!(parsed_transaction_views.len(), 2);
        assert_eq!(
            parsed_transaction_views[0].signatures(),
            first_source_transaction.signatures.as_slice()
        );
        assert_eq!(
            parsed_transaction_views[1].signatures(),
            second_source_transaction.signatures.as_slice()
        );
    }

    #[test_case(VersionedBlockMarker::from_block_footer(sample_footer()); "block_footer")]
    #[test_case(
        VersionedBlockMarker::from_block_header(BlockHeaderV1 {
            parent_slot: 42,
            parent_block_id: Hash::new_unique(),
        });
        "block_header"
    )]
    #[test_case(
        VersionedBlockMarker::from_update_parent(UpdateParentV1 {
            new_parent_slot: 43,
            new_parent_block_id: Hash::new_unique(),
        });
        "update_parent"
    )]
    #[test_case(
        VersionedBlockMarker::from_genesis_cert_block_marker(GenesisCertBlockMarker {
            slot: 44,
            block_id: Hash::new_unique(),
            bls_signature: BlsKeypair::new().sign(b"genesis").into(),
            bitmap: vec![1, 2, 3],
        });
        "genesis_certificate"
    )]
    fn parsed_block_marker_view_matches_source_marker_for_each_variant(
        source_marker: VersionedBlockMarker,
    ) {
        let component = BlockComponent::new_block_marker(source_marker.clone());
        let bytes = wincode::serialize(&component).unwrap();

        let view = parse(bytes).unwrap();

        let ParsedBlockComponent::BlockMarker(parsed_marker) = view else {
            panic!("expected BlockMarker");
        };
        assert_eq!(parsed_marker, source_marker);
    }

    #[test]
    fn parse_rejects_all_zero_empty_entry_batch_payload() {
        let empty_entry_batch_payload = Bytes::from(BlockComponent::EMPTY_ENTRY_BATCH.to_vec());

        let parse_result = parse(empty_entry_batch_payload);

        assert!(parse_result.is_err());
    }

    #[test]
    fn parse_rejects_entry_count_header_claiming_max_entries_with_no_further_bytes() {
        let mut header_claiming_max_entries =
            (BlockComponent::MAX_ENTRIES as u64).to_le_bytes().to_vec();
        header_claiming_max_entries.resize(BlockComponent::ENTRY_COUNT_SIZE, 0);

        let parse_result = parse(header_claiming_max_entries);

        assert_matches!(parse_result, Err(ParseError::TooManyEntries { .. }));
    }

    #[test]
    fn parse_ignores_extra_trailing_byte_appended_after_valid_entry_batch() {
        let source_entries = tick_entries(1);
        let component = BlockComponent::new_entry_batch(source_entries.clone()).unwrap();
        let mut entry_batch_bytes_with_trailing_byte = wincode::serialize(&component).unwrap();
        entry_batch_bytes_with_trailing_byte.push(0);

        let parse_result = parse(entry_batch_bytes_with_trailing_byte);

        // trailing bytes are ignored matching legacy `wincode::deserialize`
        let Ok(ParsedBlockComponent::EntryBatch(views)) = parse_result else {
            panic!("expected EntryBatch");
        };
        assert_eq!(views.len(), source_entries.len());
    }

    #[test]
    fn parse_ignores_extra_trailing_byte_appended_after_valid_block_marker() {
        let marker = VersionedBlockMarker::from_block_footer(sample_footer());
        let component = BlockComponent::new_block_marker(marker.clone());
        let mut block_marker_bytes_with_trailing_byte = wincode::serialize(&component).unwrap();
        block_marker_bytes_with_trailing_byte.push(0);

        let parse_result = parse(block_marker_bytes_with_trailing_byte);

        let Ok(ParsedBlockComponent::BlockMarker(parsed_marker)) = parse_result else {
            panic!("expected BlockMarker");
        };
        assert_eq!(parsed_marker, marker);
    }

    #[test]
    fn parse_rejects_entry_count_over_prealloc_limit() {
        // One past the largest entry_count that fits the preallocation-size guard, but
        // still well under MAX_ENTRIES, so only the prealloc check can reject it.
        let entry_count = MAX_DATA_SHREDS_SIZE / size_of::<Entry>() + 1;
        assert!(entry_count < BlockComponent::MAX_ENTRIES);

        // Just the entry_count header; no entry data needs to follow it.
        let header = (entry_count as u64).to_le_bytes().to_vec();

        let owned_result = wincode::deserialize::<BlockComponent>(&header);
        let view_result = parse(header);

        assert_matches!(
            owned_result,
            Err(wincode::ReadError::PreallocationSizeLimit { .. })
        );
        assert_matches!(view_result, Err(ParseError::EntryCountPreallocLimit { .. }));
    }

    #[test]
    fn parse_accepts_entry_count_at_prealloc_limit() {
        // The largest entry_count that still fits the preallocation-size guard. No
        // entry data follows it, so both decoders still error out - just not because
        // of this guard, which is the only thing this test checks.
        let entry_count = MAX_DATA_SHREDS_SIZE / size_of::<Entry>();
        let header = (entry_count as u64).to_le_bytes().to_vec();

        let owned_result = wincode::deserialize::<BlockComponent>(&header);
        let view_result = parse(header);

        assert!(!matches!(
            owned_result,
            Err(wincode::ReadError::PreallocationSizeLimit { .. })
        ));
        assert!(!matches!(
            view_result,
            Err(ParseError::EntryCountPreallocLimit { .. })
        ));
    }

    #[test]
    fn parse_rejects_transaction_count_over_prealloc_limit() {
        // A real single-entry, zero-transaction batch, with its tx_count field (the
        // last 8 bytes, since no transactions follow it) patched to one past the
        // largest tx_count that fits the preallocation-size guard.
        let component = BlockComponent::new_entry_batch(vec![Entry::default()]).unwrap();
        let mut bytes = wincode::serialize(&component).unwrap();

        let tx_count = MAX_DATA_SHREDS_SIZE / size_of::<VersionedTransaction>() + 1;
        let tx_count_offset = bytes.len() - size_of::<u64>();
        bytes[tx_count_offset..].copy_from_slice(&(tx_count as u64).to_le_bytes());

        let owned_result = wincode::deserialize::<BlockComponent>(&bytes);
        let view_result = parse(bytes);

        assert_matches!(
            owned_result,
            Err(wincode::ReadError::PreallocationSizeLimit { .. })
        );
        assert_matches!(
            view_result,
            Err(ParseError::TransactionCountPreallocLimit { .. })
        );
    }

    #[test]
    fn parse_accepts_transaction_count_at_prealloc_limit() {
        // The largest tx_count that still fits the preallocation-size guard, patched
        // into a real zero-transaction entry. No transaction data follows it, so both
        // decoders still error out - just not because of this guard.
        let component = BlockComponent::new_entry_batch(vec![Entry::default()]).unwrap();
        let mut bytes = wincode::serialize(&component).unwrap();

        let tx_count = MAX_DATA_SHREDS_SIZE / size_of::<VersionedTransaction>();
        let tx_count_offset = bytes.len() - size_of::<u64>();
        bytes[tx_count_offset..].copy_from_slice(&(tx_count as u64).to_le_bytes());

        let owned_result = wincode::deserialize::<BlockComponent>(&bytes);
        let view_result = parse(bytes);

        assert!(!matches!(
            owned_result,
            Err(wincode::ReadError::PreallocationSizeLimit { .. })
        ));
        assert!(!matches!(
            view_result,
            Err(ParseError::TransactionCountPreallocLimit { .. })
        ));
    }

    /// A serialized multi-entry batch mixing transaction entries and a tick.
    fn sample_entry_batch_bytes() -> Vec<u8> {
        let entries = vec![
            entry_with_transactions(1, vec![transfer_transaction(1), transfer_transaction(2)]),
            entry_with_transactions(2, vec![transfer_transaction(3)]),
            Entry {
                num_hashes: 3,
                hash: Hash::new_unique(),
                transactions: vec![],
            },
            entry_with_transactions(
                4,
                vec![
                    transfer_transaction(4),
                    transfer_transaction(5),
                    transfer_transaction(6),
                ],
            ),
        ];
        wincode::serialize(&BlockComponent::new_entry_batch(entries).unwrap()).unwrap()
    }

    type WalkedEntry = (u64, Hash, Vec<Vec<u8>>, usize);

    /// Walks `bytes` with the public helpers the way `parse` does, stopping at
    /// the first error. Returns each complete entry's num_hashes, hash, raw
    /// transaction bytes and end offset.
    fn walk_with_helpers(bytes: &Bytes) -> Vec<WalkedEntry> {
        let mut walked = vec![];
        let Ok(ComponentHeader::EntryBatch {
            entry_count,
            entries_offset,
        }) = parse_component_header(bytes)
        else {
            return walked;
        };
        let mut offset = entries_offset;
        for entry_index in 0..entry_count {
            let Ok((header, next_offset)) = parse_entry_header_at(bytes, offset, entry_index)
            else {
                return walked;
            };
            offset = next_offset;
            let mut transactions = vec![];
            for tx_index in 0..header.tx_count {
                let Ok((view, next_offset)) =
                    parse_transaction_at(bytes, offset, entry_index, tx_index)
                else {
                    return walked;
                };
                offset = next_offset;
                transactions.push(view.data().to_vec());
            }
            walked.push((header.num_hashes, header.hash, transactions, offset));
        }
        walked
    }

    #[test]
    fn helpers_reproduce_parse() {
        let bytes = Bytes::from(sample_entry_batch_bytes());
        let ParsedBlockComponent::EntryBatch(views) = parse(bytes.clone()).unwrap() else {
            panic!("expected EntryBatch");
        };
        let walked = walk_with_helpers(&bytes);
        assert_eq!(walked.len(), views.len());
        for (view, (num_hashes, hash, transactions, _)) in views.iter().zip(&walked) {
            assert_eq!(view.num_hashes, *num_hashes);
            assert_eq!(view.hash, *hash);
            let view_transactions: Vec<Vec<u8>> = view
                .transactions
                .iter()
                .map(|tx| tx.data().to_vec())
                .collect();
            assert_eq!(&view_transactions, transactions);
        }
        assert_eq!(walked.last().unwrap().3, bytes.len());
    }

    #[test]
    fn helpers_on_every_prefix_only_yield_entries_of_the_full_parse() {
        let full = sample_entry_batch_bytes();
        let full_walk = walk_with_helpers(&Bytes::from(full.clone()));
        for len in 0..=full.len() {
            let prefix_walk = walk_with_helpers(&Bytes::copy_from_slice(&full[..len]));
            // Every entry read from a prefix, including transactions cut at
            // the prefix boundary, is exactly the full parse's entry.
            assert_eq!(
                prefix_walk[..],
                full_walk[..prefix_walk.len()],
                "prefix len {len}"
            );
            // An entry is read from a prefix exactly when all its bytes are in it.
            let complete = full_walk.iter().filter(|entry| entry.3 <= len).count();
            assert_eq!(prefix_walk.len(), complete, "prefix len {len}");
        }
    }

    #[test]
    fn helpers_reject_offsets_past_the_end() {
        let bytes = Bytes::from(sample_entry_batch_bytes());
        assert!(parse_entry_header_at(&bytes, bytes.len() + 1, 0).is_err());
        assert!(parse_transaction_at(&bytes, bytes.len() + 1, 0, 0).is_err());
    }

    const ENTRY_PREALLOC_LIMIT: usize = MAX_DATA_SHREDS_SIZE / size_of::<Entry>();

    #[test_case(0; "zero_count")]
    #[test_case(1; "one_count")]
    #[test_case(ENTRY_PREALLOC_LIMIT; "at_limit")]
    fn check_prealloc_limit_for_entry_accepts_within_limit(count: usize) {
        assert_matches!(check_prealloc_limit::<Entry>(count), Ok(()));
    }

    #[test_case(ENTRY_PREALLOC_LIMIT + 1; "over_limit")]
    #[test_case(usize::MAX; "count_overflows_and_saturates")]
    fn check_prealloc_limit_for_entry_rejects_count_over_limit(count: usize) {
        assert_eq!(
            check_prealloc_limit::<Entry>(count),
            Err(count.saturating_mul(size_of::<Entry>()))
        );
    }

    #[test]
    fn check_prealloc_limit_treats_zero_sized_types_as_one_byte_each() {
        struct ZeroSized;
        assert_eq!(size_of::<ZeroSized>(), 0);

        let count = MAX_DATA_SHREDS_SIZE + 1;

        assert_eq!(check_prealloc_limit::<ZeroSized>(count), Err(count));
    }
}
