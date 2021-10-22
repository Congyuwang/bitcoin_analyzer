use ahash::{AHasher, RandomState};
use bitcoin_explorer::{Address, BitcoinDB, SBlock, SConnectedBlock};
use chrono::{Date, NaiveDateTime, Utc};
use crossbeam::channel;
use crossbeam::channel::Sender;
use ena::unify::{InPlaceUnificationTable, UnifyKey};
use hash_hasher::{HashBuildHasher, HashedMap};
use indicatif;
use indicatif::ProgressStyle;
use log::{info, LevelFilter};
use num_cpus;
use par_iter_sync::*;
use rayon::prelude::*;
use rocksdb::{Options, PlainTableFactoryOptions, SliceTransform, WriteOptions, DB};
use rustc_hash::{FxHashMap, FxHashSet};
use simple_logger::SimpleLogger;
use std::collections::hash_map::Entry;
use std::fs;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
struct AddressKey(u32);

impl Into<u32> for AddressKey {
    fn into(self) -> u32 {
        self.0
    }
}

impl From<u32> for AddressKey {
    fn from(v: u32) -> Self {
        AddressKey(v)
    }
}

impl UnifyKey for AddressKey {
    type Value = ();
    fn index(&self) -> u32 {
        self.0
    }
    fn from_index(u: u32) -> AddressKey {
        AddressKey(u)
    }
    fn tag() -> &'static str {
        "AddressKey"
    }
}

#[derive(Clone)]
struct AsyncBufWriter {
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    sender: Sender<Box<[u8]>>,
}

impl AsyncBufWriter {
    fn new(writer: File) -> AsyncBufWriter {
        let (sender, receiver) = channel::unbounded::<Box<[u8]>>();
        let mut writer = BufWriter::with_capacity(128 * 1024 * 1024, writer);
        let worker = thread::spawn(move || {
            for line in receiver.into_iter() {
                writer.write_all(&line).unwrap();
            }
            writer.flush().unwrap();
        });
        AsyncBufWriter {
            worker: Arc::new(Mutex::new(Some(worker))),
            sender,
        }
    }

    fn write_all(&self, buf: &[u8]) {
        self.sender.send(Box::from(buf)).unwrap();
    }

    // join the writer thread back to main thread
    fn pop_handle(&mut self) -> Option<JoinHandle<()>> {
        self.worker.lock().unwrap().take()
    }

    fn write_u32_le(&self, n: u32) {
        self.write_all(&n.to_le_bytes()[..])
    }
}

#[derive(Clone)]
struct AddressCacheRead {
    // (index, count)
    address_index: Arc<HashedMap<u128, (AddressKey, u32)>>,
    union_find: Arc<Mutex<InPlaceUnificationTable<AddressKey>>>,
    random_state_1: RandomState,
    random_state_2: RandomState,
}

impl AddressCacheRead {
    pub fn from_address_cache(cache: AddressCache) -> Self {
        let get_inner_lock = Arc::try_unwrap(cache.address_index);
        AddressCacheRead {
            address_index: Arc::new(get_inner_lock.unwrap().into_inner().unwrap()),
            union_find: cache.union_find,
            random_state_1: cache.random_state_1,
            random_state_2: cache.random_state_2,
        }
    }

    #[inline]
    pub fn connect_addresses_index(&self, addresses_index: &Vec<(AddressKey, u32)>) {
        if addresses_index.len() > 1 {
            let (first, _) = addresses_index.first().unwrap();
            for (rest, _) in addresses_index.iter().skip(1) {
                // connect rest to first
                self.union(first, rest);
            }
        }
    }

    #[inline]
    pub fn assert_get_address_index(&self, addresses: Box<[Address]>) -> Option<(AddressKey, u32)> {
        if let Some(address_string) = AddressCache::addresses_to_string(&addresses) {
            Some(
                self.get_address_index(self.hash(&address_string))
                    .expect("inputs must exist in UTXO"),
            )
        } else {
            None
        }
    }

    #[inline]
    fn union(&self, i1: &AddressKey, i2: &AddressKey) {
        self.union_find.lock().unwrap().union(*i1, *i2)
    }

    #[inline]
    fn get_address_index(&self, hash: u128) -> Option<(AddressKey, u32)> {
        // sync
        self.address_index.get(&hash).map(|x| x.to_owned())
    }

    #[inline]
    fn hash(&self, address_string: &str) -> u128 {
        let mut hasher_0 = (&self.random_state_1).build_hasher();
        let mut hasher_1 = (&self.random_state_2).build_hasher();
        hash_string_with_hasher(&mut hasher_0, &mut hasher_1, address_string)
    }

    fn dump(self, path: &Path) {
        let write_handle = {
            let clustering_path = path.to_path_buf().join("clustering.u32little");
            let counting_path = path.to_path_buf().join("counting.u32little");
            let mut clustering_writer = AsyncBufWriter::new(File::create(clustering_path).unwrap());
            let mut counting_writer = AsyncBufWriter::new(File::create(counting_path).unwrap());
            let union_find =
                Arc::try_unwrap(self.union_find).expect("some other still owns self.union_find");
            let union_find = Arc::new(union_find.into_inner().expect("failed to unlock"));

            let total_keys = union_find.len() as u32;
            assert_eq!(
                total_keys,
                self.address_index.len() as u32,
                "union find does not agree with address index"
            );

            info!("Converting address hashmap to address counting array");
            let consumes_hash_map = Arc::try_unwrap(self.address_index)
                .expect("some other still owns self.address_index");
            let mut counting_vec = vec![1u32; consumes_hash_map.len()];
            consumes_hash_map
                .into_iter()
                .for_each(|(_, (index, count))| {
                    // reduce the number of array access
                    if count > 1 {
                        counting_vec[index.index() as usize] = count
                    }
                });

            info!("start dumping address counting and clustering result");
            let bar = indicatif::ProgressBar::new(total_keys as u64);
            bar.set_style(ProgressStyle::default_bar().progress_chars("=>-").template(
                "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>10}/{len:10} ({per_sec}, {eta})",
            ));
            (0u32..total_keys)
                .into_par_iter_sync(move |i| Ok(union_find.get_find(i).index()))
                .zip(counting_vec)
                .map(|(cluster, count)| {
                    clustering_writer.write_u32_le(cluster);
                    counting_writer.write_u32_le(count);
                    ()
                })
                .enumerate()
                .filter(|(i, _)| i % 10000 == 0)
                .for_each(|_| bar.inc(10000));
            bar.finish();
            (
                clustering_writer.pop_handle().unwrap(),
                counting_writer.pop_handle().unwrap(),
            )
        };
        write_handle.0.join().unwrap();
        write_handle.1.join().unwrap();
    }
}

#[derive(Clone)]
struct AddressCache {
    // (index, count)
    address_index: Arc<Mutex<HashedMap<u128, (AddressKey, u32)>>>,
    union_find: Arc<Mutex<InPlaceUnificationTable<AddressKey>>>,
    permanent_store: Arc<DB>,
    random_state_1: RandomState,
    random_state_2: RandomState,
}

impl AddressCache {
    pub fn new(out_dir: &Path, capacity: usize) -> AddressCache {
        let permanent_store = {
            let mut options = Options::default();
            options.set_error_if_exists(true);
            options.create_if_missing(true);
            options.set_max_background_jobs(num_cpus::get() as i32);
            options.set_write_buffer_size(0x10000000);
            options.set_max_bytes_for_level_base(0x40000000);
            options.set_target_file_size_base(0x10000000);
            options.set_prefix_extractor(SliceTransform::create_fixed_prefix(4));
            options.set_plain_table_factory(&PlainTableFactoryOptions {
                user_key_length: 4,
                bloom_bits_per_key: 10,
                hash_table_ratio: 0.75,
                index_sparseness: 16,
            });
            Arc::new(match DB::open(&options, out_dir) {
                Ok(db) => db,
                Err(e) => {
                    panic!("failed to create temp rocksDB for UTXO: {}", e);
                }
            })
        };
        let mut union_table = InPlaceUnificationTable::new();
        union_table.reserve(900_000_000);
        let union_find = Arc::new(Mutex::new(union_table));
        AddressCache {
            address_index: Arc::new(Mutex::new(HashedMap::with_capacity_and_hasher(
                capacity,
                HashBuildHasher::default(),
            ))),
            union_find,
            permanent_store,
            random_state_1: RandomState::with_seed(12345),
            random_state_2: RandomState::with_seed(54321),
        }
    }

    #[inline]
    pub fn add_new_address(&self, addresses: Box<[Address]>) {
        if let Some(addresses_string) = Self::addresses_to_string(&addresses) {
            let address_hash = self.hash(&addresses_string);
            let mut is_new_address = false;
            // cache lock scope
            let write_opt = {
                let mut o = WriteOptions::default();
                o.disable_wal(true);
                o
            };
            let index = {
                let mut cache = self.address_index.lock().unwrap();
                let entry = cache.entry(address_hash);
                let (index, _) = match entry {
                    Entry::Occupied(mut o) => {
                        let (index, count) = *o.get();
                        // count number of occurrences
                        o.insert((index, count + 1))
                    }
                    Entry::Vacant(v) => {
                        is_new_address = true;
                        let new_key = self.union_find.lock().unwrap().new_key(());
                        *v.insert((new_key, 1))
                    }
                };
                index
            };
            if is_new_address {
                self.permanent_store
                    .put_opt(
                        &index.index().to_le_bytes()[..],
                        addresses_string,
                        &write_opt,
                    )
                    .expect("fail to write to rocksdb");
            }
        }
    }

    #[inline]
    fn addresses_to_string(addresses: &Box<[Address]>) -> Option<String> {
        match addresses.len() {
            0 => None,
            1 => Some(addresses.first().unwrap().to_string()),
            _ => {
                let mut addresses: Vec<String> = addresses.iter().map(|a| a.to_string()).collect();
                // sort addresses
                addresses.sort();
                Some(addresses.join("-"))
            }
        }
    }

    #[inline]
    fn hash(&self, address_string: &str) -> u128 {
        let mut hasher_0 = (&self.random_state_1).build_hasher();
        let mut hasher_1 = (&self.random_state_2).build_hasher();
        hash_string_with_hasher(&mut hasher_0, &mut hasher_1, address_string)
    }
}

#[inline(always)]
fn hash_string_with_hasher(hasher_0: &mut AHasher, hasher_1: &mut AHasher, string: &str) -> u128 {
    string.hash(hasher_0);
    string.hash(hasher_1);
    let hash_0 = (hasher_0.finish() as u128) << 64;
    let hash_1 = hasher_1.finish() as u128;
    hash_0 ^ hash_1
}

///
/// Update balances from a block.
/// Update address_index with new addresses.
///
fn update_balance(
    block: SConnectedBlock,
    balance: &Arc<Mutex<FxHashMap<AddressKey, i64>>>,
    cache: &AddressCacheRead,
) {
    block
        .txdata
        .into_par_iter()
        .for_each_with(cache.clone(), |cache, tx| {
            for tx_out in tx.output {
                if let Some((address_number, _)) = cache.assert_get_address_index(tx_out.addresses)
                {
                    let mut balance = balance.lock().unwrap();
                    *balance.entry(address_number).or_insert(0) += tx_out.value as i64;
                }
            }
            for tx_in in tx.input {
                if let Some((address_number, _)) = cache.assert_get_address_index(tx_in.addresses) {
                    let mut balance = balance.lock().unwrap();
                    *balance.entry(address_number).or_insert(0) -= tx_in.value as i64;
                }
            }
        });
}

fn write_balance(balance: &FxHashMap<AddressKey, i64>, out_dir: &Path, date: Date<Utc>) {
    let file_name = date.format("%Y-%m-%d").to_string() + ".csv";
    let mut out_file = out_dir.to_path_buf();
    out_file.extend(Path::new(&file_name));
    let f = File::create(out_file).unwrap();
    let mut writer = BufWriter::new(f);
    let header = "address_number,balance_change\n";
    writer.write(header.as_bytes()).unwrap();
    for (k, v) in balance {
        if *v != 0 {
            let line = (k.index().to_string() + "," + &v.to_string() + "\n").into_bytes();
            writer.write(&line).unwrap();
        }
    }
    writer.flush().unwrap();
}

fn trailing_zero(value: u64) -> u8 {
    if value == 0 {
        return 0;
    }
    let mut value = value;
    let mut count = 0u8;
    while value % 10 == 0 {
        count += 1;
        value = value / 10;
    }
    count
}

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();
    let db = BitcoinDB::new(Path::new("/116020237/bitcoin"), false).unwrap();
    let end = db.get_block_count();
    let out_dir = Path::new("./out/");
    let bal_out_dir = out_dir.to_path_buf().join("balance");
    let address_out_dir = out_dir.to_path_buf().join("address");
    if !bal_out_dir.exists() {
        fs::create_dir_all(&bal_out_dir).unwrap();
    }
    if !address_out_dir.exists() {
        fs::create_dir_all(&address_out_dir).unwrap();
    }
    info!("launching DB finished");

    // preparing progress bar
    let total_number_of_transactions = (0..end)
        .map(|i| db.get_header(i).unwrap().n_tx)
        .sum::<u32>() as u64;

    info!("load all addresses");
    // preparing progress bar
    let bar = indicatif::ProgressBar::new(total_number_of_transactions);
    bar.set_style(ProgressStyle::default_bar().progress_chars("=>-").template(
        "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>10}/{len:10} ({per_sec}, {eta})",
    ));

    let address_cache = AddressCache::new(&address_out_dir, 900000000);

    // balance writer
    let (balance_sender, balance_receiver) =
        channel::unbounded::<(Arc<Mutex<FxHashMap<AddressKey, i64>>>, Date<Utc>)>();

    // start balance writer thread
    let balance_writer = thread::spawn(move || {
        for (balance, date) in balance_receiver.into_iter() {
            write_balance(&balance.lock().unwrap(), &bal_out_dir, date);
        }
    });

    // load all addresses
    let address_cache_clone = address_cache.clone();
    let address_loader = db
        .iter_block::<SBlock>(0, end)
        .into_par_iter_async(move |blk| {
            let len = blk.txdata.len();
            for tx in blk.txdata {
                for o in tx.output {
                    address_cache_clone.add_new_address(o.addresses);
                }
            }
            Ok(len)
        });
    let mut progress_tracker = 0;
    for progress in address_loader {
        progress_tracker += progress;
        if progress_tracker > 10000 {
            bar.inc(progress_tracker as u64);
            progress_tracker = 0;
        }
    }
    bar.finish();
    info!("finished loading all addresses");

    let address_cache = AddressCacheRead::from_address_cache(address_cache);
    let address_cache_to_dump = address_cache.clone();

    // logic to cluster addresses
    let block_iter = db
        .iter_connected_block::<SConnectedBlock>(end)
        .into_par_iter_sync(move |blk| {
            for tx in &blk.txdata {
                let in_addresses: Vec<(AddressKey, u32)> = tx
                    .input
                    .iter()
                    .map(|i| address_cache.assert_get_address_index(i.addresses.clone()))
                    .filter_map(|x| x)
                    .collect();

                // H1: common spending, all input addresses are connected
                if tx.output.len() == 1 {
                    address_cache.connect_addresses_index(&in_addresses);
                }
                let in_addresses: FxHashSet<AddressKey> = in_addresses
                    .into_iter()
                    .map(|(address, _)| address)
                    .collect();

                // H2: OTX
                let mut otx_address: Option<AddressKey> = None;
                // two output case
                if tx.output.len() == 2 {
                    let first = tx.output.first().unwrap().clone();
                    let last = tx.output.last().unwrap().clone();
                    let first_index = address_cache.assert_get_address_index(first.addresses);
                    let last_index = address_cache.assert_get_address_index(last.addresses);

                    let mut candidate = None;
                    // first has more decimal places, fewer zeros
                    if trailing_zero(first.value) + 4 <= trailing_zero(last.value) {
                        candidate = first_index;
                    } else if trailing_zero(last.value) + 4 <= trailing_zero(first.value) {
                        candidate = last_index;
                    }
                    if let Some((index, count)) = candidate {
                        // only appeared once
                        if count == 1 {
                            otx_address = Some(index);
                        }
                    }
                } else if tx.output.len() > 2 {
                    // H2.4 H2.1 check which index appears only once
                    let all_out_address_index: Vec<Option<(AddressKey, u32)>> = tx
                        .output
                        .iter()
                        .map(|o| address_cache.assert_get_address_index(o.addresses.clone()))
                        .collect();
                    let only_once_index: Vec<AddressKey> = all_out_address_index
                        .iter()
                        .filter_map(|x| {
                            if let Some((index, count)) = *x {
                                if count == 1 {
                                    Some(index)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();
                    // only one address appears only once
                    if only_once_index.len() == 1 {
                        // check that other addresses are not self change
                        let other_index: Vec<AddressKey> = all_out_address_index
                            .iter()
                            .filter_map(|x| {
                                if let Some((index, count)) = *x {
                                    if count > 1 {
                                        Some(index)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect();
                        let mut any_self_change = false;
                        for other in other_index.iter() {
                            if in_addresses.contains(other) {
                                any_self_change = true;
                                break;
                            }
                        }
                        // H2.3: No self-change
                        if !any_self_change {
                            otx_address = Some(*only_once_index.first().unwrap())
                        }
                    }
                }
                if let Some(index) = otx_address {
                    // H2.2 if not coinbase
                    if in_addresses.len() > 0 {
                        let first_in_index = *in_addresses
                            .into_iter()
                            .take(1)
                            .collect::<Vec<AddressKey>>()
                            .first()
                            .unwrap();
                        address_cache.union(&first_in_index, &index)
                    }
                }
            }
            Ok(blk)
        });

    // clustering and balance computing
    info!("start clustering address and compute daily balance");
    let bar = indicatif::ProgressBar::new(total_number_of_transactions);
    bar.set_style(ProgressStyle::default_bar().progress_chars("=>-").template(
        "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>10}/{len:10} ({per_sec}, {eta})",
    ));
    let mut bal_change: Arc<Mutex<FxHashMap<AddressKey, i64>>> =
        Arc::new(Mutex::new(FxHashMap::default()));
    let mut prev_date: Option<Date<Utc>> = None;
    let mut progress = 0;
    for blk in block_iter {
        let datetime = NaiveDateTime::from_timestamp(blk.header.time as i64, 0);
        let date = Date::from_utc(datetime.date(), Utc);
        if let Some(prev_date) = prev_date {
            if date > prev_date {
                balance_sender
                    .send((bal_change, prev_date.clone()))
                    .unwrap();
                bal_change = Arc::new(Mutex::new(FxHashMap::default()));
            }
        }
        prev_date = Some(date);
        let len = blk.txdata.len();
        update_balance(blk, &bal_change, &address_cache_to_dump);
        progress += len;
        if progress > 10000 {
            bar.inc(progress as u64);
            progress = 0;
        }
    }
    bar.finish();

    // no more other reference to address_cache, dump clustering and counting result
    address_cache_to_dump.dump(&out_dir);

    // drop sender
    drop(balance_sender);
    // join receiver thread
    balance_writer.join().unwrap();

    info!("job finished");
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test() {
        assert_eq!(trailing_zero(120), 1u8);
        assert_eq!(trailing_zero(99999999990), 1u8);
        assert_eq!(trailing_zero(9000000000000000000), 18u8);
        assert_eq!(trailing_zero(0), 0u8);
        assert_eq!(trailing_zero(1200), 2u8);
    }
}
