use ahash::AHasher;
use bitcoin_explorer::{Address, BitcoinDB, SBlock, SConnectedBlock};
use chrono::{Date, NaiveDateTime, Utc};
use db_key::Key;
use hash_hasher::{HashBuildHasher, HashedMap};
use indicatif;
use indicatif::ProgressStyle;
use leveldb::database::Database;
use leveldb::kv::KV;
use leveldb::options::{Options, WriteOptions};
use log::{info, LevelFilter};
use par_iter_sync::*;
use partitions::PartitionVec;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use simple_logger::SimpleLogger;
use std::collections::hash_map::Entry;
use std::convert::TryInto;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::ops::Deref;
use std::path::Path;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
struct AddressKey(u32);

impl Deref for AddressKey {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Key for AddressKey {
    fn from_u8(key: &[u8]) -> Self {
        AddressKey(u32::from_le_bytes(key.try_into().unwrap()))
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(&self.0.to_le_bytes()[..])
    }
}

#[derive(Clone)]
struct AsyncBufWriter {
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    sender: Sender<Box<[u8]>>,
}

impl AsyncBufWriter {
    fn new(writer: File) -> AsyncBufWriter {
        let (sender, receiver) = mpsc::channel::<Box<[u8]>>();
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
    union_find: Arc<Mutex<PartitionVec<()>>>,
}

impl AddressCacheRead {
    pub fn from_address_cache(cache: AddressCache) -> Self {
        let get_inner_lock = Arc::try_unwrap(cache.address_index);
        AddressCacheRead {
            address_index: Arc::new(get_inner_lock.unwrap().into_inner().unwrap()),
            union_find: cache.union_find,
        }
    }

    #[inline]
    pub fn connect_addresses_index(&self, addresses_index: &Vec<(AddressKey, u32)>) {
        if addresses_index.len() > 1 {
            let (first, _) = addresses_index.first().unwrap();
            for (rest, _) in addresses_index.iter().skip(1) {
                // connect rest to first
                self.union(*first, *rest);
            }
        }
    }

    #[inline]
    pub fn assert_get_address_index(&self, addresses: Box<[Address]>) -> Option<(AddressKey, u32)> {
        if let Some(address_string) = AddressCache::addresses_to_string(&addresses) {
            Some(
                self.get_address_index(AddressCache::hash(&address_string))
                    .expect("inputs must exist in UTXO"),
            )
        } else {
            None
        }
    }

    #[inline]
    fn union(&self, i1: AddressKey, i2: AddressKey) {
        self.union_find
            .lock()
            .unwrap()
            .union(*i1 as usize, *i2 as usize)
    }

    #[inline]
    fn get_address_index(&self, hash: u128) -> Option<(AddressKey, u32)> {
        // sync
        self.address_index.get(&hash).map(|x| x.to_owned())
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
                        counting_vec[*index as usize] = count
                    }
                });

            info!("start dumping address counting and clustering result");
            let bar = indicatif::ProgressBar::new(total_keys as u64);
            bar.set_style(ProgressStyle::default_bar().progress_chars("=>-").template(
                "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>10}/{len:10} ({per_sec}, {eta})",
            ));
            (0u32..total_keys)
                .into_par_iter_sync(move |i| Ok(union_find.find_final(i as usize) as u32))
                .zip(counting_vec)
                .for_each(|(cluster, count)| {
                    clustering_writer.write_u32_le(cluster);
                    counting_writer.write_u32_le(count);
                    bar.inc(1);
                });
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
    union_find: Arc<Mutex<PartitionVec<()>>>,
    permanent_store: Arc<Database<AddressKey>>,
}

impl AddressCache {
    pub fn new(out_dir: &Path, capacity: usize) -> AddressCache {
        let option = {
            let mut o = Options::new();
            o.create_if_missing = true;
            o.block_size = Some(1024 * 1024 * 1024);
            o.write_buffer_size = Some(128 * 1024 * 1024);
            o.error_if_exists = true;
            o
        };
        let permanent_store = Arc::new(match Database::open(out_dir, option) {
            Ok(db) => db,
            Err(e) => panic!("Failed to store addresses index: {}", e),
        });
        let union_find = Arc::new(Mutex::new(PartitionVec::with_capacity(900_000_000)));
        AddressCache {
            address_index: Arc::new(Mutex::new(HashedMap::with_capacity_and_hasher(
                capacity,
                HashBuildHasher::default(),
            ))),
            union_find,
            permanent_store,
        }
    }

    #[inline]
    pub fn add_new_address(&self, addresses: Box<[Address]>) {
        if let Some(addresses_string) = Self::addresses_to_string(&addresses) {
            let address_hash = Self::hash(&addresses_string);
            let mut is_new_address = false;
            // cache lock scope
            let index = {
                let mut cache = self.address_index.lock().unwrap();
                let new_index = AddressKey(cache.len() as u32);
                let entry = cache.entry(address_hash);
                let (index, _) = match entry {
                    Entry::Occupied(mut o) => {
                        let (index, count) = *o.get();
                        // count number of occurrences
                        o.insert((index, count + 1))
                    }
                    Entry::Vacant(v) => {
                        is_new_address = true;
                        *v.insert((new_index, 1))
                    }
                };
                index
            };
            if is_new_address {
                self.permanent_store
                    .put(WriteOptions::new(), index, addresses_string.as_bytes()).expect("failed to write to levelDB");
                self.union_find.lock().unwrap().push(());
            }
        }
    }

    #[inline]
    fn addresses_to_string(addresses: &Box<[Address]>) -> Option<String> {
        match addresses.len() {
            0 => None,
            1 => Some(addresses.get(0).unwrap().to_string()),
            _ => {
                let mut addresses: Vec<String> = addresses.iter().map(|a| a.to_string()).collect();
                // sort addresses
                addresses.sort();
                Some(addresses.join("-"))
            }
        }
    }

    #[inline]
    fn hash(address_string: &str) -> u128 {
        let mut hasher_0 = AHasher::new_with_keys(54321, 12345);
        let mut hasher_1 = AHasher::new_with_keys(12345, 54321);
        address_string.hash(&mut hasher_0);
        address_string.hash(&mut hasher_1);
        let hash_0 = (hasher_0.finish() as u128) << 64;
        let hash_1 = hasher_1.finish() as u128;
        hash_0 ^ hash_1
    }
}

///
/// Update balances from a block.
/// Update address_index with new addresses.
///
fn update_balance(
    block: SConnectedBlock,
    balance: &Arc<Mutex<FxHashMap<u32, i64>>>,
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
                    *balance.entry(*address_number).or_insert(0) += tx_out.value as i64;
                }
            }
            for tx_in in tx.input {
                if let Some((address_number, _)) = cache.assert_get_address_index(tx_in.addresses) {
                    let mut balance = balance.lock().unwrap();
                    *balance.entry(*address_number).or_insert(0) -= tx_in.value as i64;
                }
            }
        });
}

fn write_balance(balance: &FxHashMap<u32, i64>, out_dir: &Path, date: Date<Utc>) {
    let file_name = date.format("%Y-%m-%d").to_string() + ".csv";
    let mut out_file = out_dir.to_path_buf();
    out_file.extend(Path::new(&file_name));
    let f = File::create(out_file).unwrap();
    let mut writer = BufWriter::new(f);
    let header = "address_number,balance_change\n";
    writer.write(header.as_bytes()).unwrap();
    for (k, v) in balance {
        if *v != 0 {
            let line = (k.to_string() + "," + &v.to_string() + "\n").into_bytes();
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
        mpsc::channel::<(Arc<Mutex<FxHashMap<u32, i64>>>, Date<Utc>)>();

    // start balance writer thread
    let balance_writer = thread::spawn(move || {
        for (balance, date) in balance_receiver.into_iter() {
            write_balance(&balance.lock().unwrap(), &bal_out_dir, date);
        }
    });

    // load all addresses
    let address_cache_clone = address_cache.clone();
    db.iter_block::<SBlock>(0, end)
        .into_par_iter_async(move |blk| {
            let len = blk.txdata.len();
            for tx in blk.txdata {
                for o in tx.output {
                    address_cache_clone.add_new_address(o.addresses);
                }
            }
            Ok(len)
        })
        .for_each(|p| bar.inc(p as u64));
    info!("finished loading all addresses");

    let address_cache = AddressCacheRead::from_address_cache(address_cache);
    let address_cache_to_dump = address_cache.clone();

    // logic to cluster addresses
    let block_iter = db
        .iter_connected_block::<SConnectedBlock>(end)
        .into_par_iter_async(move |blk| {
            for tx in &blk.txdata {
                let in_addresses: Vec<(AddressKey, u32)> = tx
                    .input
                    .iter()
                    .map(|i| address_cache.assert_get_address_index(i.addresses.clone()))
                    .filter_map(|x| x)
                    .collect();

                // H1: common spending, all input addresses are connected
                address_cache.connect_addresses_index(&in_addresses);

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
                    if trailing_zero(first.value) + 3 <= trailing_zero(last.value) {
                        candidate = first_index;
                    } else if trailing_zero(last.value) + 3 <= trailing_zero(first.value) {
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
                    let only_once_index: Vec<AddressKey> = tx
                        .output
                        .iter()
                        .map(|o| address_cache.assert_get_address_index(o.addresses.clone()))
                        .filter_map(|x| {
                            if let Some((index, count)) = x {
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
                        otx_address = Some(*only_once_index.first().unwrap())
                    }
                }
                if let Some(index) = otx_address {
                    // H2.2 if not coinbase, H2.3 can be ignored
                    if in_addresses.len() > 0 {
                        let (first_in_index, _) = *in_addresses.first().unwrap();
                        address_cache.union(first_in_index, index)
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
    let mut bal_change: Arc<Mutex<FxHashMap<u32, i64>>> =
        Arc::new(Mutex::new(FxHashMap::default()));
    let mut prev_date: Option<Date<Utc>> = None;
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
        bar.inc(len as u64)
    }
    bar.finish();

    // no more other reference to address_cache, dump clustering and counting result
    address_cache_to_dump.dump(&out_dir);

    // join remaining threads
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
