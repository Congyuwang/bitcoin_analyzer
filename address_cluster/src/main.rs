use ahash::AHasher;
use bitcoin_explorer::{Address, BitcoinDB, SBlock, SConnectedBlock};
use ena::unify::{InPlaceUnificationTable, UnifyKey};
use hash_hasher::HashedMap;
use indicatif;
use indicatif::ProgressStyle;
use log::{info, LevelFilter};
use par_iter_sync::IntoParallelIteratorSync;
use simple_logger::SimpleLogger;
use std::collections::hash_map::Entry;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread;
use std::thread::JoinHandle;

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
struct UnitKey(u32);

impl Into<u32> for UnitKey {
    fn into(self) -> u32 {
        self.0
    }
}

impl From<u32> for UnitKey {
    fn from(v: u32) -> Self {
        UnitKey(v)
    }
}

impl UnifyKey for UnitKey {
    type Value = ();
    fn index(&self) -> u32 {
        self.0
    }
    fn from_index(u: u32) -> UnitKey {
        UnitKey(u)
    }
    fn tag() -> &'static str {
        "UnitKey"
    }
}

#[derive(Clone)]
struct AsyncBufWriter {
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    sender: Sender<Box<[u8]>>,
}

impl AsyncBufWriter {
    fn new(mut writer: File) -> AsyncBufWriter {
        let (sender, receiver) = mpsc::channel::<Box<[u8]>>();
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

    fn write_all(&self, buf: Box<[u8]>) {
        self.sender.send(buf).unwrap();
    }

    // join the writer thread back to main thread
    fn pop_handle(&mut self) -> Option<JoinHandle<()>> {
        self.worker.lock().unwrap().take()
    }
}

#[derive(Clone)]
struct AddressCache {
    // (index, count)
    address_index: Arc<Mutex<HashedMap<u128, (u32, u32)>>>,
    union_find: Arc<Mutex<InPlaceUnificationTable<UnitKey>>>,
    permanent_store: AsyncBufWriter,
}

impl AddressCache {
    pub fn new(out_dir: &Path) -> AddressCache {
        let file_name = "addresses.csv";
        let mut out_file = out_dir.to_path_buf();
        out_file.extend(Path::new(&file_name));
        let out_file = File::create(out_file).unwrap();
        let permanent_store = AsyncBufWriter::new(out_file);
        permanent_store.write_all(
            "address_number,address\n"
                .to_owned()
                .into_boxed_str()
                .into_boxed_bytes(),
        );
        let union_find = {
            let mut tb = InPlaceUnificationTable::new();
            tb.reserve(900_000_000);
            Arc::new(Mutex::new(tb))
        };
        AddressCache {
            address_index: Arc::new(Mutex::new(HashedMap::default())),
            union_find,
            permanent_store,
        }
    }

    pub fn connect_addresses_index(&self, addresses_index: &Vec<u32>) {
        if addresses_index.len() > 1 {
            let first = addresses_index.first().unwrap();
            for rest in addresses_index.iter().skip(1) {
                // connect rest to first
                self.union(*first, *rest);
            }
        }
    }

    #[inline]
    pub fn add_new_address(&self, addresses: Box<[Address]>) {
        if let Some(addresses_string) = Self::addresses_to_string(addresses) {
            let address_hash = Self::hash(&addresses_string);
            let mut is_new_address = false;
            // cache lock scope
            let index = {
                let mut cache = self.address_index.lock().unwrap();
                let new_index = cache.len() as u32;
                let entry = cache.entry(address_hash);
                let (index, _) = match entry {
                    Entry::Occupied(mut o) => {
                        let (index, count) = o.get();
                        o.insert((*index, count + 1))
                    }
                    Entry::Vacant(v) => {
                        *v.insert((new_index, 1))
                    }
                };
                index
            };
            if is_new_address {
                let line = (index.to_string() + "," + &addresses_string + "\n").into_bytes();
                // sync
                self.permanent_store.write_all(line.into_boxed_slice());
            }
        }
    }

    pub fn assert_get_address_index(&self, addresses: Box<[Address]>) -> Option<u32> {
        if let Some(address_string) = AddressCache::addresses_to_string(addresses) {
            Some(
                self.get_address_index(AddressCache::hash(&address_string))
                    .expect("inputs must exist in UTXO"),
            )
        } else {
            None
        }
    }

    #[inline]
    fn connected(&self, hash1: u32, hash2: u32) -> bool {
        self.union_find.lock().unwrap().unioned(hash1, hash2)
    }

    #[inline]
    fn union(&self, i1: u32, i2: u32) {
        self.union_find.lock().unwrap().union(i1, i2)
    }

    #[inline]
    fn find(&self, i: u32) -> u32 {
        self.union_find.lock().unwrap().find(i).into()
    }

    #[inline]
    pub fn address_hash(addresses: Box<[Address]>) -> Option<u128> {
        if let Some(addresses) = Self::addresses_to_string(addresses) {
            Some(Self::hash(&addresses))
        } else {
            None
        }
    }

    #[inline]
    pub fn get_address_index(&self, hash: u128) -> Option<u32> {
        // sync
        self.address_index
            .lock()
            .unwrap()
            .get(&hash)
            .map(|(x, _)| x.to_owned())
    }

    #[inline]
    fn addresses_to_string(addresses: Box<[Address]>) -> Option<String> {
        match addresses.len() {
            0 => None,
            1 => Some(addresses.get(0).unwrap().to_string()),
            _ => {
                let mut addresses: Vec<String> =
                    addresses.into_iter().map(|a| a.to_string()).collect();
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

    fn pop_handle(&mut self) -> Option<JoinHandle<()>> {
        self.permanent_store.pop_handle()
    }
}

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();
    let db = BitcoinDB::new(Path::new("/116020237/bitcoin"), false).unwrap();
    let end = db.get_block_count();
    let out_dir = Path::new("./out/balances/");
    if !out_dir.exists() {
        fs::create_dir_all(out_dir).unwrap();
    }
    info!("launching DB finished");

    info!("load all addresses");
    // preparing progress bar
    let total_number_of_transactions = (0..end)
        .map(|i| db.get_header(i).unwrap().n_tx)
        .sum::<u32>() as u64;
    let bar = indicatif::ProgressBar::new(total_number_of_transactions);
    bar.set_style(ProgressStyle::default_bar().progress_chars("=>-").template(
        "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>10}/{len:10} ({per_sec}, {eta})",
    ));

    let mut address_cache = AddressCache::new(out_dir);
    let address_writer_handle = address_cache.pop_handle();

    // load all addresses
    let address_cache_clone = address_cache.clone();
    db.iter_block::<SBlock>(0, end)
        .into_par_iter_sync(move |blk| {
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

    // preparing progress bar
    let total_number_of_transactions = (0..end)
        .map(|i| db.get_header(i).unwrap().n_tx)
        .sum::<u32>() as u64;
    let bar = indicatif::ProgressBar::new(total_number_of_transactions);
    bar.set_style(ProgressStyle::default_bar().progress_chars("=>-").template(
        "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>10}/{len:10} ({per_sec}, {eta})",
    ));

    // producer thread
    let producer = thread::spawn(move || {
        let mut address_cache_clone = address_cache.clone();
        let par_iter = db
            .iter_connected_block::<SConnectedBlock>(end)
            .into_par_iter_sync(move |blk| {
                // let mut count = 0;
                for tx in &blk.txdata {
                    let out_addresses: Vec<u32> = tx
                        .output
                        .iter()
                        .map(|o| address_cache_clone.assert_get_address_index(o.addresses.clone()))
                        .filter_map(|x| x)
                        .collect();
                    // H3: coinbase tx
                    // if count == 0 {
                    //     address_cache.connect_addresses_index(&out_addresses);
                    // }
                    // count += 1;
                }
                Ok(blk)
            })
            .into_par_iter_sync(move |blk| {
                let progress = blk.txdata.len() as u64;
                for tx in &blk.txdata {
                    let in_addresses: Vec<u32> = tx
                        .input
                        .iter()
                        .map(|i| address_cache.assert_get_address_index(i.addresses.clone()))
                        .filter_map(|x| x)
                        .collect();
                    // H1: common spending
                    address_cache.connect_addresses_index(&in_addresses);
                }
                Ok(progress)
            })
            .for_each(|p| bar.inc(p));
        bar.finish();
        println!("job finished");
    });

    // drop producer first
    producer.join().unwrap();

    // address_cache_writer wait for all address_cache to be dropped
    address_writer_handle.unwrap().join().unwrap();
}
