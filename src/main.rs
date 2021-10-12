use bitcoin_explorer::{Address, BitcoinDB, SConnectedBlock};
use chrono::{Date, NaiveDateTime, Utc};
use hash_hasher::HashedMap;
use indicatif;
use indicatif::ProgressStyle;
use log::info;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use simple_logger::SimpleLogger;
use ahash::AHasher;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

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
}

impl Drop for AsyncBufWriter {
    fn drop(&mut self) {
        if let Some(handle) = self.worker.lock().unwrap().take() {
            handle.join().unwrap();
        }
    }
}

#[derive(Clone)]
struct AddressCache {
    address_index: Arc<Mutex<HashedMap<u128, usize>>>,
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
        AddressCache {
            address_index: Arc::new(Mutex::new(HashedMap::default())),
            permanent_store,
        }
    }

    #[inline]
    pub fn try_get_or_add_address_index(&self, addresses: Box<[Address]>) -> Option<usize> {
        if let Some(addresses_string) = Self::addresses_to_string(addresses) {
            let address_hash = Self::hash(&addresses_string);
            let mut is_new_address = false;
            let index = {
                let mut cache = self.address_index.lock().unwrap();
                let new_index = cache.len();
                *cache.entry(address_hash).or_insert_with(|| {
                    is_new_address = true;
                    new_index
                })
            };
            if is_new_address {
                let line = (index.to_string() + "," + &addresses_string + "\n").into_bytes();
                // sync
                self.permanent_store.write_all(line.into_boxed_slice());
            }
            Some(index)
        } else {
            None
        }
    }

    #[inline]
    pub fn get_address_hash(&self, addresses: Box<[Address]>) -> Option<u128> {
        if let Some(addresses) = Self::addresses_to_string(addresses) {
            Some(Self::hash(&addresses))
        } else {
            None
        }
    }

    #[inline]
    pub fn get_address_index(&self, hash: u128) -> Option<usize> {
        // sync
        self.address_index
            .lock()
            .unwrap()
            .get(&hash)
            .map(|x| x.to_owned())
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
}

///
/// Update balances from a block.
/// Update address_index with new addresses.
///
fn update_balance(
    block: SConnectedBlock,
    balance: &Arc<Mutex<FxHashMap<usize, i64>>>,
    cache: AddressCache,
) {
    let mut ins = Vec::new();
    let mut outs = Vec::new();
    for tx in block.txdata {
        ins.extend(tx.input);
        outs.extend(tx.output);
    }
    outs.into_par_iter().for_each_with(cache.clone(), |cache, tx_out| {
        // skip those without addresses
        if let Some(address_number) = cache.try_get_or_add_address_index(tx_out.addresses) {
            let mut balance = balance.lock().unwrap();
            *balance.entry(address_number).or_insert(0) += tx_out.value as i64;
        }
    });
    ins.into_par_iter().for_each_with(cache, |cache, tx_in| {
        // skip those without addresses
        if let Some(address_hash) = cache.get_address_hash(tx_in.addresses) {
            let address_number = cache
                .get_address_index(address_hash)
                .expect("new addresses only appear in tx_out");
            let mut balance = balance.lock().unwrap();
            *balance.entry(address_number).or_insert(0) -= tx_in.value as i64;
        }
    });
}

fn write_balance(balance: &FxHashMap<usize, i64>, out_dir: &Path, date: Date<Utc>) {
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

fn main() {
    SimpleLogger::new().init().unwrap();
    let end = 700000;
    let db = BitcoinDB::new(Path::new("/116020237/bitcoin"), false).unwrap();
    let out_dir = Path::new("./out/balances/");
    if !out_dir.exists() {
        fs::create_dir_all(out_dir).unwrap();
    }
    info!("launching DB finished");

    // preparing progress bar
    let total_number_of_transactions = (0..end)
        .map(|i| db.get_header(i).unwrap().n_tx)
        .sum::<u32>() as u64;
    let bar = indicatif::ProgressBar::new(total_number_of_transactions);
    bar.set_style(ProgressStyle::default_bar().progress_chars("=>-").template(
        "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>10}/{len:10} ({per_sec}, {eta})",
    ));

    let (sender, receiver) = mpsc::channel::<(Arc<Mutex<FxHashMap<usize, i64>>>, Date<Utc>)>();

    // start writer thread
    let writer = thread::spawn(move || {
        for (balance, date) in receiver.into_iter() {
            write_balance(&balance.lock().unwrap(), out_dir, date);
        }
    });

    // producer thread
    let producer = thread::spawn(move || {
        // initialize
        let address_cache = AddressCache::new(out_dir);
        let mut bal_change: Arc<Mutex<FxHashMap<usize, i64>>> =
            Arc::new(Mutex::new(FxHashMap::default()));
        let mut prev_date: Option<Date<Utc>> = None;

        for blk in db.iter_connected_block::<SConnectedBlock>(end as u32) {
            let datetime = NaiveDateTime::from_timestamp(blk.header.time as i64, 0);
            let date = Date::from_utc(datetime.date(), Utc);
            if let Some(prev_date) = prev_date {
                if date > prev_date {
                    sender.send((bal_change, prev_date.clone())).unwrap();
                    bal_change = Arc::new(Mutex::new(FxHashMap::default()));
                }
            }
            prev_date = Some(date);
            let len = blk.txdata.len();
            update_balance(blk, &bal_change, address_cache.clone());
            bar.inc(len as u64)
        }
        bar.finish();
        println!("job finished");
    });

    producer.join().unwrap();
    writer.join().unwrap();
}
