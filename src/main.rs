use bitcoin_explorer::{BitcoinDB, Address, SConnectedBlock};
use chrono::{Date, NaiveDateTime, Utc};
use indicatif;
use indicatif::ProgressStyle;
use log::info;
use simple_logger::SimpleLogger;
use siphasher::sip128::{Hasher128, SipHasher13};
use std::fs;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;
use std::thread;
use std::thread::JoinHandle;
use hash_hasher::HashedMap;
use rustc_hash::FxHashMap;

struct AsyncBufWriter<W: Write> {
    worker: Option<JoinHandle<()>>,
    sender: SyncSender<Box<[u8]>>,
    p: PhantomData<W>,
}

impl<W: 'static + Write + Send> AsyncBufWriter<W> {

    fn new(mut writer: W) -> AsyncBufWriter<W> {
        let (sender, receiver) = mpsc::sync_channel::<Box<[u8]>>(1000);
        let worker = thread::spawn(move || {
            for line in receiver.into_iter() {
                writer.write_all(&line).unwrap();
            }
            writer.flush().unwrap();
        });
        AsyncBufWriter {
            worker: Some(worker),
            sender,
            p: PhantomData::default(),
        }
    }

    fn write_all(&self, buf: Box<[u8]>) {
        self.sender.send(buf).unwrap();
    }

}

impl<W: Write> Drop for AsyncBufWriter<W> {
    fn drop(&mut self) {
        self.worker.take().unwrap().join().unwrap()
    }
}

struct AddressCache {
    address_index: HashedMap<u128, usize>,
    permanent_store: AsyncBufWriter<File>,
}

impl AddressCache {

    pub fn new(out_dir: &Path) -> AddressCache {
        let file_name = "addresses.csv";
        let mut out_file = out_dir.to_path_buf();
        out_file.extend(Path::new(&file_name));
        let out_file = File::create(out_file).unwrap();
        let permanent_store = AsyncBufWriter::new(out_file);
        permanent_store
            .write_all("address_number,address\n".to_owned().into_boxed_str().into_boxed_bytes());
        AddressCache {
            address_index: HashedMap::default(),
            permanent_store,
        }
    }

    pub fn get_address_hash(&self, addresses: Box<[Address]>) -> Option<u128> {
        if let Some(addresses) = Self::addresses_to_string(addresses) {
            Some(Self::hash(&addresses))
        } else {
            None
        }
    }

    pub fn get_address_index(&self, hash: u128) -> Option<usize> {
        self.address_index.get(&hash).map(|x| x.to_owned())
    }

    pub fn try_get_or_add_address_index(&mut self, addresses: Box<[Address]>) -> Option<usize> {
        if let Some(addresses_string) = Self::addresses_to_string(addresses) {
            let address_hash = Self::hash(&addresses_string);
            if !self.contains_address(&address_hash) {
                let new_index = self.address_index.len();
                let line = (new_index.to_string() + "," + &addresses_string + "\n").into_bytes();
                self.permanent_store.write_all(line.into_boxed_slice());
                self.address_index.insert(address_hash, new_index);
                Some(new_index)
            } else {
                self.get_address_index(address_hash)
            }
        } else {
            None
        }
    }

    #[inline]
    fn contains_address(&self, hash: &u128) -> bool {
        self.address_index.contains_key(hash)
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
        let mut hasher = SipHasher13::new();
        address_string.hash(&mut hasher);
        hasher.finish128().as_u128()
    }
}

///
/// Update balances from a block.
/// Update address_index with new addresses.
///
fn update_balance(
    block: SConnectedBlock,
    balance: &mut FxHashMap<usize, i64>,
    cache: &mut AddressCache,
) {
    for tx in block.txdata {
        for tx_in in tx.input {
            // skip those without addresses
            if let Some(address_hash) = cache.get_address_hash(tx_in.addresses) {
                let address_number = cache
                    .get_address_index(address_hash)
                    .expect("new addresses only appear in tx_out");
                if !balance.contains_key(&address_number) {
                    balance.insert(address_number, -(tx_in.value as i64));
                } else {
                    *balance.get_mut(&address_number).unwrap() -= tx_in.value as i64;
                }
            }
        }
        for tx_out in tx.output {
            // skip those without addresses
            if let Some(address_number) = cache.try_get_or_add_address_index(tx_out.addresses) {
                if !balance.contains_key(&address_number) {
                    balance.insert(address_number, tx_out.value as i64);
                } else {
                    *balance.get_mut(&address_number).unwrap() += tx_out.value as i64;
                }
            }
        }
    }
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
    let out_dir = Path::new("/out/balances/");
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

    let (sender, receiver) = mpsc::sync_channel::<(FxHashMap<usize, i64>, Date<Utc>)>(10);

    // start writer thread
    let writer = thread::spawn(move || {
        for (balance, date) in receiver.into_iter() {
            write_balance(&balance, out_dir, date);
        }
    });

    // producer thread
    let producer = thread::spawn(move || {
        // initialize
        let mut address_cache = AddressCache::new(out_dir);
        let mut bal_change: FxHashMap<usize, i64> = FxHashMap::default();
        let mut prev_date: Option<Date<Utc>> = None;

        for blk in db.iter_connected_block::<SConnectedBlock>(end as u32) {
            let datetime = NaiveDateTime::from_timestamp(blk.header.time as i64, 0);
            let date = Date::from_utc(datetime.date(), Utc);
            if let Some(prev_date) = prev_date {
                if date > prev_date {
                    sender
                        .send((bal_change.clone(), prev_date.clone()))
                        .unwrap();
                    bal_change = FxHashMap::default();
                }
            }
            prev_date = Some(date);
            let len = blk.txdata.len();
            update_balance(blk, &mut bal_change, &mut address_cache);
            bar.inc(len as u64)
        }
        bar.finish();
    });

    producer.join().unwrap();
    writer.join().unwrap();
}
