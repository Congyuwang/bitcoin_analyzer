mod address;
mod utility;

use crate::utility::trailing_zero;
use address::*;
use bitcoin_explorer::{BitcoinDB, SBlock, SConnectedBlock, SConnectedTransaction};
use chrono::{Date, NaiveDateTime, Utc};
use crossbeam::channel;
use ena::unify::UnifyKey;
use indicatif;
use indicatif::ProgressStyle;
use log::{info, LevelFilter};
use par_iter_sync::*;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use simple_logger::SimpleLogger;
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;

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
                if let Some((address_number, _)) = cache.assert_get_address_index(&tx_out.addresses)
                {
                    let mut balance = balance.lock().unwrap();
                    *balance.entry(address_number).or_insert(0) += tx_out.value as i64;
                }
            }
            for tx_in in tx.input {
                if let Some((address_number, _)) = cache.assert_get_address_index(&tx_in.addresses)
                {
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

fn h2(
    tx: &SConnectedTransaction,
    cache: &AddressCacheRead,
    input_addresses: &Vec<AddressKey>,
) -> Option<AddressKey> {
    // convert to hashmap to check self-change
    let in_addresses_set: FxHashSet<AddressKey> = input_addresses.iter().map(|x| *x).collect();
    if tx.output.len() == 2 {
        h2_two_input_case(tx, cache)
    } else if tx.output.len() > 2 {
        h2_multiple_input_case(tx, cache, &in_addresses_set)
    } else {
        None
    }
}

fn h2_two_input_case(tx: &SConnectedTransaction, cache: &AddressCacheRead) -> Option<AddressKey> {
    let first = tx.output.first().unwrap();
    let last = tx.output.last().unwrap();

    let first_trailing_zeros = trailing_zero(first.value);
    let last_trailing_zeros = trailing_zero(last.value);

    // first has more decimal places, fewer zeros
    let candidate = if first_trailing_zeros + 4 <= last_trailing_zeros {
        cache.assert_get_address_index(&first.addresses)
    } else if last_trailing_zeros + 4 <= first_trailing_zeros {
        cache.assert_get_address_index(&last.addresses)
    } else {
        None
    };
    if let Some((index, count)) = candidate {
        // only appeared once
        if count == 1 {
            Some(index)
        } else {
            None
        }
    } else {
        None
    }
}

fn h2_multiple_input_case(
    tx: &SConnectedTransaction,
    cache: &AddressCacheRead,
    input_addresses: &FxHashSet<AddressKey>,
) -> Option<AddressKey> {
    // H2.4 H2.1 check which index appears only once
    let out_address: Vec<Option<(AddressKey, u32)>> = tx
        .output
        .iter()
        .map(|o| cache.assert_get_address_index(&o.addresses))
        .collect();
    let only_once_index: Vec<AddressKey> = out_address
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

    if only_once_index.len() == 1 {
        // H2.3: No self-change
        for (other, _) in out_address.iter().filter_map(|x| *x) {
            if input_addresses.contains(&other) {
                return None;
            }
        }
        Some(*only_once_index.first().unwrap())
    } else {
        None
    }
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
                let in_addresses: Vec<AddressKey> = tx
                    .input
                    .iter()
                    .map(|i| address_cache.assert_get_address_index(&i.addresses))
                    .filter_map(|x| x)
                    .map(|(x, _)| x)
                    .collect();

                // H1: common spending, all input addresses are connected
                if tx.output.len() == 1 {
                    address_cache.connect_addresses_index(&in_addresses);
                }

                // H2: OTX
                if let Some(otx_index) = h2(tx, &address_cache, &in_addresses) {
                    // H2.2 if not coinbase, (coinbase does not have input)
                    for in_add in in_addresses.iter() {
                        address_cache.union(in_add, &otx_index)
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

    // balance computation
    for blk in block_iter {
        let date = Date::from_utc(
            NaiveDateTime::from_timestamp(blk.header.time as i64, 0).date(),
            Utc,
        );
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
