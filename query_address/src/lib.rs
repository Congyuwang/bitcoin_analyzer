mod error;

use indicatif;
use indicatif::ProgressStyle;
#[cfg(feature = "debug")]
use log::debug;
use log::{info, warn, LevelFilter};
use num_cpus;
use rocksdb::{
    DBRawIteratorWithThreadMode, IteratorMode, Options, PlainTableFactoryOptions, ReadOptions,
    SliceTransform, DB,
};
use simple_logger::SimpleLogger;
use std::fs::File;
use std::io::{stdin, stdout, BufWriter, Write};
use std::path::Path;
use std::str;
use crate::error::DBError;
use crate::error::DBError::{NotFoundErr, QueryErr, UtfDecodeErr};

pub fn run() {
    #[cfg(feature = "debug")]
    SimpleLogger::new()
        .with_level(LevelFilter::Debug)
        .init()
        .unwrap();
    #[cfg(not(feature = "debug"))]
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();
    // launchDB
    let db: DB = loop {
        println!(">> enter path to addresses, or press `enter` to use default `./out/address` (or type `exit` to quit)");
        print!("> ");
        stdout().flush().unwrap();
        let mut db_path = String::new();
        stdin().read_line(&mut db_path).unwrap();

        // exit command
        if db_path.trim() == "exit" {
            return ();
        }

        // if not exit
        let db_path = if db_path.trim().is_empty() {
            Path::new("./out/address")
        } else {
            Path::new(db_path.trim())
        };
        if !db_path.exists() {
            warn!("bitcoin path: {} not found", db_path.display());
            continue;
        }
        let mut options = Options::default();
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
        info!("start reading DB");
        match DB::open(&options, db_path) {
            Ok(db) => {
                info!("successfully opened DB");
                break db;
            }
            Err(e) => {
                warn!("failed to open address DB: {}", e);
                continue;
            }
        }
    };

    let mut query = String::new();
    println!(">> enter `exit` to quit, `help` for help");
    loop {
        print!("> ");
        stdout().flush().unwrap();
        query.clear();
        stdin().read_line(&mut query).unwrap();

        // exit
        if query.trim() == "exit" {
            break;
        }

        // exit
        if query.trim() == "help" {
            print_help();
            continue;
        }

        // range print
        if query.trim() == "range" {
            print!("start > ");
            stdout().flush().unwrap();
            query.clear();
            stdin().read_line(&mut query).unwrap();
            let start = query.trim().to_string();

            print!("end   > ");
            stdout().flush().unwrap();
            query.clear();
            stdin().read_line(&mut query).unwrap();
            let end = query.trim().to_string();

            match (start.parse::<u32>(), end.parse::<u32>()) {
                (Ok(start), Ok(end)) => {
                    if start < end {
                        if end - start > 10000 {
                            println!("Use `export` command for more than 10000 output");
                            continue;
                        }
                        for i in start..end {
                            if let Err(e) = get_pinned(&db, i, |s| println!("{},{}", i, s)) {
                                warn!("get error: {}", e);
                                break;
                            }
                        }
                    } else {
                        warn!("`start` must be smaller than `end`");
                    }
                }
                _ => {
                    warn!("invalid start or end (not a valid u32 integer)")
                }
            }
            continue;
        }

        // export to csv
        if query.trim() == "export" {
            // create output path
            println!(">> enter a directory to store `address.csv`, or press `enter` to use default `./address_export`");
            print!("> ");
            stdout().flush().unwrap();
            let mut out_dir = String::new();
            stdin().read_line(&mut out_dir).unwrap();
            let out_dir = if out_dir.trim().is_empty() {
                Path::new("./address_export")
            } else {
                Path::new(out_dir.trim())
            };
            if !out_dir.exists() {
                if let Err(e) = std::fs::create_dir_all(out_dir) {
                    warn!("failed to create output dir: {}", e);
                    continue;
                }
            }

            let mut address_csv = BufWriter::new(match File::create(out_dir.join("address.csv")) {
                Ok(f) => f,
                Err(e) => {
                    warn!("failed to create address.csv: {}", e);
                    break;
                }
            });

            print!("start > ");
            stdout().flush().unwrap();
            query.clear();
            stdin().read_line(&mut query).unwrap();
            let start = query.trim().to_string();

            print!("end   > ");
            stdout().flush().unwrap();
            query.clear();
            stdin().read_line(&mut query).unwrap();
            let end = query.trim().to_string();

            match (start.parse::<u32>(), end.parse::<u32>()) {
                (Ok(start), Ok(end)) => {
                    if start < end {
                        let bar = indicatif::ProgressBar::new((end - start) as u64);
                        bar.set_style(ProgressStyle::default_bar().progress_chars("=>-").template(
                            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>10}/{len:10} tx ({per_sec}, {eta})",
                        ));
                        let mut progress = 0u64;
                        let update_freq = match end - start {
                            0..=100 => 1u64,
                            101..=10000 => 10,
                            10001..=10000000 => 100,
                            _ => 10000,
                        };

                        let mut i = start;
                        loop {
                            // break if end is reached
                            if i == end {
                                break;
                            }

                            // instantiate an iterator from i to end
                            let mut iterator = create_iterator(&db, i, end);
                            while iterator.valid() {
                                // key, val is None only when invalid
                                let key = iterator.key().unwrap();
                                let address = iterator.value().unwrap();

                                let key = u32::from_be_bytes(key.as_ref().try_into().unwrap());
                                if key != i {
                                    // fall back to get() for this address
                                    #[cfg(feature = "debug")]
                                    debug!("address index {} missing from iterator.", i);
                                    break;
                                }
                                match str::from_utf8(address.as_ref()) {
                                    Ok(address) => {
                                        write!(address_csv, "{},{}\n", key, address).unwrap();
                                    }
                                    Err(e) => {
                                        warn!(
                                            "address of index {} cannot be utf-8 decoded: {}",
                                            key, e
                                        );
                                        break;
                                    }
                                }

                                if progress % update_freq == 0 {
                                    bar.set_position(progress);
                                }
                                progress += 1;
                                i += 1;

                                iterator.next();
                            }
                            #[cfg(feature = "debug")]
                            if let Err(e) = iterator.status() {
                                debug!("iterator ended unexpectedly: {}", e);
                            }
                            drop(iterator);

                            // if the iterator fails, fall back to get() for one address key
                            if i < end {
                                #[cfg(feature = "debug")]
                                debug!("falling back to get() for address index {}.", i);
                                if let Err(e) = get_pinned(&db, i, |s| {
                                    write!(address_csv, "{},{}\n", i, s).unwrap()
                                }) {
                                    warn!("fail to get: {}", e);
                                    break;
                                }
                                if progress % update_freq == 0 {
                                    bar.set_position(progress);
                                }
                                progress += 1;
                                i += 1;
                            }
                        }
                    } else {
                        warn!("`start` must be smaller than `end`");
                    }
                }
                _ => {
                    warn!("invalid start or end (not a valid u32 integer)")
                }
            }
            continue;
        }

        match query.trim().parse::<u32>() {
            Ok(address_key) => {
                if let Err(e) = get_pinned(&db, address_key, |s| println!("{}", s)) {
                    print!("fail to get: {}", e);
                }
            },
            Err(_) => {
                warn!("Unknown Command");
                print_help();
                continue;
            }
        };
    }
}

fn get_pinned<F>(db: &DB, i: u32, mut call_back: F) -> Result<(), DBError>
where
    F: FnMut(&str) -> (),
{
    match db.get_pinned(&i.to_be_bytes()[..]) {
        Ok(optional) => match optional {
            None => Err(NotFoundErr(i)),
            Some(s) => match str::from_utf8(s.as_ref()) {
                Ok(s) => {
                    call_back(s);
                    Ok(())
                }
                Err(e) => Err(UtfDecodeErr(i, e))
            },
        },
        Err(e) => Err(QueryErr(i, e))
    }
}

fn create_iterator(db: &DB, start: u32, stop: u32) -> DBRawIteratorWithThreadMode<DB> {
    let mut opt = ReadOptions::default();
    opt.set_pin_data(true);
    opt.set_verify_checksums(false);
    opt.set_iterate_lower_bound(&start.to_be_bytes()[..]);
    opt.set_iterate_upper_bound(&stop.to_be_bytes()[..]);
    opt.set_ignore_range_deletions(true);
    db.iterator_opt(IteratorMode::Start, opt).into()
}

fn print_help() {
    println!(
        r#"Help:
    - Enter an integer (u32) to get address of that index.
    - Enter `range` to query a range on addresses.
    - Enter `export` to export a range of addresses to csv.
    - Enter `exit` to quit"#
    );
}
