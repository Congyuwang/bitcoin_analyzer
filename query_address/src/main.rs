use indicatif;
use indicatif::ProgressStyle;
use log::{info, warn};
use num_cpus;
use rocksdb::{
    DBIteratorWithThreadMode, Direction, IteratorMode, Options, PlainTableFactoryOptions,
    ReadOptions, SliceTransform, DB,
};
use simple_logger::SimpleLogger;
use std::fs::File;
use std::io::{stdin, stdout, BufWriter, Write};
use std::path::Path;
use std::str;

fn main() {
    SimpleLogger::new().init().unwrap();
    // launchDB
    let db: DB = loop {
        println!(">> Enter path address DB directory (or `exit` to quit) <<");
        print!("> ");
        stdout().flush().unwrap();
        let mut db_path = String::new();
        stdin().read_line(&mut db_path).unwrap();
        if db_path.trim() == "exit" {
            return ();
        }
        let db_path = Path::new(db_path.trim());
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
    println!(">> Enter `exit` to quit, `help` for help <<");
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
            println!(
                r#"Help:
    - Enter an integer (u32) to get address of that index.
    - Enter `range` to query a range on addresses.
    - Enter `export` to export a range of addresses to csv.
    - Enter `exit` to quit"#
            );
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
                            warn!("use `export` command for more than 10000 output");
                            continue;
                        }
                        for i in start..end {
                            get_pinned(&db, i, |s| println!("{}", s));
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
            println!(">> enter a directory to store `address.csv`: <<");
            print!("> ");
            stdout().flush().unwrap();
            let mut out_dir = String::new();
            stdin().read_line(&mut out_dir).unwrap();
            let out_dir = Path::new(out_dir.trim());
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
                            _ => 100,
                        };

                        let mut i = start;
                        loop {
                            // break if end is reached
                            if i == end {
                                break;
                            }

                            // instantiate an iterator from i to end
                            for (key, address) in create_iterator(&db, i, end) {
                                let key = u32::from_be_bytes(key.as_ref().try_into().unwrap());
                                if key != i {
                                    warn!("address index {} missing, corrupted data.", i);
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
                            }

                            // if the iterator fails, fall back to get() for one address key
                            if i < end {
                                get_pinned(&db, i, |s| write!(address_csv, "{},{}\n", i, s).unwrap());
                                if progress % update_freq == 0 {
                                    bar.set_position(progress);
                                }
                                progress += 1;
                                i += 1;
                            }
                        }
                        bar.finish();
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
            Ok(address_key) => get_pinned(&db, address_key, |s| println!("{}", s)),
            Err(_) => {
                warn!("Unknown Command");
                continue;
            }
        };
    }
}

fn get_pinned<F>(db: &DB, i: u32, mut call_back: F)
where F: FnMut(&str) -> ()
{
    match db.get_pinned(&i.to_be_bytes()[..]) {
        Ok(optional) => match optional {
            None => warn!("range exceeded"),
            Some(s) => match str::from_utf8(s.as_ref()) {
                Ok(s) => call_back(s),
                Err(e) => warn!("address of index {} cannot be utf-8 decoded: {}", i, e),
            },
        },
        Err(e) => warn!("failed to query DB: {}", e),
    }
}

fn create_iterator(db: &DB, start: u32, stop: u32) -> DBIteratorWithThreadMode<DB> {
    let mut opt = ReadOptions::default();
    opt.set_pin_data(true);
    opt.set_verify_checksums(false);
    opt.set_iterate_lower_bound(&start.to_be_bytes()[..]);
    opt.set_ignore_range_deletions(true);
    opt.set_iterate_upper_bound(&stop.to_be_bytes()[..]);
    db.iterator_opt(
        IteratorMode::From(&start.to_be_bytes()[..], Direction::Forward),
        opt,
    )
}
