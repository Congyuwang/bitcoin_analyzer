use crate::utility::*;
use ahash::{AHasher, RandomState};
use bitcoin_explorer::Address;
use ena::unify::{InPlaceUnificationTable, UnifyKey};
use hash_hasher::{HashBuildHasher, HashedMap};
use indicatif::{ProgressBar, ProgressStyle};
use log::info;
use num_cpus;
use par_iter_sync::IntoParallelIteratorSync;
use rocksdb::{Options, PlainTableFactoryOptions, SliceTransform, WriteOptions, DB};
use std::collections::hash_map::Entry;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct AddressKey(u32);

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
pub struct AddressCacheRead {
    // (index, count)
    address_index: Arc<HashedMap<u128, (AddressKey, u32)>>,
    union_find: Arc<Mutex<InPlaceUnificationTable<AddressKey>>>,
    random_state_1: RandomState,
    random_state_2: RandomState,
}

impl AddressCacheRead {
    pub fn from_address_cache(cache: AddressCache) -> Self {
        AddressCacheRead {
            address_index: unwrap_mutex(cache.address_index),
            union_find: cache.union_find,
            random_state_1: cache.random_state_1,
            random_state_2: cache.random_state_2,
        }
    }

    #[inline]
    pub fn connect_addresses_index(&self, addresses_index: &Vec<AddressKey>) {
        if addresses_index.len() > 1 {
            let mut iter = addresses_index.iter();
            let first = iter.next().unwrap();
            for rest in iter {
                self.union(first, rest);
            }
        }
    }

    #[inline]
    pub fn assert_get_address_index(
        &self,
        addresses: &Box<[Address]>,
    ) -> Option<(AddressKey, u32)> {
        if let Some(address_string) = AddressCache::addresses_to_string(addresses) {
            Some(
                self.get_address_index(self.hash(&address_string))
                    .expect("inputs must exist in UTXO"),
            )
        } else {
            None
        }
    }

    #[inline]
    pub fn union(&self, i1: &AddressKey, i2: &AddressKey) {
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

    pub fn dump(self, path: &Path) {
        let write_handle = {
            let clustering_path = path.to_path_buf().join("clustering.u32little");
            let counting_path = path.to_path_buf().join("counting.u32little");
            let mut clustering_writer = AsyncBufWriter::new(File::create(clustering_path).unwrap());
            let mut counting_writer = AsyncBufWriter::new(File::create(counting_path).unwrap());
            let union_find = unwrap_mutex(self.union_find);

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
            let bar = ProgressBar::new(total_keys as u64);
            bar.set_style(ProgressStyle::default_bar().progress_chars("=>-").template(
                "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos:>10}/{len:10} ({per_sec}, {eta})",
            ));
            (0u32..total_keys)
                .into_par_iter_sync(move |i| {
                    let cluster = union_find
                        .get_find(i)
                        .index()
                        .to_le_bytes()
                        .to_vec()
                        .into_boxed_slice();
                    let count = counting_vec
                        .get(i as usize)
                        .unwrap()
                        .to_le_bytes()
                        .to_vec()
                        .into_boxed_slice();
                    Ok((cluster, count))
                })
                .map(|(cluster, count)| {
                    clustering_writer.write_all(cluster);
                    counting_writer.write_all(count);
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
pub struct AddressCache {
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
