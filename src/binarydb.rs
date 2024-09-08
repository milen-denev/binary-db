use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write, BufReader, Read};
use std::num::NonZero;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use bloom::{BloomFilter, ASMS};
use dashmap::DashMap;
use lru::LruCache;
use lz4::EncoderBuilder;
use lz4::Decoder;
use rayon::prelude::*;

use crate::key::Key;
use crate::row::Row;
use crate::value::Value;

use super::simd::*;

pub struct BinaryDb {
    map: Arc<DashMap<Key, Row>>,
    cache: Arc<Mutex<LruCache<Key, Row>>>,  
    size_limit: usize,                      
    sstable_count: u32,                     
    key_index: Arc<DashMap<Key, usize>>,     
    column_index: Arc<RwLock<BTreeMap<Value, Vec<Key>>>>, 
    bloom_filter: BloomFilter, // Add Bloom Filter here
    index_file_path: String,
}

impl BinaryDb {
    pub fn new(size_limit: usize, cache_size: usize, expected_items: usize) -> Self {
        let bloom_filter = BloomFilter::with_rate(0.01, expected_items as u32); // 1% false positive rate
    
        let mut db = Self {
            map: Arc::new(DashMap::new()),
            cache: Arc::new(Mutex::new(LruCache::new(NonZero::new(cache_size).unwrap()))),
            size_limit,
            sstable_count: 0,
            key_index: Arc::new(DashMap::new()),
            column_index: Arc::new(RwLock::new(BTreeMap::new())),
            bloom_filter,
            index_file_path: "persistent_indexes.bin".to_string(),
        };
    
        db.load_indexes_from_disk();

        db
    }

    // Insert a row into the in-memory map, with disk flush when limit is exceeded
    pub fn insert(&mut self, key: Key, columns: Vec<Value>) {
        let row = Row { key: key.clone(), columns: columns.clone() };
    
        // Insert the row into the in-memory map
        self.map.insert(key.clone(), row.clone());
    
        // Insert into LRU cache
        let mut cache = self.cache.lock().unwrap();
        cache.put(key.clone(), row.clone());
        drop(cache);
    
        // Insert into Bloom filter
        self.bloom_filter.insert(&key);
    
        // Update primary index
        self.key_index.insert(key.clone(), self.map.len());
    
        // Update secondary index using BTreeMap
        {
            let mut column_index = self.column_index.write().unwrap();
            for column_value in columns {
                column_index
                    .entry(column_value.clone())
                    .or_insert_with(Vec::new)
                    .push(key.clone());
            }
        }
    
        // Flush to disk if needed
        if self.map.len() >= self.size_limit {
            self.flush_to_disk();
        }
    
        // Save indexes to disk
        self.save_indexes_to_disk();
    }

    // Save indexes to disk (persist them) using MessagePack
    fn save_indexes_to_disk(&self) {
        // Step 1: Acquire locks and extract data to serialize
        let key_index_data: Vec<_> = self.key_index.iter().map(|kv| (kv.key().clone(), *kv.value())).collect();
        
        let column_index_data: Vec<_> = {
            let column_index = self.column_index.read().unwrap();  // Acquire a read lock
            column_index.iter().map(|(value, keys)| (value.clone(), keys.clone())).collect()
        };
    
        // Step 2: Serialize data
        let index_data = (key_index_data, column_index_data);
    
        // Step 3: Save to disk
        if let Ok(file) = File::create(&self.index_file_path) {
            let mut writer = BufWriter::new(file);
    
            // Serialize to MessagePack format (or bincode for compact binary format)
            if bincode::serialize_into(&mut writer, &index_data).is_err() {
                eprintln!("Failed to serialize indexes to disk");
            }
        } else {
            eprintln!("Failed to create index file");
        }
    }

    // Load indexes from disk during initialization using MessagePack
    fn load_indexes_from_disk(&mut self) {
        // Step 1: Open the file and deserialize data
        if let Ok(file) = File::open(&self.index_file_path) {
            let reader = BufReader::new(file);
    
            let index_data: Result<(Vec<(Key, usize)>, Vec<(Value, Vec<Key>)>), _> = 
                bincode::deserialize_from(reader);
    
            // Step 2: If deserialization succeeds, update the in-memory structures
            if let Ok((key_index_data, column_index_data)) = index_data {
                // Update key_index
                for (key, position) in key_index_data {
                    self.key_index.insert(key, position);
                }
    
                // Update column_index
                {
                    let mut column_index = self.column_index.write().unwrap();  // Acquire a write lock
                    for (value, keys) in column_index_data {
                        column_index.insert(value, keys);
                    }
                }
    
                println!("Indexes successfully loaded from disk.");
            } else {
                eprintln!("Failed to deserialize indexes from disk.");
            }
        } else {
            eprintln!("Failed to open index file.");
        }
    }

    // Flush the in-memory map to disk as an SSTable file
    pub fn flush_to_disk(&mut self) {
        let filename = format!("sstable_{}.bin", self.sstable_count);
        self.sstable_count += 1;

        let path = std::path::Path::new(&filename);
        let file = match std::fs::File::create(&path) {
            Ok(file) => file,
            Err(e) => {
                eprintln!("Failed to create SSTable file: {}", e);
                return;
            }
        };

        let writer = BufWriter::with_capacity(64 * 1024, file);
        let mut encoder = EncoderBuilder::new().build(writer).unwrap();

        for row in self.map.iter().map(|x| x.to_bytes()) {
            if let Err(e) = encoder.write_all(&row) {
                eprintln!("Failed to write to SSTable file: {}", e);
                return;
            }
        }

        if let (_, Err(e)) = encoder.finish() {
            eprintln!("Failed to finalize SSTable file: {}", e);
        }

        self.map.clear();
        self.key_index.clear(); // Clear the in-memory index after flush

        let mut buffer = vec![0u8; 64 * 1024];
        simd_zero_memory(&mut buffer);

        self.check_and_compact_sstables();

        // Save the updated indexes after flushing
        self.save_indexes_to_disk();
    }

    // Optimized `get` method: Search in-memory, cache, and then disk
    pub fn get(&self, key: &Key) -> Option<Row> {
        // Step 1: Check Bloom filter first
        if !self.bloom_filter.contains(key) {
            return None;  // Key is unlikely to exist
        }
    
        // Step 2: Check in-memory map
        if self.key_index.contains_key(key) {
            if let Some(row) = self.map.get(key) {
                return Some(row.clone());
            }
        }
    
        // Step 3: Check LRU cache
        let mut cache = self.cache.lock().unwrap();
        if let Some(cached_row) = cache.get(key) {
            return Some(cached_row.clone());
        }
    
        // Step 4: Load from SSTable if not found in memory
        let sstable_files = self.get_sstable_files();
        for file in sstable_files {
            if let Some(row) = self.load_row_from_sstable(&file, key) {
                cache.put(key.clone(), row.clone());
                return Some(row);
            }
        }
    
        None
    }
    
    // Helper method to get all SSTable files (unchanged)
    fn get_sstable_files(&self) -> Vec<String> {
        let entries = match std::fs::read_dir(".") {
            Ok(entries) => entries.collect::<Vec<_>>(),
            Err(_) => return Vec::new(),
        };

        entries
            .into_par_iter()
            .filter_map(|entry| {
                if let Ok(entry) = entry {
                    let filename = entry.file_name().into_string().unwrap();
                    if filename.starts_with("sstable_") && filename.ends_with(".bin") {
                        Some(filename)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    }

    // Method to load a single row from the SSTable file on disk based on the key
    fn load_row_from_sstable(&self, filename: &str, key: &Key) -> Option<Row> {
        let path = std::path::Path::new(filename);
        let file = std::fs::File::open(&path).ok()?;
        let reader = std::io::BufReader::new(file);
        let mut buffer = [0u8; 64 * 1024]; // 64 KB buffer
        let mut decompressed_data = Vec::new();
    
        // Step 1: Read and decompress the SSTable file in chunks
        let mut decoder = lz4::Decoder::new(reader).ok()?;
        loop {
            let n = decoder.read(&mut buffer).ok()?;
            if n == 0 {
                break;
            }
            decompressed_data.extend_from_slice(&buffer[..n]);
        }
    
        // Step 2: Deserialize rows and search for the matching key
        let mut cursor = 0;
        while cursor < decompressed_data.len() {
            let (row, bytes_read) = Row::from_bytes(&decompressed_data[cursor..]);
            cursor += bytes_read;

            // Step 3: Compare the deserialized row's key with the provided key
            if row.key == *key {
                return Some(row); // Return the matching row
            }
        }
    
        None // Return None if no matching key is found
    }

    pub fn search_columns(&self, target: &Value) -> Vec<Row> {
        let mut result = Vec::new();
    
        // Step 1: Check if the value exists in the secondary index (BTreeMap)
        {
            let column_index = self.column_index.read().unwrap();
            if let Some(keys) = column_index.get(target) {
                for key in keys.iter() {
                    if let Some(row) = self.get(key) {
                        result.push(row);
                    }
                }
            }
        }
    
        // Step 2: Check SSTables if more results are needed
        if result.is_empty() {
            let sstable_files = self.get_sstable_files();
            let mut sstable_results: Vec<Row> = sstable_files
                .into_par_iter()
                .flat_map(|file| self.search_columns_in_sstable_file(&file, target))
                .collect();
    
            result.append(&mut sstable_results);
        }
    
        result
    }

    // Search for columns in a given SSTable file that match the target string
   fn search_columns_in_sstable_file(&self, filename: &str, target: &Value) -> Vec<Row> {
        let rows = self.read_sstable(filename);  // Read the SSTable from disk

        // Parallelize the search using Rayon, filtering rows where any column matches the target value
        rows.into_par_iter()
            .filter(|row| {
                row.columns.iter().any(|col| {
                    // Compare each column's value with the target value
                    simd_compare_values(col, target)
                })
            })
            .collect()
    }

    // Helper method to check if any column in a row matches the target string
    fn row_has_matching_column(&self, row: &Row, target: &Value) -> bool {
        row.columns
            .iter()
            .any(|col| simd_compare_values(col, target))
    }

    pub fn read_sstable(&self, filename: &str) -> Vec<Row> {
        let path = Path::new(filename);
    
        // Open the file
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(e) => {
                eprintln!("Failed to open SSTable file '{}': {}", filename, e);
                return Vec::new();  // Return an empty vector if file not found
            }
        };
    
        // Use a larger buffer to optimize I/O operations
        let reader = BufReader::with_capacity(64 * 1024, file);  // 64 KB buffer for reading
        let mut decoder = Decoder::new(reader).unwrap();
        
        // Instead of reading the entire file into memory, read in chunks
        let mut buffer = [0u8; 64 * 1024];  // 64 KB buffer for decompression
        let mut decompressed_data = Vec::new();
    
        loop {
            match decoder.read(&mut buffer) {
                Ok(0) => break,  // End of file
                Ok(n) => decompressed_data.extend_from_slice(&buffer[..n]),
                Err(e) => {
                    eprintln!("Error while reading SSTable: {}", e);
                    return Vec::new();
                }
            }
        }
    
        // Parallel deserialization of rows
        let mut cursor = 0;
        let mut row_data = Vec::new();
        
        // Collect byte slices for each row
        while cursor < decompressed_data.len() {
            let (row, bytes_read) = Row::from_bytes(&decompressed_data[cursor..]);
            row_data.push((row, bytes_read));
            cursor += bytes_read;
        }
    
        // Parallelize the row processing using Rayon
        let rows = row_data
            .into_par_iter()
            .map(|(row, _)| row)
            .collect();
    
        rows
    }

     // Trigger compaction and update the persistent index file
     pub fn check_and_compact_sstables(&mut self) {
        let sstable_files = self.get_sstable_files();
        let threshold = 5;  // Example threshold for when to trigger compaction

        if sstable_files.len() > threshold {
            self.compact_sstables(&sstable_files);
            // Save indexes after compaction
            self.save_indexes_to_disk();
        }
    }

    // Helper function for compacting SSTables
    fn compact_sstables(&mut self, sstable_files: &[String]) {
        let mut all_rows = HashMap::new();  // Temporary storage for merged rows

        for file in sstable_files {
            let rows = self.read_sstable(file);

            for row in rows {
                let key = row.key.clone();
                all_rows.insert(key, row);
            }
        }

        let new_filename = format!("sstable_{}.bin", self.sstable_count);
        self.sstable_count += 1;

        let path = Path::new(&new_filename);
        let file = File::create(&path).expect("Failed to create new SSTable file");

        let writer = BufWriter::with_capacity(64 * 1024, file);
        let mut encoder = EncoderBuilder::new().build(writer).unwrap();

        for row in all_rows.values() {
            let row_bytes = row.to_bytes();
            encoder.write_all(&row_bytes).expect("Failed to write row during compaction");
        }

        encoder.finish().1.expect("Failed to finalize compacted SSTable");

        for file in sstable_files {
            std::fs::remove_file(file).expect("Failed to delete old SSTable during compaction");
        }

        // Save updated indexes after compaction
        self.save_indexes_to_disk();
        println!("Compaction completed, {} SSTables merged.", sstable_files.len());
    }
}