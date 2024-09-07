use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write, BufReader, Read};
use std::num::NonZero;
use std::path::Path;
use std::sync::{Arc, Mutex};
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
    cache: Arc<Mutex<LruCache<Key, Row>>>,  // LRU cache for recently accessed data
    size_limit: usize,                 // Max entries before flushing
    sstable_count: u32,                // Counter for SSTables
}

impl BinaryDb {
    pub fn new(size_limit: usize, cache_size: usize) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            cache: Arc::new(Mutex::new(LruCache::new(NonZero::new(cache_size).unwrap()))),
            size_limit,
            sstable_count: 0
        }
    }

    // Insert a row into the in-memory map, with disk flush when limit is exceeded
    pub fn insert(&mut self, key: Key, columns: Vec<Value>) {
        let row = Row { key: key.clone(), columns };
    
        // Step 1: Insert the row into the in-memory BTreeMap
        self.map.insert(key.clone(), row.clone());
    
        // Step 2: Insert the row into the LRU cache for quick access
        let mut cache = self.cache.lock().unwrap();
        cache.put(key.clone(), row);
    
        drop(cache);

        // Step 3: Check the size of the in-memory map without borrowing self mutably yet
        let needs_flush = self.map.len() >= self.size_limit;
    
        // Step 4: If the in-memory map exceeds size_limit, flush it to disk
        if needs_flush {
            self.flush_to_disk();
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
    
        // Use a buffered writer to minimize I/O operations
        let writer = BufWriter::with_capacity(64 * 1024, file);  // 64 KB buffer
        let mut encoder = EncoderBuilder::new().build(writer).unwrap();
    
        // Step 1: Write all rows from the in-memory map to disk
        for row in self.map.iter().map(|x| x.to_bytes()) {
            if let Err(e) = encoder.write_all(&row) {
                eprintln!("Failed to write to SSTable file: {}", e);
                return;
            }
        }
    
        // Step 2: Finish encoding and writing to disk
        if let (_, Err(e)) = encoder.finish() {
            eprintln!("Failed to finalize SSTable file: {}", e);
        }
    
        // Step 3: Clear the in-memory map to free up memory
        self.map.clear();
    
        // Step 4: Clear temporary buffers using SIMD zeroing for efficient memory reset
        let mut buffer = vec![0u8; 64 * 1024]; // Example buffer size
        simd_zero_memory(&mut buffer); // Zero out the buffer using SIMD

        // Check if compaction is needed after creating a new SSTable
        self.check_and_compact_sstables();
    }

    // Optimized `get` method: Search in-memory, cache, and then disk
    pub fn get(&self, key: &Key) -> Option<Row> {
        // Step 1: Search in-memory (hot data in the HashMap)
        if let Some(row) = self.map.get(key) {
            return Some(row.clone());
        }
    
        // Step 2: Search in LRU cache (recently accessed data)
        let mut cache = self.cache.lock().unwrap();
        if let Some(cached_row) = cache.get(key) {
            return Some(cached_row.clone());
        }
    
        // Step 3: If not found in memory or cache, load from SSTable files
        let sstable_files = self.get_sstable_files();
        for file in sstable_files {
            if let Some(row) = self.load_row_from_sstable(&file, key) {
                // Step 4: Store loaded row into LRU cache
                cache.put(key.clone(), row.clone());
                return Some(row);
            }
        }

        // Step 5: Return None if not found
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
    
        // Search in-memory (parallel search of columns)
        let mut memory_results: Vec<Row> = self.map
            .iter()
            .par_bridge()
            .filter(|row| self.row_has_matching_column(row, target))
            .map(|x| x.clone())
            .collect();
    
        result.append(&mut memory_results);
    
        // Search in SSTable files in parallel
        let sstable_files = self.get_sstable_files();
        let mut sstable_results: Vec<Row> = sstable_files
            .into_par_iter() // Use Rayon to parallelize file search
            .flat_map(|file| self.search_columns_in_sstable_file(&file, target))
            .collect();
    
        result.append(&mut sstable_results);
    
        result // Return all found rows where columns match
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

    // Trigger compaction based on the number of SSTables
    pub fn check_and_compact_sstables(&mut self) {
        let sstable_files = self.get_sstable_files();
        let threshold = 5;  // Example threshold for when to trigger compaction

        // Trigger compaction if we exceed the threshold number of SSTables
        if sstable_files.len() > threshold {
            self.compact_sstables(&sstable_files);
        }
    }

    // Compacts the given SSTables into a single SSTable
    fn compact_sstables(&mut self, sstable_files: &[String]) {
        let mut all_rows = HashMap::new();  // Temporary storage for merged rows

        // Step 1: Read and merge all rows from the SSTable files
        for file in sstable_files {
            let rows = self.read_sstable(file);

            for row in rows {
                let key = row.key.clone();
                // Insert the row into the map, overwriting older entries with the same key
                all_rows.insert(key, row);
            }
        }

        // Step 2: Write the merged rows into a new SSTable
        let new_filename = format!("sstable_{}.bin", self.sstable_count);
        self.sstable_count += 1;

        let path = Path::new(&new_filename);
        let file = File::create(&path).expect("Failed to create new SSTable file");

        let writer = BufWriter::with_capacity(64 * 1024, file);  // 64 KB buffer
        let mut encoder = EncoderBuilder::new().build(writer).unwrap();

        // Write all merged rows to the new SSTable
        for row in all_rows.values() {
            let row_bytes = row.to_bytes();
            encoder.write_all(&row_bytes).expect("Failed to write row during compaction");
        }

        // Finish writing and close the file
        encoder.finish().1.expect("Failed to finalize compacted SSTable");

        // Step 3: Delete the old SSTable files
        for file in sstable_files {
            std::fs::remove_file(file).expect("Failed to delete old SSTable during compaction");
        }

        println!("Compaction completed, {} SSTables merged into {}", sstable_files.len(), new_filename);
    }
}