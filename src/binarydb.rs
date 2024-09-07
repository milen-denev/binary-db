use std::arch::x86_64::*;
use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::fs::File;
use std::io::{BufWriter, Write, BufReader, Read};
use std::path::Path;
use std::sync::{Arc, Mutex};
use lru::LruCache;
use lz4::EncoderBuilder;
use lz4::Decoder;
use rayon::prelude::*;

#[derive(Debug, Clone)]
pub struct Row {
    pub key: String,
    pub columns: Vec<String>,
}

impl Row {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Serialize key
        let key_len = self.key.len() as u32;
        bytes.extend_from_slice(&key_len.to_le_bytes());

        let key_bytes = self.key.as_bytes();
        // Use SIMD for memory copying
        bytes.extend(simd_memcpy(key_bytes));  // Optimized memcpy for key

        // Serialize each column
        let col_count = self.columns.len() as u32;
        bytes.extend_from_slice(&col_count.to_le_bytes());

        for col in &self.columns {
            let col_len = col.len() as u32;
            bytes.extend_from_slice(&col_len.to_le_bytes());

            let col_bytes = col.as_bytes();
            // Use SIMD for memory copying
            bytes.extend(simd_memcpy(col_bytes));  // Optimized memcpy for columns
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> (Self, usize) {
        let mut cursor = 0;

        // Deserialize key
        let key_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let key = simd_from_utf8(&bytes[cursor..cursor + key_len]); // Use SIMD here for string conversion
        cursor += key_len;

        // Deserialize columns
        let col_count = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let mut columns = Vec::with_capacity(col_count);

        for _ in 0..col_count {
            let col_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let col = simd_from_utf8(&bytes[cursor..cursor + col_len]); // Use SIMD here for string conversion
            cursor += col_len;
            columns.push(col);
        }

        (Row { key, columns }, cursor) // Return the row and the number of bytes read
    }
}

pub struct BinaryDb {
    map: HashMap<String, Row>,
    cache: Arc<Mutex<LruCache<String, Row>>>,  // LRU cache for recently accessed data
    size_limit: usize,                 // Max entries before flushing
    sstable_count: u32,                // Counter for SSTables
    cache_size: usize, 
}

impl BinaryDb {
    pub fn new(size_limit: usize, cache_size: usize) -> Self {
        Self {
            map: HashMap::new(),
            cache: Arc::new(Mutex::new(LruCache::new(cache_size))),
            size_limit,
            sstable_count: 0,
            cache_size,
        }
    }

    // Insert a row into the in-memory map, with disk flush when limit is exceeded
    pub fn insert(&mut self, key: String, columns: Vec<String>) {
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
        for row in self.map.values() {
            let row_bytes = row.to_bytes();
            if let Err(e) = encoder.write_all(&row_bytes) {
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
    }

    // Optimized `get` method: Search in-memory, cache, and then disk
    pub fn get(&self, key: &str) -> Option<Row> {
        // Step 1: Search in-memory (hot data in BTreeMap)
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
                cache.put(key.to_string(), row.clone());
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

    // Method to load a single row from the SSTable file on disk
    fn load_row_from_sstable(&self, filename: &str, key: &str) -> Option<Row> {
        let path = std::path::Path::new(filename);
        let file = std::fs::File::open(&path).ok()?;
        let reader = std::io::BufReader::new(file);
        let mut buffer = [0u8; 64 * 1024]; // 64 KB buffer
        let mut decompressed_data = Vec::new();
    
        // Read and decompress the SSTable file in chunks
        let mut decoder = lz4::Decoder::new(reader).ok()?;
        loop {
            let n = decoder.read(&mut buffer).ok()?;
            if n == 0 {
                break;
            }
            decompressed_data.extend_from_slice(&buffer[..n]);
        }
    
        // Deserialize the rows and search for the matching key
        let mut cursor = 0;
        while cursor < decompressed_data.len() {
            let (row, bytes_read) = Row::from_bytes(&decompressed_data[cursor..]);
            cursor += bytes_read;
    
            if row.key == key {
                return Some(row); // Return the row if key matches
            }
        }
    
        None
    }

    pub fn search_columns(&self, target: &str) -> Vec<Row> {
        let mut result = Vec::new();

        // Step 1: Search in memory (parallel search of columns)
        let mut memory_results: Vec<Row> = self.map
            .values()
            .par_bridge() // Convert standard iterator to a parallel iterator
            .filter(|row| self.row_has_matching_column(row, target.as_bytes()))
            .cloned()
            .collect();
        
        result.append(&mut memory_results);

        // Step 2: Search in SSTable files in parallel
        let sstable_files = self.get_sstable_files();
        let mut sstable_results: Vec<Row> = sstable_files
            .into_par_iter() // Use Rayon to parallelize file search
            .flat_map(|file| self.search_columns_in_sstable_file(&file, target))
            .collect();

        result.append(&mut sstable_results);

        result // Return all found rows where columns match
    }

    // Search for columns in a given SSTable file that match the target string
    fn search_columns_in_sstable_file(&self, filename: &str, target: &str) -> Vec<Row> {
        let rows = self.read_sstable(filename);
        let target_bytes = target.as_bytes(); // Convert target string to bytes for comparison
    
        rows.into_par_iter()
            .filter(|row| {
                row.columns.iter().any(|col| {
                    // Use SIMD to compare column data
                    simd_compare_keys(&simd_from_utf8(col.as_bytes()), target_bytes)
                })
            })
            .collect()
    }

    // Helper method to check if any column in a row matches the target string
    fn row_has_matching_column(&self, row: &Row, target_bytes: &[u8]) -> bool {
        row.columns
            .iter()
            .any(|col| simd_compare_keys(col, target_bytes)) // SIMD-enhanced string comparison
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
}

fn simd_memcpy(src: &[u8]) -> Vec<u8> {
    let mut dst = vec![0u8; src.len()];
    let mut i = 0;

    // Copy 16 bytes (128 bits) at a time with SIMD
    while i + 16 <= src.len() {
        unsafe {
            let data = _mm_loadu_si128(src.as_ptr().add(i) as *const __m128i);
            _mm_storeu_si128(dst.as_mut_ptr().add(i) as *mut __m128i, data);
        }
        i += 16;
    }

    // Copy remaining bytes
    dst[i..].copy_from_slice(&src[i..]);
    dst
}

fn simd_from_utf8(bytes: &[u8]) -> String {
    let mut dst = vec![0u8; bytes.len()];
    let mut i = 0;

    // Copy 16 bytes (128 bits) at a time with SIMD
    while i + 16 <= bytes.len() {
        unsafe {
            let data = _mm_loadu_si128(bytes.as_ptr().add(i) as *const __m128i);
            _mm_storeu_si128(dst.as_mut_ptr().add(i) as *mut __m128i, data);
        }
        i += 16;
    }

    // Copy remaining bytes
    dst[i..].copy_from_slice(&bytes[i..]);

    String::from_utf8(dst).unwrap()
}

// SIMD-optimized key comparison (for string equality)
fn simd_compare_keys(key: &str, target: &[u8]) -> bool {
    let key_bytes = key.as_bytes();

    if key_bytes.len() != target.len() {
        return false;
    }

    let len = key_bytes.len();
    let mut i = 0;

    // Compare 16 bytes (128 bits) at a time using SIMD
    while i + 16 <= len {
        unsafe {
            let key_chunk = _mm_loadu_si128(key_bytes.as_ptr().add(i) as *const __m128i);
            let target_chunk = _mm_loadu_si128(target.as_ptr().add(i) as *const __m128i);
            let cmp = _mm_cmpeq_epi8(key_chunk, target_chunk);

            if _mm_movemask_epi8(cmp) != 0xFFFF {
                return false;
            }
        }
        i += 16;
    }

    // Compare remaining bytes
    key_bytes[i..] == target[i..]
}

fn simd_zero_memory(buffer: &mut [u8]) {
    let mut i = 0;
    let len = buffer.len();

    // Zero 16 bytes at a time using SIMD
    while i + 16 <= len {
        unsafe {
            let zero = _mm_setzero_si128(); // Create a 128-bit zero value
            _mm_storeu_si128(buffer.as_mut_ptr().add(i) as *mut __m128i, zero);
        }
        i += 16;
    }

    // Zero any remaining bytes
    for b in buffer.iter_mut().skip(i) {
        *b = 0;
    }
}