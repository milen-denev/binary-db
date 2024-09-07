use std::arch::x86_64::*;
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fs::{read_dir, File};
use std::io::{BufWriter, Write, BufReader, Read};
use std::path::Path;
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
        bytes.extend(simd_memcpy(key_bytes));

        // Serialize each column
        let col_count = self.columns.len() as u32;
        bytes.extend_from_slice(&col_count.to_le_bytes());

        for col in &self.columns {
            let col_len = col.len() as u32;
            bytes.extend_from_slice(&col_len.to_le_bytes());

            let col_bytes = col.as_bytes();
            bytes.extend(simd_memcpy(col_bytes));  // SIMD-enhanced memory copying
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> (Self, usize) {
        let mut cursor = 0;

        // Deserialize key
        let key_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let key = String::from_utf8(bytes[cursor..cursor + key_len].to_vec()).unwrap();
        cursor += key_len;

        // Deserialize columns
        let col_count = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let mut columns = Vec::with_capacity(col_count);

        for _ in 0..col_count {
            let col_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let col = String::from_utf8(bytes[cursor..cursor + col_len].to_vec()).unwrap();
            cursor += col_len;
            columns.push(col);
        }

        (Row { key, columns }, cursor) // Return the row and the number of bytes read
    }
}

pub struct Memtable {
    map: BTreeMap<String, Row>,
    size_limit: usize,
    sstable_count: u32,
}

impl Memtable {
    pub fn new(size_limit: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            size_limit,
            sstable_count: 0,
        }
    }

    pub fn insert(&mut self, key: String, columns: Vec<String>) {
        let row = Row { key: key.clone(), columns };
        self.map.insert(key, row);

        if self.map.len() >= self.size_limit {
            self.flush_to_disk();
        }
    }

    pub fn flush_to_disk(&mut self) {
        let filename = format!("sstable_{}.bin", self.sstable_count);
        self.sstable_count += 1;
    
        let path = Path::new(&filename);
        let file = match File::create(&path) {
            Ok(file) => file,
            Err(e) => {
                eprintln!("Failed to create SSTable file: {}", e);
                return;
            }
        };
        
        // Use a larger buffer size to reduce the number of write operations
        let writer = BufWriter::with_capacity(64 * 1024, file);  // 64 KB buffer
        let mut encoder = EncoderBuilder::new().build(writer).unwrap();
    
        for row in self.map.values() {
            let row_bytes = row.to_bytes();
            if let Err(e) = encoder.write_all(&row_bytes) {
                eprintln!("Failed to write to SSTable file: {}", e);
                return;
            }
        }
    
        // Finish encoding
        _ = encoder.finish();
    
        self.map.clear();
    }
    
    pub fn get(&self, key: &str) -> Vec<Row> {
        let mut result = Vec::new();
    
        // Step 1: Search in memory (this is fast, so no parallelization here)
        if let Some(row) = self.map.get(key) {
            result.push(row.clone());
        }
    
        // Step 2: Search in SSTable files in parallel
        let sstable_files = self.get_sstable_files();
        let mut sstable_results: Vec<Row> = sstable_files
            .into_par_iter() // Use Rayon to parallelize file search
            .flat_map(|file| self.search_sstable_file(&file, key))
            .collect();
    
        // Append the results from SSTables to in-memory result
        result.append(&mut sstable_results);
    
        result // Return all found rows
    }
    
    // Helper method to get all SSTable files
    fn get_sstable_files(&self) -> Vec<String> {
        let entries = match read_dir(".") {
            Ok(entries) => entries.collect::<Vec<_>>(),
            Err(_) => return Vec::new(),
        };
    
        // Filter SSTable files in parallel and collect filenames
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
    
    // Optimized search for a key in a given SSTable file
    fn search_sstable_file(&self, filename: &str, key: &str) -> Vec<Row> {
        let rows = self.read_sstable(filename);
        let key_bytes = key.as_bytes(); // Use bytes for faster comparison

        // Use parallel iterator to search rows
        rows.into_par_iter()
            .filter(|row| simd_compare_keys(&row.key, key_bytes)) // Use SIMD-optimized key comparison
            .collect() // Collect the matching rows into Vec<Row>
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
        let target_bytes = target.as_bytes(); // Convert the target string to bytes for faster comparison

        rows.into_par_iter()
            .filter(|row| self.row_has_matching_column(row, target_bytes))
            .collect() // Collect matching rows into Vec<Row>
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