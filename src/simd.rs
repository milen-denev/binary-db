use std::arch::x86_64::*;

use crate::value::Value;

pub fn simd_memcpy(src: &[u8]) -> Vec<u8> {
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

pub fn simd_from_utf8(bytes: &[u8]) -> String {
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
pub fn simd_compare_keys(key: &str, target: &[u8]) -> bool {
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

pub fn simd_zero_memory(buffer: &mut [u8]) {
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

pub fn simd_compare_values(val: &Value, target: &Value) -> bool {
    match (val, target) {
        (Value::Int(a), Value::Int(b)) => a == b,          // Fast int comparison
        (Value::Bool(a), Value::Bool(b)) => a == b,        // Fast bool comparison
        (Value::Str(a), Value::Str(b)) => simd_compare_keys(a, b.as_bytes()),  // SIMD string comparison
        //(Value::Float(a), Value::Float(b)) => a == b,      // Fast float comparison
        (Value::Binary(a), Value::Binary(b)) => a == b,    // Raw binary comparison
        _ => false,                                        // Different types can't be compared
    }
}