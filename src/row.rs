use crate::{key::Key, simd::{simd_from_utf8, simd_memcpy}, value::Value};


#[derive(Debug, Clone)]
pub struct Row {
    pub key: Key,
    pub columns: Vec<Value>,
}

impl Row {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Serialize key
        match &self.key {
            Key::Int(i) => {
                bytes.push(0); // Type marker for Int key
                bytes.extend_from_slice(&i.to_le_bytes());
            }
            Key::Str(s) => {
                bytes.push(1); // Type marker for String key
                let len = s.len() as u32;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend(simd_memcpy(s.as_bytes()));  // Using SIMD for string key data
            }
        }

        // Serialize columns
        let col_count = self.columns.len() as u32;
        bytes.extend_from_slice(&col_count.to_le_bytes());

        for col in &self.columns {
            match col {
                Value::Int(i) => {
                    bytes.push(0); // Type marker for Int
                    bytes.extend_from_slice(&i.to_le_bytes());
                }
                Value::Bool(b) => {
                    bytes.push(1); // Type marker for Bool
                    bytes.push(*b as u8);
                }
                Value::Str(s) => {
                    bytes.push(2); // Type marker for String
                    let len = s.len() as u32;
                    bytes.extend_from_slice(&len.to_le_bytes());
                    bytes.extend(simd_memcpy(s.as_bytes()));  // Using SIMD for string column data
                }
                Value::Float(f) => {
                    bytes.push(3); // Type marker for Float
                    bytes.extend_from_slice(&f.to_le_bytes());
                }
                Value::Binary(bin) => {
                    bytes.push(4); // Type marker for Binary data
                    let len = bin.len() as u32;
                    bytes.extend_from_slice(&len.to_le_bytes());
                    bytes.extend(simd_memcpy(bin));  // Using SIMD for binary data
                }
            }
        }

        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> (Self, usize) {
        let mut cursor = 0;

        // Deserialize key
        let key_type = bytes[cursor];
        cursor += 1;

        let key = match key_type {
            0 => {
                let int_value = i64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                Key::Int(int_value)
            }
            1 => {
                let str_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let str_value = simd_from_utf8(&bytes[cursor..cursor + str_len]);  // SIMD-optimized string conversion
                cursor += str_len;
                Key::Str(str_value)
            }
            _ => panic!("Unknown key type"),
        };

        // Deserialize columns
        let col_count = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let mut columns = Vec::with_capacity(col_count);

        for _ in 0..col_count {
            let value_type = bytes[cursor];
            cursor += 1;

            let value = match value_type {
                0 => {  // Int
                    let int_value = i64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    Value::Int(int_value)
                }
                1 => {  // Bool
                    let bool_value = bytes[cursor] != 0;
                    cursor += 1;
                    Value::Bool(bool_value)
                }
                2 => {  // String
                    let str_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    let str_value = simd_from_utf8(&bytes[cursor..cursor + str_len]);  // SIMD-optimized string conversion
                    cursor += str_len;
                    Value::Str(str_value)
                }
                3 => {  // Float
                    let float_value = f64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                    cursor += 8;
                    Value::Float(float_value)
                }
                4 => {  // Binary
                    let bin_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    let bin_value = simd_memcpy(&bytes[cursor..cursor + bin_len]);  // SIMD-optimized binary data copy
                    cursor += bin_len;
                    Value::Binary(bin_value)
                }
                _ => panic!("Unknown column type"),
            };

            columns.push(value);
        }

        (Row { key, columns }, cursor)
    }
}