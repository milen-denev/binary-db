
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Key {
    Int(i64),
    Str(String),
}

impl Key {
    // Convert Key to bytes for serialization
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Key::Int(i) => {
                let mut bytes = vec![0]; // Type marker for Int
                bytes.extend_from_slice(&i.to_le_bytes());
                bytes
            }
            Key::Str(s) => {
                let mut bytes = vec![1]; // Type marker for String
                let len = s.len() as u32;
                bytes.extend_from_slice(&len.to_le_bytes());
                bytes.extend_from_slice(s.as_bytes());
                bytes
            }
        }
    }

    // Deserialize Key from bytes
    pub fn from_bytes(bytes: &[u8]) -> (Self, usize) {
        let mut cursor = 0;
        let key_type = bytes[cursor];
        cursor += 1;

        match key_type {
            0 => {
                let int_value = i64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                (Key::Int(int_value), cursor)
            }
            1 => {
                let str_len = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
                cursor += 4;
                let str_value = String::from_utf8(bytes[cursor..cursor + str_len].to_vec()).unwrap();
                cursor += str_len;
                (Key::Str(str_value), cursor)
            }
            _ => panic!("Unknown key type"),
        }
    }
}