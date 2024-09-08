use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Hash, Eq, Deserialize, Serialize, PartialOrd, Ord)]
pub enum Value {
    Int(i64),
    Bool(bool),
    Str(String),
    //Float(f64),
    Binary(Vec<u8>),
}