#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    Bool(bool),
    Str(String),
    Float(f64),
    Binary(Vec<u8>),
}