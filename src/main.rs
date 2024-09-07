use std::time::Duration;

use binary_db::{binarydb::BinaryDb, key::Key, value::Value};

fn main() {
    let mut binary_db = BinaryDb::new(1000, 100);

    //println!("{:?}", binary_db.search_columns("value2"));

    // Insert rows with multiple columns
    // for i in 0..40_000 {
    //     binary_db.insert(Key::Str(format!("key{}", i)), 
    //     vec![
    //         Value::Str(format!("value{}", i)),
    //         Value::Str(format!("value{}", 10_000_000 + i)),
    //         Value::Float(15.0),
    //         Value::Int(300),
    //         Value::Bool(false),
    //         Value::Bool(true),
    //         ]);
    // }

    //println!("{:?}", memtable.search_columns("value2"));

    let mut stopwatch = stopwatch::Stopwatch::new();

    stopwatch.start();
    println!("{:?}", binary_db.get(&Key::Str("key25000".to_string())));
    stopwatch.stop();

    println!("{}", stopwatch.elapsed_ms());
    stopwatch.reset();

    stopwatch.start();
    println!("{:?}", binary_db.get(&Key::Str("key30000".to_string())));
    stopwatch.stop();
    
    println!("{}", stopwatch.elapsed_ms());
    stopwatch.reset();

    stopwatch.start();
    println!("{:?}", binary_db.get(&Key::Str("key35000".to_string())));
    stopwatch.stop();
    
    println!("{}", stopwatch.elapsed_ms());
    stopwatch.reset();

    // Flush the binary_db to disk
    //binary_db.flush_to_disk();

    // Retrieve a row by key (from binary_db)
    //if let Some(row) = binary_db.get("key1") {
    //    println!("Found row in binary_db: key = {}, columns = {:?}", row.key, row.columns);
    //}

    // Retrieve rows from the SSTable
    //let rows = binary_db.read_sstable("sstable_0.bin");
    //for row in rows {
    //    println!("Found row in SSTable: key = {}, columns = {:?}", row.key, row.columns);
    //}

    std::thread::sleep(Duration::from_secs(10000));
}