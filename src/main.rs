use binary_db::binarydb::BinaryDb;

fn main() {
    let binary_db = BinaryDb::new(10_000_000, 100);

    //println!("{:?}", binary_db.search_columns("value2"));

    // Insert rows with multiple columns
    // for i in 0..10_000_000 {
    //     binary_db.insert(format!("key{}", i), vec!["value1".to_string(), "value2".to_string()]);
    // }

    //println!("{:?}", memtable.search_columns("value2"));
    println!("{:?}", binary_db.get("key190000"));

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
}