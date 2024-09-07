use binary_db::binarydb::BinaryDb;

fn main() {
    let memtable = BinaryDb::new(10_000_000, 100);

    //println!("{:?}", memtable.search_columns("value2"));

    // Insert rows with multiple columns
    // for i in 0..10_000_000 {
    //     memtable.insert(format!("key{}", i), vec!["value1".to_string(), "value2".to_string()]);
    // }

    //println!("{:?}", memtable.search_columns("value2"));
    println!("{:?}", memtable.get("key190000"));

    // Flush the memtable to disk
    //memtable.flush_to_disk();

    // Retrieve a row by key (from memtable)
    //if let Some(row) = memtable.get("key1") {
    //    println!("Found row in memtable: key = {}, columns = {:?}", row.key, row.columns);
    //}

    // Retrieve rows from the SSTable
    //let rows = memtable.read_sstable("sstable_0.bin");
    //for row in rows {
    //    println!("Found row in SSTable: key = {}, columns = {:?}", row.key, row.columns);
    //}
}