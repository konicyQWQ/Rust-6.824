use itertools::Itertools;
use rust_6824::mrapps::get_map_reduce;
use rust_6824::mrapps::wc::KeyValue;
use std::io::{Read, Write};
use std::{env, fs};

fn main() {
    let args: Vec<String> = env::args().collect();
    let map_reduce = get_map_reduce("wc").unwrap();

    let mut kvs: Vec<KeyValue> = vec![];

    // read each input file,
    // using map function to split word,
    // append to kvs vector
    for filename in &args[1..] {
        let mut contents = String::new();

        fs::File::open(filename)
            .expect(&format!("Failed to open file {}", filename))
            .read_to_string(&mut contents)
            .expect(&format!("Failed to read file {}", filename));

        let mut map_output = map_reduce.map(filename, &contents);

        kvs.append(&mut map_output);
    }

    let mut output_file = fs::File::create("mr-out-0").expect("Failed to create mr-out-0");

    // call reduce function for distinct key,
    // output to mr-out-0
    kvs.sort();
    for (key, chunk) in &kvs.iter().chunk_by(|kv| &kv.key) {
        let values: Vec<String> = chunk.map(|kv| kv.value.clone()).collect();
        let reduce_output = map_reduce.reduce(key, &values);

        writeln!(output_file, "{} {}", key, reduce_output).expect("Failed to write output");
    }
}
