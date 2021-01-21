mod mr;

use std::io::{Write};
use std::fs::File;
use mr::mr;
use std::collections::HashMap;
use itertools::Itertools;
use clap::Clap;

#[derive(Clap)]
#[clap(version = "1.0", author = "Kevin M. <kevin.margueritte@gmail.com>", about= "MapReduce Word Count with Tokio.rs")]
struct Opts {
    /// Sets number of mappers
    #[clap(short, long, default_value = "5")]
    mappers_number: usize,
    /// Sets number of reducers
    #[clap(short, long, default_value = "2")]
    reducers_number: usize,
    /// Sets file to read
    #[clap(short, long, default_value = "samples/The_Extraordinary_Adventures_of_Arsene_Lupin.txt")]
    input: String,
    /// Sets file to write, by default the result is printed on stdout
    #[clap(short, long)]
    output: Option<String>,
}

#[tokio::main]
async fn main() {
    let opts: Opts = Opts::parse();

    let result =
        mr(&opts.input, map_func, reduce_func, opts.mappers_number, opts.reducers_number)
            .await;

    match (result, opts.output) {
        (Ok(values), Some(file_name)) => write_result(&values, &file_name),
        (Ok(values), _) => stdout_result(&values),
        (Err(error), _) => panic!(error)
    }
}

fn stdout_result(result: &Vec<(String, u32)>) {
    for (word, number_occurrence) in result.iter() {
        println!("{} : {} hits", word, number_occurrence);
    }
}

fn write_result(result: &Vec<(String, u32)>, file_name: &str) {
    let mut file = File::create(file_name).unwrap();
    for (word, number_occurrence) in result.iter() {
        file.write(format!("{} : {} hits \n", word, number_occurrence).as_bytes()).unwrap();
    }
}

fn map_func(values: Vec<String>) -> Vec<(String, u32)> {
    let mut maps: HashMap<String, u32> = HashMap::new();

    values
        .iter()
        .flat_map(|x| x.split(&[' ', '\n', '.', ','][..]))
        .for_each(|x| {
            let counters = maps.entry(x.to_owned()).or_insert(0);
            *counters += 1
        });

    maps
        .into_iter()
        .collect_vec()
}

fn reduce_func(shuffled_data: Vec<(String, Vec<u32>)>) -> Vec<(String, u32)> {
    shuffled_data
        .into_iter()
        .map(| (word, counter)| {
            let number_occurrence = counter
                .into_iter()
                .fold(0, |acc, count| acc + count);
            (word, number_occurrence)
        }).collect_vec()
}
