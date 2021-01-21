use std::collections::HashMap;
use itertools::Itertools;
use std::sync::{Arc, Mutex};
use std::io::{self, BufRead};
use std::fs::File;
use futures::{FutureExt, Future, TryFutureExt};

pub fn mr<Fm, Fr>(file_name: &str, map_func: Fm, reduce_func: Fr, num_mappers: usize, num_reducer: usize) -> impl Future<Output=Result<Vec<(String, u32)>, String>>
    where
        Fm: Fn(Vec<String>) -> Vec<(String, u32)> + Send + 'static + Copy,
        Fr: Fn(Vec<(String, Vec<u32>)>) -> Vec<(String, u32)> + Send + 'static + Copy,
{
    mapper(file_name, num_mappers, map_func)
        .and_then( move |x| reducer(x, num_reducer, reduce_func))
        .map( | result | {
            match result {
                Ok(values) => Ok(values),
                Err(_) => Err(String::from("An error occurred"))
            }
        })
}

fn mapper<F>(file_name: &str, num_threads: usize, f: F) -> impl Future<Output=Result<Vec<(String, Vec<u32>)>, ()>>
    where F: Fn(Vec<String>) -> Vec<(String, u32)> + Send + 'static + Copy, {
    fn count_number_lines(file_name: &str) -> usize {
        let file = File::open(file_name).unwrap();
        let mut buf = io::BufReader::new(file);
        let mut line = String::new();
        let mut count = 0;
        while buf.read_line( &mut line).unwrap() > 0 {
            count += 1;
            line.clear();
        }
        count
    }

    let chunk_size = (count_number_lines(file_name) / num_threads) + (count_number_lines(file_name) % num_threads);

    let file = File::open(file_name).unwrap();
    let buffer = io::BufReader::new(file);

    let db: Arc<Mutex<HashMap<String, Vec<u32>>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut handles_mappers = vec![];


    for chunk in &buffer.lines().map(|x| x.unwrap()).chunks(chunk_size) {
        let values_chunked = chunk.collect_vec();
        let db_values = db.clone();
        handles_mappers.push(tokio::spawn(async move {
            let maps = f(values_chunked);

            maps
                .into_iter()
                .for_each( |(key, value)| {
                    let mut db = db_values.lock().unwrap();
                    let counters = db.entry(key.to_owned()).or_insert(Vec::new());
                    counters.push(value);
                });
        }));
    }

    futures::future::join_all(handles_mappers).map( move |_| {
        let mut result: Vec<(String, Vec<u32>)> = Vec::new();

        db
            .lock()
            .unwrap()
            .iter()
            .for_each(|(key, values)| {
                result.push((key.to_owned(), values.to_owned()));
            });

        Ok(result)
    })
}

fn reducer<F>(data_shuffled: Vec<(String, Vec<u32>)>, num_threads: usize, f: F) -> impl Future<Output=Result<Vec<(String, u32)>, ()>>
    where F: Fn(Vec<(String, Vec<u32>)>) -> Vec<(String, u32)> + Send + 'static + Copy, {

    let mut handles_reducers = vec![];

    for chunk in &data_shuffled.into_iter().chunks(num_threads) {
        let values_chunked = chunk.collect_vec();
        handles_reducers.push(tokio::spawn(async move {
            f(values_chunked)
        }));
    };

    futures::future::join_all(handles_reducers)
        .map(|result| {
            Ok(result.into_iter().flat_map( |x| x.unwrap()).collect_vec())
        })
}