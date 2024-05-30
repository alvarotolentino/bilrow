use memmap2::MmapOptions;
use rand::distributions::{Distribution, Uniform};
use rayon::prelude::*;
use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use std::time::Instant;

fn check_args(args: Vec<String>) -> Result<usize, &'static str> {
    if args.len() != 2 {
        return Err("Usage: create_measurements <positive integer number of records to create>");
    }
    match args[1].parse::<usize>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err("Usage: create_measurements <positive integer number of records to create>"),
    }
}

fn build_weather_station_name_list() -> Vec<String> {
    let mut current_dir: PathBuf = env::current_dir().unwrap();
    current_dir.push("data/weather_stations.csv");

    let file = File::open(current_dir);
    let file = match file {
        Ok(file) => file,
        Err(e) => {
            println!("Error opening file: {}", e);
            std::process::exit(1);
        }
    };

    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    let mut last_pos = 0;
    let mut name_set: hashbrown::HashSet<String> = hashbrown::HashSet::new();
    for next_pos in memchr::memchr_iter(b'\n', &mmap) {
        let line: &[u8] = &mmap[last_pos..next_pos];
        last_pos = next_pos + 1;

        if line.is_empty() {
            continue;
        }
        let separator: usize = memchr::memchr(b';', line).unwrap();
        let line: &[u8] = &line[..separator];
        let name = unsafe { std::str::from_utf8_unchecked(line) };
        name_set.insert(name.into());
    }

    let mut name_vec = Vec::with_capacity(name_set.len());
    name_set.iter().for_each(|name| {
        name_vec.push(name.to_owned());
    });

    name_vec
}

fn build_test_data(weather_station_names: &[String], num_rows_to_create: usize) -> io::Result<()> {
    let coldest_temp: f32 = -99.9;
    let hottest_temp: f32 = 99.9;

    let length = weather_station_names.len();

    let temp_range = Uniform::new(coldest_temp, hottest_temp);
    let station_range = Uniform::new(0, length);

    let mut file = BufWriter::new(File::create("data/measurements.txt")?);
    let file_mutex = std::sync::Mutex::new(&mut file);

    let precomputed_strings: Vec<_> = weather_station_names
        .iter()
        .map(|name| format!("{};", name).into_bytes())
        .collect();

    (0..num_rows_to_create)
        .into_par_iter()
        .map_init(
            || rand::thread_rng(),
            |rng, _| {
                let station_index = station_range.sample(rng);
                let temp = temp_range.sample(rng);
                let mut line = precomputed_strings[station_index].clone();
                line.extend_from_slice(format!("{:.1}\n", temp).as_bytes());
                line
            },
        )
        .chunks(1000)
        .for_each(|lines| {
            let mut file = file_mutex.lock().unwrap();

            for line in lines {
                file.write_all(&line[..]).unwrap();
            }
        });

    Ok(())
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let num_rows_to_create = check_args(args).expect("Invalid arguments");

    let start_time = Instant::now();
    let weather_station_names = build_weather_station_name_list();
    println!("build seed:{:?}", start_time.elapsed());

    let start_time = Instant::now();
    build_test_data(&weather_station_names, num_rows_to_create)?;
    println!("generate file:{:?}", start_time.elapsed());
    println!("Test data successfully written to data/measurements.txt");
    Ok(())
}
