use memmap2::MmapOptions;
use rand::distributions::{Distribution, Uniform};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;
use std::env;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use std::time::Instant;

static COLDEST_TEMP: f32 = -99.9;
static HOTTEST_TEMP: f32 = 99.9;
static BATCHES: u64 = 100_000;
static SEED: u64 = 0;

fn check_args(args: Vec<String>) -> Result<usize, &'static str> {
    if args.len() != 2 {
        return Err("Usage: create_measurements <positive integer number of records to create>");
    }
    match args[1].parse::<usize>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err("Usage: create_measurements <positive integer number of records to create>"),
    }
}

fn build_weather_station_name_list() -> Vec<Vec<u8>> {
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
    let mut name_set = hashbrown::HashSet::new();
    for next_pos in memchr::memchr_iter(b'\n', &mmap) {
        let line: &[u8] = &mmap[last_pos..next_pos];
        last_pos = next_pos + 1;

        if line.is_empty() {
            continue;
        }
        let separator: usize = memchr::memchr(b';', line).unwrap();
        let line: &[u8] = &line[..separator];
        name_set.insert(line.to_owned());
    }

    name_set.drain().collect()
}

fn build_test_data(weather_station_names: &[Vec<u8>], num_rows_to_create: usize) -> io::Result<()> {
    let batch_size = num_rows_to_create as u64 / BATCHES;
    let length = weather_station_names.len();

    let temp_range = Uniform::new(COLDEST_TEMP, HOTTEST_TEMP);
    let station_range = Uniform::new(0, length);

    let mut file = BufWriter::new(File::create("data/measurements.txt")?);
    let file_mutex = std::sync::Mutex::new(&mut file);

    (0..BATCHES)
        .into_par_iter()
        .map(|i| {
            let mut rng = ChaCha8Rng::seed_from_u64(SEED);
            rng.set_stream(i);

            let mut buffer: Vec<u8> = Vec::with_capacity(BATCHES as usize);
            for _ in 0..batch_size {
                let station_index = station_range.sample(&mut rng);
                let temp = temp_range.sample(&mut rng);
                buffer.extend_from_slice(&weather_station_names[station_index][..]);
                write!(buffer, "{:.1}\n", temp).unwrap();
            }

            buffer
        })
        .for_each(|buffer| {
            let mut file = file_mutex.lock().unwrap();
            file.write_all(&buffer).unwrap();
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
