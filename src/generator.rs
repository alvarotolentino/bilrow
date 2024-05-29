use memmap2::MmapOptions;
use rand::distributions::{Distribution, Uniform};
use rayon::prelude::*;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
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
    let start_time = Instant::now();
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
    println!("build_weather_station_name_list {:?}", start_time.elapsed());

    name_vec
}

fn build_test_data(
    weather_station_names: &Vec<String>,
    num_rows_to_create: usize,
) -> io::Result<()> {
    let coldest_temp: f32 = -99.9;
    let hottest_temp: f32 = 99.9;

    let output = Arc::new(std::sync::Mutex::new(String::with_capacity(
        num_rows_to_create,
    )));

    let length = weather_station_names.len();

    let temp_range = Uniform::new(coldest_temp, hottest_temp);
    let station_range = Uniform::new(0, length);

    let start_time = Instant::now();
    (0..num_rows_to_create)
        .into_par_iter()
        .map_init(
            || rand::thread_rng(),
            |rng, _| {
                let station_index = station_range.sample(rng);
                let temp = temp_range.sample(rng);
                let name = &weather_station_names[station_index];
                let value = &format!("{};{:.1}\n", name, temp);
                output.lock().unwrap().push_str(value);
            },
        )
        .collect::<Vec<_>>();
    println!("gen output {:?}", start_time.elapsed());

    let lenght = output.lock().unwrap().len();
    println!("output lenght {:?}", lenght);

    let start_time = Instant::now();
    let target = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open("data/measurements.txt")
        .unwrap();
    target.set_len(lenght as u64).unwrap();

    let mut mmap = unsafe { memmap2::MmapMut::map_mut(&target)? };
    println!("mmap len {:?}", mmap.len());

    (&mut mmap[..])
        .write_all(output.lock().unwrap().as_bytes())
        .unwrap();
    mmap.flush().unwrap();
    println!("write output {:?}", start_time.elapsed());

    println!("Test data successfully written to data/measurements.txt");

    Ok(())
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let num_rows_to_create = check_args(args).expect("Invalid arguments");
    let weather_station_names = build_weather_station_name_list();
    build_test_data(&weather_station_names, num_rows_to_create)?;
    println!("Test data build complete.");
    Ok(())
}
