use memmap2::MmapOptions;
use rand::{
    distributions::{Distribution, Uniform},
    SeedableRng,
};
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;
use std::{
    env,
    fs::File,
    io::{self, BufWriter, Write},
    path::PathBuf,
    sync::Arc,
    time::Instant,
};

static SEED: u64 = 0;
static COLDEST_TEMP: f32 = -99.9;
static HOTTEST_TEMP: f32 = 99.9;
static BATCHES: u64 = 10_000;
static SOURCE_BUFFER_SIZE: usize = 40_000;

const MAP_TO_BYTE: [u8; 10] = [b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9'];

fn check_args(args: Vec<String>) -> Result<usize, &'static str> {
    if args.len() != 2 {
        return Err("Usage: create_measurements <positive integer number of records to create>");
    }
    match args[1].parse::<usize>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err("Usage: create_measurements <positive integer number of records to create>"),
    }
}

fn build_weather_station_name_list(name_set: &mut hashbrown::HashSet<Vec<u8>, ahash::RandomState>) {
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

    for next_pos in memchr::memchr_iter(b'\n', &mmap) {
        let line: &[u8] = &mmap[last_pos..next_pos];
        last_pos = next_pos + 1;

        if line.is_empty() {
            continue;
        }
        let separator: usize = memchr::memchr(b';', line).unwrap();
        let line = &line[..separator];
        name_set.insert(line.to_vec());
    }
}

fn append_bytes(temp: f32, temp_vec: &mut Vec<u8>) {
    let temp = (temp * 10.0) as i16;
    let negative = temp < 0;
    let temp = temp.abs();

    let cents = if temp >= 100 { temp / 100 } else { 0 };

    let tens = (temp / 10) % 10;
    let units = temp % 10;

    temp_vec.push(b';');

    if negative {
        temp_vec.push(b'-');
    }

    if cents > 0 {
        temp_vec.push(MAP_TO_BYTE[cents as usize]);
    }

    temp_vec.push(MAP_TO_BYTE[tens as usize]);
    temp_vec.push(b'.');
    temp_vec.push(MAP_TO_BYTE[units as usize]);
    temp_vec.push(b'\n');
}

pub fn build_test_data(num_rows_to_create: usize) -> io::Result<()> {
    let batch_size = num_rows_to_create as u64 / BATCHES;
    let hasher = ahash::RandomState::default();
    let mut name_set: hashbrown::HashSet<Vec<u8>, ahash::RandomState> =
        hashbrown::HashSet::with_capacity_and_hasher(SOURCE_BUFFER_SIZE, hasher);
    build_weather_station_name_list(&mut name_set);
    let name_vec: Vec<Vec<u8>> = name_set.drain().collect();

    let temp_range: Uniform<f32> = Uniform::new(COLDEST_TEMP, HOTTEST_TEMP);
    let index_range: Uniform<u16> = Uniform::new(0, name_vec.len() as u16);

    let mut writer = BufWriter::new(File::create("data/measurements.txt")?);
    let writer = Arc::new(std::sync::Mutex::new(&mut writer));
    let buffer: Vec<u8> = Vec::with_capacity(batch_size as usize * std::mem::size_of::<Vec<u8>>());

    (0..BATCHES)
        .into_par_iter()
        .for_each_with(buffer, |buffer, i| {
            let mut rng = ChaCha8Rng::seed_from_u64(SEED);
            rng.set_stream(i);

            for _ in 0..batch_size {
                let station_index = index_range.sample(&mut rng);
                let temp = temp_range.sample(&mut rng);

                name_vec[station_index as usize]
                    .iter()
                    .for_each(|&c| buffer.push(c));

                append_bytes(temp, buffer);
            }

            let mut writer = writer.lock().unwrap();
            (*writer).write_all(buffer).unwrap();
            (*writer).flush().unwrap();
            buffer.clear();
        });

    Ok(())
}

pub fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let num_rows_to_create = check_args(args).expect("Invalid arguments");

    let start = Instant::now();
    build_test_data(num_rows_to_create)?;
    println!("Time elapsed: {:?}", start.elapsed());
    println!("Test data successfully written to data/measurements.txt");
    Ok(())
}
