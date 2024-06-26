use memmap2::MmapOptions;
use std::fs::File;
use std::sync::Arc;
use std::{io, path::Path, process::Command, time::Instant};
static SOURCE_BUFFER_SIZE: usize = 40_000;

pub struct ProcessedStation(i16, i16, i16, usize);

pub fn split_file(num_threads: usize, data: &memmap2::Mmap) -> Vec<usize> {
    let mut split_points = Vec::with_capacity(num_threads);
    for i in 1..num_threads {
        let start = data.len() / num_threads * i;
        let nearest_new_line = memchr::memchr(b'\n', &data[start..]).unwrap();
        let pos = start + nearest_new_line + 1;
        split_points.push(pos);
    }
    split_points
}

fn parse_to_i16(slice: &[u8]) -> i16 {
    let mut temp = 0_i16;
    let is_negative = slice[0] == b'-';

    let mut pos = 0;
    if is_negative {
        pos += 1;
    };

    temp += (slice[pos] - b'0') as i16;
    pos += 1;
    if slice[pos] != b'.' {
        temp = (temp * 10) + (slice[pos] - b'0') as i16;
        pos += 1;
    }
    pos += 1;
    temp = (temp * 10) + (slice[pos] - b'0') as i16;
    if is_negative {
        -temp
    } else {
        temp
    }
}

pub fn thread(
    data: Arc<memmap2::Mmap>,
    start_idx: usize,
    end_idx: usize,
) -> gxhash::HashMap<Vec<u8>, ProcessedStation> {
    let hash_builder = gxhash::GxBuildHasher::default();
    let mut stations: gxhash::HashMap<Vec<u8>, ProcessedStation> =
        gxhash::HashMap::with_capacity_and_hasher(
            SOURCE_BUFFER_SIZE / rayon::current_num_threads(),
            hash_builder,
        );

    let mut last_pos = 0;
    let data = &data[start_idx..end_idx];
    for next_pos in memchr::memchr_iter(b'\n', data) {
        let line: &[u8] = &data[last_pos..next_pos];
        last_pos = next_pos + 1;

        if line.is_empty() {
            continue;
        }

        let semicolon_idx = memchr::memchr(b';', line).unwrap();
        let (name, temp) = line.split_at(semicolon_idx);

        let temp = parse_to_i16(&temp[1..]);

        match stations.get_mut(name) {
            Some(station) => {
                if temp < station.0 {
                    station.0 = temp;
                }
                if temp > station.1 {
                    station.1 = temp;
                }
                station.2 += temp;
                station.3 += 1;
            }
            None => {
                stations.insert(name.to_owned(), ProcessedStation(temp, temp, temp, 1));
            }
        }
    }
    stations
}

pub fn merge_stations(
    thread_data: &Vec<gxhash::HashMap<Vec<u8>, ProcessedStation>>,
    station_map: &mut gxhash::HashMap<Vec<u8>, ProcessedStation>,
) {
    for data in thread_data {
        for (name, station) in data {
            match station_map.get_mut(name) {
                Some(s) => {
                    if station.0 < s.0 {
                        s.0 = station.0;
                    }
                    if station.1 > s.1 {
                        s.1 = station.1;
                    }
                    s.2 += station.2;
                    s.3 += station.3;
                }
                None => {
                    station_map.insert(
                        name.to_vec(),
                        ProcessedStation(station.0, station.1, station.2, station.3),
                    );
                }
            }
        }
    }
}

pub fn solution(
    station_map: &mut gxhash::HashMap<Vec<u8>, ProcessedStation>,
    input_path: &Path,
) {
    let number_of_threads = rayon::current_num_threads();
    let file = File::open(input_path);
    let file = match file {
        Ok(file) => {
            eprintln!("File opened successfully");
            file
        }
        Err(e) => {
            panic!("Error opening file: {}", e);
        }
    };

    let start = Instant::now();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    let data: Arc<memmap2::Mmap> = Arc::new(mmap);
    eprintln!("mmap: {:?}", start.elapsed());

    let start = Instant::now();
    let split_points = split_file(number_of_threads, &data);
    eprintln!("split: {:?}", start.elapsed());

    let start = Instant::now();
    let threads: Vec<_> = (0..split_points.len())
        .map(|i| {
            let data = Arc::clone(&data);
            let start = split_points[i];
            let end = if i == split_points.len() - 1 {
                data.len()
            } else {
                split_points[i + 1]
            };
            std::thread::spawn(move || thread(data, start, end))
        })
        .collect();

    let thread_data: Vec<gxhash::HashMap<Vec<u8>, ProcessedStation>> = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();

    eprintln!("thread processing: {:?}", start.elapsed());

    let start = Instant::now();
    merge_stations(&thread_data, station_map);
    eprintln!("Stations: {:?}", station_map.len());
    eprintln!("merge processing: {:?}", start.elapsed());

    eprintln!("Sorted stations");
}

pub fn format_output(stations: &gxhash::HashMap<Vec<u8>, ProcessedStation>) -> String {
    let mut output = String::new();
    output.reserve(output.len() * 50);

    output.push('{');
    stations.iter().for_each(|(name, station)| {
        use std::fmt::Write;
        let name = unsafe { std::str::from_utf8_unchecked(name) };
        let _ = write!(
            &mut output,
            "{}={:.1}/{:.1}/{:.1}, ",
            name,
            station.0 as f32 / 10.0,
            station.1 as f32 / 10.0,
            (station.2 as f32 / 10.0) / station.3 as f32
        );
    });

    output.push('}');

    output
}

fn main() -> io::Result<()> {
    let global_start = Instant::now();
    let hash = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("Failed to execute git command");
    let hash = String::from_utf8(hash.stdout).expect("Invalid UTF-8");
    let hash = hash.trim();

    let hash_builder = gxhash::GxBuildHasher::default();
    let mut station_map: gxhash::HashMap<Vec<u8>, ProcessedStation> =
    gxhash::HashMap::with_capacity_and_hasher(SOURCE_BUFFER_SIZE, hash_builder);
    solution(&mut station_map, Path::new("data/measurements.txt"));

    let _ = format_output(&station_map);

    println!("{} time: {:?}", hash, global_start.elapsed());
    Ok(())
}
