use std::fs::File;

use std::sync::Arc;
use std::{io, path::Path, process::Command, time::Instant};

use memmap2::MmapOptions;

const MAP_TO_BYTE: [u8; 10] = [b'0', b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9'];
//min_temp, max_temp, avg_temp, avg_count
pub struct ProcessedStation(f32, f32, f32, usize);

pub fn split_file(num_threads: usize, data: &[u8]) -> Vec<usize> {
    let mut split_points = Vec::with_capacity(num_threads);
    for i in 1..num_threads {
        let start = data.len() / num_threads * i;
        let new_line = memchr::memchr(b'\n', &data[start..]).unwrap();
        split_points.push(start + new_line + 1);
    }
    split_points
}

fn mapping_slice_to_f32(slice: &[u8]) -> f32 {
    let is_negative = slice[0] == b'-';
    let mut temp = 0.0;
    let start = if is_negative { 1 } else { 0 };

    for i in start..slice.len() {
        let value = &slice[i];

        if slice[i] == b'-' {
            continue;
        }
        if slice[i] == b'.' {
            let dec = &slice[i + 1];
            let dec = MAP_TO_BYTE.iter().position(|&x| x == *dec).unwrap() as f32 / 10.0;
            temp += dec;

            break;
        }

        let value_pos = MAP_TO_BYTE.iter().position(|&x| x == *value).unwrap();
        if temp == 0.0 {
            temp = value_pos as f32;
        } else {
            temp = temp * 10.0 + value_pos as f32;
        }
    }
    if is_negative {
        temp *= -1.0;
    }
    temp
}

fn mapping_slice_to_f32_alt(slice: &[u8]) -> f32 {
    let s = std::str::from_utf8(slice).unwrap();
    s.parse::<f32>().unwrap()
}

pub fn thread(
    data: Arc<memmap2::Mmap>,
    start_idx: usize,
    end_idx: usize,
) -> hashbrown::HashMap<Vec<u8>, ProcessedStation> {
    let mut stations: hashbrown::HashMap<Vec<u8>, ProcessedStation> = hashbrown::HashMap::new();

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

        let temp = mapping_slice_to_f32(&temp[1..]);

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
                stations.insert(name.into(), ProcessedStation(temp, temp, temp, 1));
            }
        }
    }
    stations
}

pub fn merge_stations(
    thread_data: &Vec<hashbrown::HashMap<Vec<u8>, ProcessedStation>>,
    station_map: &mut hashbrown::HashMap<Vec<u8>, ProcessedStation>,
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
    station_map: &mut hashbrown::HashMap<Vec<u8>, ProcessedStation>,
    input_path: &Path,
) {
    let num_threads = rayon::current_num_threads();
    let file = File::open(input_path);
    let file = match file {
        Ok(file) => {
            println!("File opened successfully");
            file
        }
        Err(e) => {
            panic!("Error opening file: {}", e);
        }
    };

    let start = Instant::now();
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    let data: Arc<memmap2::Mmap> = Arc::new(mmap);

    let split_points = split_file(num_threads, &data);

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

    let thread_data: Vec<hashbrown::HashMap<Vec<u8>, ProcessedStation>> = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();

    println!("thread processing: {:?}", start.elapsed());

    let start = Instant::now();
    merge_stations(&thread_data, station_map);
    println!("Stations: {:?}", station_map.len());
    println!("merge processing: {:?}", start.elapsed());

    println!("Sorted stations");
}

pub fn format_output(stations: &hashbrown::HashMap<Vec<u8>, ProcessedStation>) -> String {
    let mut output = String::new();
    output.reserve(1024);

    println!("count: {:?}", stations.len());
    output.push('{');
    stations.iter().for_each(|(name, station)| {
        let name = unsafe { std::str::from_utf8_unchecked(name) };
        output.push_str(&format!(
            "{}={:.1}/{:.1}/{:.1}, ",
            name,
            station.0,
            station.1,
            station.2 / station.3 as f32
        ));
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

    let mut station_map: hashbrown::HashMap<Vec<u8>, ProcessedStation> = hashbrown::HashMap::new();
    solution(&mut station_map, Path::new("data/measurements.txt"));

    // let output = format_output(&station_map);
    // println!("output: {}", output);

    println!("{} time: {:?}", hash, global_start.elapsed());
    Ok(())
}
