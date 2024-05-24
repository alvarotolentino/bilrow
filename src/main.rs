use hashbrown::HashMap;
use std::fmt::Write;
use std::fs::File;
use std::{io, path::Path, process::Command, time::Instant};

use memmap2::MmapOptions;
pub struct ProcessedStation {
    min_temp: f32,
    max_temp: f32,
    avg_temp: f32,
    avg_count: usize,
}

pub fn solution(input_path: &Path) -> HashMap<Box<str>, ProcessedStation> {
    let mut station_map: HashMap<Box<str>, ProcessedStation> = HashMap::new();

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

    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };

    let mut last_pos = 0;
    for next_pos in memchr::memchr_iter(b'\n', &mmap) {
        let line = &mmap[last_pos..next_pos];
        last_pos = next_pos + 1;

        if line.is_empty() {
            continue;
        }

        let separator = b';';
        let index = line.iter().position(|&c| c == separator).unwrap();

        let (name, temp) = line.split_at(index);
        let temp = unsafe { std::str::from_utf8_unchecked(&temp[1..]) };
        let temp: f32 = temp.parse::<f32>().unwrap();
        let name = unsafe { std::str::from_utf8_unchecked(name) };
        let name = Box::from(name);

        let station = station_map
            .entry(name)
            .or_insert_with(|| ProcessedStation {
                min_temp: temp,
                max_temp: temp,
                avg_temp: temp,
                avg_count: 1,
            });

        if temp < station.min_temp {
            station.min_temp = temp;
        }
        if temp > station.max_temp {
            station.max_temp = temp;
        }
        station.avg_temp += temp;
        station.avg_count += 1;
    }

    println!("Stations: {:?}", station_map.len());

    println!("Sorted stations");

    station_map
}

pub fn format_output(stations: &HashMap<Box<str>, ProcessedStation>) -> String {
    let mut output = String::new();

    println!("count: {:?}", stations.len());
    output.push('{');
    stations.iter().for_each(|(name, station)| {
        let min = station.min_temp;
        let max = station.max_temp;
        let avg = station.avg_temp / station.avg_count as f32;
        let _ = write!(&mut output, "{}={:.1}/{:.1}/{:.1}, ", name, min, max, avg);
    });

    output.push('}');
    output
}

fn main() -> io::Result<()> {
    let hash = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("Failed to execute git command");
    let hash = String::from_utf8(hash.stdout).expect("Invalid UTF-8");
    let hash = hash.trim();

    let start = Instant::now();
    let stations = solution(Path::new("data/measurements.txt"));
    let elapsed = start.elapsed();

    let formatted = format_output(&stations);
    println!("{}", formatted);
    println!("{}: {elapsed:?}", hash);
    Ok(())
}
