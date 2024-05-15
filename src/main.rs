use std::fmt::Write;
use std::fs::File;
use std::{io, path::Path, process::Command, time::Instant};

use memmap2::MmapOptions;
pub struct ProcessedStation {
    name: String,
    min_temp: f32,
    max_temp: f32,
    avg_temp: f32,
    avg_count: usize,
}

pub fn solution(input_path: &Path) -> Vec<ProcessedStation> {
    let mut stations: Vec<ProcessedStation> = vec![];
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

        let line = std::str::from_utf8(line).unwrap();

        let Some((name, temp)) = line.split_once(';') else {
            panic!("Invalid line: {}", line);
        };
        let temp: f32 = temp.parse::<f32>().unwrap();

        match stations.iter_mut().find(|s| s.name == name) {
            Some(station) => {
                if temp < station.min_temp {
                    station.min_temp = temp;
                }
                if temp > station.max_temp {
                    station.max_temp = temp;
                }
                station.avg_temp += temp;
                station.avg_count += 1;
            }
            None => {
                stations.push(ProcessedStation {
                    name: name.to_owned(),
                    min_temp: temp,
                    max_temp: temp,
                    avg_temp: temp,
                    avg_count: 1,
                });
            }
        }
    }

    println!("Stations: {:?}", stations.len());
    stations.sort_unstable_by_key(|s| s.name.clone());
    println!("Sorted stations");

    stations
}

pub fn format_output(stations: &[ProcessedStation]) -> String {
    let mut output = String::new();

    println!("count: {:?}", stations.len());
    output.push('{');
    for (i, station) in stations.iter().enumerate() {
        let min = station.min_temp / 10_f32;
        let max = station.max_temp / 10_f32;
        let avg = station.avg_temp / 10_f32 / station.avg_count as f32;
        let _ = write!(
            &mut output,
            "{}={:.1}/{:.1}/{:.1}",
            station.name, min, max, avg
        );
        if i != stations.len() - 1 {
            let _ = write!(&mut output, ", ");
        }
    }
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
