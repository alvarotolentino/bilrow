use rand::Rng;
use rayon::prelude::*;
use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
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

fn build_weather_station_name_list() -> io::Result<HashSet<String>> {
    //open file from current path using env::current_dir()
    let mut current_dir: PathBuf = env::current_dir()?;
    current_dir.push("data/weather_stations.csv");

    let file = File::open(current_dir);
    let file = match file {
        Ok(file) => file,
        Err(e) => {
            println!("Error opening file: {}", e);
            std::process::exit(1);
        }
    };

    let reader = io::BufReader::new(file);
    let mut station_names = HashSet::new();
    for line in reader.lines() {
        let line = line?;
        if !line.starts_with('#') {
            let station = line.split(';').next().unwrap().to_string();
            station_names.insert(station);
        }
    }
    Ok(station_names)
}

fn build_test_data(
    weather_station_names: HashSet<String>,
    num_rows_to_create: usize,
) -> io::Result<()> {
    let start_time = Instant::now();
    let coldest_temp = -99.9;
    let hottest_temp = 99.9;
    let station_names: Vec<_> = weather_station_names.iter().collect();
    let mut file = BufWriter::new(File::create("data/measurements.txt")?);
    let file_mutex = std::sync::Mutex::new(&mut file);
    (0..num_rows_to_create).into_par_iter().for_each(|_| {
        let mut rng = rand::thread_rng();
        let station = rng.gen_range(0..station_names.len());
        let temp = rng.gen_range(coldest_temp..hottest_temp);
        let line = format!("{};{:.1}", station_names[station], temp);
        let mut file = file_mutex.lock().unwrap();
        writeln!(file, "{}", line).unwrap();
    });
    let elapsed_time = start_time.elapsed();
    println!("Test data successfully written to data/measurements.txt");
    println!("Elapsed time: {:?}", elapsed_time);
    Ok(())
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let num_rows_to_create = check_args(args).expect("Invalid arguments");
    let weather_station_names = build_weather_station_name_list()?;
    build_test_data(weather_station_names, num_rows_to_create)?;
    println!("Test data build complete.");
    Ok(())
}
