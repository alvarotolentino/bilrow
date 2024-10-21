use memmap2::MmapOptions;

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

use std::fs::File;
use std::sync::Arc;
use std::{io, path::Path, process::Command, time::Instant};

static SOURCE_BUFFER_SIZE: usize = 40_000;
static SEMICOLON_BYTE: u8 = b';';
static NEW_LINE_BYTE: u8 = b'\n';

pub struct ProcessedStation(i16, i16, i16, usize);

pub fn split_file(num_threads: usize, data: &memmap2::Mmap) -> Vec<usize> {
    let mut split_points = Vec::with_capacity(num_threads);
    for i in 1..num_threads {
        let start = data.len() / num_threads * i;
        let nearest_new_line: usize;

        #[cfg(target_feature = "avx2")]
        {
            nearest_new_line = search_u8_avx2(&data[start..], NEW_LINE_BYTE).unwrap();
        }

        #[cfg(not(target_feature = "avx2"))]
        {
            nearest_new_line = memchr::memchr(b'\n', &data[start..]).unwrap();
        }

        let pos = start + nearest_new_line + 1;
        split_points.push(pos);
    }
    split_points
}

#[cfg(not(target_arch = "x86_64"))]
#[cfg(not(target_feature = "sse2"))]
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

#[cfg(target_arch = "x86_64")]
#[cfg(target_feature = "sse2")]
fn parse_to_i16_simd(slice: &[u8]) -> i16 {
    let is_negative = slice[0] == b'-';
    let index = if is_negative { 1 } else { 0 };

    let result: i16;

    unsafe {
        let simd_vec = _mm_loadu_si64(slice.as_ptr().add(index));

        let zero = _mm_set1_epi8(b'0' as i8);
        let digits = _mm_sub_epi8(simd_vec, zero);

        let digit1 = _mm_extract_epi8(digits, 0) as i16;
        let digit2 = _mm_extract_epi8(digits, 1) as i16;
        let digit3 = _mm_extract_epi8(digits, 2) as i16;
        let digit4 = _mm_extract_epi8(digits, 3) as i16;

        if slice[index..].len() < 4 {
            result = digit1 * 10 + digit3;
        } else {
            result = digit1 * 100 + digit2 * 10 + digit4;
        }
    }

    if is_negative {
        -result
    } else {
        result
    }
}

#[cfg(target_arch = "x86_64")]
#[cfg(target_feature = "avx2")]
fn search_u8_avx2(haystack: &[u8], needle: u8) -> Option<usize> {
    unsafe {
        let len = haystack.len();
        let needle_vec: __m256i = _mm256_set1_epi8(needle as i8);

        let mut i = 0;
        while i + 32 <= len {
            let chunk = _mm256_loadu_si256(haystack.as_ptr().add(i) as *const __m256i);
            let cmp_result = _mm256_cmpeq_epi8(chunk, needle_vec);

            let mask = _mm256_movemask_epi8(cmp_result);

            if mask != 0 {
                let bit_pos = mask.trailing_zeros() as usize;
                return Some(i + bit_pos);
            }

            i += 32;
        }

        while i < len {
            if haystack[i] == needle {
                return Some(i);
            }
            i += 1;
        }

        None
    }
}

#[cfg(target_arch = "x86_64")]
#[cfg(target_feature = "sse2")]
pub fn search_u8_sse2_reverse(haystack: &[u8], needle: u8) -> Option<usize> {
    unsafe {
        let len = haystack.len();
        let needle_vec: __m128i = _mm_set1_epi8(needle as i8);

        let mut i = len as isize;

        while i >= 8 {
            i -= 8;
            let chunk = _mm_loadu_si64(haystack.as_ptr().offset(i));
            let cmp_result = _mm_cmpeq_epi8(chunk, needle_vec);

            let mask = _mm_movemask_epi8(cmp_result);

            if mask != 0 {
                let first_set_bit = mask.trailing_zeros() as usize;
                return Some(i as usize + first_set_bit);
            }
        }

        while i > 0 {
            i -= 1;
            if haystack[i as usize] == needle {
                return Some(i as usize);
            }
        }

        None
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

        let semicolon_idx: usize;
        #[cfg(target_feature = "sse2")]
        {
            semicolon_idx = search_u8_sse2_reverse(line, SEMICOLON_BYTE).unwrap();
        }

        #[cfg(not(target_feature = "sse2"))]
        {
            semicolon_idx = memchr::memchr(SEMICOLON_BYTE, line).unwrap();
        }

        let (name, temp) = line.split_at(semicolon_idx);


        let value: i16;
        #[cfg(target_feature = "sse2")]
        {
            value = parse_to_i16_simd(&temp[1..]);
        }

        #[cfg(not(target_feature = "sse2"))]
        {
            value = parse_to_i16(&temp[1..]);
        }

        match stations.get_mut(name) {
            Some(station) => {
                if value < station.0 {
                    station.0 = value;
                }
                if value > station.1 {
                    station.1 = value;
                }
                station.2 += value;
                station.3 += 1;
            }
            None => {
                stations.insert(name.to_owned(), ProcessedStation(value, value, value, 1));
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

pub fn solution(station_map: &mut gxhash::HashMap<Vec<u8>, ProcessedStation>, input_path: &Path) {
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

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    #[cfg(not(target_feature = "sse2"))]
    fn test_parse_to_i16() {
        assert_eq!(parse_to_i16(b"0.1"), 1);
        assert_eq!(parse_to_i16(b"1.1"), 11);
        assert_eq!(parse_to_i16(b"9.9"), 99);
        assert_eq!(parse_to_i16(b"-0.1"), -1);
        assert_eq!(parse_to_i16(b"-1.1"), -11);
        assert_eq!(parse_to_i16(b"-9.9"), -99);
    }

    #[test]
    #[cfg(target_feature = "sse2")]
    #[cfg(target_feature = "sse2")]
    fn test_parse_to_i16_simd() {
        assert_eq!(parse_to_i16_simd(b"0.1"), 1);
        assert_eq!(parse_to_i16_simd(b"1.1"), 11);
        assert_eq!(parse_to_i16_simd(b"9.9"), 99);
        assert_eq!(parse_to_i16_simd(b"-0.1"), -1);
        assert_eq!(parse_to_i16_simd(b"-1.1"), -11);
        assert_eq!(parse_to_i16_simd(b"-9.9"), -99);
    }

    #[test]
    #[cfg(target_feature = "avx2")]
    fn test_search_u8_avx2() {
        let haystack = b"Kladanj;85.3";
        let needle = b';';
        let index = search_u8_avx2(haystack, needle);
        assert_eq!(index, Some(7));
    }

    #[test]
    #[cfg(target_feature = "sse2")]
    fn test_search_u8_sse2_reverse() {
        assert_eq!(
            search_u8_sse2_reverse(b"Phra Nakhon Si Ayutthaya;14.3478", b';'),
            Some(24)
        );
        assert_eq!(
            search_u8_sse2_reverse(b"Colonia del Sacramento;-34.4714", b';'),
            Some(22)
        );
        assert_eq!(
            search_u8_sse2_reverse(b"Yunxian Chengguanzhen;32.8082", b';'),
            Some(21)
        );
        assert_eq!(
            search_u8_sse2_reverse(b"Fernando de la Mora;-25.3200", b';'),
            Some(19)
        );
        assert_eq!(
            search_u8_sse2_reverse("Kamensk-Ural’skiy;56.4000".as_bytes(), b';'),
            Some(19)
        );
        assert_eq!(
            search_u8_sse2_reverse(b"Huntington Beach;33.6960", b';'),
            Some(16)
        );
        assert_eq!(
            search_u8_sse2_reverse(b"Rafael Calzada;-34.7833", b';'),
            Some(14)
        );
        assert_eq!(
            search_u8_sse2_reverse("Harrow Weald;51.6040".as_bytes(), b';'),
            Some(12)
        );
        assert_eq!(
            search_u8_sse2_reverse("Rafsanjān;30.4067".as_bytes(), b';'),
            Some(10)
        );
        assert_eq!(search_u8_sse2_reverse(b"Kladanj;85.3", b';'), Some(7));
    }
}
