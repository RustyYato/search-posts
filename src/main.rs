use bincode::config::Options;
use hashbrown::HashMap;
use itertools::Itertools;
use rayon::prelude::*;
use serde::Deserialize;
use unicode_segmentation::UnicodeSegmentation;
use walkdir::WalkDir;

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufWriter, Read, Write};
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

mod value;

mod config {
    include!(concat!(env!("OUT_DIR"), "/out.rs"));
}

#[derive(Deserialize)]
struct Json<'a> {
    #[serde(borrow)]
    users: Vec<User<'a>>,
}

#[derive(Deserialize)]
struct User<'a> {
    #[serde(borrow)]
    posts: Vec<value::Value<'a>>,
}

fn find_desc(input: value::Value<'_>, search: &mut HashMap<[Box<str>; config::WORD_COUNT], u32>) {
    use value::Value::*;

    match input {
        Ignored | String(_) => (),
        Array(array) => {
            array.into_iter().for_each(|value| find_desc(value, search));
        }
        Object(map) => {
            if let Some(item) = map.iter().find_map(|(key, value)| {
                if key == "text" || key == "description" {
                    value.as_str()
                } else {
                    None
                }
            }) {
                // for &word in [
                //     "beloye prevoskhodstvo",
                //     "белое превосходство",
                //     "Покорение материковой России",
                //     "Pokoreniye materikovoy Rossii",
                //     "завоевание материка",
                //     "zavoyevaniye materika",
                // ]
                // .iter()
                // {
                //     let count = item.split(word).count().saturating_sub(1);
                //     *search.entry(word).or_default() += count as u64;
                // }

                for sentence in item.unicode_sentences() {
                    for chunk in sentence.unicode_words().tuple_windows() {
                        let chunk = config::get_chunks(chunk);

                        let mut hasher = search.hasher().build_hasher();
                        chunk.hash(&mut hasher);
                        let hash = hasher.finish();

                        *search
                            .raw_entry_mut()
                            .from_hash(hash, |item| {
                                item.iter()
                                    .map(AsRef::<str>::as_ref)
                                    .eq(chunk.iter().copied())
                            })
                            .or_insert_with(|| (config::to_owned(chunk), 0))
                            .1 += 1;
                    }
                }
            }

            map.into_iter()
                .for_each(|(_, value)| find_desc(value, search));
        }
    }
}

fn main() {
    static COUNT: AtomicU32 = AtomicU32::new(1);
    static TEMP_FILE_COUNT: AtomicU32 = AtomicU32::new(0);

    let start = std::time::Instant::now();

    let paths: Vec<String> = std::env::args().collect();

    let temp_dir = tempfile::tempdir().unwrap();
    let temp_dir = &temp_dir;
    let save_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build()
        .unwrap();

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .unwrap();

    let files: Vec<_> = paths
        .into_iter()
        .flat_map(|path| {
            let walk_dir = WalkDir::new(path).into_iter();
            walk_dir.filter_entry(|dir_entry| {
                if dir_entry.file_type().is_dir() {
                    return true;
                }

                if !dir_entry.file_type().is_file() {
                    return false;
                }

                let path = dir_entry.path();

                path.extension()
                    .map_or(false, |ext| ext.to_str() == Some("json"))
            })
        })
        .flatten()
        .filter(|dir_entry| dir_entry.file_type().is_file())
        .map(|dir_entry| dir_entry.path().to_owned())
        .collect();

    let total = files.len();

    let mut words = save_pool.scope(move |save_pool| {
        files
            .into_par_iter()
            .fold_with(
                (String::new(), HashMap::new()),
                |(mut file_contents, mut words), file_path| {
                    let mut file = match std::fs::File::open(&file_path) {
                        Ok(file) => file,
                        Err(_) => {
                            let count = COUNT.fetch_add(1, Relaxed);
                            eprintln!("CANNOT OPEN ({:4}/{:4}) {:?}", count, total, file_path);
                            return (file_contents, words);
                        }
                    };

                    file_contents.clear();
                    let size = file.read_to_string(&mut file_contents).unwrap();
                    let file = &file_contents[..size];

                    match serde_json::from_str(&file) {
                        Ok(json) => {
                            let _: Json = json;
                            for user in json.users {
                                for post in user.posts {
                                    find_desc(post, &mut words);
                                }
                            }

                            let count = COUNT.fetch_add(1, Relaxed);

                            eprintln!(
                                "FINISHED ({:4}/{:4}) {:.2} {:?}",
                                count,
                                total,
                                start.elapsed().as_secs_f32(),
                                file_path
                            );
                        }
                        Err(_) => {
                            let count = COUNT.fetch_add(1, Relaxed);
                            eprintln!(
                                "NO POSTS ({:4}/{:4}) {:.2} {:?}",
                                count,
                                total,
                                start.elapsed().as_secs_f32(),
                                file_path
                            );
                        }
                    }

                    (file_contents, words)
                },
            )
            .map(|(s, words)| {
                let now = std::time::Instant::now();
                save_pool.spawn(move |_| drop(s));
                eprintln!(
                    "drop: {} ({})",
                    start.elapsed().as_secs_f32(),
                    now.elapsed().as_secs_f32() * 1000.0
                );
                words
            })
            .reduce(HashMap::new, |mut a, mut b| {
                let now = std::time::Instant::now();
                for a in &mut [&mut a, &mut b] {
                    if a.len() > 1_000_000 {
                        let a = std::mem::take(&mut **a);
                        save_pool.spawn(move |_| {
                            eprintln!(
                                "start save ({}): {}",
                                a.len(),
                                start.elapsed().as_secs_f32(),
                            );
                            let file_id = TEMP_FILE_COUNT.fetch_add(1, Relaxed);
                            let file = std::fs::OpenOptions::new()
                                .write(true)
                                .read(false)
                                .create_new(true)
                                .open(temp_dir.path().join(format!("temp-{}", file_id)))
                                .unwrap();
                            let file = BufWriter::new(file);
                            let now = std::time::Instant::now();

                            let a_len = a.len();

                            bincode::serialize_into(file, &a).unwrap();

                            eprintln!(
                                "save ({}): {} ({})",
                                a_len,
                                start.elapsed().as_secs_f32(),
                                now.elapsed().as_secs_f32() * 1000.0
                            );
                        });
                    }
                }

                if b.capacity() > a.capacity() {
                    std::mem::swap(&mut a, &mut b);
                }

                a.reserve(b.len());

                for (b, v) in b {
                    *a.entry(b).or_default() += v;
                }

                eprintln!(
                    "reduce: {} ({})",
                    start.elapsed().as_secs_f32(),
                    now.elapsed().as_secs_f32() * 1000.0
                );

                a
            })
    });

    eprintln!("time: {}", start.elapsed().as_secs_f32());

    drop(save_pool);

    let mut file_contents = Vec::new();
    let deser = bincode::config::DefaultOptions::default().with_no_limit();

    for file in walkdir::WalkDir::new(temp_dir.path()) {
        let file = file.unwrap();

        if !file.file_type().is_file() {
            continue;
        }

        let file = file.path();

        eprintln!("read temp: {:?}", file);

        let file = std::fs::OpenOptions::new()
            .write(false)
            .read(true)
            .open(file)
            .unwrap();

        // TODO: use a custom DeserializeSeed to avoid this hashmap all together
        (&file).read_to_end(&mut file_contents).unwrap();
        let map: HashMap<[&str; config::WORD_COUNT], u32> =
            deser.deserialize(&file_contents).unwrap();

        for (word, count) in map {
            let mut hasher = words.hasher().build_hasher();
            word.hash(&mut hasher);
            let hash = hasher.finish();

            *words
                .raw_entry_mut()
                .from_hash(hash, |w| {
                    w.iter().map(AsRef::<str>::as_ref).eq(word.iter().copied())
                })
                .or_insert_with(|| (config::to_owned(word), 0))
                .1 += count;
        }
    }

    let mut table = BTreeMap::<_, Vec<_>>::new();

    for (word, count) in words {
        table.entry(Reverse(count)).or_default().push(word);
    }

    let table_len = table.len();

    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .read(false)
        .open("out.txt")
        .unwrap();
    let mut file = BufWriter::new(file);
    let file = &mut file;

    #[allow(unused_must_use)]
    for (i, (Reverse(key), words)) in table.into_iter().enumerate() {
        write!(file, "{}\t{}", key, words.len());
        eprintln!("prepare: {}/{} - {} ", i, table_len, words.len());
        if words.len() < 1_000_000 {
            for chunk in words {
                config::print_result(&mut *file, chunk);
            }
        }
        writeln!(file);
        eprintln!("writen: {}/{}", i, table_len);
    }

    eprintln!("time: {}", start.elapsed().as_secs_f32());
}
