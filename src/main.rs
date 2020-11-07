use bincode::config::Options;
use hashbrown::HashMap;
use log::{error, info, warn};
use rayon::prelude::*;
use serde::de::DeserializeSeed;
use walkdir::WalkDir;

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

mod des_collect;
mod proc_file;

mod config {
    include!(concat!(env!("OUT_DIR"), "/out.rs"));
}

static FILE_PROCESED_COUNT: AtomicU32 = AtomicU32::new(1);
static TOTAL_FILE_COUNT: AtomicUsize = AtomicUsize::new(0);

type Phrase<'a> = [&'a str; config::WORD_COUNT];
type PhraseBuf = [Box<str>; config::WORD_COUNT];
type Map = HashMap<PhraseBuf, u32>;

fn insert_value(phrase: Phrase, count: u32, phrase_counts: &mut Map) {
    use std::hash::{BuildHasher, Hash, Hasher};

    let mut hasher = phrase_counts.hasher().build_hasher();
    phrase.hash(&mut hasher);
    let hash = hasher.finish();

    *phrase_counts
        .raw_entry_mut()
        .from_hash(hash, |item| {
            item.iter()
                .map(AsRef::<str>::as_ref)
                .eq(phrase.iter().copied())
        })
        .or_insert_with(|| (config::to_owned(phrase), 0))
        .1 += count;
}

fn process_file(
    start: Instant,
    file_contents: &mut String,
    phrase_counts: &mut Map,
    file_path: impl AsRef<Path>,
) {
    let file_path = file_path.as_ref();
    let mut file = match std::fs::File::open(file_path) {
        Ok(file) => file,
        Err(_) => {
            let count = FILE_PROCESED_COUNT.fetch_add(1, Relaxed);
            error!(
                "CANNOT OPEN ({:4}/{:4}) {:?}",
                count,
                TOTAL_FILE_COUNT.load(Relaxed),
                file_path
            );
            return;
        }
    };

    file_contents.clear();
    let size = file.read_to_string(file_contents).unwrap();
    let file = &file_contents[..size];

    let result = proc_file::ProcFile::new(phrase_counts)
        .deserialize(&mut serde_json::Deserializer::from_str(file));

    match result {
        Ok(()) => {
            let count = FILE_PROCESED_COUNT.fetch_add(1, Relaxed);

            info!(
                "FINISHED ({:4}/{:4}) {:.2} {:?}",
                count,
                TOTAL_FILE_COUNT.load(Relaxed),
                start.elapsed().as_secs_f32(),
                file_path
            );
        }
        Err(_) => {
            let count = FILE_PROCESED_COUNT.fetch_add(1, Relaxed);
            warn!(
                "EMPTY FILE ({:4}/{:4}) {:.2} {:?}",
                count,
                TOTAL_FILE_COUNT.load(Relaxed),
                start.elapsed().as_secs_f32(),
                file_path
            );
        }
    }
}

fn serialize_to_temp(temp_dir: &tempfile::TempDir, phrase_counts: Map) {
    static TEMP_FILE_COUNT: AtomicU32 = AtomicU32::new(0);

    info!("start save: {}", phrase_counts.len());

    let file_id = TEMP_FILE_COUNT.fetch_add(1, Relaxed);
    let file_path = temp_dir.path().join(format!("temp-{}", file_id));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .read(false)
        .create_new(true)
        .open(&file_path)
        .unwrap();

    let file = BufWriter::new(&file);
    let now = Instant::now();

    let a_len = phrase_counts.len();

    bincode::config::DefaultOptions::default()
        .with_no_limit()
        .serialize_into(file, &phrase_counts)
        .unwrap();

    info!(
        "finish save: {} ({} ms)",
        a_len,
        now.elapsed().as_secs_f32() * 1000.0
    );
}

fn main() {
    stderrlog::new()
        .module(module_path!())
        .timestamp(stderrlog::Timestamp::Second)
        .color(stderrlog::ColorChoice::Auto)
        .verbosity(4)
        .init()
        .unwrap();

    let start = Instant::now();

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

    TOTAL_FILE_COUNT.store(files.len(), Relaxed);

    let mut words = save_pool.scope(move |save_pool| {
        files
            .into_par_iter()
            .fold_with(
                (String::new(), HashMap::new()),
                |(mut file_contents, mut phrase_counts), file_path| {
                    process_file(start, &mut file_contents, &mut phrase_counts, file_path);
                    (file_contents, phrase_counts)
                },
            )
            .map(|(s, words)| {
                save_pool.spawn(move |_| drop(s));
                words
            })
            .reduce(HashMap::new, |mut a, mut b| {
                let now = Instant::now();

                for a in &mut [&mut a, &mut b] {
                    if a.len() > 1_000_000 {
                        let phrase_counts = std::mem::take(&mut **a);
                        save_pool.spawn(move |_| {
                            serialize_to_temp(&temp_dir, phrase_counts);
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

                info!("reduce: {} ms", now.elapsed().as_secs_f32() * 1000.0);

                a
            })
    });

    info!("data collection: {} s", start.elapsed().as_secs_f32());

    drop(save_pool);

    let deser = bincode::config::DefaultOptions::default().with_no_limit();
    let mut file_contents = Vec::new();

    let temp_files = walkdir::WalkDir::new(temp_dir.path())
        .into_iter()
        .flatten()
        .filter(|file| file.file_type().is_file())
        .filter_map(|file| {
            let file_path = file.path();
            match std::fs::OpenOptions::new()
                .write(false)
                .read(true)
                .open(file_path)
            {
                Ok(file) => {
                    info!("read temp: {:?}", file_path);
                    Some(file)
                }
                Err(_) => {
                    warn!("unable to read: {:?}", file_path);
                    None
                }
            }
        });

    for file in temp_files {
        let len = file.metadata().unwrap().len();

        file_contents.clear();
        file_contents.resize(len as usize, 0);
        BufReader::new(file).read_exact(&mut file_contents).unwrap();

        deser
            .deserialize_seed(des_collect::DesCollect(&mut words), &file_contents)
            .unwrap();
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
        let i = i + 1;
        write!(file, "{}\t{}", key, words.len());
        info!("prepare to emit: {}/{} - {} ", i, table_len, words.len());

        if words.len() < 1_000_000 {
            for chunk in words {
                config::print_result(&mut *file, chunk);
            }
        }

        writeln!(file);
        info!("writen: {}/{}", i, table_len);
    }

    info!("total time: {}", start.elapsed().as_secs_f32());
}
