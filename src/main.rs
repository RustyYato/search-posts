use hashbrown::HashMap;
use rayon::prelude::*;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::io::Read;
use unicode_segmentation::UnicodeSegmentation;
use walkdir::WalkDir;

use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

mod value;

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

fn find_desc(input: value::Value<'_>, search: &mut HashMap<[String; 2], u32>) {
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

                use itertools::Itertools;
                use std::hash::{BuildHasher, Hash, Hasher};

                for sentence in item.unicode_sentences() {
                    for mut chunk in &sentence.unicode_words().chunks(2) {
                        let chunk = match (chunk.next(), chunk.next()) {
                            (Some(a), Some(b)) => [a, b],
                            _ => continue,
                        };

                        let mut hasher = search.hasher().build_hasher();
                        chunk.hash(&mut hasher);
                        let hash = hasher.finish();

                        *search
                            .raw_entry_mut()
                            .from_hash(hash, |[a, b]| a == chunk[0] && b == chunk[1])
                            .or_insert_with(|| ([chunk[0].to_string(), chunk[1].to_string()], 0))
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
    let start = std::time::Instant::now();

    let paths: Vec<String> = std::env::args().collect();

    static COUNT: AtomicU32 = AtomicU32::new(1);

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

    let words = files
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

                        eprintln!("FINISHED ({:4}/{:4}) {:?}", count, total, file_path);
                    }
                    Err(_) => {
                        let count = COUNT.fetch_add(1, Relaxed);
                        eprintln!("NO POSTS ({:4}/{:4}) {:?}", count, total, file_path);
                    }
                }

                (file_contents, words)
            },
        )
        .map(|(_, a)| a)
        .reduce(HashMap::new, |mut a, mut b| {
            if b.capacity() > a.capacity() {
                std::mem::swap(&mut a, &mut b);
            }

            a.reserve(b.len());

            for (b, v) in b {
                *a.entry(b).or_default() += v;
            }

            a
        });

    use std::cmp::Reverse;

    let mut table = BTreeMap::<_, Vec<_>>::new();

    for (key, value) in words {
        table.entry(Reverse(value)).or_default().push(key);
    }

    for (Reverse(key), words) in table {
        print!("{}", key);
        for [a, b] in words {
            print!("\t{} {}", a, b);
        }
        println!()
    }

    eprintln!("time: {}", start.elapsed().as_secs_f32());
}
