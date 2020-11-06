use std::fmt;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::Path;

fn main() {
    let out_dir = std::env::var_os("OUT_DIR").unwrap();
    let out_dir = Path::new(&out_dir).join("out.rs");
    let out_dir = &out_dir;
    let out_dir = OpenOptions::new()
        .write(true)
        .read(false)
        .truncate(true)
        .create(true)
        .open(out_dir)
        .unwrap();
    let mut out_dir = BufWriter::new(out_dir);
    let out_dir = &mut out_dir;

    let word_count = std::env::var("WORD_COUNT").ok();
    let word_count = word_count.as_deref().unwrap_or("1");
    let word_count = word_count.parse().unwrap_or(1);

    struct ToOwnedImpl(usize);

    impl fmt::Display for ToOwnedImpl {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "[")?;
            for entry in 0..self.0 {
                write!(f, "chunks[{0}].to_string(),", entry)?
            }
            write!(f, "]")
        }
    }

    struct PrintFmtImpl(usize);

    impl fmt::Display for PrintFmtImpl {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            for entry in 0..self.0 {
                if entry != 0 {
                    write!(f, " ")?;
                }
                write!(f, "{{}}")?;
            }
            Ok(())
        }
    }

    struct PrintArgsImpl(usize);

    impl fmt::Display for PrintArgsImpl {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            for entry in 0..self.0 {
                write!(f, ", chunk[{0}]", entry)?
            }
            Ok(())
        }
    }

    struct TupleTy(usize);

    impl fmt::Display for TupleTy {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "(")?;
            for _ in 0..self.0 {
                write!(f, "&'a str,")?
            }
            write!(f, ")")
        }
    }

    struct ArrayVal(usize);

    impl fmt::Display for ArrayVal {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "[")?;
            for entry in 0..self.0 {
                write!(f, "tuple.{0},", entry)?
            }
            write!(f, "]")
        }
    }

    write!(
        out_dir,
        r#"
pub const WORD_COUNT: usize = {wc};
pub fn get_chunks<'a>(tuple: {tuple_ty}) -> [&'a str; WORD_COUNT] {{
    {array_val}
}}
pub fn to_owned(chunks: [&str; WORD_COUNT]) -> [String; WORD_COUNT] {{
    {to_owned}
}}
pub fn print_result(chunk: [String; WORD_COUNT]) {{
    print!("\t{print_fmt}"{print_arg})
}}
"#,
        wc = word_count,
        to_owned = ToOwnedImpl(word_count),
        print_fmt = PrintFmtImpl(word_count),
        print_arg = PrintArgsImpl(word_count),
        tuple_ty = TupleTy(word_count),
        array_val = ArrayVal(word_count)
    )
    .unwrap();

    println!("cargo:rerun-if-env-changed=WORD_COUNT");
}
