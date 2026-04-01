use std::path::Path;
use std::process::Command;
use std::{env, fs};

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(|s| s.as_str()) {
        Some("samples") => build_samples(),
        Some("clean-samples") => clean_samples(),
        _ => {
            eprintln!("Usage: cargo xtask <command>");
            eprintln!();
            eprintln!("Commands:");
            eprintln!("  samples         Build sample .db files from schema + dataset .sql files");
            eprintln!("  clean-samples   Remove built sample .db files");
            eprintln!();
            eprintln!("Sample structure:");
            eprintln!("  samples/<name>-schema.sql          Schema (CREATE TABLE statements)");
            eprintln!("  samples/<name>-dataset/<set>.sql    Dataset (INSERT statements)");
            eprintln!("  → builds samples/gen/<name>-<set>.db");
            std::process::exit(1);
        }
    }
}

fn project_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

fn build_samples() {
    let samples_dir = project_root().join("samples");
    let gen_dir = samples_dir.join("gen");
    fs::create_dir_all(&gen_dir).expect("failed to create samples/gen/");
    let mut built = 0;

    for entry in fs::read_dir(&samples_dir).expect("failed to read samples/") {
        let path = entry.unwrap().path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.ends_with("-schema.sql") => n.strip_suffix("-schema.sql").unwrap().to_string(),
            _ => continue,
        };

        let schema_sql = fs::read_to_string(&path).expect("failed to read schema file");

        let dataset_dir = samples_dir.join(format!("{}-dataset", name));
        if !dataset_dir.is_dir() {
            eprintln!("warning: no dataset directory for {}: {}", name, dataset_dir.display());
            continue;
        }

        for ds_entry in fs::read_dir(&dataset_dir).expect("failed to read dataset dir") {
            let ds_path = ds_entry.unwrap().path();
            if ds_path.extension().is_some_and(|e| e == "sql") {
                let dataset_name = ds_path.file_stem().unwrap().to_str().unwrap();
                let db_path = gen_dir.join(format!("{}-{}.db", name, dataset_name));
                let _ = fs::remove_file(&db_path);

                let dataset_sql = fs::read_to_string(&ds_path).expect("failed to read dataset file");
                let combined = format!("{}\n{}", schema_sql, dataset_sql);

                let status = Command::new("sqlite3")
                    .arg(&db_path)
                    .stdin(std::process::Stdio::piped())
                    .spawn()
                    .and_then(|mut child| {
                        use std::io::Write;
                        child.stdin.take().unwrap().write_all(combined.as_bytes())?;
                        child.wait()
                    })
                    .expect("failed to run sqlite3");

                if !status.success() {
                    eprintln!("sqlite3 failed for {} + {}", path.display(), ds_path.display());
                    std::process::exit(1);
                }
                println!("built {}", db_path.display());
                built += 1;
            }
        }
    }

    if built == 0 {
        eprintln!("no schemas found in {}", samples_dir.display());
        std::process::exit(1);
    }
}

fn clean_samples() {
    let gen_dir = project_root().join("samples").join("gen");
    if !gen_dir.is_dir() {
        return;
    }
    for entry in fs::read_dir(&gen_dir).expect("failed to read samples/gen/") {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "db") {
            fs::remove_file(&path).expect("failed to remove .db file");
            println!("removed {}", path.display());
        }
    }
}
