use anyhow::Result;
use indicatif::ProgressBar;
use jwalk::WalkDir;
use rayon::prelude::*;
use std::{time::Instant, path::PathBuf, sync::{Arc, Mutex}, ffi::OsStr};
use chrono::prelude::{DateTime, Utc};
use clap::Parser;
use csv::Writer;

#[derive(Parser)]
struct Cli {
    #[clap(short, long, value_parser)]
    path: PathBuf,

    #[clap(short, long, default_value_t = usize::MAX, value_parser)]
    depth: usize,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FileInfo<'a> {
    name: &'a OsStr, 
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    capacity: u64
}

impl <'t>FileInfo<'t> {
    fn new(name: &'t OsStr, created:DateTime<Utc>, modified: DateTime<Utc>, capacity: u64 ) -> Self {
        FileInfo { name, created, modified, capacity }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let start = Instant::now();

    let files: Vec<_> = WalkDir::new(cli.path)
        .max_depth(cli.depth)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|d| d.file_type().is_file())
        .collect();

    let bar = ProgressBar::new(files.len() as u64);

    let file_infos = Arc::new(Mutex::new(Vec::new()));

    files.par_iter().for_each(|e| {
        bar.inc(1);
        let file_name = e.file_name();

        let modified: DateTime<Utc> = e.metadata().unwrap().modified().unwrap().into();
        let created: DateTime<Utc> = e.metadata().unwrap().created().unwrap().into();
        let capacity = e.metadata().unwrap().len();

        let file_info = FileInfo::new(file_name, created, modified, capacity);


        file_infos.lock().unwrap().push(file_info);
        
    });

    let mut results = file_infos.lock().unwrap();

    results.sort();

    let mut wtr = Writer::from_path("meta-dir.csv")?;
    wtr.write_record(&["Name", "Created", "Modified", "Capacity"])?;
    for item in results.iter() {
        // println!("Name: {:?}, Created: {}, Modified: {}", item.name, item.created, item.modified)
        let name = item.name.to_str().unwrap();
        let created = item.created.to_string();
        let modified = item.modified.to_string();
        let capacity = item.capacity.to_string();
        wtr.write_record(&[name, &created, &modified, &capacity])?;
    }

    wtr.flush()?;

    let duration = start.elapsed();
    println!("Time elapsed: {:?}, files: {}", duration, results.len());

    Ok(())
}
