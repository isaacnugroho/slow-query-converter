//! # MariaDB Slow Query to CSV Converter
//!
//! This program parses a MariaDB slow query log file and converts its contents
//! into a CSV file where each query and its associated metadata are on a single row.
//! The 'query' field is multiline-aware and quoted, making it suitable for direct
//! import into spreadsheet software like Microsoft Excel. It handles optional '# Time'
//! headers by carrying forward the last seen time value.

use chrono::NaiveDateTime;
use clap::Parser;
use csv::Writer;
use once_cell::sync::Lazy;
use regex::Regex;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

// Statically compiled regular expressions for efficient parsing of log lines.
static RE_TIME: Lazy<Regex> = Lazy::new(|| Regex::new(r"^# Time: (.*)").unwrap());
static RE_USER_HOST: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^# User@Host: (.*?) @\s*(.*)").unwrap());
static RE_METADATA_1: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^# Thread_id: (\d+)\s+Schema: (.*?)\s+QC_hit: (\S+)").unwrap());
static RE_METADATA_2: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"^# Query_time: ([\d.]+)\s+Lock_time: ([\d.]+)\s+Rows_sent: (\d+)\s+Rows_examined: (\d+)",
    )
    .unwrap()
});
static RE_METADATA_3: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^# Rows_affected: (\d+)\s+Bytes_sent: (\d+)").unwrap());
static RE_METADATA_4: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^# Tmp_tables: (\d+)\s+Tmp_disk_tables: (\d+)\s+Tmp_table_sizes: (\d+)").unwrap()
});
static RE_METADATA_5: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"^# Full_scan: (\S+)\s+Full_join: (\S+)\s+Tmp_table: (\S+)\s+Tmp_table_on_disk: (\S+)",
    )
    .unwrap()
});
static RE_METADATA_6: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"^# Filesort: (\S+)\s+Filesort_on_disk: (\S+)\s+Merge_passes: (\d+)\s+Priority_queue: (\S+)",
    )
    .unwrap()
});

// Regex to find and extract specific statements from the query body.
// The `(?i)` flag makes the match case-insensitive.
static RE_SET_TIMESTAMP_EXTRACT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\s*SET timestamp=\d+;\s*").unwrap());
static RE_USE_SCHEMA_EXTRACT: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\s*use `?\w+`?;\s*").unwrap());

// Regex to find skipped lines.
// The `(?i)` flag makes the match case-insensitive.
static RE_SKIPPED_1: Lazy<Regex> = Lazy::new(|| Regex::new(r"started with:\s*$").unwrap());
static RE_SKIPPED_2: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^((Tcp port:)|(Time\s+Id\s+Command))").unwrap());

/// Represents a single entry from the slow query log.
#[derive(Debug, Default, Clone)]
struct SlowQueryEntry {
    time: String,
    user: String,
    host: String,
    thread_id: String,
    schema: String,
    qc_hit: String,
    query_time: f64,
    lock_time: f64,
    rows_sent: u64,
    rows_examined: u64,
    rows_affected: u64,
    bytes_sent: u64,
    query: String,
    tmp_tables: u64,
    tmp_disk_tables: u64,
    tmp_table_sizes: u64,
    full_scan: String,
    full_join: String,
    tmp_table: String,
    tmp_table_on_disk: String,
    filesort: String,
    filesort_on_disk: String,
    merge_passes: u64,
    priority_queue: String,
}

impl SlowQueryEntry {
    /// Writes the contents of the struct as a single record to a CSV writer.
    /// This function also performs the logic to split the query column.
    fn write_to_csv<W: Write>(&self, wtr: &mut Writer<W>) -> Result<(), Box<dyn Error>> {
        // Use string slices to avoid unnecessary clones
        let query = &self.query;

        // 1. Extract 'SET timestamp' statement if it exists.
        let set_timestamp_str = if let Some(mat) = RE_SET_TIMESTAMP_EXTRACT.find(query) {
            mat.as_str().trim().to_string()
        } else {
            String::new()
        };

        // 2. Extract 'use schema' statement if it exists.
        let use_schema_str = if let Some(mat) = RE_USE_SCHEMA_EXTRACT.find(query) {
            mat.as_str().trim().to_string()
        } else {
            String::new()
        };

        // 3. Process the query: extract remaining content after removing extracted statements
        // let mut single_line_query = String::with_capacity(query.len());
        let mut remaining_query = query.to_string();

        // Remove SET timestamp statement if found
        if let Some(mat) = RE_SET_TIMESTAMP_EXTRACT.find(&remaining_query) {
            let before = &remaining_query[..mat.start()];
            let after = &remaining_query[mat.end()..];
            remaining_query = format!("{before}{after}");
        }

        // Remove USE schema statement if found
        if let Some(mat) = RE_USE_SCHEMA_EXTRACT.find(&remaining_query) {
            let before = &remaining_query[..mat.start()];
            let after = &remaining_query[mat.end()..];
            remaining_query = format!("{before}{after}");
        }

        // Process remaining query: single pass with minimal allocations
        // let mut first = true;
        // for line in remaining_query.lines() {
        //     let trimmed = line.trim();
        //     if !trimmed.is_empty() {
        //         if !first {
        //             single_line_query.push(' ');
        //         }
        //         single_line_query.push_str(trimmed);
        //         first = false;
        //     }
        // }

        wtr.write_record([
            &self.time,
            &self.user,
            &self.host,
            &self.thread_id,
            &self.schema,
            &self.qc_hit,
            &set_timestamp_str,
            &use_schema_str,
            &remaining_query,
            &self.query_time.to_string(),
            &self.lock_time.to_string(),
            &self.rows_sent.to_string(),
            &self.rows_examined.to_string(),
            &self.rows_affected.to_string(),
            &self.bytes_sent.to_string(),
            &self.tmp_tables.to_string(),
            &self.tmp_disk_tables.to_string(),
            &self.tmp_table_sizes.to_string(),
            &self.full_scan,
            &self.full_join,
            &self.tmp_table,
            &self.tmp_table_on_disk,
            &self.filesort,
            &self.filesort_on_disk,
            &self.merge_passes.to_string(),
            &self.priority_queue,
        ])?;
        Ok(())
    }

    /// Checks if the entry has enough data to be considered a valid, writeable record.
    /// We use thread_id as a proxy for a complete metadata block.
    fn is_valid(&self) -> bool {
        !self.thread_id.is_empty()
    }
}

/// Defines the command-line arguments accepted by the program.
#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about,
    long_about = "A utility to parse MariaDB slow query logs and convert them to a clean, multiline CSV format."
)]
struct Args {
    /// Path to the input MariaDB slow query log file.
    #[arg(short, long)]
    input: PathBuf,

    /// Path for the output CSV file. If omitted, output will be sent to stdout.
    #[arg(short, long)]
    output: Option<PathBuf>,
}

/// Parses MariaDB's log time format ("yymmdd H:M:S") into a standard
/// "yyyy-mm-dd HH:MM:SS" format.
fn format_log_time(log_time: &str) -> Result<String, chrono::ParseError> {
    let combined_str: String = log_time.split_whitespace().collect::<Vec<&str>>().join(" ");
    let dt = NaiveDateTime::parse_from_str(&combined_str, "%y%m%d %H:%M:%S")?;
    Ok(dt.format("%Y-%m-%d %H:%M:%S").to_string())
}

/// Main function to orchestrate the file reading, parsing, and writing.
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    eprintln!("Starting conversion...");
    eprintln!("Input file: {}", args.input.display());

    let writer: Box<dyn Write> = match args.output {
        Some(path) => {
            eprintln!("Output file: {}", path.display());
            Box::new(File::create(path)?)
        }
        None => Box::new(std::io::stdout()),
    };

    let input_file = File::open(&args.input)?;
    let reader = BufReader::new(input_file);

    let mut wtr = csv::WriterBuilder::new()
        .quote_style(csv::QuoteStyle::NonNumeric)
        .from_writer(writer);

    // Write the updated header row to the CSV file.
    wtr.write_record([
        "time",
        "user",
        "host",
        "thread_id",
        "schema",
        "qc_hit",
        "set_timestamp",
        "use_schema",
        "query",
        "query_time",
        "lock_time",
        "rows_sent",
        "rows_examined",
        "rows_affected",
        "bytes_sent",
        "tmp_tables",
        "tmp_disk_tables",
        "tmp_table_sizes",
        "full_scan",
        "full_join",
        "tmp_table",
        "tmp_table_on_disk",
        "filesort",
        "filesort_on_disk",
        "merge_passes",
        "priority_queue",
    ])?;

    let mut current_entry = SlowQueryEntry::default();
    let mut last_seen_time = String::new();
    let mut entry_count = 0;

    for line_result in reader.lines() {
        let line = line_result?;

        if RE_SKIPPED_1.is_match(&line) || RE_SKIPPED_2.is_match(&line) {
            continue;
        }

        if let Some(caps) = RE_TIME.captures(&line) {
            let raw_time = caps.get(1).map_or("", |m| m.as_str()).trim();
            last_seen_time = format_log_time(raw_time).unwrap_or_else(|_| raw_time.to_string());
        } else if let Some(caps) = RE_USER_HOST.captures(&line) {
            if current_entry.is_valid() {
                current_entry.write_to_csv(&mut wtr)?;
                entry_count += 1;
            }
            current_entry = SlowQueryEntry {
                time: last_seen_time.clone(),
                ..Default::default()
            };
            let user_full = caps.get(1).map_or("", |m| m.as_str()).trim();
            current_entry.user = user_full.split('[').next().unwrap_or("").to_string();
            let host_full = caps.get(2).map_or("", |m| m.as_str()).trim();
            current_entry.host = host_full
                .trim_matches(|c| c == '[' || c == ']' || c == ' ')
                .to_string();
        } else if let Some(caps) = RE_METADATA_1.captures(&line) {
            current_entry.thread_id = caps.get(1).map_or("", |m| m.as_str()).to_string();
            current_entry.schema = caps.get(2).map_or("", |m| m.as_str()).trim().to_string();
            current_entry.qc_hit = caps.get(3).map_or("", |m| m.as_str()).trim().to_string();
        } else if let Some(caps) = RE_METADATA_2.captures(&line) {
            current_entry.query_time = caps
                .get(1)
                .map_or(0.0, |m| m.as_str().parse().unwrap_or(0.0));
            current_entry.lock_time = caps
                .get(2)
                .map_or(0.0, |m| m.as_str().parse().unwrap_or(0.0));
            current_entry.rows_sent = caps.get(3).map_or(0, |m| m.as_str().parse().unwrap_or(0));
            current_entry.rows_examined =
                caps.get(4).map_or(0, |m| m.as_str().parse().unwrap_or(0));
        } else if let Some(caps) = RE_METADATA_3.captures(&line) {
            current_entry.rows_affected =
                caps.get(1).map_or(0, |m| m.as_str().parse().unwrap_or(0));
            current_entry.bytes_sent = caps.get(2).map_or(0, |m| m.as_str().parse().unwrap_or(0));
        } else if let Some(caps) = RE_METADATA_4.captures(&line) {
            current_entry.tmp_tables = caps.get(1).map_or(0, |m| m.as_str().parse().unwrap_or(0));
            current_entry.tmp_disk_tables =
                caps.get(2).map_or(0, |m| m.as_str().parse().unwrap_or(0));
            current_entry.tmp_table_sizes =
                caps.get(3).map_or(0, |m| m.as_str().parse().unwrap_or(0));
        } else if let Some(caps) = RE_METADATA_5.captures(&line) {
            current_entry.full_scan = caps.get(1).map_or("", |m| m.as_str()).trim().to_string();
            current_entry.full_join = caps.get(2).map_or("", |m| m.as_str()).trim().to_string();
            current_entry.tmp_table = caps.get(3).map_or("", |m| m.as_str()).trim().to_string();
            current_entry.tmp_table_on_disk =
                caps.get(4).map_or("", |m| m.as_str()).trim().to_string();
        } else if let Some(caps) = RE_METADATA_6.captures(&line) {
            current_entry.filesort = caps.get(1).map_or("", |m| m.as_str()).trim().to_string();
            current_entry.filesort_on_disk =
                caps.get(2).map_or("", |m| m.as_str()).trim().to_string();
            current_entry.merge_passes = caps.get(3).map_or(0, |m| m.as_str().parse().unwrap_or(0));
            current_entry.priority_queue =
                caps.get(4).map_or("", |m| m.as_str()).trim().to_string();
        } else if !line.starts_with('#') && !line.trim().is_empty() {
            // Pre-allocate capacity for better performance
            if current_entry.query.is_empty() {
                current_entry.query.reserve(line.len() + 1);
            }
            current_entry.query.push_str(&line);
            current_entry.query.push('\n');
        }
    }

    if current_entry.is_valid() {
        current_entry.write_to_csv(&mut wtr)?;
        entry_count += 1;
    }

    wtr.flush()?;

    eprintln!("\nSuccess! Converted {entry_count} slow query entries.");

    Ok(())
}
