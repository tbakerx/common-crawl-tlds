use anyhow::{Context, Result};
use duckdb::Connection;
use flate2::read::MultiGzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

struct DomainProcessor {
    conn: Connection,
}

impl DomainProcessor {
    fn new() -> Result<Self> {
        let conn = Connection::open(":memory:")?;
        conn.execute_batch(
            "INSTALL httpfs;
                LOAD httpfs;
                SET threads TO 0;
                SET memory_limit='12GB';",
        )?;
        Ok(Self { conn })
    }

    async fn process_parquet_files(&self, paths: &[String], output_path: &Path) -> Result<()> {
        let pb = ProgressBar::new(paths.len() as u64);
        pb.set_style(ProgressStyle::default_bar().template(
            "{spinner:.green} [{elapsed_precise}] [{bar:50.cyan/blue}] {pos}/{len} ({eta})",
        )?);

        // Create output table
        self.conn.execute_batch(
            "CREATE TABLE domains (domain VARCHAR, tld VARCHAR);
             CREATE UNIQUE INDEX domain_idx ON domains(domain, tld);",
        )?;

        for path in paths {
            let file = format!("https://data.commoncrawl.org/{}", path);
            self.conn.execute_batch(&format!(
                "INSERT OR IGNORE INTO domains 
                     SELECT DISTINCT domain, tld FROM (
                         SELECT url_host_registered_domain as domain,
                                url_host_registry_suffix as tld 
                         FROM read_parquet('{}')
                         WHERE url_host_registered_domain IS NOT NULL 
                           AND url_host_registry_suffix IS NOT NULL
                           AND url_host_registered_domain != ''
                           AND url_host_registry_suffix != ''
                     )",
                &file
            ))?;

            pb.inc(1);
        }

        // Write final results
        println!("Writing results to {}...", output_path.display());
        self.conn.execute_batch(&format!(
            "COPY (
                SELECT * FROM domains ORDER BY domain, tld
             ) TO '{}' (FORMAT PARQUET)",
            output_path.display()
        ))?;

        Ok(())
    }
}

async fn get_parquet_paths(crawl_id: &str) -> Result<Vec<String>> {
    let url = format!(
        "https://data.commoncrawl.org/crawl-data/{}/cc-index-table.paths.gz",
        crawl_id
    );

    let response = reqwest::get(&url)
        .await
        .context("Failed to download paths file")?;

    let bytes = response
        .bytes()
        .await
        .context("Failed to read paths file")?;

    let reader = BufReader::new(MultiGzDecoder::new(std::io::Cursor::new(bytes)));

    Ok(reader
        .lines()
        .map_while(Result::ok)
        .filter(|path| path.ends_with(".parquet"))
        .collect())
}

#[tokio::main]
async fn main() -> Result<()> {
    let crawl_id = "CC-MAIN-2024-46";
    let processor = DomainProcessor::new()?;
    let paths = get_parquet_paths(crawl_id).await?;
    println!("Found {} parquet files", paths.len());

    let output_path = PathBuf::from("crawl_data/unique_domains.parquet");
    processor
        .process_parquet_files(&paths, &output_path)
        .await?;
    Ok(())
}
