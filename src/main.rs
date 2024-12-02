use anyhow::Result;
use flate2::bufread::MultiGzDecoder;
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use reqwest::Client;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufRead, BufReader, Write},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
use url::Url;

struct CdxProcessor {
    client: Client,
    domains: Arc<Mutex<HashSet<String>>>,
    urls_processed: Arc<Mutex<usize>>,
}

impl CdxProcessor {
    fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(Duration::from_secs(3600))
                .build()
                .expect("Failed to create HTTP client"),
            domains: Arc::new(Mutex::new(HashSet::new())),
            urls_processed: Arc::new(Mutex::new(0)),
        }
    }

    async fn process_index_files(&self, paths: Vec<String>, concurrent_files: usize) -> Result<()> {
        let m = MultiProgress::new();
        let total_pb = m.add(ProgressBar::new(paths.len() as u64));
        total_pb.set_style(ProgressStyle::default_bar()
            .template("{prefix:.bold.dim} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} files ({per_sec}, {eta})")?);
        total_pb.set_prefix("Total Progress");

        let mut tasks = FuturesUnordered::new();
        for paths_chunk in paths.chunks(concurrent_files) {
            for path in paths_chunk {
                let path = path.clone();
                let client = self.client.clone();
                let domains = Arc::clone(&self.domains);
                let urls_processed = Arc::clone(&self.urls_processed);
                let pb = m.add(ProgressBar::new_spinner());
                pb.set_style(ProgressStyle::default_spinner().template(
                    "{spinner:.green} [{elapsed_precise}] {prefix}: {msg} ({per_sec})",
                )?);

                tasks.push(tokio::spawn(async move {
                    let url = format!("https://data.commoncrawl.org/{}", &path);
                    pb.set_prefix(path);

                    let response = client.get(&url).send().await?;
                    let reader = BufReader::new(MultiGzDecoder::new(BufReader::new(
                        std::io::Cursor::new(response.bytes().await?),
                    )));

                    let chunk_size = 100_000;
                    let mut lines = Vec::with_capacity(chunk_size);
                    let mut current_domains = HashSet::new();

                    for line in reader.lines() {
                        let line = line?;
                        lines.push(line);

                        if lines.len() >= chunk_size {
                            let extracted = process_cdx_chunk(&lines);
                            current_domains.extend(extracted);
                            lines.clear();

                            let mut count = urls_processed.lock().await;
                            *count += chunk_size;
                            pb.set_message(format!(
                                "{} URLs, {} domains",
                                *count,
                                current_domains.len()
                            ));
                        }
                    }

                    if !lines.is_empty() {
                        let extracted = process_cdx_chunk(&lines);
                        current_domains.extend(extracted);
                    }

                    let mut domains_lock = domains.lock().await;
                    domains_lock.extend(current_domains);

                    // Save intermediate results
                    let filename = format!("domains_batch_{}.txt", chrono::Utc::now().timestamp());
                    let mut file = File::create(&filename)?;
                    for domain in domains_lock.iter() {
                        writeln!(file, "{}", domain)?;
                    }

                    pb.finish_with_message(format!("Complete - {} domains", domains_lock.len()));
                    Ok::<(), anyhow::Error>(())
                }));
            }

            while let Some(result) = tasks.next().await {
                total_pb.inc(1);
                result??;
            }
        }

        total_pb.finish();
        Ok(())
    }
}

fn process_cdx_chunk(lines: &[String]) -> HashSet<String> {
    lines
        .par_iter()
        .filter_map(|line| {
            line.split_whitespace()
                .next() // Get first field (SURT domain)
                .map(|surt| {
                    // Remove path component and convert SURT to normal domain
                    let domain = surt
                        .split(')')
                        .next()?
                        .replace(',', ".")
                        .split('.')
                        .rev()
                        .collect::<Vec<_>>()
                        .join(".");
                    Some(domain)
                })
                .flatten()
        })
        .collect()
}

async fn get_index_paths(crawl_id: &str) -> Result<Vec<String>> {
    let url = format!(
        "https://data.commoncrawl.org/crawl-data/{}/cc-index.paths.gz",
        crawl_id
    );

    let response = reqwest::get(&url).await?;
    let reader = BufReader::new(MultiGzDecoder::new(BufReader::new(std::io::Cursor::new(
        response.bytes().await?,
    ))));

    Ok(reader.lines().filter_map(Result::ok).collect())
}

#[tokio::main]
async fn main() -> Result<()> {
    let crawl_id = "CC-MAIN-2024-46";
    let paths = get_index_paths(crawl_id).await?;
    println!("Found {} index files", paths.len());

    let processor = CdxProcessor::new();
    processor
        .process_index_files(paths, num_cpus::get().min(10))
        .await
}
