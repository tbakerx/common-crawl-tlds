use anyhow::{Context, Result};
use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use dashmap::DashSet;
use flate2::read::MultiGzDecoder;
use futures::{StreamExt, TryStreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, AsyncArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use rayon::prelude::*;
use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;

#[derive(Clone, PartialEq, Eq, Hash)]
struct DomainInfo {
    domain: String,
    tld: String,
}

struct BatchWriter {
    batch_dir: PathBuf,
    max_batch_size: usize,
    current_domains: DashSet<DomainInfo>,
    current_batch: usize,
    total_domains: usize,
}

impl BatchWriter {
    async fn new(batch_dir: PathBuf, max_batch_size: usize) -> Result<Self> {
        Ok(Self {
            batch_dir,
            max_batch_size,
            current_domains: DashSet::with_capacity(max_batch_size),
            current_batch: 0,
            total_domains: 0,
        })
    }

    async fn add_domains(&mut self, domains: &[DomainInfo]) -> Result<()> {
        let mut write_batch = false;
        for domain in domains {
            if self.current_domains.insert(domain.clone()) {
                self.total_domains += 1;
                if self.current_domains.len() >= self.max_batch_size {
                    write_batch = true;
                    break;
                }
            }
        }
        if write_batch {
            self.write_batch().await?;
        }
        Ok(())
    }

    async fn write_batch(&mut self) -> Result<()> {
        if self.current_domains.is_empty() {
            return Ok(());
        }

        self.current_batch += 1;
        let batch_file = self
            .batch_dir
            .join(format!("batch_{}.parquet", self.current_batch - 1));

        let schema = Schema::new(vec![
            Field::new("domain", DataType::Utf8, false),
            Field::new("tld", DataType::Utf8, false),
        ]);

        let mut domains: Vec<_> = self
            .current_domains
            .iter()
            .map(|d| (*d.key()).clone())
            .collect();
        domains.par_sort_unstable_by(|a, b| a.domain.cmp(&b.domain).then(a.tld.cmp(&b.tld)));

        let (domain_vec, tld_vec): (Vec<_>, Vec<_>) = domains
            .par_iter()
            .map(|d| (d.domain.as_str(), d.tld.as_str()))
            .unzip();

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(StringArray::from(domain_vec)),
                Arc::new(StringArray::from(tld_vec)),
            ],
        )?;

        let file = tokio::fs::File::create(&batch_file).await?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(1024 * 1024)
            .build();

        let mut writer = AsyncArrowWriter::try_new(file, Arc::new(schema), Some(props))?;
        writer.write(&batch).await?;
        writer.close().await?;

        println!(
            "Wrote batch {} with {} domains",
            self.current_batch - 1,
            domains.len()
        );
        self.current_domains.clear();
        Ok(())
    }
}

struct ParquetProcessor {
    client: reqwest::Client,
    batch_writer: Arc<Mutex<BatchWriter>>,
    progress: MultiProgress,
    work_dir: PathBuf,
}

impl ParquetProcessor {
    async fn new(work_dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&work_dir).await?;
        let batch_dir = work_dir.join("batches");
        tokio::fs::create_dir_all(&batch_dir).await?;

        Ok(Self {
            client: reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(90))
                .pool_max_idle_per_host(0)
                .timeout(Duration::from_secs(300))
                .tcp_nodelay(true)
                .tcp_keepalive(Duration::from_secs(60))
                .build()
                .unwrap(),
            batch_writer: Arc::new(Mutex::new(BatchWriter::new(batch_dir, 1_000_000).await?)),
            progress: MultiProgress::new(),
            work_dir,
        })
    }

    async fn process_parquet_files(&self, paths: Vec<String>) -> Result<()> {
        let total_pb = self.progress.add(ProgressBar::new(paths.len() as u64));
        total_pb.set_style(
           ProgressStyle::default_bar()
               .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} files ({eta}) - {msg}")
               .expect("Valid template")
               .progress_chars("#>-"),
       );

        let chunk_size = 20;
        for paths_chunk in paths.chunks(chunk_size) {
            let downloads = futures::future::try_join_all(paths_chunk.iter().map(|path| async {
                let pb = self.progress.add(ProgressBar::new_spinner());
                let path = path.clone();
                let temp_path = self
                    .work_dir
                    .join(format!("temp_{}.parquet", path.replace('/', "_")));

                pb.set_message(format!("Downloading {}", path));

                let response = self
                    .client
                    .get(&format!("https://data.commoncrawl.org/{}", path))
                    .send()
                    .await?;

                let bytes = response.bytes().await?;

                tokio::fs::write(&temp_path, bytes).await?;

                Ok::<(String, PathBuf, ProgressBar), anyhow::Error>((path, temp_path, pb))
            }))
            .await?;

            let results: Vec<_> = downloads
                .into_iter()
                .map(|(path, temp_path, pb)| {
                    let batch_writer = self.batch_writer.clone();
                    async move {
                        pb.set_message(format!("Processing {}", path));
                        self.process_downloaded_file(&temp_path, &batch_writer, &pb)
                            .await?;
                        tokio::fs::remove_file(&temp_path).await?;
                        pb.finish_and_clear();
                        Ok::<(), anyhow::Error>(())
                    }
                })
                .collect();

            futures::future::try_join_all(results).await?;

            total_pb.inc(chunk_size as u64);

            let writer = self.batch_writer.lock().await;
            total_pb.set_message(format!(
                "Processed {}/{} chunks - {} domains in {} batches",
                total_pb.position() as usize / chunk_size + 1,
                (paths.len() + chunk_size - 1) / chunk_size,
                writer.total_domains,
                writer.current_batch
            ));
        }

        self.batch_writer.lock().await.write_batch().await?;

        let writer = self.batch_writer.lock().await;
        total_pb.finish_with_message(format!(
            "Processing complete - {} domains in {} batches",
            writer.total_domains, writer.current_batch
        ));

        self.merge_batches().await?;
        Ok(())
    }

    async fn process_downloaded_file(
        &self,
        temp_path: &PathBuf,
        batch_writer: &Arc<Mutex<BatchWriter>>,
        pb: &ProgressBar,
    ) -> Result<()> {
        let file = tokio::fs::File::open(temp_path).await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file.into_std().await)?.build()?;

        let mut batch_count = 0;
        let mut total_rows = 0;
        for batch_result in reader {
            let batch = batch_result?;
            self.process_record_batch(batch, batch_writer, &mut batch_count, &mut total_rows, pb)
                .await?;
        }

        Ok(())
    }

    async fn process_record_batch(
        &self,
        batch: RecordBatch,
        batch_writer: &Arc<Mutex<BatchWriter>>,
        batch_count: &mut usize,
        total_rows: &mut usize,
        pb: &ProgressBar,
    ) -> Result<()> {
        if let (Some(domains), Some(tlds)) = (
            batch
                .column_by_name("url_host_registered_domain")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            batch
                .column_by_name("url_host_private_suffix")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
        ) {
            let domains: Vec<_> = domains
                .iter()
                .zip(tlds.iter())
                .filter_map(|(domain, tld)| match (domain, tld) {
                    (Some(domain), Some(tld)) if !domain.is_empty() && !tld.is_empty() => {
                        Some(DomainInfo {
                            domain: domain.to_string(),
                            tld: tld.to_string(),
                        })
                    }
                    _ => None,
                })
                .collect();

            batch_writer.lock().await.add_domains(&domains).await?;

            *batch_count += 1;
            *total_rows += batch.num_rows();
            pb.set_message(format!(
                "Processed {} rows in {} batches - {} domains written",
                total_rows,
                batch_count,
                batch_writer.lock().await.total_domains
            ));
        }

        Ok(())
    }

    async fn merge_batches(&self) -> Result<()> {
        println!("Merging batch files into final output...");

        let schema = Schema::new(vec![
            Field::new("domain", DataType::Utf8, false),
            Field::new("tld", DataType::Utf8, false),
        ]);

        let output_file = tokio::fs::File::create("unique_domains.parquet").await?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(1024 * 1024)
            .build();

        let mut writer = AsyncArrowWriter::try_new(output_file, Arc::new(schema), Some(props))?;

        let batch_dir = self.work_dir.join("batches");
        let mut entries = Vec::new();
        let mut dir = tokio::fs::read_dir(&batch_dir).await?;
        while let Some(entry) = dir.next_entry().await? {
            entries.push(entry);
        }

        entries.par_sort_by_key(|entry| entry.path());

        let progress = ProgressBar::new(entries.len() as u64);
        progress.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} Merging batches [{wide_bar:.cyan/blue}] {pos}/{len}")
                .expect("Valid template"),
        );

        for chunk in entries.chunks(5) {
            for entry in chunk {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                    let file = tokio::fs::File::open(&path).await?;
                    let reader =
                        ParquetRecordBatchReaderBuilder::try_new(file.into_std().await)?.build()?;

                    for batch_result in reader {
                        writer.write(&batch_result?).await?;
                    }
                }
            }
            progress.inc(chunk.len() as u64);
        }

        writer.close().await?;
        progress.finish_with_message("Merge complete");
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
        .filter_map(Result::ok)
        .filter(|path| path.ends_with(".parquet"))
        .collect())
}

#[tokio::main]
async fn main() -> Result<()> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .build_global()
        .unwrap();

    let crawl_id = "CC-MAIN-2024-46";
    let paths = get_parquet_paths(crawl_id).await?;
    println!("Found {} parquet files", paths.len());

    let work_dir = PathBuf::from("crawl_data");
    let processor = ParquetProcessor::new(work_dir).await?;
    processor.process_parquet_files(paths).await?;
    Ok(())
}
