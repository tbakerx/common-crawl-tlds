use anyhow::{Context, Result};
use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use flate2::read::MultiGzDecoder;
use futures::future;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, AsyncArrowWriter},
    basic::Compression,
    file::properties::WriterProperties,
};
use std::{
    collections::HashSet,
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
struct DomainInfo {
    domain: String,
    tld: String,
}

struct BatchWriter {
    batch_dir: PathBuf,
    max_batch_size: usize,
    current_domains: HashSet<DomainInfo>, // Changed back to HashSet for immediate deduplication
    current_batch: usize,
    total_domains: usize,
}

impl BatchWriter {
    fn new(batch_dir: PathBuf, max_batch_size: usize) -> Self {
        Self {
            batch_dir,
            max_batch_size,
            current_domains: HashSet::with_capacity(max_batch_size),
            current_batch: 0,
            total_domains: 0,
        }
    }

    async fn add_domain(&mut self, domain: DomainInfo) -> Result<bool> {
        if self.current_domains.insert(domain) {
            self.total_domains += 1;
        }

        if self.current_domains.len() >= self.max_batch_size {
            self.write_batch().await?;
            return Ok(true);
        }
        Ok(false)
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

        // Convert to sorted Vec for consistent output
        let mut domains: Vec<_> = self.current_domains.iter().collect();
        domains.sort_unstable_by(|a, b| a.domain.cmp(&b.domain).then(a.tld.cmp(&b.tld)));

        println!(
            "Writing batch {} with {} domains",
            self.current_batch - 1,
            domains.len()
        );

        let (domain_vec, tld_vec): (Vec<_>, Vec<_>) = domains
            .iter()
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

        // Clear the domains after successful write
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
        // Create directories if they don't exist
        tokio::fs::create_dir_all(&work_dir).await?;

        let batch_dir = work_dir.join("batches");
        tokio::fs::create_dir_all(&batch_dir).await?;

        Ok(Self {
            client: reqwest::Client::builder()
                .pool_idle_timeout(Some(std::time::Duration::from_secs(30)))
                .build()
                .unwrap(),
            batch_writer: Arc::new(Mutex::new(BatchWriter::new(batch_dir, 1_000_000))),
            progress: MultiProgress::new(),
            work_dir,
        })
    }

    async fn process_parquet_files(&self, paths: Vec<String>) -> Result<()> {
        let total_pb = self.progress.add(ProgressBar::new(paths.len() as u64));
        total_pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} files ({eta}) - {msg}")
            .expect("Valid template")
            .progress_chars("#>-"));

        let chunk_size = 5;
        for (chunk_idx, chunk) in paths.chunks(chunk_size).enumerate() {
            let futures: Vec<_> = chunk
                .iter()
                .map(|path| {
                    let pb = self.progress.add(ProgressBar::new_spinner());
                    self.process_single_file(path.clone(), pb)
                })
                .collect();

            future::join_all(futures).await;
            total_pb.inc(chunk.len() as u64);

            let writer = self.batch_writer.lock().await;
            total_pb.set_message(format!(
                "Processed {}/{} chunks - {} domains in {} batches",
                chunk_idx + 1,
                (paths.len() + chunk_size - 1) / chunk_size,
                writer.total_domains,
                writer.current_batch
            ));
        }

        // Ensure final batch is written
        self.batch_writer.lock().await.write_batch().await?;

        let writer = self.batch_writer.lock().await;
        total_pb.finish_with_message(format!(
            "Processing complete - {} domains in {} batches",
            writer.total_domains, writer.current_batch
        ));

        self.merge_batches().await?;
        Ok(())
    }

    async fn process_single_file(&self, path: String, pb: ProgressBar) -> Result<()> {
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .expect("Valid template"),
        );

        pb.set_message(format!("Downloading {}", path));

        let temp_path = self
            .work_dir
            .join(format!("temp_{}.parquet", path.replace('/', "_")));

        let response = self
            .client
            .get(&format!("https://data.commoncrawl.org/{}", path))
            .send()
            .await
            .context("Failed to download file")?;

        let bytes = response
            .bytes()
            .await
            .context("Failed to read response bytes")?;

        tokio::fs::write(&temp_path, bytes)
            .await
            .context("Failed to write temporary file")?;

        pb.set_message(format!("Processing {}", path));

        let file = tokio::fs::File::open(&temp_path).await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file.into_std().await)?.build()?;

        let mut batch_count = 0;
        for batch_result in reader {
            let record_batch = batch_result?;

            if let (Some(domains), Some(tlds)) = (
                record_batch
                    .column_by_name("url_host_registered_domain")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
                record_batch
                    .column_by_name("url_host_private_suffix")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>()),
            ) {
                for (domain, tld) in domains.iter().zip(tlds.iter()) {
                    if let (Some(domain), Some(tld)) = (domain, tld) {
                        if !domain.is_empty() && !tld.is_empty() {
                            let mut writer = self.batch_writer.lock().await;
                            writer
                                .add_domain(DomainInfo {
                                    domain: domain.to_string(),
                                    tld: tld.to_string(),
                                })
                                .await?;
                        }
                    }
                }
            }

            batch_count += 1;
            let writer = self.batch_writer.lock().await;
            pb.set_message(format!(
                "Processing {} - {} record batches, {} domains in {} batches",
                path, batch_count, writer.total_domains, writer.current_batch
            ));
        }

        tokio::fs::remove_file(temp_path).await?;
        pb.finish_and_clear();
        Ok(())
    }

    async fn merge_batches(&self) -> Result<()> {
        println!("Merging batch files into final output...");

        let schema = Schema::new(vec![
            Field::new("domain", DataType::Utf8, false),
            Field::new("tld", DataType::Utf8, false),
        ]);

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(1024 * 1024)
            .build();

        let output_file = tokio::fs::File::create("unique_domains.parquet").await?;
        let mut writer = AsyncArrowWriter::try_new(output_file, Arc::new(schema), Some(props))?;

        let batch_dir = self.work_dir.join("batches");
        let mut entries = tokio::fs::read_dir(&batch_dir).await?;

        let progress = ProgressBar::new_spinner();
        progress.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} Merging batch {msg}")
                .expect("Valid template"),
        );

        let mut merged_count = 0;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                merged_count += 1;
                progress.set_message(format!("{}", merged_count));

                let file = tokio::fs::File::open(&path).await?;
                let reader =
                    ParquetRecordBatchReaderBuilder::try_new(file.into_std().await)?.build()?;

                for batch_result in reader {
                    writer.write(&batch_result?).await?;
                }
            }
        }

        writer.close().await?;
        progress.finish_with_message(format!("Merged {} batch files", merged_count));
        println!("Final parquet file created successfully!");
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
    let crawl_id = "CC-MAIN-2024-46";
    let paths = get_parquet_paths(crawl_id).await?;
    println!("Found {} parquet files", paths.len());

    // Create a work directory in the current directory
    let work_dir = PathBuf::from("crawl_data");
    let processor = ParquetProcessor::new(work_dir).await?;
    processor.process_parquet_files(paths).await?;
    Ok(())
}
