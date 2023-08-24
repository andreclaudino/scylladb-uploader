use std::{path::Path};

use tokio::{io::{AsyncBufReadExt, BufReader, Lines}, fs::File, sync::RwLock};
use async_trait::async_trait;

use super::{file_type::FileType, dataset_ext::DatasetExt};

pub struct LocalDataset {
    lines: RwLock<Lines<BufReader<File>>>,
    file_type: FileType,
    csv_header: Option<String>,
}


impl LocalDataset {
    pub async fn new(source_path: &str, file_type: &FileType) -> anyhow::Result<Self> {
        match file_type {
            FileType::JSON => LocalDataset::load_json(source_path).await,
            FileType::CSV => LocalDataset::load_csv(source_path).await,
        }
    }

    async fn load_json(source_path: &str) -> anyhow::Result<Self> {
        let lines = open_local_file(source_path).await?;
        let lines_lock = RwLock::new(lines);
        
        let dataset = LocalDataset {
            lines: lines_lock, file_type: FileType::JSON,
            csv_header: None
        };

        Ok(dataset)
    }

    async fn load_csv(source_path: &str) -> anyhow::Result<Self> {
        let mut lines = open_local_file(source_path).await?;
        let csv_header = lines.next_line().await?;

        let lines_lock = RwLock::new(lines);
        let dataset = LocalDataset {
            lines: lines_lock,
            file_type: FileType::CSV,
            csv_header
        };

        Ok(dataset)
    }    
}


#[async_trait]
impl DatasetExt for LocalDataset {
    
    type DatasetType = Self;

    async fn next_line(&self) -> anyhow::Result<Option<serde_json::Value>> {
        let mut unlocked_lines = self.lines.write().await;
        
        if let Some(current_line) = unlocked_lines.next_line().await? {
            match self.file_type {
                FileType::JSON => {        
                    let value = serde_json::from_str(&current_line)?;
                    Ok(value)
                },
                FileType::CSV => {
                    let csv_line_with_header = self.csv_header.clone().unwrap() + "\n" + &current_line;
                    let mut csv_reader = csv::Reader::from_reader(csv_line_with_header.as_bytes());
                    let mut csv_iter = csv_reader.deserialize();

                    let value: serde_json::Value = csv_iter.next().unwrap()?;
                    Ok(Some(value))
                }
            }
        } else {
            Ok(None)
        }
    }

}


async fn open_local_file(source_path: &str) -> anyhow::Result<Lines<BufReader<File>>> {
    let path = Path::new(source_path);
    let file = File::open(path).await?;
    let reader = BufReader::new(file).lines();
    
    log::info!("Opening file {filename}", filename=source_path);

    Ok(reader)
}