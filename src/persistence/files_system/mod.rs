use self::{local_dataset::LocalDataset, s3_dataset::S3Dataset};

mod dataset_ext;
mod file_type;
mod local_dataset;
mod s3_dataset;

use async_trait::async_trait;
pub use file_type::FileType;
pub use dataset_ext::DatasetExt;


pub enum Dataset {
    S3(S3Dataset),
    Local(LocalDataset),
}

impl Dataset {
    pub async fn load(source_path: &str, file_type: &FileType, access_key: Option<String>,
        secret_key: Option<String>, region: Option<String>, endpoint: Option<String>) -> anyhow::Result<Dataset> {
        let dataset = if source_path.starts_with("s3://") || source_path.starts_with("s3a://") {
            let dataset = S3Dataset::new(source_path, file_type, access_key,
                secret_key, region, endpoint).await?;
            Dataset::S3(dataset)
        } else {
            let dataset = LocalDataset::new(source_path, file_type).await?;
            Dataset::Local(dataset)
        };

        Ok(dataset)
    }
}


#[async_trait]
impl DatasetExt for Dataset {
    type DatasetType = Dataset;

    async fn next_line(&self) -> anyhow::Result<Option<serde_json::Value>> {
        match self {
            Dataset::S3(dataset) => dataset.next_line().await,
            Dataset::Local(dataset) => dataset.next_line().await
        }
    }
}
