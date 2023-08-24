use std::{borrow::Cow, sync::Arc};

use async_trait::async_trait;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::{Credentials, Region, types::ByteStream};
use tokio::io::Lines;
use tokio::{sync::RwLock};
use tokio_util::io::StreamReader;
use url::Url;
use tokio::io::BufReader;
use tokio::io::AsyncBufReadExt;

use super::{file_type::FileType, dataset_ext::DatasetExt};

pub struct S3Dataset {
    lines: Arc<RwLock<Lines<tokio::io::BufReader<StreamReader<ByteStream, bytes::Bytes>>>>>,
    file_type: FileType,
    csv_header: Option<String>,
}

impl S3Dataset {
    pub async fn new(source_path: &str, file_type: &FileType,
                    access_key: Option<String>, secret_key: Option<String>, region_name: Option<String>, endpoint_url: Option<String>) -> anyhow::Result<Self> {
        
        let (bucket, key) = split_bucket_and_key(source_path)?;
        let s3_config = make_s3_config(access_key, secret_key, region_name, endpoint_url);
        let s3_client = make_s3_client(s3_config)?;

        match file_type {
            FileType::JSON => S3Dataset::load_json(&bucket, &key, &s3_client).await,
            FileType::CSV => S3Dataset::load_csv(source_path, &key, &s3_client).await,
        }
    }

    async fn load_json(bucket_name: &str, key: &str, s3_client: &aws_sdk_s3::Client) -> anyhow::Result<Self> {
        let lines = open_s3_file(bucket_name, key, s3_client).await?;
        let lines_lock = Arc::new(RwLock::new(lines));

        let dataset = Self {
            lines: lines_lock,
            file_type: FileType::JSON,
            csv_header: None
        };

        Ok(dataset)
    }

    async fn load_csv(bucket_name: &str, key: &str, s3_client: &aws_sdk_s3::Client) -> anyhow::Result<Self> {
        let mut lines = open_s3_file(bucket_name, key, s3_client).await?;
        let csv_header = lines.next_line().await?;
        
        let lines_lock = Arc::new(RwLock::new(lines));

        let database = S3Dataset {
            file_type: FileType::CSV,
            lines: lines_lock,
            csv_header
        };

        Ok(database)
    }

}

#[async_trait]
impl DatasetExt for S3Dataset {
    
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

fn make_s3_client(s3_config: aws_sdk_s3::Config) -> anyhow::Result<aws_sdk_s3::Client> { 
    let client = aws_sdk_s3::Client::from_conf(s3_config);
    Ok(client)
}

fn make_s3_config(access_key: Option<String>, secret_key: Option<String>, region_name: Option<String>, endpoint_url: Option<String>) -> aws_sdk_s3::Config {
    let credentials = Credentials::new(
        access_key.unwrap_or(String::new()),
        secret_key.unwrap_or(String::new()),
        None,
        None,
        "InternalProvider"
    );
    
    let credential_provider = SharedCredentialsProvider::new(credentials);
    let region_name_cow = region_name.map(|region_name_| Cow::Owned(region_name_.to_owned()));
    let region = region_name_cow.map(|region_name_cow_| Region::new(region_name_cow_));
    
    let mut s3_config_builder = aws_sdk_s3::Config::builder().region(region);
    
    s3_config_builder.set_force_path_style(Some(true));
    s3_config_builder.set_endpoint_url(endpoint_url);
    s3_config_builder.set_credentials_provider(Some(credential_provider));

    let s3_config = s3_config_builder.build();

    s3_config
}


fn split_bucket_and_key(source_path: &str) -> anyhow::Result<(String, String)> {
    let url = Url::parse(source_path)?;
    if let Some(bucket) = url.host_str() {
        let encoded_key = url.path().strip_prefix("/").unwrap_or("");
        let key = urldecode::decode(encoded_key.to_string());
        Ok((bucket.to_owned(), key.to_owned()))
    } else {
        anyhow::bail!("Invalid source-path: {source_path}")
    }
    
}

async fn open_s3_file(bucket: &str, key: &str, s3_client: &aws_sdk_s3::Client) -> anyhow::Result<Lines<tokio::io::BufReader<StreamReader<ByteStream, bytes::Bytes>>>> {   
    let stream = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?
        .body;

    // Convert the stream into an AsyncRead
    let stream_reader = StreamReader::new(stream);
    let buff_reader = BufReader::new(stream_reader).lines();

    log::info!("Opening file s3://{bucket}/{filename}", bucket=bucket, filename=key);

    Ok(buff_reader)
}