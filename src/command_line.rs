use clap::Parser;

use crate::persistence::files_system::FileType;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about=None)]
pub struct CommandLine {
    /// Source path
    #[clap(long, short, env = "SOURCE_PATH")]
    pub source_path: String,

    /// Source file type
    #[clap(long, default_value = "json", env = "SOURCE_FILE_TYPE")]
    pub source_file_type: FileType,

    /// Database username
    #[clap(long, env = "DATABASE_USERNAME")]
    pub database_username: String,

    /// Database password
    #[clap(long, env = "DATABASE_PASSWORD")]
    pub database_password: String,

    /// Comma separated database nodes (host:port) list
    #[clap(long, env = "DATABASE_NODES")]
    pub database_nodes: String,

    /// Scylla Keyspace name
    #[clap(long, env = "DATABASE_KEYSPACE_NAME")]
    pub database_keyspace_name: String,

    /// Scylla table name
    #[clap(long, env = "DATABASE_TABLE")]
    pub database_table: String,

    /// Upload Batch size
    #[clap(long, env = "BATCH_SIZE")]
    pub batch_size: u32,

    /// Number of simultaneous batches to process simultaneously
    #[clap(long, env = "CONCURRENT_BATCHES")]
    pub concurrent_batches: usize,

    /// The S3 endpoint to connect and save file
    #[clap(long, env = "S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// S3 Access key
    #[clap(long, env = "S3_ACCESS_KEY")]
    pub s3_access_key: Option<String>,

    /// S3 Secret Access key
    #[clap(long, env = "S3_SECRET_ACCESS_KEY")]
    pub s3_secret_access_key: Option<String>,

    /// S3 Region to connect
    #[clap(long, default_value="minio", env = "S3_REGION")]
    pub s3_region: Option<String>,
}