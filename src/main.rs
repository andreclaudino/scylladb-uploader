use clap::Parser;
use persistence::files_system::Dataset;
use processors::run_transference;
use crate::{command_line::CommandLine, persistence::DatabaseClient};

mod persistence;
mod command_line;
mod processors;
mod entities;


#[tokio::main(flavor="current_thread")]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let arguments = CommandLine::parse();
    
    let database_client =
        DatabaseClient::new(&arguments.database_nodes, &arguments.database_username, &arguments.database_password, &arguments.database_keyspace_name, &arguments.database_table).await?;

    let dataset = Dataset::load(&arguments.source_path, &arguments.source_file_type, arguments.s3_access_key, arguments.s3_secret_access_key,
                                        arguments.s3_region, arguments.s3_endpoint).await?;

    run_transference(&database_client, &dataset, arguments.batch_size).await?;

    Ok(())
}
