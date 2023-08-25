use futures::stream::{FuturesUnordered, StreamExt};

use crate::persistence::{DatabaseClient, files_system::Dataset};
use crate::persistence::files_system::DatasetExt;



pub async fn run_transference(database_client: &DatabaseClient, dataset: &Dataset,
                              batch_size: u32, concurrent_batches_size: usize) -> anyhow::Result<()> {

    let mut batch_futures = FuturesUnordered::new();
       
    while let Some(batch) = dataset.next_batch(batch_size).await? {
        let batch_future = async {
            let result = database_client.insert_batch(batch).await;
            
            if let Err(error) = result {
                log::error!("An error occurred while inserting batch: {}", error);
            }
        };

        batch_futures.push(batch_future);

        if batch_futures.len() == concurrent_batches_size {
            batch_futures.next().await.unwrap();
        }
    }

    while let Some(_) = batch_futures.next().await {}

    Ok(())
}