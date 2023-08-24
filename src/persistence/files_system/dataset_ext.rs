use async_trait::async_trait;

#[async_trait]
pub trait DatasetExt {
    type DatasetType: DatasetExt;

    async fn next_line(&self) -> anyhow::Result<Option<serde_json::Value>>;

    async fn next_batch(&self, batch_size: u32) -> anyhow::Result<Option<Vec<serde_json::Value>>> {
        let mut batch = Vec::new();

        for _ in 0..batch_size {
            let next_value = self.next_line().await?;
            
            if let Some(value) = next_value {
                batch.push(value);
            } else {
                break;
            }
        }
        
        if batch.is_empty() {
            Ok(None)
        } else {
            let batch_size = batch.len();
            log::info!("Total of {batch_size} elements loaded in batch (maximum is {batch_size})");
            Ok(Some(batch))
        }
    }

}