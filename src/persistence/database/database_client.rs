use std::{cell::RefCell, collections::HashMap, sync::Arc};
use atomic_counter::{AtomicCounter, RelaxedCounter};
use scylla::{Session, SessionBuilder, prepared_statement::PreparedStatement};
use wg::AsyncWaitGroup;
use crate::entities::DataValue;


pub struct DatabaseClient {
    session: Arc<Session>,
    keyspace_name: String,
    table_name: String,

    total_batches: Arc<RelaxedCounter>,
    prepared_statement: RefCell<Option<PreparedStatement>>,
    field_names: RefCell<Vec<String>>,
    wait_group: RefCell<AsyncWaitGroup>,
}

impl DatabaseClient {
    
    pub async fn new(nodes_string: &str, username: &str, password: &str, keyspace_name: &str,  table_name: &str) -> anyhow::Result<DatabaseClient> {
        let nodes = nodes_string.split(",").map(|u| u.to_owned() ).collect();
        let session = make_session(username, password, nodes).await?;

        let database_client =
            DatabaseClient {
                session,
                keyspace_name: keyspace_name.to_owned(),
                table_name: table_name.to_owned(),
                total_batches: Arc::new(RelaxedCounter::new(0)),
                prepared_statement: RefCell::new(None),
                field_names: RefCell::new(Vec::new()),
                wait_group: RefCell::new(AsyncWaitGroup::new()),
            };

        Ok(database_client)
    }

    pub async fn insert_batch(&self, batch: Vec<serde_json::Value>) -> anyhow::Result<()> {
        if self.prepared_statement.borrow().is_none() {
            self.update_prepared_statement_and_field_names(&batch).await?;
        }
        
        let preapared_statement = self.prepared_statement.borrow().clone().unwrap();
        let session = self.session.clone();

        let upload_batch_task = upload_batch(session, batch, preapared_statement, self.wait_group.borrow().clone(), self.total_batches.clone());

        self.wait_group.borrow().add(1);
        tokio::spawn(upload_batch_task);
        
        Ok(())
    }

    async fn update_prepared_statement_and_field_names(&self, batch: &Vec<serde_json::Value>) -> anyhow::Result<()> {
        if let Some(first_batch_element) = batch.first() {
            if let Some(sample_line) = first_batch_element.as_object() {
                let field_names = sample_line.keys().map(|field_name| field_name.to_owned()).collect::<Vec<_>>();
                let prepared_statement =
                    make_prepared_statement(&self.session, &self.keyspace_name, &self.table_name, &field_names[..])
                    .await?;
                
                self.prepared_statement.replace_with(|_| Some(prepared_statement));
                self.field_names.replace_with(|_| field_names);
            }    
        }

        Ok(())
    }

    pub async fn wait(&self) -> () {
        self.wait_group.borrow().wait().await;
    }
}


async fn make_session(username: &str, password: &str, nodes: Vec<String>) -> anyhow::Result<Arc<Session>> {
    let session =
        SessionBuilder::new()
            .known_nodes(nodes)
            .user(username, password)
            .build()
            .await?;

    Ok(Arc::new(session))
}


async fn make_prepared_statement(session: &Session, keyspace_name: &str, table_name: &str, field_names: &[String]) -> anyhow::Result<PreparedStatement> {
    let placeholders = field_names.iter().map(|field_name| format!(":{field_name}")).collect::<Vec<_>>().join(", ");
    let field_names_string = field_names.join(", ");

    let query = format!("INSERT INTO {keyspace_name}.{table_name} ({field_names_string}) VALUES ({placeholders})");
    let prepared =
        session
            .prepare(query)
            .await?;

    Ok(prepared)
}

async fn upload_batch(session: Arc<Session>, batch: Vec<serde_json::Value>, preapared_statement: PreparedStatement, wait_group: AsyncWaitGroup, total_batches: Arc<RelaxedCounter>) -> Result<(), anyhow::Error> {

    for serde_values in batch.iter() {
        let values: HashMap<String, DataValue> = DataValue::new(serde_values.to_owned()).into();
        session.execute(&preapared_statement, &values).await?;
    }

    wait_group.done();
    total_batches.inc();

    log::info!("Batch #{batch_id} uploaded", batch_id=total_batches.get());

    Ok(())
}