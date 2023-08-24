use std::{cell::RefCell, collections::HashMap};
use scylla::{Session, SessionBuilder, prepared_statement::PreparedStatement, batch::Batch};

use crate::entities::DataValue;


pub struct DatabaseClient {
    session: Session,
    keyspace_name: String,
    table_name: String,

    prepared_statement: RefCell<Option<PreparedStatement>>,
    field_names: RefCell<Vec<String>>
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
                prepared_statement: RefCell::new(None),
                field_names: RefCell::new(Vec::new()),
            };

        Ok(database_client)
    }

    pub async fn insert_batch(&self, batch: Vec<serde_json::Value>) -> anyhow::Result<()> {
        if self.prepared_statement.borrow().is_none() {
            self.update_prepared_statement_and_field_names(&batch).await?;
        }
        
        let mut command_batch = Batch::default();

        let preapared_statement = self.prepared_statement.borrow().clone().unwrap();
        let mut values_batch: Vec<HashMap<String, DataValue>> = Vec::new();

        for serde_values in batch.iter() {
            command_batch.append_statement(preapared_statement.clone());
            let values: HashMap<String, DataValue> = DataValue::new(serde_values.to_owned()).into();
            values_batch.push(values);

            // self.session.execute(&preapared_statement, &values).await?;
        }
        
        // let values: Vec<HashMap<String, DataValue>> = batch.iter().map(|serde_value| DataValue::new(serde_value.to_owned()).into()).collect();
        self.session.batch(&command_batch, &values_batch[..]).await?;

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

}


async fn make_session(username: &str, password: &str, nodes: Vec<String>) -> anyhow::Result<Session> {
    let session =
        SessionBuilder::new()
            .known_nodes(nodes)
            .user(username, password)
            .build()
            .await?;

    Ok(session)
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