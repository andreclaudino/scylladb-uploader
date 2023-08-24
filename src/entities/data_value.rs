use std::collections::HashMap;
use scylla::_macro_internal::Value as ScylaValue;
use scylla::_macro_internal::ValueTooBig;
use scylla::frame::value::Unset;
use serde_json::Value as SerdeValue;

pub struct DataValue(SerdeValue);

impl DataValue {
    pub fn new(serde_value: SerdeValue) -> DataValue { DataValue(serde_value) }
}


impl ScylaValue for DataValue {
    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), ValueTooBig> {
        let json_value = self.into();

        match json_value {
            SerdeValue::String(string_value) => {
                string_value.serialize(buf)?;
            }
            SerdeValue::Null => {
                let unset = Unset;
                unset.serialize(buf)?;
            },
            SerdeValue::Bool(bool_value) => {
                bool_value.serialize(buf)?;
            },
            SerdeValue::Number(value_number) => {               
                if value_number.is_u64() {
                    let value = value_number.as_u64().unwrap() as i64;
                    value.serialize(buf)?;
                } else if value_number.is_i64() {
                    let value = value_number.as_i64().unwrap();
                    value.serialize(buf)?;
                } else if value_number.is_f64() {
                    let value = value_number.as_f64().unwrap();
                    value.serialize(buf)?;
                }
            },
            SerdeValue::Array(value_array) => {
                let data_value_array: Vec<_> = value_array.iter().map(|item| DataValue(item.clone())).collect();
                data_value_array.serialize(buf)?;
            },
            SerdeValue::Object(value_map) => {
                let hash_map: HashMap<String, DataValue> = value_map.iter().map(|(key, value)| {
                    let data_value = DataValue(value.to_owned());
                    (key.to_owned(), data_value)
                }).collect();
                
                hash_map.serialize(buf)?;
            },
        };
        
        Ok(())
    }
}

impl From<serde_json::Value> for DataValue {
    fn from(value: serde_json::Value) -> Self {
        DataValue(value)
    }
}

impl Into<serde_json::Value> for &DataValue {
    fn into(self) -> serde_json::Value {
        self.0.clone()
    }
}


impl Into<HashMap<String, DataValue>> for DataValue {

    fn into(self) -> HashMap<String, DataValue> {
        let mut serialized_value = HashMap::new();

        if let Some(serde_json_map) = self.0.as_object() {
            for (name, value) in serde_json_map {
                let data_value = DataValue(value.to_owned());
                serialized_value.insert(name.to_owned(), data_value);
            }
        }

        serialized_value
    }
}

