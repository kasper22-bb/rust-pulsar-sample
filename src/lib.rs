use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use pulsar::{
    producer, Error as PulsarError, SerializeMessage, Payload, DeserializeMessage
};
use core::marker;

#[derive(Serialize, Deserialize)]
pub struct PulsarMessageWrapper<T> {
    pub request_id: String,
    pub data: T,
}

impl<T: Serialize> SerializeMessage for PulsarMessageWrapper<T> {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl<'de: 'a, 'a, T> DeserializeMessage for PulsarMessageWrapper<T>
    where
        T: Deserialize<'de> {
    type Output = Result<PulsarMessageWrapper<T>, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}