use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use pulsar::{
    producer, Error as PulsarError, SerializeMessage, Payload, DeserializeMessage
};
use core::marker;

#[derive(Serialize, Deserialize)]
pub struct PulsarMessageWrapper<'a, T> {
    pub request_id: String,
    pub session_id: String,
    pub initiating_service: String,
    pub node: String,
    pub user_id: String,
    pub headers: HashMap<String, String>,
    pub data: T,
    _marker: marker::PhantomData<&'a T>,
}

impl<'a, T: Serialize> SerializeMessage for PulsarMessageWrapper<'a, T> {
    fn serialize_message(input: Self) -> Result<producer::Message, PulsarError> {
        let payload = serde_json::to_vec(&input).map_err(|e| PulsarError::Custom(e.to_string()))?;
        Ok(producer::Message {
            payload,
            ..Default::default()
        })
    }
}

impl<'de: 'a, 'a, T> DeserializeMessage for PulsarMessageWrapper<'a, T>
    where
        T: Deserialize<'de> {
    type Output = Result<PulsarMessageWrapper<'a, T>, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}