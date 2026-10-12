use std::io;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn encode<T>(message: &T) -> io::Result<Vec<u8>>
where
    T: Serialize,
{
    let mut bytes = serde_json::to_vec(message).map_err(io::Error::other)?;
    bytes.push(b'\n');

    Ok(bytes)
}

pub fn decode<T>(bytes: &[u8]) -> io::Result<T>
where
    T: DeserializeOwned,
{
    serde_json::from_slice(bytes).map_err(io::Error::other)
}
