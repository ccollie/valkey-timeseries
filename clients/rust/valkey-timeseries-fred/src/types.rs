use fred::prelude::{RedisError, RedisValue};

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub timestamp: i64,
    pub value: f64,
}

pub fn parse_i64(value: RedisValue) -> Result<i64, RedisError> {
    value.convert()
}

pub fn parse_status_ok(value: RedisValue) -> Result<(), RedisError> {
    let status: String = value.convert()?;
    if status.eq_ignore_ascii_case("OK") {
        Ok(())
    } else {
        Err(RedisError::new(
            fred::error::RedisErrorKind::Protocol,
            format!("expected OK reply, got {status}"),
        ))
    }
}

pub fn parse_sample(value: RedisValue) -> Result<Option<Sample>, RedisError> {
    let tuple: Option<(i64, f64)> = value.convert()?;
    Ok(tuple.map(|(timestamp, value)| Sample { timestamp, value }))
}

pub fn parse_samples(value: RedisValue) -> Result<Vec<Sample>, RedisError> {
    let tuples: Vec<(i64, f64)> = value.convert()?;
    Ok(tuples
        .into_iter()
        .map(|(timestamp, value)| Sample { timestamp, value })
        .collect())
}
