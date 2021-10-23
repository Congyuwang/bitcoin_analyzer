use std::fmt::{Display, Formatter};
use std::str::Utf8Error;

pub enum DBError {
    NotFoundErr(u32),
    UtfDecodeErr(u32, Utf8Error),
    QueryErr(u32, rocksdb::Error)
}

impl Display for DBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::NotFoundErr(i) => f.write_fmt(format_args!("key {}: not found", i)),
            DBError::UtfDecodeErr(i, e) => f.write_fmt(format_args!("key {}: value utf-8 decode error: {}", i, e)),
            DBError::QueryErr(i, e) => f.write_fmt(format_args!("key {}, db query error: {}", i, e)),
        }
    }
}
