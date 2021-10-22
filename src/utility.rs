use std::fs::File;
use crossbeam::channel;
use crossbeam::channel::Sender;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

///
///  destroy arc mutex
///
pub(crate) fn unwrap_mutex<T: std::fmt::Debug>(t: Arc<Mutex<T>>) -> Arc<T> {
    let inner = Arc::try_unwrap(t).unwrap();
    Arc::new(inner.into_inner().unwrap())
}

///
/// Zero-runtime Asynchronous Buffered Writer
///
#[derive(Clone)]
pub(crate) struct AsyncBufWriter {
    worker: Arc<Mutex<Option<JoinHandle<()>>>>,
    sender: Sender<Box<[u8]>>,
}

impl AsyncBufWriter {
    pub(crate) fn new(writer: File) -> AsyncBufWriter {
        let (sender, receiver) = channel::unbounded::<Box<[u8]>>();
        let mut writer = BufWriter::with_capacity(128 * 1024 * 1024, writer);
        let worker = thread::spawn(move || {
            for line in receiver.into_iter() {
                writer.write_all(&line).unwrap();
            }
            writer.flush().unwrap();
        });
        AsyncBufWriter {
            worker: Arc::new(Mutex::new(Some(worker))),
            sender,
        }
    }

    pub(crate) fn write_all(&self, buf: Box<[u8]>) {
        self.sender.send(buf).unwrap();
    }

    // join the writer thread back to main thread
    pub(crate) fn pop_handle(&mut self) -> Option<JoinHandle<()>> {
        self.worker.lock().unwrap().take()
    }
}

///
/// compute number of trailing zeros for u64
///
pub(crate) fn trailing_zero(value: u64) -> u8 {
    if value == 0 {
        return 0;
    }
    let mut value = value;
    let mut count = 0u8;
    while value % 10 == 0 {
        count += 1;
        value = value / 10;
    }
    count
}

#[test]
fn test_trailing_zero() {
    assert_eq!(trailing_zero(120), 1u8);
    assert_eq!(trailing_zero(99999999990), 1u8);
    assert_eq!(trailing_zero(9000000000000000000), 18u8);
    assert_eq!(trailing_zero(0), 0u8);
    assert_eq!(trailing_zero(1200), 2u8);
}
