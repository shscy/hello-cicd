use arrow::array::{Array, GenericStringBuilder, PrimitiveBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::{Float64BufferBuilder, StringBuilder, UInt64BufferBuilder};
use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, Float64Array, Int32Array, StringArray, UInt64Array, UInt8Array};
use chrono::{Local, Utc};
use parking_lot::Mutex;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::hash::Hash;
use std::mem::ManuallyDrop;
use std::sync::atomic::AtomicU8;
use std::sync::Arc;

use super::s3::Uploader;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::thread_rng;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::channel;
use log::{debug, error, info, log_enabled, Level, warn};

#[repr(C)]
#[derive(Debug)]
pub struct Event {
    // 0 表示 trade 成交类型
    // 1 表示 BALANCE-CHANGE 事件
    // 2 表示 SETTLE-FEE 事件
    pub event_type: u8,
    pub account_id: u64,
    pub strategy_id: u64,
    pub coin: *const libc::c_char,
    pub amount: *const libc::c_char,
    // trade 的是 tradeId
    // BALANCE-CHANGE 表示 eventId
    // SETTLE-FEE 表示eventId
    pub trace_id: *const libc::c_char,
}

#[derive(Default, Debug)]
struct BatchWrite {
    event_type: PrimitiveBuilder<arrow_array::types::UInt8Type>,
    account_id: PrimitiveBuilder<arrow_array::types::UInt64Type>,
    strategy_id: PrimitiveBuilder<arrow_array::types::UInt64Type>,
    coin: GenericStringBuilder<i32>,
    amount: GenericStringBuilder<i32>,
    trace_id: GenericStringBuilder<i32>,
}

impl BatchWrite {
    fn append(&mut self, event: &Event) {
        use std::ffi::CStr;
        self.event_type.append_value(event.event_type);
        self.account_id.append_value(event.account_id);
        self.strategy_id.append_value(event.strategy_id);
        let coin = unsafe { CStr::from_ptr(event.coin).to_str().unwrap().to_owned() };
        //println!("append coin {}", coin.as_str());
        self.coin.append_value(coin);

        let amount = unsafe { CStr::from_ptr(event.amount).to_str().unwrap().to_owned() };
        //println!("append amount  {}", amount.as_str());
        self.amount.append_value(amount);
        let trace_id = unsafe { CStr::from_ptr(event.trace_id).to_str().unwrap().to_owned() };
        //println!("append trace {}", trace_id.as_str());
        self.trace_id.append_value(trace_id);
    }
}

unsafe impl Send for Db {}

unsafe impl Sync for Db {}

struct Writer {
    done: bool,
    event: *const Event,
    cond: Arc<parking_lot::Condvar>,
}

use std::sync::mpsc::{Sender, Receiver};
pub struct Db {
    mu: parking_lot::Mutex<()>,
    buffer: std::ptr::NonNull<VecDeque<*const Writer>>,
    cond: Arc<parking_lot::Condvar>,
    log_writer: std::ptr::NonNull<ParFile>,
    close_recv: Receiver<()>,
}

impl Db {
    pub fn init(root: &str) -> Self {        
        let buffer = unsafe {
            let buf = Box::into_raw(Box::new(VecDeque::new()));
            std::ptr::NonNull::new_unchecked(buf)
        };

        let (sender, rec) = channel();
        let par_file = ParFile::new(root, sender);
        let active_files = par_file.active_file_nams.clone();
       
        let (close_send, close_recv) = channel();
       
        let par_file = unsafe {
            let buf = Box::into_raw(Box::new(par_file));
            std::ptr::NonNull::new_unchecked(buf)
        };

        std::thread::spawn(move || {
            loop {
                {
                    if let Ok(Some(v)) = rec.recv() {
                        let mut failed = Vec::new();
                        loop {
                            let mut guand = active_files.lock();
                            if (*guand).len() == 1 {
                                break;
                            }

                            let file_name = (*guand).pop_front().unwrap();
                            drop(guand);
                            let up = Uploader {};
                            if let Err(e) = up.upload_retry(file_name.as_str()) {
                                failed.push(file_name);
                            }
                        }
                        for item in failed {
                            let mut guand = active_files.lock();
                            (*guand).push_back(item);
                        }
                    } else {
                        info!("recv close signal, start to upload all files to ");
                        loop {
                            let mut guand = active_files.lock();
                            if (*guand).is_empty() {
                                break;
                            }

                            let file_name = (*guand).pop_front().unwrap();
                            drop(guand);
                            let up = Uploader {};
                            if let Err(e) = up.upload_retry(file_name.as_str()) {
                                let mut guand = active_files.lock();
                                (*guand).push_back(file_name);
                            }
                        }
                        close_send.send(()).unwrap();
                        return ;
                    }
                }
            }
        });
        Db {
            mu: parking_lot::Mutex::new(()),
            buffer: buffer,
            cond: Arc::new(parking_lot::Condvar::new()),
            log_writer: par_file,
            close_recv,
        }
    }


    pub fn close(&self) {
        let log_writer = unsafe { &mut *self.log_writer.as_ptr() };
        log_writer.close();
    }

    pub fn close_db(&self) {
        self.close();
        let _ = self.close_recv.recv();
    }

    pub fn write(&self, event: *const Event) {
        let wr = Writer {
            event: event,
            cond: self.cond.clone(),
            done: false,
        };

        let mut _guand = self.mu.lock();
        let bw = unsafe { &mut *self.buffer.as_ptr() };
        bw.push_back(&wr as *const Writer);

        while let Some(item) = bw.front() {
            if *item == &wr as *const Writer || wr.done {
                // 如果是头部 或者done = true
                break;
            } else {
                self.cond.wait(&mut _guand);
            }
        }
        if wr.done {
            return;
        }

        let mut bwg = BatchWrite::default();
        let (last_one, size) = Db::build_batch_group(bw, &mut bwg);
        assert_eq!(bw.len(), size, "VecDeque should be empty");
        drop(_guand); // unlock

        let log_writer = unsafe { &mut *self.log_writer.as_ptr() };
        log_writer.append(&mut bwg); // not call file.sync

        let mut _guand = self.mu.lock();

        while let Some(item) = bw.front() {
            if *item != &wr as *const Writer {
                let c = unsafe { &mut *(*item as *mut Writer) };
                c.done = true;
                c.cond.notify_all();
            }
            if *item == last_one {
                bw.pop_front();
                break;
            }
            bw.pop_front();
        }
        if !bw.is_empty() {
            let front = bw.front().unwrap();
            let c = unsafe { &mut *(*front as *mut Writer) };
            c.cond.notify_one();
        }
    }

    fn build_batch_group(
        deque: &mut VecDeque<*const Writer>,
        batch_writer: &mut BatchWrite,
    ) -> (*const Writer, usize) {
        let mut last_one = std::ptr::null();
        let mut index = 0;
        while let Some(item) = deque.get(index) {
            last_one = *item;
            let item = unsafe { &**item };
            batch_writer.append(unsafe { &*item.event });
            index += 1;
        }
        return (last_one, index);
    }
}

#[cfg(target_os = "linux")]
fn create_file() -> std::fs::File {
    use std::{fs::OpenOptions, os::unix::fs::OpenOptionsExt};
    const O_DIRECT: i32 = 0o0040000;
    OpenOptions::new()
        .write(true)
        .create(true)
        .custom_flags(O_DIRECT)
        .open(file_name)
        .unwrap()
}

#[cfg(target_os = "macos")]
fn create_file(file_name: &str) -> std::fs::File {
    use std::fs::OpenOptions;
    // const O_DIRECT: i32 = 0o0040000;
    OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(file_name)
        .unwrap()
}

pub struct ParFile {
    root: String,
    file_map: Option<(String, ArrowWriter<File>)>,
    schema: Arc<Schema>,
    suffix: u32,
    active_file_nams: Arc<Mutex<VecDeque<String>>>,
    close: AtomicU8,
    file_name_sender: Sender<Option<String>>
}

impl ParFile {
    pub fn new(root: &str, file_name_sender: Sender<Option<String>>) -> Self {
        let schema = Schema::new(vec![
            Field::new("event_type", DataType::UInt8, false),
            Field::new("account_id", DataType::UInt64, false),
            Field::new("strategy_id", DataType::UInt64, false),
            Field::new("coin", DataType::Utf8, false),
            Field::new("amount", DataType::Utf8, false),
            Field::new("trace_id", DataType::Utf8, false),
        ]);

        ParFile {
            root: root.to_string(),
            file_map: None,
            schema: Arc::new(schema),
            suffix: 1,
            active_file_nams: Arc::new(Mutex::new(VecDeque::new())),
            close: AtomicU8::new(0),
            file_name_sender,
        }
    }

    pub fn close(&mut self) {
        self.close.store(1, std::sync::atomic::Ordering::SeqCst);
        let _ = self.file_name_sender.send(None);
        if let Some(file) = self.file_map.take() {
            let _ = file.1.close();
        }
    }

    pub fn read_file(file_name: &str) -> usize {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::fs::File;

        let file = File::open(file_name).unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        println!("Converted arrow schema is: {}", builder.schema());

        let mut reader = builder.build().unwrap();
        //  return reader.count();
        let mut size = 0;
        while let Some(item) = reader.next() {
            let item = item.unwrap();
            size += item.num_rows();
            // println!("record batch {:?}", item.columns())
        }
        return size;
        // let record_batch = reader.next().unwrap().unwrap();
        // println!("Read {} records.", record_batch.num_rows());
    }

    fn build_filename(&mut self, prefix: &str) -> String {
        for i in self.suffix..self.suffix + 1000000000 {
            let name = format!("{}/{}_{}.parquet", self.root.as_str(), prefix, i);
            if !std::path::Path::new(name.as_str()).exists() {
                return name;
            }
        }
        return "".to_string();
    }
    fn append(&mut self, bwg: &mut BatchWrite) {
        if self.close.load(std::sync::atomic::Ordering::SeqCst) == 1 {
            warn!("close is closestatus, ignore append");
            return 
        }
        // Local::now();
        let now = Local::now();
        let now = now.format("%Y%m%d_%H").to_string();
        match &mut self.file_map {
            Some(file) => {
                if now.as_str() != file.0.as_str() {
                    let mut file = self.file_map.take().unwrap();
                    file.1.close().unwrap();
                    let name = self.build_filename(now.as_str());
                    {
                        let mut guand = self.active_file_nams.lock();
                        (*guand).push_back(name.clone());
                    }
                    let _file = create_file(name.as_str());
                    let writer = ArrowWriter::try_new(_file, self.schema.clone(), None).unwrap();
                    self.file_map = Some((now, writer));
                }
            }
            None => {
                let name = self.build_filename(now.as_str());
                info!("file is None, create one {} ", name.as_str());   
                {
                    let mut guand = self.active_file_nams.lock();
                    (*guand).push_back(name.clone());
                }

                let _file = create_file(name.as_str());
                let writer = ArrowWriter::try_new(_file, self.schema.clone(), None).unwrap();
                self.file_map = Some((now, writer))
            }
        };
        let event_type = bwg.event_type.finish();
        let account = bwg.account_id.finish();
        let strategy = bwg.strategy_id.finish();
        let coin = bwg.coin.finish();
        let amount = bwg.amount.finish();
        let trace = bwg.trace_id.finish();

        let record_batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(UInt8Array::from(event_type)),
                Arc::new(UInt64Array::from(account)),
                Arc::new(UInt64Array::from(strategy)),
                Arc::new(StringArray::from(coin)),
                Arc::new(StringArray::from(amount)),
                Arc::new(StringArray::from(trace)),
            ],
        )
        .unwrap();

        let writer = &mut self.file_map.as_mut().unwrap().1;
        writer.write(&record_batch).expect("Writing batch failed");
    }
}

// pub fn write_to_batch(batch: &mut )
