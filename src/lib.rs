use std::{os::raw::c_char, sync::Arc};
mod db;
mod s3;

use db::*;
use std::ffi::{CStr, CString};

#[repr(C)]
pub struct Writer {
    _db: *mut Arc<db::Db>,
}

#[no_mangle]
pub extern "C" fn write_db(w: *mut Writer, event: *const Event) {
    let _db = unsafe { &*(*w)._db.clone() };
    _db.write(event);
}

#[no_mangle]
pub extern "C" fn init_db(w: *mut Writer) {
    env_logger::init();
    let _db = Box::new(Arc::new(db::Db::init(
        "/Users/zuoxiaoliang/project/rust/experence_persist/db",
    )));
    let ptr = Box::into_raw(_db);
    unsafe {
        (*w)._db = ptr;
    }
}

#[no_mangle]
pub extern "C" fn close_db(w: *mut Writer) {
    let _db = unsafe { &*(*w)._db.clone() };
    _db.close_db();
}

#[cfg(target_feature = "avx2")]
pub fn hello() {
    // Inlining `foo_impl` here is fine because `foo_sse4`
    // extends `foo_impl` feature set
    println!("enable sse4");
}

#[cfg(not(target_feature = "avx2"))]
pub fn hello() {
    // Inlining `foo_impl` here is fine because `foo_sse4`
    // extends `foo_impl` feature set
    println!("not sse4");
}

// fn main(){
//     env_logger::init();
//     let mut db = Arc::new(Db::init(
//         "/Users/zuoxiaoliang/project/rust/experence_persist/db",
//     ));
//     println!("init db success\n");
//     let coin = CString::new("usdt").unwrap();
//     let amount = CString::new("1234").unwrap();
//     let trace_id = CString::new("traceId").unwrap();

//     let event = Event {
//         event_type: 1,
//         account_id: 1 as u64,
//         strategy_id: 2 as u64,
//         coin: unsafe { coin.as_ptr() as *const libc::c_char },
//         amount: unsafe { amount.as_ptr() as *const libc::c_char },
//         trace_id: unsafe { trace_id.as_ptr() as *const libc::c_char },
//     };
//     println!("db start write \n");
//     db.write(&event as *const Event);
//     // db.close();
//     db.close_db();

// }
#[cfg(test)]
mod tests {
    use super::db::*;
    use super::*;
    use chrono::{Local, Utc};
    use std::ffi::{CStr, CString};
    #[test]
    fn a() {
        println!("11");
    }

    fn signal_close() {
        let mut db = Arc::new(Db::init(
            "/Users/zuoxiaoliang/project/rust/experence_persist/db",
        ));
        println!("init db success\n");
        let coin = CString::new("usdt").unwrap();
        let amount = CString::new("1234").unwrap();
        let trace_id = CString::new("traceId").unwrap();

        let event = Event {
            event_type: 1,
            account_id: 1 as u64,
            strategy_id: 2 as u64,
            coin: unsafe { coin.as_ptr() as *const libc::c_char },
            amount: unsafe { amount.as_ptr() as *const libc::c_char },
            trace_id: unsafe { trace_id.as_ptr() as *const libc::c_char },
        };
        println!("db start write \n");
        // db.write(&event as *const Event);
        // db.close();
        // db.close_db();
    }

    #[test]
    fn signal_thread() {
        hello();

        // let mut db = Arc::new(Db::init(
        //     "~/project/rust/xxx/db",
        // ));
        // let db1 = db.clone();
        // let db2 = db.clone();
        // let mut ret = Vec::new();
        // let now = Utc::now().timestamp_millis();
        // for i in 0..100 {
        //     // println!("index {}\n", i);
        //     let db1 = db.clone();
        //     let t1 = std::thread::spawn(move || {
        //         for j in 0..100000 {
        //             let coin = CString::new("usdt").unwrap();
        //             let amount = CString::new("1234").unwrap();
        //             let trace_id = CString::new("traceId").unwrap();

        //             let event = Event {
        //                 event_type: 1,
        //                 account_id: i * j as u64,
        //                 strategy_id: i * j as u64,
        //                 coin: unsafe { coin.as_ptr() as *const libc::c_char },
        //                 amount: unsafe { amount.as_ptr() as *const libc::c_char },
        //                 trace_id: unsafe { trace_id.as_ptr() as *const libc::c_char },
        //             };
        //             db1.write(&event as *const Event);
        //         }
        //     });
        //     ret.push(t1);
        // }

        // for ele in ret {
        //     ele.join().unwrap();
        // }

        // db.close();
        // let end = Utc::now().timestamp_millis();
        // println!("cost {:?}", (end - now));
        let size = ParFile::read_file(
            "/Users/zuoxiaoliang/project/rust/experence_persist/db/20230316_17_1.parquet",
        );
        println!("all record size  {} \n", size);
        assert_eq!(size, 10 * 1000 as usize, "size shoud be 10*1000")
    }
}
