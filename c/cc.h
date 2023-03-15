#pragma 
#include <stdio.h>
#include <stdint.h>

typedef  struct Writer {
    void *_db; 
} Writer;


/**
 * pub event_type: u8,
    pub account_id: u64,
    pub strategy_id: u64,
    pub coin: *const libc::c_char,
    pub amount: *const libc::c_char,
    // trade 的是 tradeId
    // BALANCE-CHANGE 表示 eventId
    // SETTLE-FEE 表示eventId
    pub trace_id: *const libc::c_char,
*/

typedef struct Event {
    char event_type;
    uint64_t account_id;
    uint64_t strategy_id;
    char *coin;
    char *amount;
    char *trace_id;
} Event;

void init_db(Writer* db);
void write_db(Writer *db, Event* event);
void close_db(Writer *db);