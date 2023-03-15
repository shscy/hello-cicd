#include "cc.h"

int main(){
    Writer w;
    init_db(&w);
    Event e;
    e.account_id = 12,
    e.strategy_id = 1;
    e.amount = "123\0";
    e.coin = "USDT\0";
    e.event_type = 0;
    e.trace_id = "trace_id_0\0";

    write_db(&w, &e);
    close_db(&w);
    return 0;
}