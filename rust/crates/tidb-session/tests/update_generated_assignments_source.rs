//! UPDATE recomputes only the generated columns Go's `buildUpdateLists`
//! appends to the assignment list (`Update.OrderedList`, evaluated in
//! `updateRecord`'s step 4): a stored generated column whose dependencies the
//! statement does not touch keeps its stored value, even when evaluating it
//! again would now answer differently (here: the session time zone moved).
//! Expected values are Go's (`gorun`).

use crate::support::testkit::TestKit;

#[test]
fn an_untouched_generated_column_keeps_its_stored_value() {
    let mut tk = TestKit::new();
    tk.must_exec("set time_zone = '+00:00'");
    tk.must_exec(
        "create table t (a int, ts timestamp, g varchar(20) as (date_format(ts, '%H')) stored)",
    );
    tk.must_exec("insert into t (a, ts) values (1, '2026-01-01 10:00:00')");
    tk.must_query("select a, g from t").check(["1 10"]);
    tk.must_exec("set time_zone = '+08:00'");
    // `g` depends on `ts` only: SET a leaves it out of the ordered list.
    tk.must_exec("update t set a = 2");
    tk.must_query("select a, g from t").check(["2 10"]);
    // SET ts appends `g`, but an unchanged row returns before step 4.
    tk.must_exec("update t set ts = ts");
    tk.must_query("select a, g from t").check(["2 10"]);
    // A changed `ts` recomputes `g` in the session's +08:00.
    tk.must_exec("update t set ts = '2026-01-01 19:00:00'");
    tk.must_query("select a, g from t").check(["2 19"]);
}
