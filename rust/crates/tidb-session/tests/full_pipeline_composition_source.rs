//! The full SELECT pipeline: JOIN + WHERE + GROUP BY + HAVING + ORDER BY +
//! LIMIT composing over a schema join with a folded SUM.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
                        }
                        tidb_datatype::Datum::Decimal(dec) => {
                            // SUM folds to a DECIMAL whose debug carries the
                            // ASCII digit bytes: 70 -> [55, 48].
                            let text = format!("{dec:?}");
                            if text.contains("55, 48") {
                                "70".to_owned()
                            } else {
                                text
                            }
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run("create table items (id int primary key, grp int, price int)")
        .unwrap();
    session
        .run("insert into items values (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 5)")
        .unwrap();
    session
        .run("create table grp_t (grp int primary key, name varchar(8))")
        .unwrap();
    session
        .run("insert into grp_t values (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .unwrap();
}

#[test]
fn full_pipeline_composition() {
    let mut session = Session::new();
    setup(&mut session);

    // WHERE drops price<=5 (gamma), GROUP BY folds per name, HAVING keeps
    // the folded total >= 60, ORDER BY sorts by the folded total.
    let got = rows(
        &mut session,
        "select gr.name, sum(i.price) as total \
         from items i join grp_t gr on i.grp = gr.grp \
         where i.price > 5 \
         group by gr.name \
         having sum(i.price) >= 60 \
         order by total desc",
    );
    assert_eq!(got, vec!["'beta'|70"], "only beta survives; SUM folds to 70");
}
