//! SHOW CREATE DATABASE prints Go's `ConstructResultOfShowCreateDatabase`
//! (show.go:1704-1743): the version-gated `/*!40100 DEFAULT CHARACTER SET`
//! comment, with a non-default collation added before the closing `*/`.

use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("expected rows, got {other:?}"),
    }
}

#[test]
fn database_ddl_round_trips() {
    let mut session = Session::new();
    session.run("create database app").unwrap();

    assert_eq!(
        strings(&mut session, "show create database app"),
        "app|CREATE DATABASE `app` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"
    );

    session
        .run(
            "create database app2 default character set utf8mb4 \
             collate utf8mb4_general_ci",
        )
        .unwrap();
    assert_eq!(
        strings(&mut session, "show create database app2"),
        "app2|CREATE DATABASE `app2` /*!40100 DEFAULT CHARACTER SET utf8mb4 \
         COLLATE utf8mb4_general_ci */"
    );
}
