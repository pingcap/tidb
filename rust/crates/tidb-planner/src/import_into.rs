//! Go `pkg/planner/core/planbuilder.go`: IMPORT INTO column assignments.
//!
//! This is the planner's syntax visitor, before expression rewriting and the
//! import encoder's separate optional-evaluation-property validation.

use std::any::Any;
use std::collections::HashMap;
use tidb_ast::{Assignment, ColumnOrUserVar, Expr, ImportIntoStmt, Visitable, Visitor};
use tidb_planner_core_base::PlanError;

/// Go `objstore.IsLocal`: local, file, and absent schemes denote server disk.
/// URL syntax validation belongs to the preceding `url.Parse` step.
pub fn import_from_server(path: &str) -> bool {
    let scheme = path.split_once(':').filter(|(scheme, _)| {
        !scheme.is_empty() && scheme.as_bytes()[0].is_ascii_alphabetic()
            && scheme.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
    }).map(|(scheme, _)| scheme);
    scheme.is_none_or(|scheme| scheme.eq_ignore_ascii_case("local") || scheme.eq_ignore_ascii_case("file"))
}

/// Go `importIntoCollAssignmentChecker`.
#[derive(Default)]
struct ImportIntoCollAssignmentChecker {
    idx: usize,
    needed_vars: HashMap<String, usize>,
    err: Option<PlanError>,
}

impl Visitor for ImportIntoCollAssignmentChecker {
    /// Go `importIntoCollAssignmentChecker.Enter`.
    fn enter(&mut self, _node: &mut dyn Any) -> bool {
        false
    }

    /// Go `importIntoCollAssignmentChecker.Leave`.
    fn leave(&mut self, node: &mut dyn Any) -> bool {
        let Some(expr) = node.downcast_ref::<Expr>() else {
            return self.err.is_none();
        };
        let detail = match expr {
            Expr::Column(_) => Some(
                "COLUMN reference is not supported in IMPORT INTO column assignment".to_owned(),
            ),
            // Rust embeds QueryStmt directly in these nodes; Go wraps each
            // query in a SubqueryExpr. Their children are still visited first.
            Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::CompareSubquery { .. } => {
                Some("subquery is not supported in IMPORT INTO column assignment".to_owned())
            }
            Expr::SysVar { .. } => {
                Some("system variable is not supported in IMPORT INTO column assignment".to_owned())
            }
            Expr::Assign { .. } => Some(
                "setting a variable in IMPORT INTO column assignment is not supported".to_owned(),
            ),
            Expr::UserVar(name) => {
                self.needed_vars
                    .insert(tidb_hack::go_to_lower(name), self.idx);
                None
            }
            Expr::Default(_) => Some(
                "FUNCTION default is not supported in IMPORT INTO column assignment".to_owned(),
            ),
            Expr::Window { name, .. } => Some(format!(
                "window FUNCTION {name} is not supported in IMPORT INTO column assignment"
            )),
            Expr::Aggregate { name, written, .. } => Some(format!(
                "aggregate FUNCTION {} is not supported in IMPORT INTO column assignment",
                written.as_deref().unwrap_or(name)
            )),
            Expr::GroupConcat { written, .. } => Some(format!(
                "aggregate FUNCTION {} is not supported in IMPORT INTO column assignment",
                written.as_deref().unwrap_or("GROUP_CONCAT")
            )),
            Expr::Func { name, args, .. } | Expr::GenericFuncCall { name, args, .. } => {
                let lower = tidb_hack::go_to_lower(name);
                if lower == "getvar" {
                    match args.first() {
                        Some(Expr::String(value) | Expr::RawString(value) | Expr::CharsetString { value, .. }) => {
                            self.needed_vars.insert(tidb_hack::go_to_lower(value), self.idx);
                            None
                        }
                        Some(_) => Some("the argument of getvar should be a constant string in IMPORT INTO column assignment".to_owned()),
                        None => None,
                    }
                } else if lower == "grouping"
                    || !tidb_expr::builtin_registry::is_builtin_name(&lower)
                {
                    Some(format!(
                        "FUNCTION {name} is not supported in IMPORT INTO column assignment"
                    ))
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(detail) = detail {
            self.err = Some(PlanError::internal(format!("{detail}, index {}", self.idx)));
        }
        self.err.is_none()
    }
}

/// Go `checkImportIntoColAssignments`.
pub fn check_import_into_col_assignments(
    assignments: &[Assignment],
) -> Result<HashMap<String, usize>, PlanError> {
    let mut checker = ImportIntoCollAssignmentChecker::default();
    for (idx, assignment) in assignments.iter().enumerate() {
        checker.idx = idx;
        // The shared AST visitor is mutable, but this visitor never changes
        // its input. Keep the caller's AST intact, including prepared plans.
        if !assignment.value.clone().accept(&mut checker) {
            break;
        }
    }
    match checker.err {
        Some(err) => Err(err),
        None => Ok(checker.needed_vars),
    }
}

/// Go `PlanBuilder.buildImportInto`: variables used by SET must be input fields.
pub fn check_import_into_input_vars(stmt: &ImportIntoStmt) -> Result<(), PlanError> {
    let mut needed = check_import_into_col_assignments(&stmt.column_assignments)?;
    for field in &stmt.columns_and_user_vars {
        if let ColumnOrUserVar::UserVar(name) = field {
            needed.remove(&tidb_hack::go_to_lower(name));
        }
    }
    if needed.is_empty() {
        return Ok(());
    }
    // Go iterates a map; only the set of indices, including duplicates, is
    // significant. Sort it to make the Rust diagnostic reproducible.
    let mut indices: Vec<_> = needed.values().copied().collect();
    indices.sort_unstable();
    Err(PlanError::internal(format!(
        "column assignment cannot use variables set outside IMPORT INTO statement, index {}",
        indices
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestImportIntoCollAssignmentChecker`, all original cases.
    #[test]
    fn test_import_into_coll_assignment_checker() {
        let cases: &[(&str, &str, &[&str])] = &[
            ("@a+1", "", &["a"]),
            ("@b+@c+@1", "", &["b", "c", "1"]),
            ("instr(substr(concat_ws('','b','~~'), 6)) + sysdate()", "", &[]),
            ("now() + interval 1 day", "", &[]),
            ("sysdate() + interval 1 month", "", &[]),
            ("cast('123' as unsigned)", "", &[]),
            ("getvar('c')", "", &["c"]),
            ("a", "COLUMN reference is not supported in IMPORT INTO column assignment", &[]),
            ("a+2", "COLUMN reference is not supported in IMPORT INTO column assignment", &[]),
            ("(select 1)", "subquery is not supported in IMPORT INTO column assignment", &[]),
            ("exists(select 1)", "subquery is not supported in IMPORT INTO column assignment", &[]),
            ("1 in (select 1)", "subquery is not supported in IMPORT INTO column assignment", &[]),
            ("1 + (select 1)", "subquery is not supported in IMPORT INTO column assignment", &[]),
            ("@@sql_mode", "system variable is not supported in IMPORT INTO column assignment", &[]),
            ("@@global.sql_mode", "system variable is not supported in IMPORT INTO column assignment", &[]),
            ("@a:=1", "setting a variable in IMPORT INTO column assignment is not supported", &[]),
            ("default(t.a)", "FUNCTION default is not supported in IMPORT INTO column assignment", &[]),
            ("ROW_NUMBER() OVER(PARTITION BY 1)", "window FUNCTION ROW_NUMBER is not supported in IMPORT INTO column assignment", &[]),
            ("COUNT(1)", "aggregate FUNCTION COUNT is not supported in IMPORT INTO column assignment", &[]),
            ("grouping(1)", "FUNCTION grouping is not supported in IMPORT INTO column assignment", &[]),
            ("getvar(concat('a', 'b'))", "the argument of getvar should be a constant string in IMPORT INTO column assignment", &[]),
            ("getvar(now())", "the argument of getvar should be a constant string in IMPORT INTO column assignment", &[]),
            ("noexist()", "FUNCTION noexist is not supported in IMPORT INTO column assignment", &[]),
            ("values(a)", "COLUMN reference is not supported in IMPORT INTO column assignment", &[]),
        ];
        assert_eq!(cases.len(), 24);
        for (idx, (expr, error, vars)) in cases.iter().enumerate() {
            let tidb_ast::Stmt::Dml(dml) =
                tidb_parser::parse(&format!("import into t set a={expr} from '/file.csv'"))
                    .unwrap()
            else {
                panic!()
            };
            let tidb_ast::DmlStmt::ImportInto(stmt) = dml.as_ref() else {
                panic!()
            };
            let mut checker = ImportIntoCollAssignmentChecker {
                idx,
                ..Default::default()
            };
            stmt.column_assignments[0]
                .value
                .clone()
                .accept(&mut checker);
            let expected = if error.is_empty() {
                None
            } else {
                Some(format!("{error}, index {idx}"))
            };
            assert_eq!(
                checker.err.as_ref().map(|err| err.message().to_owned()),
                expected,
                "{expr}"
            );
            let expected_vars: HashMap<_, _> =
                vars.iter().map(|name| ((*name).to_owned(), idx)).collect();
            assert_eq!(checker.needed_vars, expected_vars, "{expr}");
        }
    }
    /// Go testkit `zzprobe/e05.TestImportIntoInputVars` (2026-11-14),
    /// `go test --tags=intest -p 6 ./zzprobe/e05 -run '^TestImportIntoInputVars$'`.
    /// The accepted mappings reach InitDataFiles' relative-path error, proving
    /// the planner accepted them before the executor rejected their source.
    #[test]
    fn test_import_into_input_vars() {
        for line in include_str!("../testdata/import_into_input_vars_go.txt").lines() {
            let (sql, error) = line.split_once('\t').unwrap();
            let tidb_ast::Stmt::Dml(dml) = tidb_parser::parse(&sql).unwrap() else {
                panic!()
            };
            let tidb_ast::DmlStmt::ImportInto(stmt) = dml.as_ref() else {
                panic!()
            };
            let result = check_import_into_input_vars(stmt);
            if error.starts_with("[executor:8158]") {
                assert!(result.is_ok(), "{sql}: {result:?}");
            } else {
                assert_eq!(result.unwrap_err().message(), error, "{sql}");
            }
        }
    }
}
