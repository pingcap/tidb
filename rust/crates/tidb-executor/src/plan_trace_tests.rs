use super::*;

fn index_join_text(outer_row_size: f64, inner_row_size: f64) -> IndexJoinText {
    IndexJoinText {
        reader: "IndexReader",
        keys: vec![("a".to_owned(), "b".to_owned())],
        lookup_is_left: false,
        outer_row_size,
        inner_row_size,
    }
}

#[test]
fn both_index_join_kinds_are_reachable() {
    assert_eq!(
        index_join_operator(Some(10000.0), Some(10.0), &index_join_text(16.0, 16.0), 1),
        "IndexHashJoin",
    );
    assert_eq!(
        index_join_operator(Some(10000.0), Some(1.0), &index_join_text(400.0, 8.0), 1),
        "IndexJoin",
    );
}

#[test]
fn an_exact_tie_keeps_the_kind_go_enumerates_first() {
    assert_eq!(
        index_join_operator(Some(10000.0), Some(1.0), &index_join_text(16.0, 16.0), 1),
        "IndexJoin",
    );
}

#[test]
fn a_retraction_walks_past_a_projection_and_stops_at_a_merge_join() {
    let leaf = || {
        PlanNode::new(
            "TableFullScan",
            Some(1.0),
            "table:t".to_owned(),
            "keep order:true, stats:pseudo".to_owned(),
        )
    };
    let mut through = PlanNode::new("Projection", None, String::new(), String::new());
    through.children.push(leaf());
    retract_keep_order(&mut through);
    assert_eq!(through.children[0].info, "keep order:false, stats:pseudo");

    let mut relied_on = PlanNode::new("MergeJoin", None, String::new(), String::new());
    relied_on.children.push(leaf());
    retract_keep_order(&mut relied_on);
    assert_eq!(relied_on.children[0].info, "keep order:true, stats:pseudo",);
}
