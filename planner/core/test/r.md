==Before use column_prune optimize: Apply{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c) eq(test.t.c, test.t.c)])->Aggr(count(1),firstrow(test.t.c),firstrow(test.t.d),firstrow(test.t._tidb_rowid),firstrow(test.t.c),firstrow(test.t.d),firstrow(test.t._tidb_rowid))->Projection->MaxOneRow}->Projection
==Before after column_prune optimize: Apply{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c) eq(test.t.c, test.t.c)])->Aggr(count(1))->Projection->MaxOneRow}->Projection

==Before use build_keys optimize: Apply{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c) eq(test.t.c, test.t.c)])->Aggr(count(1))->Projection->MaxOneRow}->Projection
==Before after build_keys optimize: Apply{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c) eq(test.t.c, test.t.c)])->Aggr(count(1))->Projection->MaxOneRow}->Projection

==Before use decorrelate optimize: Apply{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c) eq(test.t.c, test.t.c)])->Aggr(count(1))->Projection->MaxOneRow}->Projection
==Before after decorrelate optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c)])->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection->Projection->Projection

==Before use aggregation_eliminate optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c)])->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection->Projection->Projection
==Before after aggregation_eliminate optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c)])->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection->Projection->Projection

==Before use projection_eliminate optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c)])->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection->Projection->Projection
==Before after projection_eliminate optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c)])->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection

==Before use max_min_eliminate optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c)])->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection
==Before after max_min_eliminate optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c)])->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection

==Before use predicate_push_down optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}->Sel([eq(test.t.c, test.t.c)])->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection
==Before after predicate_push_down optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection

==Before use outer_join_eliminate optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection
==Before after outer_join_eliminate optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection

==Before use aggregation_push_down optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection
==Before after aggregation_push_down optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection

==Before use topn_push_down optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection
==Before after topn_push_down optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection

==Before use join_reorder optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection
==Before after join_reorder optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection

==Before use column_prune optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection
==Before after column_prune optimize: Join{DataScan(t)->Join{DataScan(s)->DataScan(t1)}(test.t.c,test.t.c)->Aggr(count(1),firstrow(test.t.c))}(test.t.c,test.t.c)->Projection
