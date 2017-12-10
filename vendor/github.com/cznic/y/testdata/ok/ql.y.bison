Gramatika

    0 $accept: StatementList $end

    1 AlterTableStmt: alter tableKwd TableName add ColumnDef
    2               | alter tableKwd TableName drop column ColumnName

    3 Assignment: ColumnName '=' Expression

    4 AssignmentList: Assignment AssignmentList1 AssignmentList2

    5 AssignmentList1: %empty
    6                | AssignmentList1 ',' Assignment

    7 AssignmentList2: %empty
    8                | ','

    9 BeginTransactionStmt: begin transaction

   10 Call: '(' Call1 ')'

   11 Call1: %empty
   12      | ExpressionList

   13 ColumnDef: ColumnName Type

   14 ColumnName: identifier

   15 ColumnNameList: ColumnName ColumnNameList1 ColumnNameList2

   16 ColumnNameList1: %empty
   17                | ColumnNameList1 ',' ColumnName

   18 ColumnNameList2: %empty
   19                | ','

   20 CommitStmt: commit

   21 Conversion: Type '(' Expression ')'

   22 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier ')'
   23                | create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier '(' ')' ')'

   24 CreateIndexIfNotExists: %empty
   25                       | ifKwd not exists

   26 CreateIndexStmtUnique: %empty
   27                      | unique

   28 CreateTableStmt: create tableKwd TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'
   29                | create tableKwd ifKwd not exists TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'

   30 CreateTableStmt1: %empty
   31                 | CreateTableStmt1 ',' ColumnDef

   32 CreateTableStmt2: %empty
   33                 | ','

   34 DeleteFromStmt: deleteKwd from TableName
   35               | deleteKwd from TableName WhereClause

   36 DropIndexStmt: drop index DropIndexIfExists identifier

   37 DropIndexIfExists: %empty
   38                  | ifKwd exists

   39 DropTableStmt: drop tableKwd TableName
   40              | drop tableKwd ifKwd exists TableName

   41 EmptyStmt: %empty

   42 Expression: Term
   43           | Expression oror Term

   44 ExpressionList: Expression ExpressionList1 ExpressionList2

   45 ExpressionList1: %empty
   46                | ExpressionList1 ',' Expression

   47 ExpressionList2: %empty
   48                | ','

   49 Factor: Factor1
   50       | Factor1 in '(' ExpressionList ')'
   51       | Factor1 not in '(' ExpressionList ')'
   52       | Factor1 between PrimaryFactor and PrimaryFactor
   53       | Factor1 not between PrimaryFactor and PrimaryFactor
   54       | Factor1 is null
   55       | Factor1 is not null

   56 Factor1: PrimaryFactor
   57        | Factor1 ge PrimaryFactor
   58        | Factor1 '>' PrimaryFactor
   59        | Factor1 le PrimaryFactor
   60        | Factor1 '<' PrimaryFactor
   61        | Factor1 neq PrimaryFactor
   62        | Factor1 eq PrimaryFactor
   63        | Factor1 like PrimaryFactor

   64 Field: Expression Field1

   65 Field1: %empty
   66       | as identifier

   67 FieldList: Field
   68          | FieldList ',' Field

   69 GroupByClause: group by ColumnNameList

   70 Index: '[' Expression ']'

   71 InsertIntoStmt: insert into TableName InsertIntoStmt1 values '(' ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3
   72               | insert into TableName InsertIntoStmt1 SelectStmt

   73 InsertIntoStmt1: %empty
   74                | '(' ColumnNameList ')'

   75 InsertIntoStmt2: %empty
   76                | InsertIntoStmt2 ',' '(' ExpressionList ')'

   77 InsertIntoStmt3: %empty
   78                | ','

   79 Literal: falseKwd
   80        | null
   81        | trueKwd
   82        | floatLit
   83        | imaginaryLit
   84        | intLit
   85        | stringLit

   86 Operand: Literal
   87        | qlParam
   88        | QualifiedIdent
   89        | '(' Expression ')'

   90 OrderBy: order by ExpressionList OrderBy1

   91 OrderBy1: %empty
   92         | asc
   93         | desc

   94 PrimaryExpression: Operand
   95                  | Conversion
   96                  | PrimaryExpression Index
   97                  | PrimaryExpression Slice
   98                  | PrimaryExpression Call

   99 PrimaryFactor: PrimaryTerm
  100              | PrimaryFactor '^' PrimaryTerm
  101              | PrimaryFactor '|' PrimaryTerm
  102              | PrimaryFactor '-' PrimaryTerm
  103              | PrimaryFactor '+' PrimaryTerm

  104 PrimaryTerm: UnaryExpr
  105            | PrimaryTerm andnot UnaryExpr
  106            | PrimaryTerm '&' UnaryExpr
  107            | PrimaryTerm lsh UnaryExpr
  108            | PrimaryTerm rsh UnaryExpr
  109            | PrimaryTerm '%' UnaryExpr
  110            | PrimaryTerm '/' UnaryExpr
  111            | PrimaryTerm '*' UnaryExpr

  112 QualifiedIdent: identifier
  113               | identifier '.' identifier

  114 RecordSet: RecordSet1 RecordSet2

  115 RecordSet1: identifier
  116           | '(' SelectStmt RecordSet11 ')'

  117 RecordSet11: %empty
  118            | ';'

  119 RecordSet2: %empty
  120           | as identifier

  121 RecordSetList: RecordSet
  122              | RecordSetList ',' RecordSet

  123 RollbackStmt: rollback

  124 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset

  126 SelectStmtLimit: %empty
  127                | limit Expression

  128 SelectStmtOffset: %empty
  129                 | offset Expression

  130 SelectStmtDistinct: %empty
  131                   | distinct

  132 SelectStmtFieldList: '*'
  133                    | FieldList
  134                    | FieldList ','

  135 SelectStmtWhere: %empty
  136                | WhereClause

  137 SelectStmtGroup: %empty
  138                | GroupByClause

  139 SelectStmtOrder: %empty
  140                | OrderBy

  141 Slice: '[' ':' ']'
  142      | '[' ':' Expression ']'
  143      | '[' Expression ':' ']'
  144      | '[' Expression ':' Expression ']'

  145 Statement: EmptyStmt
  146          | AlterTableStmt
  147          | BeginTransactionStmt
  148          | CommitStmt
  149          | CreateIndexStmt
  150          | CreateTableStmt
  151          | DeleteFromStmt
  152          | DropIndexStmt
  153          | DropTableStmt
  154          | InsertIntoStmt
  155          | RollbackStmt
  156          | SelectStmt
  157          | TruncateTableStmt
  158          | UpdateStmt

  159 StatementList: Statement
  160              | StatementList ';' Statement

  161 TableName: identifier

  162 Term: Factor
  163     | Term andand Factor

  164 TruncateTableStmt: truncate tableKwd TableName

  165 Type: bigIntType
  166     | bigRatType
  167     | blobType
  168     | boolType
  169     | byteType
  170     | complex128Type
  171     | complex64Type
  172     | durationType
  173     | floatType
  174     | float32Type
  175     | float64Type
  176     | intType
  177     | int16Type
  178     | int32Type
  179     | int64Type
  180     | int8Type
  181     | runeType
  182     | stringType
  183     | timeType
  184     | uintType
  185     | uint16Type
  186     | uint32Type
  187     | uint64Type
  188     | uint8Type

  189 UpdateStmt: update TableName oSet AssignmentList UpdateStmt1

  190 UpdateStmt1: %empty
  191            | WhereClause

  192 UnaryExpr: PrimaryExpression
  193          | '^' PrimaryExpression
  194          | '!' PrimaryExpression
  195          | '-' PrimaryExpression
  196          | '+' PrimaryExpression

  197 WhereClause: where Expression

  198 oSet: %empty
  199     | set


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'!' (33) 194
'%' (37) 109
'&' (38) 106
'(' (40) 10 21 22 23 28 29 50 51 71 74 76 89 116
')' (41) 10 21 22 23 28 29 50 51 71 74 76 89 116
'*' (42) 111 132
'+' (43) 103 196
',' (44) 6 8 17 19 31 33 46 48 68 76 78 122 125 134
'-' (45) 102 195
'.' (46) 113
'/' (47) 110
':' (58) 141 142 143 144
';' (59) 118 160
'<' (60) 60
'=' (61) 3
'>' (62) 58
'[' (91) 70 141 142 143 144
']' (93) 70 141 142 143 144
'^' (94) 100 193
'|' (124) 101
error (256)
add (258) 1
alter (259) 1 2
and (260) 52 53
andand (261) 163
andnot (262) 105
as (263) 66 120
asc (264) 92
begin (265) 9
between (266) 52 53
bigIntType (267) 165
bigRatType (268) 166
blobType (269) 167
boolType (270) 168
by (271) 69 90
byteType (272) 169
column (273) 2
commit (274) 20
complex128Type (275) 170
complex64Type (276) 171
create (277) 22 23 28 29
deleteKwd (278) 34 35
desc (279) 93
distinct (280) 131
drop (281) 2 36 39 40
durationType (282) 172
eq (283) 62
exists (284) 25 29 38 40
falseKwd (285) 79
floatType (286) 173
float32Type (287) 174
float64Type (288) 175
floatLit (289) 82
from (290) 34 35 124 125
ge (291) 57
group (292) 69
identifier (293) 14 22 23 36 66 112 113 115 120 161
ifKwd (294) 25 29 38 40
imaginaryLit (295) 83
in (296) 50 51
index (297) 22 23 36
insert (298) 71 72
intType (299) 176
int16Type (300) 177
int32Type (301) 178
int64Type (302) 179
int8Type (303) 180
into (304) 71 72
intLit (305) 84
is (306) 54 55
le (307) 59
like (308) 63
limit (309) 127
lsh (310) 107
neq (311) 61
not (312) 25 29 51 53 55
null (313) 54 55 80
offset (314) 129
on (315) 22 23
order (316) 90
oror (317) 43
qlParam (318) 87
rollback (319) 123
rsh (320) 108
runeType (321) 181
selectKwd (322) 124 125
set (323) 199
stringType (324) 182
stringLit (325) 85
tableKwd (326) 1 2 28 29 39 40 164
timeType (327) 183
transaction (328) 9
trueKwd (329) 81
truncate (330) 164
uintType (331) 184
uint16Type (332) 185
uint32Type (333) 186
uint64Type (334) 187
uint8Type (335) 188
unique (336) 27
update (337) 189
values (338) 71
where (339) 197


Neterminály s pravidly, ve kterých se objevují

$accept (105)
    vlevo: 0
AlterTableStmt (106)
    vlevo: 1 2, vpravo: 146
Assignment (107)
    vlevo: 3, vpravo: 4 6
AssignmentList (108)
    vlevo: 4, vpravo: 189
AssignmentList1 (109)
    vlevo: 5 6, vpravo: 4 6
AssignmentList2 (110)
    vlevo: 7 8, vpravo: 4
BeginTransactionStmt (111)
    vlevo: 9, vpravo: 147
Call (112)
    vlevo: 10, vpravo: 98
Call1 (113)
    vlevo: 11 12, vpravo: 10
ColumnDef (114)
    vlevo: 13, vpravo: 1 28 29 31
ColumnName (115)
    vlevo: 14, vpravo: 2 3 13 15 17
ColumnNameList (116)
    vlevo: 15, vpravo: 69 74
ColumnNameList1 (117)
    vlevo: 16 17, vpravo: 15 17
ColumnNameList2 (118)
    vlevo: 18 19, vpravo: 15
CommitStmt (119)
    vlevo: 20, vpravo: 148
Conversion (120)
    vlevo: 21, vpravo: 95
CreateIndexStmt (121)
    vlevo: 22 23, vpravo: 149
CreateIndexIfNotExists (122)
    vlevo: 24 25, vpravo: 22 23
CreateIndexStmtUnique (123)
    vlevo: 26 27, vpravo: 22 23
CreateTableStmt (124)
    vlevo: 28 29, vpravo: 150
CreateTableStmt1 (125)
    vlevo: 30 31, vpravo: 28 29 31
CreateTableStmt2 (126)
    vlevo: 32 33, vpravo: 28 29
DeleteFromStmt (127)
    vlevo: 34 35, vpravo: 151
DropIndexStmt (128)
    vlevo: 36, vpravo: 152
DropIndexIfExists (129)
    vlevo: 37 38, vpravo: 36
DropTableStmt (130)
    vlevo: 39 40, vpravo: 153
EmptyStmt (131)
    vlevo: 41, vpravo: 145
Expression (132)
    vlevo: 42 43, vpravo: 3 21 43 44 46 64 70 89 127 129 142 143 144
    197
ExpressionList (133)
    vlevo: 44, vpravo: 12 50 51 71 76 90
ExpressionList1 (134)
    vlevo: 45 46, vpravo: 44 46
ExpressionList2 (135)
    vlevo: 47 48, vpravo: 44
Factor (136)
    vlevo: 49 50 51 52 53 54 55, vpravo: 162 163
Factor1 (137)
    vlevo: 56 57 58 59 60 61 62 63, vpravo: 49 50 51 52 53 54 55 57
    58 59 60 61 62 63
Field (138)
    vlevo: 64, vpravo: 67 68
Field1 (139)
    vlevo: 65 66, vpravo: 64
FieldList (140)
    vlevo: 67 68, vpravo: 68 133 134
GroupByClause (141)
    vlevo: 69, vpravo: 138
Index (142)
    vlevo: 70, vpravo: 96
InsertIntoStmt (143)
    vlevo: 71 72, vpravo: 154
InsertIntoStmt1 (144)
    vlevo: 73 74, vpravo: 71 72
InsertIntoStmt2 (145)
    vlevo: 75 76, vpravo: 71 76
InsertIntoStmt3 (146)
    vlevo: 77 78, vpravo: 71
Literal (147)
    vlevo: 79 80 81 82 83 84 85, vpravo: 86
Operand (148)
    vlevo: 86 87 88 89, vpravo: 94
OrderBy (149)
    vlevo: 90, vpravo: 140
OrderBy1 (150)
    vlevo: 91 92 93, vpravo: 90
PrimaryExpression (151)
    vlevo: 94 95 96 97 98, vpravo: 96 97 98 192 193 194 195 196
PrimaryFactor (152)
    vlevo: 99 100 101 102 103, vpravo: 52 53 56 57 58 59 60 61 62 63
    100 101 102 103
PrimaryTerm (153)
    vlevo: 104 105 106 107 108 109 110 111, vpravo: 99 100 101 102
    103 105 106 107 108 109 110 111
QualifiedIdent (154)
    vlevo: 112 113, vpravo: 88
RecordSet (155)
    vlevo: 114, vpravo: 121 122
RecordSet1 (156)
    vlevo: 115 116, vpravo: 114
RecordSet11 (157)
    vlevo: 117 118, vpravo: 116
RecordSet2 (158)
    vlevo: 119 120, vpravo: 114
RecordSetList (159)
    vlevo: 121 122, vpravo: 122 124 125
RollbackStmt (160)
    vlevo: 123, vpravo: 155
SelectStmt (161)
    vlevo: 124 125, vpravo: 72 116 156
SelectStmtLimit (162)
    vlevo: 126 127, vpravo: 124 125
SelectStmtOffset (163)
    vlevo: 128 129, vpravo: 124 125
SelectStmtDistinct (164)
    vlevo: 130 131, vpravo: 124 125
SelectStmtFieldList (165)
    vlevo: 132 133 134, vpravo: 124 125
SelectStmtWhere (166)
    vlevo: 135 136, vpravo: 124 125
SelectStmtGroup (167)
    vlevo: 137 138, vpravo: 124 125
SelectStmtOrder (168)
    vlevo: 139 140, vpravo: 124 125
Slice (169)
    vlevo: 141 142 143 144, vpravo: 97
Statement (170)
    vlevo: 145 146 147 148 149 150 151 152 153 154 155 156 157 158,
    vpravo: 159 160
StatementList (171)
    vlevo: 159 160, vpravo: 0 160
TableName (172)
    vlevo: 161, vpravo: 1 2 28 29 34 35 39 40 71 72 164 189
Term (173)
    vlevo: 162 163, vpravo: 42 43 163
TruncateTableStmt (174)
    vlevo: 164, vpravo: 157
Type (175)
    vlevo: 165 166 167 168 169 170 171 172 173 174 175 176 177 178
    179 180 181 182 183 184 185 186 187 188, vpravo: 13 21
UpdateStmt (176)
    vlevo: 189, vpravo: 158
UpdateStmt1 (177)
    vlevo: 190 191, vpravo: 189
UnaryExpr (178)
    vlevo: 192 193 194 195 196, vpravo: 104 105 106 107 108 109 110
    111
WhereClause (179)
    vlevo: 197, vpravo: 35 136 191
oSet (180)
    vlevo: 198 199, vpravo: 189


State 0

    0 $accept: . StatementList $end
    1 AlterTableStmt: . alter tableKwd TableName add ColumnDef
    2               | . alter tableKwd TableName drop column ColumnName
    9 BeginTransactionStmt: . begin transaction
   20 CommitStmt: . commit
   22 CreateIndexStmt: . create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier ')'
   23                | . create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier '(' ')' ')'
   28 CreateTableStmt: . create tableKwd TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'
   29                | . create tableKwd ifKwd not exists TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'
   34 DeleteFromStmt: . deleteKwd from TableName
   35               | . deleteKwd from TableName WhereClause
   36 DropIndexStmt: . drop index DropIndexIfExists identifier
   39 DropTableStmt: . drop tableKwd TableName
   40              | . drop tableKwd ifKwd exists TableName
   41 EmptyStmt: . %empty  [$end, ';']
   71 InsertIntoStmt: . insert into TableName InsertIntoStmt1 values '(' ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3
   72               | . insert into TableName InsertIntoStmt1 SelectStmt
  123 RollbackStmt: . rollback
  124 SelectStmt: . selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | . selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  145 Statement: . EmptyStmt
  146          | . AlterTableStmt
  147          | . BeginTransactionStmt
  148          | . CommitStmt
  149          | . CreateIndexStmt
  150          | . CreateTableStmt
  151          | . DeleteFromStmt
  152          | . DropIndexStmt
  153          | . DropTableStmt
  154          | . InsertIntoStmt
  155          | . RollbackStmt
  156          | . SelectStmt
  157          | . TruncateTableStmt
  158          | . UpdateStmt
  159 StatementList: . Statement
  160              | . StatementList ';' Statement
  164 TruncateTableStmt: . truncate tableKwd TableName
  189 UpdateStmt: . update TableName oSet AssignmentList UpdateStmt1

    alter      posunout a přejít do stavu 1
    begin      posunout a přejít do stavu 2
    commit     posunout a přejít do stavu 3
    create     posunout a přejít do stavu 4
    deleteKwd  posunout a přejít do stavu 5
    drop       posunout a přejít do stavu 6
    insert     posunout a přejít do stavu 7
    rollback   posunout a přejít do stavu 8
    selectKwd  posunout a přejít do stavu 9
    truncate   posunout a přejít do stavu 10
    update     posunout a přejít do stavu 11

    $výchozí  reduce using rule 41 (EmptyStmt)

    AlterTableStmt        přejít do stavu 12
    BeginTransactionStmt  přejít do stavu 13
    CommitStmt            přejít do stavu 14
    CreateIndexStmt       přejít do stavu 15
    CreateTableStmt       přejít do stavu 16
    DeleteFromStmt        přejít do stavu 17
    DropIndexStmt         přejít do stavu 18
    DropTableStmt         přejít do stavu 19
    EmptyStmt             přejít do stavu 20
    InsertIntoStmt        přejít do stavu 21
    RollbackStmt          přejít do stavu 22
    SelectStmt            přejít do stavu 23
    Statement             přejít do stavu 24
    StatementList         přejít do stavu 25
    TruncateTableStmt     přejít do stavu 26
    UpdateStmt            přejít do stavu 27


State 1

    1 AlterTableStmt: alter . tableKwd TableName add ColumnDef
    2               | alter . tableKwd TableName drop column ColumnName

    tableKwd  posunout a přejít do stavu 28


State 2

    9 BeginTransactionStmt: begin . transaction

    transaction  posunout a přejít do stavu 29


State 3

   20 CommitStmt: commit .

    $výchozí  reduce using rule 20 (CommitStmt)


State 4

   22 CreateIndexStmt: create . CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier ')'
   23                | create . CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier '(' ')' ')'
   26 CreateIndexStmtUnique: . %empty  [index]
   27                      | . unique
   28 CreateTableStmt: create . tableKwd TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'
   29                | create . tableKwd ifKwd not exists TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'

    tableKwd  posunout a přejít do stavu 30
    unique    posunout a přejít do stavu 31

    $výchozí  reduce using rule 26 (CreateIndexStmtUnique)

    CreateIndexStmtUnique  přejít do stavu 32


State 5

   34 DeleteFromStmt: deleteKwd . from TableName
   35               | deleteKwd . from TableName WhereClause

    from  posunout a přejít do stavu 33


State 6

   36 DropIndexStmt: drop . index DropIndexIfExists identifier
   39 DropTableStmt: drop . tableKwd TableName
   40              | drop . tableKwd ifKwd exists TableName

    index     posunout a přejít do stavu 34
    tableKwd  posunout a přejít do stavu 35


State 7

   71 InsertIntoStmt: insert . into TableName InsertIntoStmt1 values '(' ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3
   72               | insert . into TableName InsertIntoStmt1 SelectStmt

    into  posunout a přejít do stavu 36


State 8

  123 RollbackStmt: rollback .

    $výchozí  reduce using rule 123 (RollbackStmt)


State 9

  124 SelectStmt: selectKwd . SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | selectKwd . SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  130 SelectStmtDistinct: . %empty  [bigIntType, bigRatType, blobType, boolType, byteType, complex128Type, complex64Type, durationType, falseKwd, floatType, float32Type, float64Type, floatLit, identifier, imaginaryLit, intType, int16Type, int32Type, int64Type, int8Type, intLit, null, qlParam, runeType, stringType, stringLit, timeType, trueKwd, uintType, uint16Type, uint32Type, uint64Type, uint8Type, '(', '^', '-', '+', '*', '!']
  131                   | . distinct

    distinct  posunout a přejít do stavu 37

    $výchozí  reduce using rule 130 (SelectStmtDistinct)

    SelectStmtDistinct  přejít do stavu 38


State 10

  164 TruncateTableStmt: truncate . tableKwd TableName

    tableKwd  posunout a přejít do stavu 39


State 11

  161 TableName: . identifier
  189 UpdateStmt: update . TableName oSet AssignmentList UpdateStmt1

    identifier  posunout a přejít do stavu 40

    TableName  přejít do stavu 41


State 12

  146 Statement: AlterTableStmt .

    $výchozí  reduce using rule 146 (Statement)


State 13

  147 Statement: BeginTransactionStmt .

    $výchozí  reduce using rule 147 (Statement)


State 14

  148 Statement: CommitStmt .

    $výchozí  reduce using rule 148 (Statement)


State 15

  149 Statement: CreateIndexStmt .

    $výchozí  reduce using rule 149 (Statement)


State 16

  150 Statement: CreateTableStmt .

    $výchozí  reduce using rule 150 (Statement)


State 17

  151 Statement: DeleteFromStmt .

    $výchozí  reduce using rule 151 (Statement)


State 18

  152 Statement: DropIndexStmt .

    $výchozí  reduce using rule 152 (Statement)


State 19

  153 Statement: DropTableStmt .

    $výchozí  reduce using rule 153 (Statement)


State 20

  145 Statement: EmptyStmt .

    $výchozí  reduce using rule 145 (Statement)


State 21

  154 Statement: InsertIntoStmt .

    $výchozí  reduce using rule 154 (Statement)


State 22

  155 Statement: RollbackStmt .

    $výchozí  reduce using rule 155 (Statement)


State 23

  156 Statement: SelectStmt .

    $výchozí  reduce using rule 156 (Statement)


State 24

  159 StatementList: Statement .

    $výchozí  reduce using rule 159 (StatementList)


State 25

    0 $accept: StatementList . $end
  160 StatementList: StatementList . ';' Statement

    $end  posunout a přejít do stavu 42
    ';'   posunout a přejít do stavu 43


State 26

  157 Statement: TruncateTableStmt .

    $výchozí  reduce using rule 157 (Statement)


State 27

  158 Statement: UpdateStmt .

    $výchozí  reduce using rule 158 (Statement)


State 28

    1 AlterTableStmt: alter tableKwd . TableName add ColumnDef
    2               | alter tableKwd . TableName drop column ColumnName
  161 TableName: . identifier

    identifier  posunout a přejít do stavu 40

    TableName  přejít do stavu 44


State 29

    9 BeginTransactionStmt: begin transaction .

    $výchozí  reduce using rule 9 (BeginTransactionStmt)


State 30

   28 CreateTableStmt: create tableKwd . TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'
   29                | create tableKwd . ifKwd not exists TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'
  161 TableName: . identifier

    identifier  posunout a přejít do stavu 40
    ifKwd       posunout a přejít do stavu 45

    TableName  přejít do stavu 46


State 31

   27 CreateIndexStmtUnique: unique .

    $výchozí  reduce using rule 27 (CreateIndexStmtUnique)


State 32

   22 CreateIndexStmt: create CreateIndexStmtUnique . index CreateIndexIfNotExists identifier on identifier '(' identifier ')'
   23                | create CreateIndexStmtUnique . index CreateIndexIfNotExists identifier on identifier '(' identifier '(' ')' ')'

    index  posunout a přejít do stavu 47


State 33

   34 DeleteFromStmt: deleteKwd from . TableName
   35               | deleteKwd from . TableName WhereClause
  161 TableName: . identifier

    identifier  posunout a přejít do stavu 40

    TableName  přejít do stavu 48


State 34

   36 DropIndexStmt: drop index . DropIndexIfExists identifier
   37 DropIndexIfExists: . %empty  [identifier]
   38                  | . ifKwd exists

    ifKwd  posunout a přejít do stavu 49

    $výchozí  reduce using rule 37 (DropIndexIfExists)

    DropIndexIfExists  přejít do stavu 50


State 35

   39 DropTableStmt: drop tableKwd . TableName
   40              | drop tableKwd . ifKwd exists TableName
  161 TableName: . identifier

    identifier  posunout a přejít do stavu 40
    ifKwd       posunout a přejít do stavu 51

    TableName  přejít do stavu 52


State 36

   71 InsertIntoStmt: insert into . TableName InsertIntoStmt1 values '(' ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3
   72               | insert into . TableName InsertIntoStmt1 SelectStmt
  161 TableName: . identifier

    identifier  posunout a přejít do stavu 40

    TableName  přejít do stavu 53


State 37

  131 SelectStmtDistinct: distinct .

    $výchozí  reduce using rule 131 (SelectStmtDistinct)


State 38

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   64 Field: . Expression Field1
   67 FieldList: . Field
   68          | . FieldList ',' Field
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  124 SelectStmt: selectKwd SelectStmtDistinct . SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | selectKwd SelectStmtDistinct . SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  132 SelectStmtFieldList: . '*'
  133                    | . FieldList
  134                    | . FieldList ','
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '*'             posunout a přejít do stavu 91
    '!'             posunout a přejít do stavu 92

    Conversion           přejít do stavu 93
    Expression           přejít do stavu 94
    Factor               přejít do stavu 95
    Factor1              přejít do stavu 96
    Field                přejít do stavu 97
    FieldList            přejít do stavu 98
    Literal              přejít do stavu 99
    Operand              přejít do stavu 100
    PrimaryExpression    přejít do stavu 101
    PrimaryFactor        přejít do stavu 102
    PrimaryTerm          přejít do stavu 103
    QualifiedIdent       přejít do stavu 104
    SelectStmtFieldList  přejít do stavu 105
    Term                 přejít do stavu 106
    Type                 přejít do stavu 107
    UnaryExpr            přejít do stavu 108


State 39

  161 TableName: . identifier
  164 TruncateTableStmt: truncate tableKwd . TableName

    identifier  posunout a přejít do stavu 40

    TableName  přejít do stavu 109


State 40

  161 TableName: identifier .

    $výchozí  reduce using rule 161 (TableName)


State 41

  189 UpdateStmt: update TableName . oSet AssignmentList UpdateStmt1
  198 oSet: . %empty  [identifier]
  199     | . set

    set  posunout a přejít do stavu 110

    $výchozí  reduce using rule 198 (oSet)

    oSet  přejít do stavu 111


State 42

    0 $accept: StatementList $end .

    $výchozí  přijmout


State 43

    1 AlterTableStmt: . alter tableKwd TableName add ColumnDef
    2               | . alter tableKwd TableName drop column ColumnName
    9 BeginTransactionStmt: . begin transaction
   20 CommitStmt: . commit
   22 CreateIndexStmt: . create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier ')'
   23                | . create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier '(' ')' ')'
   28 CreateTableStmt: . create tableKwd TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'
   29                | . create tableKwd ifKwd not exists TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'
   34 DeleteFromStmt: . deleteKwd from TableName
   35               | . deleteKwd from TableName WhereClause
   36 DropIndexStmt: . drop index DropIndexIfExists identifier
   39 DropTableStmt: . drop tableKwd TableName
   40              | . drop tableKwd ifKwd exists TableName
   41 EmptyStmt: . %empty  [$end, ';']
   71 InsertIntoStmt: . insert into TableName InsertIntoStmt1 values '(' ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3
   72               | . insert into TableName InsertIntoStmt1 SelectStmt
  123 RollbackStmt: . rollback
  124 SelectStmt: . selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | . selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  145 Statement: . EmptyStmt
  146          | . AlterTableStmt
  147          | . BeginTransactionStmt
  148          | . CommitStmt
  149          | . CreateIndexStmt
  150          | . CreateTableStmt
  151          | . DeleteFromStmt
  152          | . DropIndexStmt
  153          | . DropTableStmt
  154          | . InsertIntoStmt
  155          | . RollbackStmt
  156          | . SelectStmt
  157          | . TruncateTableStmt
  158          | . UpdateStmt
  160 StatementList: StatementList ';' . Statement
  164 TruncateTableStmt: . truncate tableKwd TableName
  189 UpdateStmt: . update TableName oSet AssignmentList UpdateStmt1

    alter      posunout a přejít do stavu 1
    begin      posunout a přejít do stavu 2
    commit     posunout a přejít do stavu 3
    create     posunout a přejít do stavu 4
    deleteKwd  posunout a přejít do stavu 5
    drop       posunout a přejít do stavu 6
    insert     posunout a přejít do stavu 7
    rollback   posunout a přejít do stavu 8
    selectKwd  posunout a přejít do stavu 9
    truncate   posunout a přejít do stavu 10
    update     posunout a přejít do stavu 11

    $výchozí  reduce using rule 41 (EmptyStmt)

    AlterTableStmt        přejít do stavu 12
    BeginTransactionStmt  přejít do stavu 13
    CommitStmt            přejít do stavu 14
    CreateIndexStmt       přejít do stavu 15
    CreateTableStmt       přejít do stavu 16
    DeleteFromStmt        přejít do stavu 17
    DropIndexStmt         přejít do stavu 18
    DropTableStmt         přejít do stavu 19
    EmptyStmt             přejít do stavu 20
    InsertIntoStmt        přejít do stavu 21
    RollbackStmt          přejít do stavu 22
    SelectStmt            přejít do stavu 23
    Statement             přejít do stavu 112
    TruncateTableStmt     přejít do stavu 26
    UpdateStmt            přejít do stavu 27


State 44

    1 AlterTableStmt: alter tableKwd TableName . add ColumnDef
    2               | alter tableKwd TableName . drop column ColumnName

    add   posunout a přejít do stavu 113
    drop  posunout a přejít do stavu 114


State 45

   29 CreateTableStmt: create tableKwd ifKwd . not exists TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'

    not  posunout a přejít do stavu 115


State 46

   28 CreateTableStmt: create tableKwd TableName . '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'

    '('  posunout a přejít do stavu 116


State 47

   22 CreateIndexStmt: create CreateIndexStmtUnique index . CreateIndexIfNotExists identifier on identifier '(' identifier ')'
   23                | create CreateIndexStmtUnique index . CreateIndexIfNotExists identifier on identifier '(' identifier '(' ')' ')'
   24 CreateIndexIfNotExists: . %empty  [identifier]
   25                       | . ifKwd not exists

    ifKwd  posunout a přejít do stavu 117

    $výchozí  reduce using rule 24 (CreateIndexIfNotExists)

    CreateIndexIfNotExists  přejít do stavu 118


State 48

   34 DeleteFromStmt: deleteKwd from TableName .  [$end, ';']
   35               | deleteKwd from TableName . WhereClause
  197 WhereClause: . where Expression

    where  posunout a přejít do stavu 119

    $výchozí  reduce using rule 34 (DeleteFromStmt)

    WhereClause  přejít do stavu 120


State 49

   38 DropIndexIfExists: ifKwd . exists

    exists  posunout a přejít do stavu 121


State 50

   36 DropIndexStmt: drop index DropIndexIfExists . identifier

    identifier  posunout a přejít do stavu 122


State 51

   40 DropTableStmt: drop tableKwd ifKwd . exists TableName

    exists  posunout a přejít do stavu 123


State 52

   39 DropTableStmt: drop tableKwd TableName .

    $výchozí  reduce using rule 39 (DropTableStmt)


State 53

   71 InsertIntoStmt: insert into TableName . InsertIntoStmt1 values '(' ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3
   72               | insert into TableName . InsertIntoStmt1 SelectStmt
   73 InsertIntoStmt1: . %empty  [selectKwd, values]
   74                | . '(' ColumnNameList ')'

    '('  posunout a přejít do stavu 124

    $výchozí  reduce using rule 73 (InsertIntoStmt1)

    InsertIntoStmt1  přejít do stavu 125


State 54

  165 Type: bigIntType .

    $výchozí  reduce using rule 165 (Type)


State 55

  166 Type: bigRatType .

    $výchozí  reduce using rule 166 (Type)


State 56

  167 Type: blobType .

    $výchozí  reduce using rule 167 (Type)


State 57

  168 Type: boolType .

    $výchozí  reduce using rule 168 (Type)


State 58

  169 Type: byteType .

    $výchozí  reduce using rule 169 (Type)


State 59

  170 Type: complex128Type .

    $výchozí  reduce using rule 170 (Type)


State 60

  171 Type: complex64Type .

    $výchozí  reduce using rule 171 (Type)


State 61

  172 Type: durationType .

    $výchozí  reduce using rule 172 (Type)


State 62

   79 Literal: falseKwd .

    $výchozí  reduce using rule 79 (Literal)


State 63

  173 Type: floatType .

    $výchozí  reduce using rule 173 (Type)


State 64

  174 Type: float32Type .

    $výchozí  reduce using rule 174 (Type)


State 65

  175 Type: float64Type .

    $výchozí  reduce using rule 175 (Type)


State 66

   82 Literal: floatLit .

    $výchozí  reduce using rule 82 (Literal)


State 67

  112 QualifiedIdent: identifier .  [$end, and, andand, andnot, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, lsh, neq, not, offset, order, oror, rsh, where, ',', '(', ')', '>', '<', '[', ']', '^', '|', '-', '+', '&', '%', '/', '*', ';', ':']
  113               | identifier . '.' identifier

    '.'  posunout a přejít do stavu 126

    $výchozí  reduce using rule 112 (QualifiedIdent)


State 68

   83 Literal: imaginaryLit .

    $výchozí  reduce using rule 83 (Literal)


State 69

  176 Type: intType .

    $výchozí  reduce using rule 176 (Type)


State 70

  177 Type: int16Type .

    $výchozí  reduce using rule 177 (Type)


State 71

  178 Type: int32Type .

    $výchozí  reduce using rule 178 (Type)


State 72

  179 Type: int64Type .

    $výchozí  reduce using rule 179 (Type)


State 73

  180 Type: int8Type .

    $výchozí  reduce using rule 180 (Type)


State 74

   84 Literal: intLit .

    $výchozí  reduce using rule 84 (Literal)


State 75

   80 Literal: null .

    $výchozí  reduce using rule 80 (Literal)


State 76

   87 Operand: qlParam .

    $výchozí  reduce using rule 87 (Operand)


State 77

  181 Type: runeType .

    $výchozí  reduce using rule 181 (Type)


State 78

  182 Type: stringType .

    $výchozí  reduce using rule 182 (Type)


State 79

   85 Literal: stringLit .

    $výchozí  reduce using rule 85 (Literal)


State 80

  183 Type: timeType .

    $výchozí  reduce using rule 183 (Type)


State 81

   81 Literal: trueKwd .

    $výchozí  reduce using rule 81 (Literal)


State 82

  184 Type: uintType .

    $výchozí  reduce using rule 184 (Type)


State 83

  185 Type: uint16Type .

    $výchozí  reduce using rule 185 (Type)


State 84

  186 Type: uint32Type .

    $výchozí  reduce using rule 186 (Type)


State 85

  187 Type: uint64Type .

    $výchozí  reduce using rule 187 (Type)


State 86

  188 Type: uint8Type .

    $výchozí  reduce using rule 188 (Type)


State 87

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   89        | '(' . Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 127
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 88

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  193 UnaryExpr: '^' . PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 128
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107


State 89

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  195 UnaryExpr: '-' . PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 129
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107


State 90

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  196 UnaryExpr: '+' . PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 130
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107


State 91

  132 SelectStmtFieldList: '*' .

    $výchozí  reduce using rule 132 (SelectStmtFieldList)


State 92

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  194 UnaryExpr: '!' . PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 131
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107


State 93

   95 PrimaryExpression: Conversion .

    $výchozí  reduce using rule 95 (PrimaryExpression)


State 94

   43 Expression: Expression . oror Term
   64 Field: Expression . Field1
   65 Field1: . %empty  [from, ',']
   66       | . as identifier

    as    posunout a přejít do stavu 132
    oror  posunout a přejít do stavu 133

    $výchozí  reduce using rule 65 (Field1)

    Field1  přejít do stavu 134


State 95

  162 Term: Factor .

    $výchozí  reduce using rule 162 (Term)


State 96

   49 Factor: Factor1 .  [$end, andand, as, asc, desc, from, group, limit, offset, order, oror, where, ',', ')', ']', ';', ':']
   50       | Factor1 . in '(' ExpressionList ')'
   51       | Factor1 . not in '(' ExpressionList ')'
   52       | Factor1 . between PrimaryFactor and PrimaryFactor
   53       | Factor1 . not between PrimaryFactor and PrimaryFactor
   54       | Factor1 . is null
   55       | Factor1 . is not null
   57 Factor1: Factor1 . ge PrimaryFactor
   58        | Factor1 . '>' PrimaryFactor
   59        | Factor1 . le PrimaryFactor
   60        | Factor1 . '<' PrimaryFactor
   61        | Factor1 . neq PrimaryFactor
   62        | Factor1 . eq PrimaryFactor
   63        | Factor1 . like PrimaryFactor

    between  posunout a přejít do stavu 135
    eq       posunout a přejít do stavu 136
    ge       posunout a přejít do stavu 137
    in       posunout a přejít do stavu 138
    is       posunout a přejít do stavu 139
    le       posunout a přejít do stavu 140
    like     posunout a přejít do stavu 141
    neq      posunout a přejít do stavu 142
    not      posunout a přejít do stavu 143
    '>'      posunout a přejít do stavu 144
    '<'      posunout a přejít do stavu 145

    $výchozí  reduce using rule 49 (Factor)


State 97

   67 FieldList: Field .

    $výchozí  reduce using rule 67 (FieldList)


State 98

   68 FieldList: FieldList . ',' Field
  133 SelectStmtFieldList: FieldList .  [from]
  134                    | FieldList . ','

    ','  posunout a přejít do stavu 146

    $výchozí  reduce using rule 133 (SelectStmtFieldList)


State 99

   86 Operand: Literal .

    $výchozí  reduce using rule 86 (Operand)


State 100

   94 PrimaryExpression: Operand .

    $výchozí  reduce using rule 94 (PrimaryExpression)


State 101

   10 Call: . '(' Call1 ')'
   70 Index: . '[' Expression ']'
   96 PrimaryExpression: PrimaryExpression . Index
   97                  | PrimaryExpression . Slice
   98                  | PrimaryExpression . Call
  141 Slice: . '[' ':' ']'
  142      | . '[' ':' Expression ']'
  143      | . '[' Expression ':' ']'
  144      | . '[' Expression ':' Expression ']'
  192 UnaryExpr: PrimaryExpression .  [$end, and, andand, andnot, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, lsh, neq, not, offset, order, oror, rsh, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', '&', '%', '/', '*', ';', ':']

    '('  posunout a přejít do stavu 147
    '['  posunout a přejít do stavu 148

    $výchozí  reduce using rule 192 (UnaryExpr)

    Call   přejít do stavu 149
    Index  přejít do stavu 150
    Slice  přejít do stavu 151


State 102

   56 Factor1: PrimaryFactor .  [$end, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 56 (Factor1)


State 103

   99 PrimaryFactor: PrimaryTerm .  [$end, and, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', ';', ':']
  105 PrimaryTerm: PrimaryTerm . andnot UnaryExpr
  106            | PrimaryTerm . '&' UnaryExpr
  107            | PrimaryTerm . lsh UnaryExpr
  108            | PrimaryTerm . rsh UnaryExpr
  109            | PrimaryTerm . '%' UnaryExpr
  110            | PrimaryTerm . '/' UnaryExpr
  111            | PrimaryTerm . '*' UnaryExpr

    andnot  posunout a přejít do stavu 156
    lsh     posunout a přejít do stavu 157
    rsh     posunout a přejít do stavu 158
    '&'     posunout a přejít do stavu 159
    '%'     posunout a přejít do stavu 160
    '/'     posunout a přejít do stavu 161
    '*'     posunout a přejít do stavu 162

    $výchozí  reduce using rule 99 (PrimaryFactor)


State 104

   88 Operand: QualifiedIdent .

    $výchozí  reduce using rule 88 (Operand)


State 105

  124 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList . from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | selectKwd SelectStmtDistinct SelectStmtFieldList . from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset

    from  posunout a přejít do stavu 163


State 106

   42 Expression: Term .  [$end, as, asc, desc, from, group, limit, offset, order, oror, where, ',', ')', ']', ';', ':']
  163 Term: Term . andand Factor

    andand  posunout a přejít do stavu 164

    $výchozí  reduce using rule 42 (Expression)


State 107

   21 Conversion: Type . '(' Expression ')'

    '('  posunout a přejít do stavu 165


State 108

  104 PrimaryTerm: UnaryExpr .

    $výchozí  reduce using rule 104 (PrimaryTerm)


State 109

  164 TruncateTableStmt: truncate tableKwd TableName .

    $výchozí  reduce using rule 164 (TruncateTableStmt)


State 110

  199 oSet: set .

    $výchozí  reduce using rule 199 (oSet)


State 111

    3 Assignment: . ColumnName '=' Expression
    4 AssignmentList: . Assignment AssignmentList1 AssignmentList2
   14 ColumnName: . identifier
  189 UpdateStmt: update TableName oSet . AssignmentList UpdateStmt1

    identifier  posunout a přejít do stavu 166

    Assignment      přejít do stavu 167
    AssignmentList  přejít do stavu 168
    ColumnName      přejít do stavu 169


State 112

  160 StatementList: StatementList ';' Statement .

    $výchozí  reduce using rule 160 (StatementList)


State 113

    1 AlterTableStmt: alter tableKwd TableName add . ColumnDef
   13 ColumnDef: . ColumnName Type
   14 ColumnName: . identifier

    identifier  posunout a přejít do stavu 166

    ColumnDef   přejít do stavu 170
    ColumnName  přejít do stavu 171


State 114

    2 AlterTableStmt: alter tableKwd TableName drop . column ColumnName

    column  posunout a přejít do stavu 172


State 115

   29 CreateTableStmt: create tableKwd ifKwd not . exists TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'

    exists  posunout a přejít do stavu 173


State 116

   13 ColumnDef: . ColumnName Type
   14 ColumnName: . identifier
   28 CreateTableStmt: create tableKwd TableName '(' . ColumnDef CreateTableStmt1 CreateTableStmt2 ')'

    identifier  posunout a přejít do stavu 166

    ColumnDef   přejít do stavu 174
    ColumnName  přejít do stavu 171


State 117

   25 CreateIndexIfNotExists: ifKwd . not exists

    not  posunout a přejít do stavu 175


State 118

   22 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists . identifier on identifier '(' identifier ')'
   23                | create CreateIndexStmtUnique index CreateIndexIfNotExists . identifier on identifier '(' identifier '(' ')' ')'

    identifier  posunout a přejít do stavu 176


State 119

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression
  197 WhereClause: where . Expression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 177
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 120

   35 DeleteFromStmt: deleteKwd from TableName WhereClause .

    $výchozí  reduce using rule 35 (DeleteFromStmt)


State 121

   38 DropIndexIfExists: ifKwd exists .

    $výchozí  reduce using rule 38 (DropIndexIfExists)


State 122

   36 DropIndexStmt: drop index DropIndexIfExists identifier .

    $výchozí  reduce using rule 36 (DropIndexStmt)


State 123

   40 DropTableStmt: drop tableKwd ifKwd exists . TableName
  161 TableName: . identifier

    identifier  posunout a přejít do stavu 40

    TableName  přejít do stavu 178


State 124

   14 ColumnName: . identifier
   15 ColumnNameList: . ColumnName ColumnNameList1 ColumnNameList2
   74 InsertIntoStmt1: '(' . ColumnNameList ')'

    identifier  posunout a přejít do stavu 166

    ColumnName      přejít do stavu 179
    ColumnNameList  přejít do stavu 180


State 125

   71 InsertIntoStmt: insert into TableName InsertIntoStmt1 . values '(' ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3
   72               | insert into TableName InsertIntoStmt1 . SelectStmt
  124 SelectStmt: . selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | . selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset

    selectKwd  posunout a přejít do stavu 9
    values     posunout a přejít do stavu 181

    SelectStmt  přejít do stavu 182


State 126

  113 QualifiedIdent: identifier '.' . identifier

    identifier  posunout a přejít do stavu 183


State 127

   43 Expression: Expression . oror Term
   89 Operand: '(' Expression . ')'

    oror  posunout a přejít do stavu 133
    ')'   posunout a přejít do stavu 184


State 128

   10 Call: . '(' Call1 ')'
   70 Index: . '[' Expression ']'
   96 PrimaryExpression: PrimaryExpression . Index
   97                  | PrimaryExpression . Slice
   98                  | PrimaryExpression . Call
  141 Slice: . '[' ':' ']'
  142      | . '[' ':' Expression ']'
  143      | . '[' Expression ':' ']'
  144      | . '[' Expression ':' Expression ']'
  193 UnaryExpr: '^' PrimaryExpression .  [$end, and, andand, andnot, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, lsh, neq, not, offset, order, oror, rsh, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', '&', '%', '/', '*', ';', ':']

    '('  posunout a přejít do stavu 147
    '['  posunout a přejít do stavu 148

    $výchozí  reduce using rule 193 (UnaryExpr)

    Call   přejít do stavu 149
    Index  přejít do stavu 150
    Slice  přejít do stavu 151


State 129

   10 Call: . '(' Call1 ')'
   70 Index: . '[' Expression ']'
   96 PrimaryExpression: PrimaryExpression . Index
   97                  | PrimaryExpression . Slice
   98                  | PrimaryExpression . Call
  141 Slice: . '[' ':' ']'
  142      | . '[' ':' Expression ']'
  143      | . '[' Expression ':' ']'
  144      | . '[' Expression ':' Expression ']'
  195 UnaryExpr: '-' PrimaryExpression .  [$end, and, andand, andnot, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, lsh, neq, not, offset, order, oror, rsh, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', '&', '%', '/', '*', ';', ':']

    '('  posunout a přejít do stavu 147
    '['  posunout a přejít do stavu 148

    $výchozí  reduce using rule 195 (UnaryExpr)

    Call   přejít do stavu 149
    Index  přejít do stavu 150
    Slice  přejít do stavu 151


State 130

   10 Call: . '(' Call1 ')'
   70 Index: . '[' Expression ']'
   96 PrimaryExpression: PrimaryExpression . Index
   97                  | PrimaryExpression . Slice
   98                  | PrimaryExpression . Call
  141 Slice: . '[' ':' ']'
  142      | . '[' ':' Expression ']'
  143      | . '[' Expression ':' ']'
  144      | . '[' Expression ':' Expression ']'
  196 UnaryExpr: '+' PrimaryExpression .  [$end, and, andand, andnot, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, lsh, neq, not, offset, order, oror, rsh, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', '&', '%', '/', '*', ';', ':']

    '('  posunout a přejít do stavu 147
    '['  posunout a přejít do stavu 148

    $výchozí  reduce using rule 196 (UnaryExpr)

    Call   přejít do stavu 149
    Index  přejít do stavu 150
    Slice  přejít do stavu 151


State 131

   10 Call: . '(' Call1 ')'
   70 Index: . '[' Expression ']'
   96 PrimaryExpression: PrimaryExpression . Index
   97                  | PrimaryExpression . Slice
   98                  | PrimaryExpression . Call
  141 Slice: . '[' ':' ']'
  142      | . '[' ':' Expression ']'
  143      | . '[' Expression ':' ']'
  144      | . '[' Expression ':' Expression ']'
  194 UnaryExpr: '!' PrimaryExpression .  [$end, and, andand, andnot, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, lsh, neq, not, offset, order, oror, rsh, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', '&', '%', '/', '*', ';', ':']

    '('  posunout a přejít do stavu 147
    '['  posunout a přejít do stavu 148

    $výchozí  reduce using rule 194 (UnaryExpr)

    Call   přejít do stavu 149
    Index  přejít do stavu 150
    Slice  přejít do stavu 151


State 132

   66 Field1: as . identifier

    identifier  posunout a přejít do stavu 185


State 133

   21 Conversion: . Type '(' Expression ')'
   43 Expression: Expression oror . Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 186
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 134

   64 Field: Expression Field1 .

    $výchozí  reduce using rule 64 (Field)


State 135

   21 Conversion: . Type '(' Expression ')'
   52 Factor: Factor1 between . PrimaryFactor and PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 187
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 136

   21 Conversion: . Type '(' Expression ')'
   62 Factor1: Factor1 eq . PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 188
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 137

   21 Conversion: . Type '(' Expression ')'
   57 Factor1: Factor1 ge . PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 189
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 138

   50 Factor: Factor1 in . '(' ExpressionList ')'

    '('  posunout a přejít do stavu 190


State 139

   54 Factor: Factor1 is . null
   55       | Factor1 is . not null

    not   posunout a přejít do stavu 191
    null  posunout a přejít do stavu 192


State 140

   21 Conversion: . Type '(' Expression ')'
   59 Factor1: Factor1 le . PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 193
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 141

   21 Conversion: . Type '(' Expression ')'
   63 Factor1: Factor1 like . PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 194
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 142

   21 Conversion: . Type '(' Expression ')'
   61 Factor1: Factor1 neq . PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 195
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 143

   51 Factor: Factor1 not . in '(' ExpressionList ')'
   53       | Factor1 not . between PrimaryFactor and PrimaryFactor

    between  posunout a přejít do stavu 196
    in       posunout a přejít do stavu 197


State 144

   21 Conversion: . Type '(' Expression ')'
   58 Factor1: Factor1 '>' . PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 198
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 145

   21 Conversion: . Type '(' Expression ')'
   60 Factor1: Factor1 '<' . PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 199
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 146

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   64 Field: . Expression Field1
   68 FieldList: FieldList ',' . Field
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  134 SelectStmtFieldList: FieldList ',' .  [from]
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    $výchozí  reduce using rule 134 (SelectStmtFieldList)

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 94
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Field              přejít do stavu 200
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 147

   10 Call: '(' . Call1 ')'
   11 Call1: . %empty  [')']
   12      | . ExpressionList
   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   44 ExpressionList: . Expression ExpressionList1 ExpressionList2
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    $výchozí  reduce using rule 11 (Call1)

    Call1              přejít do stavu 201
    Conversion         přejít do stavu 93
    Expression         přejít do stavu 202
    ExpressionList     přejít do stavu 203
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 148

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   70 Index: '[' . Expression ']'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  141 Slice: '[' . ':' ']'
  142      | '[' . ':' Expression ']'
  143      | '[' . Expression ':' ']'
  144      | '[' . Expression ':' Expression ']'
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    ':'             posunout a přejít do stavu 204
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 205
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 149

   98 PrimaryExpression: PrimaryExpression Call .

    $výchozí  reduce using rule 98 (PrimaryExpression)


State 150

   96 PrimaryExpression: PrimaryExpression Index .

    $výchozí  reduce using rule 96 (PrimaryExpression)


State 151

   97 PrimaryExpression: PrimaryExpression Slice .

    $výchozí  reduce using rule 97 (PrimaryExpression)


State 152

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  100 PrimaryFactor: PrimaryFactor '^' . PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryTerm        přejít do stavu 206
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 153

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  101 PrimaryFactor: PrimaryFactor '|' . PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryTerm        přejít do stavu 207
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 154

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  102 PrimaryFactor: PrimaryFactor '-' . PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryTerm        přejít do stavu 208
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 155

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  103 PrimaryFactor: PrimaryFactor '+' . PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryTerm        přejít do stavu 209
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 156

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  105 PrimaryTerm: PrimaryTerm andnot . UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 210


State 157

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  107 PrimaryTerm: PrimaryTerm lsh . UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 211


State 158

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  108 PrimaryTerm: PrimaryTerm rsh . UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 212


State 159

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  106 PrimaryTerm: PrimaryTerm '&' . UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 213


State 160

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  109 PrimaryTerm: PrimaryTerm '%' . UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 214


State 161

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  110 PrimaryTerm: PrimaryTerm '/' . UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 215


State 162

   21 Conversion: . Type '(' Expression ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
  111 PrimaryTerm: PrimaryTerm '*' . UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 216


State 163

  114 RecordSet: . RecordSet1 RecordSet2
  115 RecordSet1: . identifier
  116           | . '(' SelectStmt RecordSet11 ')'
  121 RecordSetList: . RecordSet
  122              | . RecordSetList ',' RecordSet
  124 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from . RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | selectKwd SelectStmtDistinct SelectStmtFieldList from . RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset

    identifier  posunout a přejít do stavu 217
    '('         posunout a přejít do stavu 218

    RecordSet      přejít do stavu 219
    RecordSet1     přejít do stavu 220
    RecordSetList  přejít do stavu 221


State 164

   21 Conversion: . Type '(' Expression ')'
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  163 Term: Term andand . Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Factor             přejít do stavu 222
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 165

   21 Conversion: . Type '(' Expression ')'
   21           | Type '(' . Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 223
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 166

   14 ColumnName: identifier .

    $výchozí  reduce using rule 14 (ColumnName)


State 167

    4 AssignmentList: Assignment . AssignmentList1 AssignmentList2
    5 AssignmentList1: . %empty
    6                | . AssignmentList1 ',' Assignment

    $výchozí  reduce using rule 5 (AssignmentList1)

    AssignmentList1  přejít do stavu 224


State 168

  189 UpdateStmt: update TableName oSet AssignmentList . UpdateStmt1
  190 UpdateStmt1: . %empty  [$end, ';']
  191            | . WhereClause
  197 WhereClause: . where Expression

    where  posunout a přejít do stavu 119

    $výchozí  reduce using rule 190 (UpdateStmt1)

    UpdateStmt1  přejít do stavu 225
    WhereClause  přejít do stavu 226


State 169

    3 Assignment: ColumnName . '=' Expression

    '='  posunout a přejít do stavu 227


State 170

    1 AlterTableStmt: alter tableKwd TableName add ColumnDef .

    $výchozí  reduce using rule 1 (AlterTableStmt)


State 171

   13 ColumnDef: ColumnName . Type
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    timeType        posunout a přejít do stavu 80
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86

    Type  přejít do stavu 228


State 172

    2 AlterTableStmt: alter tableKwd TableName drop column . ColumnName
   14 ColumnName: . identifier

    identifier  posunout a přejít do stavu 166

    ColumnName  přejít do stavu 229


State 173

   29 CreateTableStmt: create tableKwd ifKwd not exists . TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'
  161 TableName: . identifier

    identifier  posunout a přejít do stavu 40

    TableName  přejít do stavu 230


State 174

   28 CreateTableStmt: create tableKwd TableName '(' ColumnDef . CreateTableStmt1 CreateTableStmt2 ')'
   30 CreateTableStmt1: . %empty
   31                 | . CreateTableStmt1 ',' ColumnDef

    $výchozí  reduce using rule 30 (CreateTableStmt1)

    CreateTableStmt1  přejít do stavu 231


State 175

   25 CreateIndexIfNotExists: ifKwd not . exists

    exists  posunout a přejít do stavu 232


State 176

   22 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier . on identifier '(' identifier ')'
   23                | create CreateIndexStmtUnique index CreateIndexIfNotExists identifier . on identifier '(' identifier '(' ')' ')'

    on  posunout a přejít do stavu 233


State 177

   43 Expression: Expression . oror Term
  197 WhereClause: where Expression .  [$end, group, limit, offset, order, ')', ';']

    oror  posunout a přejít do stavu 133

    $výchozí  reduce using rule 197 (WhereClause)


State 178

   40 DropTableStmt: drop tableKwd ifKwd exists TableName .

    $výchozí  reduce using rule 40 (DropTableStmt)


State 179

   15 ColumnNameList: ColumnName . ColumnNameList1 ColumnNameList2
   16 ColumnNameList1: . %empty
   17                | . ColumnNameList1 ',' ColumnName

    $výchozí  reduce using rule 16 (ColumnNameList1)

    ColumnNameList1  přejít do stavu 234


State 180

   74 InsertIntoStmt1: '(' ColumnNameList . ')'

    ')'  posunout a přejít do stavu 235


State 181

   71 InsertIntoStmt: insert into TableName InsertIntoStmt1 values . '(' ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3

    '('  posunout a přejít do stavu 236


State 182

   72 InsertIntoStmt: insert into TableName InsertIntoStmt1 SelectStmt .

    $výchozí  reduce using rule 72 (InsertIntoStmt)


State 183

  113 QualifiedIdent: identifier '.' identifier .

    $výchozí  reduce using rule 113 (QualifiedIdent)


State 184

   89 Operand: '(' Expression ')' .

    $výchozí  reduce using rule 89 (Operand)


State 185

   66 Field1: as identifier .

    $výchozí  reduce using rule 66 (Field1)


State 186

   43 Expression: Expression oror Term .  [$end, as, asc, desc, from, group, limit, offset, order, oror, where, ',', ')', ']', ';', ':']
  163 Term: Term . andand Factor

    andand  posunout a přejít do stavu 164

    $výchozí  reduce using rule 43 (Expression)


State 187

   52 Factor: Factor1 between PrimaryFactor . and PrimaryFactor
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    and  posunout a přejít do stavu 237
    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155


State 188

   62 Factor1: Factor1 eq PrimaryFactor .  [$end, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 62 (Factor1)


State 189

   57 Factor1: Factor1 ge PrimaryFactor .  [$end, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 57 (Factor1)


State 190

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   44 ExpressionList: . Expression ExpressionList1 ExpressionList2
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   50       | Factor1 in '(' . ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 202
    ExpressionList     přejít do stavu 238
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 191

   55 Factor: Factor1 is not . null

    null  posunout a přejít do stavu 239


State 192

   54 Factor: Factor1 is null .

    $výchozí  reduce using rule 54 (Factor)


State 193

   59 Factor1: Factor1 le PrimaryFactor .  [$end, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 59 (Factor1)


State 194

   63 Factor1: Factor1 like PrimaryFactor .  [$end, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 63 (Factor1)


State 195

   61 Factor1: Factor1 neq PrimaryFactor .  [$end, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 61 (Factor1)


State 196

   21 Conversion: . Type '(' Expression ')'
   53 Factor: Factor1 not between . PrimaryFactor and PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 240
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 197

   51 Factor: Factor1 not in . '(' ExpressionList ')'

    '('  posunout a přejít do stavu 241


State 198

   58 Factor1: Factor1 '>' PrimaryFactor .  [$end, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 58 (Factor1)


State 199

   60 Factor1: Factor1 '<' PrimaryFactor .  [$end, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 60 (Factor1)


State 200

   68 FieldList: FieldList ',' Field .

    $výchozí  reduce using rule 68 (FieldList)


State 201

   10 Call: '(' Call1 . ')'

    ')'  posunout a přejít do stavu 242


State 202

   43 Expression: Expression . oror Term
   44 ExpressionList: Expression . ExpressionList1 ExpressionList2
   45 ExpressionList1: . %empty  [$end, asc, desc, limit, offset, ',', ')', ';']
   46                | . ExpressionList1 ',' Expression

    oror  posunout a přejít do stavu 133

    $výchozí  reduce using rule 45 (ExpressionList1)

    ExpressionList1  přejít do stavu 243


State 203

   12 Call1: ExpressionList .

    $výchozí  reduce using rule 12 (Call1)


State 204

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  141 Slice: '[' ':' . ']'
  142      | '[' ':' . Expression ']'
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    ']'             posunout a přejít do stavu 244
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 245
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 205

   43 Expression: Expression . oror Term
   70 Index: '[' Expression . ']'
  143 Slice: '[' Expression . ':' ']'
  144      | '[' Expression . ':' Expression ']'

    oror  posunout a přejít do stavu 133
    ']'   posunout a přejít do stavu 246
    ':'   posunout a přejít do stavu 247


State 206

  100 PrimaryFactor: PrimaryFactor '^' PrimaryTerm .  [$end, and, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', ';', ':']
  105 PrimaryTerm: PrimaryTerm . andnot UnaryExpr
  106            | PrimaryTerm . '&' UnaryExpr
  107            | PrimaryTerm . lsh UnaryExpr
  108            | PrimaryTerm . rsh UnaryExpr
  109            | PrimaryTerm . '%' UnaryExpr
  110            | PrimaryTerm . '/' UnaryExpr
  111            | PrimaryTerm . '*' UnaryExpr

    andnot  posunout a přejít do stavu 156
    lsh     posunout a přejít do stavu 157
    rsh     posunout a přejít do stavu 158
    '&'     posunout a přejít do stavu 159
    '%'     posunout a přejít do stavu 160
    '/'     posunout a přejít do stavu 161
    '*'     posunout a přejít do stavu 162

    $výchozí  reduce using rule 100 (PrimaryFactor)


State 207

  101 PrimaryFactor: PrimaryFactor '|' PrimaryTerm .  [$end, and, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', ';', ':']
  105 PrimaryTerm: PrimaryTerm . andnot UnaryExpr
  106            | PrimaryTerm . '&' UnaryExpr
  107            | PrimaryTerm . lsh UnaryExpr
  108            | PrimaryTerm . rsh UnaryExpr
  109            | PrimaryTerm . '%' UnaryExpr
  110            | PrimaryTerm . '/' UnaryExpr
  111            | PrimaryTerm . '*' UnaryExpr

    andnot  posunout a přejít do stavu 156
    lsh     posunout a přejít do stavu 157
    rsh     posunout a přejít do stavu 158
    '&'     posunout a přejít do stavu 159
    '%'     posunout a přejít do stavu 160
    '/'     posunout a přejít do stavu 161
    '*'     posunout a přejít do stavu 162

    $výchozí  reduce using rule 101 (PrimaryFactor)


State 208

  102 PrimaryFactor: PrimaryFactor '-' PrimaryTerm .  [$end, and, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', ';', ':']
  105 PrimaryTerm: PrimaryTerm . andnot UnaryExpr
  106            | PrimaryTerm . '&' UnaryExpr
  107            | PrimaryTerm . lsh UnaryExpr
  108            | PrimaryTerm . rsh UnaryExpr
  109            | PrimaryTerm . '%' UnaryExpr
  110            | PrimaryTerm . '/' UnaryExpr
  111            | PrimaryTerm . '*' UnaryExpr

    andnot  posunout a přejít do stavu 156
    lsh     posunout a přejít do stavu 157
    rsh     posunout a přejít do stavu 158
    '&'     posunout a přejít do stavu 159
    '%'     posunout a přejít do stavu 160
    '/'     posunout a přejít do stavu 161
    '*'     posunout a přejít do stavu 162

    $výchozí  reduce using rule 102 (PrimaryFactor)


State 209

  103 PrimaryFactor: PrimaryFactor '+' PrimaryTerm .  [$end, and, andand, as, asc, between, desc, eq, from, ge, group, in, is, le, like, limit, neq, not, offset, order, oror, where, ',', ')', '>', '<', ']', '^', '|', '-', '+', ';', ':']
  105 PrimaryTerm: PrimaryTerm . andnot UnaryExpr
  106            | PrimaryTerm . '&' UnaryExpr
  107            | PrimaryTerm . lsh UnaryExpr
  108            | PrimaryTerm . rsh UnaryExpr
  109            | PrimaryTerm . '%' UnaryExpr
  110            | PrimaryTerm . '/' UnaryExpr
  111            | PrimaryTerm . '*' UnaryExpr

    andnot  posunout a přejít do stavu 156
    lsh     posunout a přejít do stavu 157
    rsh     posunout a přejít do stavu 158
    '&'     posunout a přejít do stavu 159
    '%'     posunout a přejít do stavu 160
    '/'     posunout a přejít do stavu 161
    '*'     posunout a přejít do stavu 162

    $výchozí  reduce using rule 103 (PrimaryFactor)


State 210

  105 PrimaryTerm: PrimaryTerm andnot UnaryExpr .

    $výchozí  reduce using rule 105 (PrimaryTerm)


State 211

  107 PrimaryTerm: PrimaryTerm lsh UnaryExpr .

    $výchozí  reduce using rule 107 (PrimaryTerm)


State 212

  108 PrimaryTerm: PrimaryTerm rsh UnaryExpr .

    $výchozí  reduce using rule 108 (PrimaryTerm)


State 213

  106 PrimaryTerm: PrimaryTerm '&' UnaryExpr .

    $výchozí  reduce using rule 106 (PrimaryTerm)


State 214

  109 PrimaryTerm: PrimaryTerm '%' UnaryExpr .

    $výchozí  reduce using rule 109 (PrimaryTerm)


State 215

  110 PrimaryTerm: PrimaryTerm '/' UnaryExpr .

    $výchozí  reduce using rule 110 (PrimaryTerm)


State 216

  111 PrimaryTerm: PrimaryTerm '*' UnaryExpr .

    $výchozí  reduce using rule 111 (PrimaryTerm)


State 217

  115 RecordSet1: identifier .

    $výchozí  reduce using rule 115 (RecordSet1)


State 218

  116 RecordSet1: '(' . SelectStmt RecordSet11 ')'
  124 SelectStmt: . selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | . selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset

    selectKwd  posunout a přejít do stavu 9

    SelectStmt  přejít do stavu 248


State 219

  121 RecordSetList: RecordSet .

    $výchozí  reduce using rule 121 (RecordSetList)


State 220

  114 RecordSet: RecordSet1 . RecordSet2
  119 RecordSet2: . %empty  [$end, group, limit, offset, order, where, ',', ')', ';']
  120           | . as identifier

    as  posunout a přejít do stavu 249

    $výchozí  reduce using rule 119 (RecordSet2)

    RecordSet2  přejít do stavu 250


State 221

  122 RecordSetList: RecordSetList . ',' RecordSet
  124 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList . SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  125           | selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList . ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  135 SelectStmtWhere: . %empty  [$end, group, limit, offset, order, ')', ';']
  136                | . WhereClause
  197 WhereClause: . where Expression

    where  posunout a přejít do stavu 119
    ','    posunout a přejít do stavu 251

    $výchozí  reduce using rule 135 (SelectStmtWhere)

    SelectStmtWhere  přejít do stavu 252
    WhereClause      přejít do stavu 253


State 222

  163 Term: Term andand Factor .

    $výchozí  reduce using rule 163 (Term)


State 223

   21 Conversion: Type '(' Expression . ')'
   43 Expression: Expression . oror Term

    oror  posunout a přejít do stavu 133
    ')'   posunout a přejít do stavu 254


State 224

    4 AssignmentList: Assignment AssignmentList1 . AssignmentList2
    6 AssignmentList1: AssignmentList1 . ',' Assignment
    7 AssignmentList2: . %empty  [$end, where, ';']
    8                | . ','

    ','  posunout a přejít do stavu 255

    $výchozí  reduce using rule 7 (AssignmentList2)

    AssignmentList2  přejít do stavu 256


State 225

  189 UpdateStmt: update TableName oSet AssignmentList UpdateStmt1 .

    $výchozí  reduce using rule 189 (UpdateStmt)


State 226

  191 UpdateStmt1: WhereClause .

    $výchozí  reduce using rule 191 (UpdateStmt1)


State 227

    3 Assignment: ColumnName '=' . Expression
   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 257
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 228

   13 ColumnDef: ColumnName Type .

    $výchozí  reduce using rule 13 (ColumnDef)


State 229

    2 AlterTableStmt: alter tableKwd TableName drop column ColumnName .

    $výchozí  reduce using rule 2 (AlterTableStmt)


State 230

   29 CreateTableStmt: create tableKwd ifKwd not exists TableName . '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')'

    '('  posunout a přejít do stavu 258


State 231

   28 CreateTableStmt: create tableKwd TableName '(' ColumnDef CreateTableStmt1 . CreateTableStmt2 ')'
   31 CreateTableStmt1: CreateTableStmt1 . ',' ColumnDef
   32 CreateTableStmt2: . %empty  [')']
   33                 | . ','

    ','  posunout a přejít do stavu 259

    $výchozí  reduce using rule 32 (CreateTableStmt2)

    CreateTableStmt2  přejít do stavu 260


State 232

   25 CreateIndexIfNotExists: ifKwd not exists .

    $výchozí  reduce using rule 25 (CreateIndexIfNotExists)


State 233

   22 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on . identifier '(' identifier ')'
   23                | create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on . identifier '(' identifier '(' ')' ')'

    identifier  posunout a přejít do stavu 261


State 234

   15 ColumnNameList: ColumnName ColumnNameList1 . ColumnNameList2
   17 ColumnNameList1: ColumnNameList1 . ',' ColumnName
   18 ColumnNameList2: . %empty  [$end, limit, offset, order, ')', ';']
   19                | . ','

    ','  posunout a přejít do stavu 262

    $výchozí  reduce using rule 18 (ColumnNameList2)

    ColumnNameList2  přejít do stavu 263


State 235

   74 InsertIntoStmt1: '(' ColumnNameList ')' .

    $výchozí  reduce using rule 74 (InsertIntoStmt1)


State 236

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   44 ExpressionList: . Expression ExpressionList1 ExpressionList2
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   71 InsertIntoStmt: insert into TableName InsertIntoStmt1 values '(' . ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 202
    ExpressionList     přejít do stavu 264
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 237

   21 Conversion: . Type '(' Expression ')'
   52 Factor: Factor1 between PrimaryFactor and . PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 265
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 238

   50 Factor: Factor1 in '(' ExpressionList . ')'

    ')'  posunout a přejít do stavu 266


State 239

   55 Factor: Factor1 is not null .

    $výchozí  reduce using rule 55 (Factor)


State 240

   53 Factor: Factor1 not between PrimaryFactor . and PrimaryFactor
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    and  posunout a přejít do stavu 267
    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155


State 241

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   44 ExpressionList: . Expression ExpressionList1 ExpressionList2
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   51       | Factor1 not in '(' . ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 202
    ExpressionList     přejít do stavu 268
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 242

   10 Call: '(' Call1 ')' .

    $výchozí  reduce using rule 10 (Call)


State 243

   44 ExpressionList: Expression ExpressionList1 . ExpressionList2
   46 ExpressionList1: ExpressionList1 . ',' Expression
   47 ExpressionList2: . %empty  [$end, asc, desc, limit, offset, ')', ';']
   48                | . ','

    ','  posunout a přejít do stavu 269

    $výchozí  reduce using rule 47 (ExpressionList2)

    ExpressionList2  přejít do stavu 270


State 244

  141 Slice: '[' ':' ']' .

    $výchozí  reduce using rule 141 (Slice)


State 245

   43 Expression: Expression . oror Term
  142 Slice: '[' ':' Expression . ']'

    oror  posunout a přejít do stavu 133
    ']'   posunout a přejít do stavu 271


State 246

   70 Index: '[' Expression ']' .

    $výchozí  reduce using rule 70 (Index)


State 247

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  143 Slice: '[' Expression ':' . ']'
  144      | '[' Expression ':' . Expression ']'
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    ']'             posunout a přejít do stavu 272
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 273
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 248

  116 RecordSet1: '(' SelectStmt . RecordSet11 ')'
  117 RecordSet11: . %empty  [')']
  118            | . ';'

    ';'  posunout a přejít do stavu 274

    $výchozí  reduce using rule 117 (RecordSet11)

    RecordSet11  přejít do stavu 275


State 249

  120 RecordSet2: as . identifier

    identifier  posunout a přejít do stavu 276


State 250

  114 RecordSet: RecordSet1 RecordSet2 .

    $výchozí  reduce using rule 114 (RecordSet)


State 251

  114 RecordSet: . RecordSet1 RecordSet2
  115 RecordSet1: . identifier
  116           | . '(' SelectStmt RecordSet11 ')'
  122 RecordSetList: RecordSetList ',' . RecordSet
  125 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' . SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  135 SelectStmtWhere: . %empty  [$end, group, limit, offset, order, ')', ';']
  136                | . WhereClause
  197 WhereClause: . where Expression

    identifier  posunout a přejít do stavu 217
    where       posunout a přejít do stavu 119
    '('         posunout a přejít do stavu 218

    $výchozí  reduce using rule 135 (SelectStmtWhere)

    RecordSet        přejít do stavu 277
    RecordSet1       přejít do stavu 220
    SelectStmtWhere  přejít do stavu 278
    WhereClause      přejít do stavu 253


State 252

   69 GroupByClause: . group by ColumnNameList
  124 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere . SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  137 SelectStmtGroup: . %empty  [$end, limit, offset, order, ')', ';']
  138                | . GroupByClause

    group  posunout a přejít do stavu 279

    $výchozí  reduce using rule 137 (SelectStmtGroup)

    GroupByClause    přejít do stavu 280
    SelectStmtGroup  přejít do stavu 281


State 253

  136 SelectStmtWhere: WhereClause .

    $výchozí  reduce using rule 136 (SelectStmtWhere)


State 254

   21 Conversion: Type '(' Expression ')' .

    $výchozí  reduce using rule 21 (Conversion)


State 255

    3 Assignment: . ColumnName '=' Expression
    6 AssignmentList1: AssignmentList1 ',' . Assignment
    8 AssignmentList2: ',' .  [$end, where, ';']
   14 ColumnName: . identifier

    identifier  posunout a přejít do stavu 166

    $výchozí  reduce using rule 8 (AssignmentList2)

    Assignment  přejít do stavu 282
    ColumnName  přejít do stavu 169


State 256

    4 AssignmentList: Assignment AssignmentList1 AssignmentList2 .

    $výchozí  reduce using rule 4 (AssignmentList)


State 257

    3 Assignment: ColumnName '=' Expression .  [$end, where, ',', ';']
   43 Expression: Expression . oror Term

    oror  posunout a přejít do stavu 133

    $výchozí  reduce using rule 3 (Assignment)


State 258

   13 ColumnDef: . ColumnName Type
   14 ColumnName: . identifier
   29 CreateTableStmt: create tableKwd ifKwd not exists TableName '(' . ColumnDef CreateTableStmt1 CreateTableStmt2 ')'

    identifier  posunout a přejít do stavu 166

    ColumnDef   přejít do stavu 283
    ColumnName  přejít do stavu 171


State 259

   13 ColumnDef: . ColumnName Type
   14 ColumnName: . identifier
   31 CreateTableStmt1: CreateTableStmt1 ',' . ColumnDef
   33 CreateTableStmt2: ',' .  [')']

    identifier  posunout a přejít do stavu 166

    $výchozí  reduce using rule 33 (CreateTableStmt2)

    ColumnDef   přejít do stavu 284
    ColumnName  přejít do stavu 171


State 260

   28 CreateTableStmt: create tableKwd TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 . ')'

    ')'  posunout a přejít do stavu 285


State 261

   22 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier . '(' identifier ')'
   23                | create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier . '(' identifier '(' ')' ')'

    '('  posunout a přejít do stavu 286


State 262

   14 ColumnName: . identifier
   17 ColumnNameList1: ColumnNameList1 ',' . ColumnName
   19 ColumnNameList2: ',' .  [$end, limit, offset, order, ')', ';']

    identifier  posunout a přejít do stavu 166

    $výchozí  reduce using rule 19 (ColumnNameList2)

    ColumnName  přejít do stavu 287


State 263

   15 ColumnNameList: ColumnName ColumnNameList1 ColumnNameList2 .

    $výchozí  reduce using rule 15 (ColumnNameList)


State 264

   71 InsertIntoStmt: insert into TableName InsertIntoStmt1 values '(' ExpressionList . ')' InsertIntoStmt2 InsertIntoStmt3

    ')'  posunout a přejít do stavu 288


State 265

   52 Factor: Factor1 between PrimaryFactor and PrimaryFactor .  [$end, andand, as, asc, desc, from, group, limit, offset, order, oror, where, ',', ')', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 52 (Factor)


State 266

   50 Factor: Factor1 in '(' ExpressionList ')' .

    $výchozí  reduce using rule 50 (Factor)


State 267

   21 Conversion: . Type '(' Expression ')'
   53 Factor: Factor1 not between PrimaryFactor and . PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 289
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 268

   51 Factor: Factor1 not in '(' ExpressionList . ')'

    ')'  posunout a přejít do stavu 290


State 269

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   46 ExpressionList1: ExpressionList1 ',' . Expression
   48 ExpressionList2: ',' .  [$end, asc, desc, limit, offset, ')', ';']
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    $výchozí  reduce using rule 48 (ExpressionList2)

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 291
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 270

   44 ExpressionList: Expression ExpressionList1 ExpressionList2 .

    $výchozí  reduce using rule 44 (ExpressionList)


State 271

  142 Slice: '[' ':' Expression ']' .

    $výchozí  reduce using rule 142 (Slice)


State 272

  143 Slice: '[' Expression ':' ']' .

    $výchozí  reduce using rule 143 (Slice)


State 273

   43 Expression: Expression . oror Term
  144 Slice: '[' Expression ':' Expression . ']'

    oror  posunout a přejít do stavu 133
    ']'   posunout a přejít do stavu 292


State 274

  118 RecordSet11: ';' .

    $výchozí  reduce using rule 118 (RecordSet11)


State 275

  116 RecordSet1: '(' SelectStmt RecordSet11 . ')'

    ')'  posunout a přejít do stavu 293


State 276

  120 RecordSet2: as identifier .

    $výchozí  reduce using rule 120 (RecordSet2)


State 277

  122 RecordSetList: RecordSetList ',' RecordSet .

    $výchozí  reduce using rule 122 (RecordSetList)


State 278

   69 GroupByClause: . group by ColumnNameList
  125 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere . SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset
  137 SelectStmtGroup: . %empty  [$end, limit, offset, order, ')', ';']
  138                | . GroupByClause

    group  posunout a přejít do stavu 279

    $výchozí  reduce using rule 137 (SelectStmtGroup)

    GroupByClause    přejít do stavu 280
    SelectStmtGroup  přejít do stavu 294


State 279

   69 GroupByClause: group . by ColumnNameList

    by  posunout a přejít do stavu 295


State 280

  138 SelectStmtGroup: GroupByClause .

    $výchozí  reduce using rule 138 (SelectStmtGroup)


State 281

   90 OrderBy: . order by ExpressionList OrderBy1
  124 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup . SelectStmtOrder SelectStmtLimit SelectStmtOffset
  139 SelectStmtOrder: . %empty  [$end, limit, offset, ')', ';']
  140                | . OrderBy

    order  posunout a přejít do stavu 296

    $výchozí  reduce using rule 139 (SelectStmtOrder)

    OrderBy          přejít do stavu 297
    SelectStmtOrder  přejít do stavu 298


State 282

    6 AssignmentList1: AssignmentList1 ',' Assignment .

    $výchozí  reduce using rule 6 (AssignmentList1)


State 283

   29 CreateTableStmt: create tableKwd ifKwd not exists TableName '(' ColumnDef . CreateTableStmt1 CreateTableStmt2 ')'
   30 CreateTableStmt1: . %empty
   31                 | . CreateTableStmt1 ',' ColumnDef

    $výchozí  reduce using rule 30 (CreateTableStmt1)

    CreateTableStmt1  přejít do stavu 299


State 284

   31 CreateTableStmt1: CreateTableStmt1 ',' ColumnDef .

    $výchozí  reduce using rule 31 (CreateTableStmt1)


State 285

   28 CreateTableStmt: create tableKwd TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')' .

    $výchozí  reduce using rule 28 (CreateTableStmt)


State 286

   22 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' . identifier ')'
   23                | create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' . identifier '(' ')' ')'

    identifier  posunout a přejít do stavu 300


State 287

   17 ColumnNameList1: ColumnNameList1 ',' ColumnName .

    $výchozí  reduce using rule 17 (ColumnNameList1)


State 288

   71 InsertIntoStmt: insert into TableName InsertIntoStmt1 values '(' ExpressionList ')' . InsertIntoStmt2 InsertIntoStmt3
   75 InsertIntoStmt2: . %empty
   76                | . InsertIntoStmt2 ',' '(' ExpressionList ')'

    $výchozí  reduce using rule 75 (InsertIntoStmt2)

    InsertIntoStmt2  přejít do stavu 301


State 289

   53 Factor: Factor1 not between PrimaryFactor and PrimaryFactor .  [$end, andand, as, asc, desc, from, group, limit, offset, order, oror, where, ',', ')', ']', ';', ':']
  100 PrimaryFactor: PrimaryFactor . '^' PrimaryTerm
  101              | PrimaryFactor . '|' PrimaryTerm
  102              | PrimaryFactor . '-' PrimaryTerm
  103              | PrimaryFactor . '+' PrimaryTerm

    '^'  posunout a přejít do stavu 152
    '|'  posunout a přejít do stavu 153
    '-'  posunout a přejít do stavu 154
    '+'  posunout a přejít do stavu 155

    $výchozí  reduce using rule 53 (Factor)


State 290

   51 Factor: Factor1 not in '(' ExpressionList ')' .

    $výchozí  reduce using rule 51 (Factor)


State 291

   43 Expression: Expression . oror Term
   46 ExpressionList1: ExpressionList1 ',' Expression .  [$end, asc, desc, limit, offset, ',', ')', ';']

    oror  posunout a přejít do stavu 133

    $výchozí  reduce using rule 46 (ExpressionList1)


State 292

  144 Slice: '[' Expression ':' Expression ']' .

    $výchozí  reduce using rule 144 (Slice)


State 293

  116 RecordSet1: '(' SelectStmt RecordSet11 ')' .

    $výchozí  reduce using rule 116 (RecordSet1)


State 294

   90 OrderBy: . order by ExpressionList OrderBy1
  125 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup . SelectStmtOrder SelectStmtLimit SelectStmtOffset
  139 SelectStmtOrder: . %empty  [$end, limit, offset, ')', ';']
  140                | . OrderBy

    order  posunout a přejít do stavu 296

    $výchozí  reduce using rule 139 (SelectStmtOrder)

    OrderBy          přejít do stavu 297
    SelectStmtOrder  přejít do stavu 302


State 295

   14 ColumnName: . identifier
   15 ColumnNameList: . ColumnName ColumnNameList1 ColumnNameList2
   69 GroupByClause: group by . ColumnNameList

    identifier  posunout a přejít do stavu 166

    ColumnName      přejít do stavu 179
    ColumnNameList  přejít do stavu 303


State 296

   90 OrderBy: order . by ExpressionList OrderBy1

    by  posunout a přejít do stavu 304


State 297

  140 SelectStmtOrder: OrderBy .

    $výchozí  reduce using rule 140 (SelectStmtOrder)


State 298

  124 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder . SelectStmtLimit SelectStmtOffset
  126 SelectStmtLimit: . %empty  [$end, offset, ')', ';']
  127                | . limit Expression

    limit  posunout a přejít do stavu 305

    $výchozí  reduce using rule 126 (SelectStmtLimit)

    SelectStmtLimit  přejít do stavu 306


State 299

   29 CreateTableStmt: create tableKwd ifKwd not exists TableName '(' ColumnDef CreateTableStmt1 . CreateTableStmt2 ')'
   31 CreateTableStmt1: CreateTableStmt1 . ',' ColumnDef
   32 CreateTableStmt2: . %empty  [')']
   33                 | . ','

    ','  posunout a přejít do stavu 259

    $výchozí  reduce using rule 32 (CreateTableStmt2)

    CreateTableStmt2  přejít do stavu 307


State 300

   22 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier . ')'
   23                | create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier . '(' ')' ')'

    '('  posunout a přejít do stavu 308
    ')'  posunout a přejít do stavu 309


State 301

   71 InsertIntoStmt: insert into TableName InsertIntoStmt1 values '(' ExpressionList ')' InsertIntoStmt2 . InsertIntoStmt3
   76 InsertIntoStmt2: InsertIntoStmt2 . ',' '(' ExpressionList ')'
   77 InsertIntoStmt3: . %empty  [$end, ';']
   78                | . ','

    ','  posunout a přejít do stavu 310

    $výchozí  reduce using rule 77 (InsertIntoStmt3)

    InsertIntoStmt3  přejít do stavu 311


State 302

  125 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder . SelectStmtLimit SelectStmtOffset
  126 SelectStmtLimit: . %empty  [$end, offset, ')', ';']
  127                | . limit Expression

    limit  posunout a přejít do stavu 305

    $výchozí  reduce using rule 126 (SelectStmtLimit)

    SelectStmtLimit  přejít do stavu 312


State 303

   69 GroupByClause: group by ColumnNameList .

    $výchozí  reduce using rule 69 (GroupByClause)


State 304

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   44 ExpressionList: . Expression ExpressionList1 ExpressionList2
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   90 OrderBy: order by . ExpressionList OrderBy1
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 202
    ExpressionList     přejít do stavu 313
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 305

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  127 SelectStmtLimit: limit . Expression
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 314
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 306

  124 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit . SelectStmtOffset
  128 SelectStmtOffset: . %empty  [$end, ')', ';']
  129                 | . offset Expression

    offset  posunout a přejít do stavu 315

    $výchozí  reduce using rule 128 (SelectStmtOffset)

    SelectStmtOffset  přejít do stavu 316


State 307

   29 CreateTableStmt: create tableKwd ifKwd not exists TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 . ')'

    ')'  posunout a přejít do stavu 317


State 308

   23 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier '(' . ')' ')'

    ')'  posunout a přejít do stavu 318


State 309

   22 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier ')' .

    $výchozí  reduce using rule 22 (CreateIndexStmt)


State 310

   76 InsertIntoStmt2: InsertIntoStmt2 ',' . '(' ExpressionList ')'
   78 InsertIntoStmt3: ',' .  [$end, ';']

    '('  posunout a přejít do stavu 319

    $výchozí  reduce using rule 78 (InsertIntoStmt3)


State 311

   71 InsertIntoStmt: insert into TableName InsertIntoStmt1 values '(' ExpressionList ')' InsertIntoStmt2 InsertIntoStmt3 .

    $výchozí  reduce using rule 71 (InsertIntoStmt)


State 312

  125 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit . SelectStmtOffset
  128 SelectStmtOffset: . %empty  [$end, ')', ';']
  129                 | . offset Expression

    offset  posunout a přejít do stavu 315

    $výchozí  reduce using rule 128 (SelectStmtOffset)

    SelectStmtOffset  přejít do stavu 320


State 313

   90 OrderBy: order by ExpressionList . OrderBy1
   91 OrderBy1: . %empty  [$end, limit, offset, ')', ';']
   92         | . asc
   93         | . desc

    asc   posunout a přejít do stavu 321
    desc  posunout a přejít do stavu 322

    $výchozí  reduce using rule 91 (OrderBy1)

    OrderBy1  přejít do stavu 323


State 314

   43 Expression: Expression . oror Term
  127 SelectStmtLimit: limit Expression .  [$end, offset, ')', ';']

    oror  posunout a přejít do stavu 133

    $výchozí  reduce using rule 127 (SelectStmtLimit)


State 315

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  129 SelectStmtOffset: offset . Expression
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 324
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 316

  124 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset .

    $výchozí  reduce using rule 124 (SelectStmt)


State 317

   29 CreateTableStmt: create tableKwd ifKwd not exists TableName '(' ColumnDef CreateTableStmt1 CreateTableStmt2 ')' .

    $výchozí  reduce using rule 29 (CreateTableStmt)


State 318

   23 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier '(' ')' . ')'

    ')'  posunout a přejít do stavu 325


State 319

   21 Conversion: . Type '(' Expression ')'
   42 Expression: . Term
   43           | . Expression oror Term
   44 ExpressionList: . Expression ExpressionList1 ExpressionList2
   49 Factor: . Factor1
   50       | . Factor1 in '(' ExpressionList ')'
   51       | . Factor1 not in '(' ExpressionList ')'
   52       | . Factor1 between PrimaryFactor and PrimaryFactor
   53       | . Factor1 not between PrimaryFactor and PrimaryFactor
   54       | . Factor1 is null
   55       | . Factor1 is not null
   56 Factor1: . PrimaryFactor
   57        | . Factor1 ge PrimaryFactor
   58        | . Factor1 '>' PrimaryFactor
   59        | . Factor1 le PrimaryFactor
   60        | . Factor1 '<' PrimaryFactor
   61        | . Factor1 neq PrimaryFactor
   62        | . Factor1 eq PrimaryFactor
   63        | . Factor1 like PrimaryFactor
   76 InsertIntoStmt2: InsertIntoStmt2 ',' '(' . ExpressionList ')'
   79 Literal: . falseKwd
   80        | . null
   81        | . trueKwd
   82        | . floatLit
   83        | . imaginaryLit
   84        | . intLit
   85        | . stringLit
   86 Operand: . Literal
   87        | . qlParam
   88        | . QualifiedIdent
   89        | . '(' Expression ')'
   94 PrimaryExpression: . Operand
   95                  | . Conversion
   96                  | . PrimaryExpression Index
   97                  | . PrimaryExpression Slice
   98                  | . PrimaryExpression Call
   99 PrimaryFactor: . PrimaryTerm
  100              | . PrimaryFactor '^' PrimaryTerm
  101              | . PrimaryFactor '|' PrimaryTerm
  102              | . PrimaryFactor '-' PrimaryTerm
  103              | . PrimaryFactor '+' PrimaryTerm
  104 PrimaryTerm: . UnaryExpr
  105            | . PrimaryTerm andnot UnaryExpr
  106            | . PrimaryTerm '&' UnaryExpr
  107            | . PrimaryTerm lsh UnaryExpr
  108            | . PrimaryTerm rsh UnaryExpr
  109            | . PrimaryTerm '%' UnaryExpr
  110            | . PrimaryTerm '/' UnaryExpr
  111            | . PrimaryTerm '*' UnaryExpr
  112 QualifiedIdent: . identifier
  113               | . identifier '.' identifier
  162 Term: . Factor
  163     | . Term andand Factor
  165 Type: . bigIntType
  166     | . bigRatType
  167     | . blobType
  168     | . boolType
  169     | . byteType
  170     | . complex128Type
  171     | . complex64Type
  172     | . durationType
  173     | . floatType
  174     | . float32Type
  175     | . float64Type
  176     | . intType
  177     | . int16Type
  178     | . int32Type
  179     | . int64Type
  180     | . int8Type
  181     | . runeType
  182     | . stringType
  183     | . timeType
  184     | . uintType
  185     | . uint16Type
  186     | . uint32Type
  187     | . uint64Type
  188     | . uint8Type
  192 UnaryExpr: . PrimaryExpression
  193          | . '^' PrimaryExpression
  194          | . '!' PrimaryExpression
  195          | . '-' PrimaryExpression
  196          | . '+' PrimaryExpression

    bigIntType      posunout a přejít do stavu 54
    bigRatType      posunout a přejít do stavu 55
    blobType        posunout a přejít do stavu 56
    boolType        posunout a přejít do stavu 57
    byteType        posunout a přejít do stavu 58
    complex128Type  posunout a přejít do stavu 59
    complex64Type   posunout a přejít do stavu 60
    durationType    posunout a přejít do stavu 61
    falseKwd        posunout a přejít do stavu 62
    floatType       posunout a přejít do stavu 63
    float32Type     posunout a přejít do stavu 64
    float64Type     posunout a přejít do stavu 65
    floatLit        posunout a přejít do stavu 66
    identifier      posunout a přejít do stavu 67
    imaginaryLit    posunout a přejít do stavu 68
    intType         posunout a přejít do stavu 69
    int16Type       posunout a přejít do stavu 70
    int32Type       posunout a přejít do stavu 71
    int64Type       posunout a přejít do stavu 72
    int8Type        posunout a přejít do stavu 73
    intLit          posunout a přejít do stavu 74
    null            posunout a přejít do stavu 75
    qlParam         posunout a přejít do stavu 76
    runeType        posunout a přejít do stavu 77
    stringType      posunout a přejít do stavu 78
    stringLit       posunout a přejít do stavu 79
    timeType        posunout a přejít do stavu 80
    trueKwd         posunout a přejít do stavu 81
    uintType        posunout a přejít do stavu 82
    uint16Type      posunout a přejít do stavu 83
    uint32Type      posunout a přejít do stavu 84
    uint64Type      posunout a přejít do stavu 85
    uint8Type       posunout a přejít do stavu 86
    '('             posunout a přejít do stavu 87
    '^'             posunout a přejít do stavu 88
    '-'             posunout a přejít do stavu 89
    '+'             posunout a přejít do stavu 90
    '!'             posunout a přejít do stavu 92

    Conversion         přejít do stavu 93
    Expression         přejít do stavu 202
    ExpressionList     přejít do stavu 326
    Factor             přejít do stavu 95
    Factor1            přejít do stavu 96
    Literal            přejít do stavu 99
    Operand            přejít do stavu 100
    PrimaryExpression  přejít do stavu 101
    PrimaryFactor      přejít do stavu 102
    PrimaryTerm        přejít do stavu 103
    QualifiedIdent     přejít do stavu 104
    Term               přejít do stavu 106
    Type               přejít do stavu 107
    UnaryExpr          přejít do stavu 108


State 320

  125 SelectStmt: selectKwd SelectStmtDistinct SelectStmtFieldList from RecordSetList ',' SelectStmtWhere SelectStmtGroup SelectStmtOrder SelectStmtLimit SelectStmtOffset .

    $výchozí  reduce using rule 125 (SelectStmt)


State 321

   92 OrderBy1: asc .

    $výchozí  reduce using rule 92 (OrderBy1)


State 322

   93 OrderBy1: desc .

    $výchozí  reduce using rule 93 (OrderBy1)


State 323

   90 OrderBy: order by ExpressionList OrderBy1 .

    $výchozí  reduce using rule 90 (OrderBy)


State 324

   43 Expression: Expression . oror Term
  129 SelectStmtOffset: offset Expression .  [$end, ')', ';']

    oror  posunout a přejít do stavu 133

    $výchozí  reduce using rule 129 (SelectStmtOffset)


State 325

   23 CreateIndexStmt: create CreateIndexStmtUnique index CreateIndexIfNotExists identifier on identifier '(' identifier '(' ')' ')' .

    $výchozí  reduce using rule 23 (CreateIndexStmt)


State 326

   76 InsertIntoStmt2: InsertIntoStmt2 ',' '(' ExpressionList . ')'

    ')'  posunout a přejít do stavu 327


State 327

   76 InsertIntoStmt2: InsertIntoStmt2 ',' '(' ExpressionList ')' .

    $výchozí  reduce using rule 76 (InsertIntoStmt2)
