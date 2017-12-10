Gramatika

    0 $accept: Start $end

    1 AlterTableStmt: _ALTER _TABLE TableName AlterTableStmt1

    2 AlterTableStmt1: _ADD ColumnDef
    3                | _DROP _COLUMN ColumnName

    4 Assignment: ColumnName '=' Expression

    5 AssignmentList: Assignment AssignmentList1 AssignmentList2

    6 AssignmentList1: %empty
    7                | AssignmentList1 ',' Assignment

    8 AssignmentList2: %empty
    9                | ','

   10 BeginTransactionStmt: _BEGIN _TRANSACTION

   11 Call: '(' Call1 ')'

   12 Call1: %empty
   13      | ExpressionList

   14 ColumnDef: ColumnName Type

   15 ColumnName: _IDENTIFIER

   16 ColumnNameList: ColumnName ColumnNameList1 ColumnNameList2

   17 ColumnNameList1: %empty
   18                | ColumnNameList1 ',' ColumnName

   19 ColumnNameList2: %empty
   20                | ','

   21 CommitStmt: _COMMIT

   22 Conversion: Type '(' Expression ')'

   23 CreateIndexStmt: _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON TableName '(' CreateIndexStmt3 ')'

   24 CreateIndexStmt1: %empty
   25                 | _UNIQUE

   26 CreateIndexStmt2: %empty
   27                 | _IF _NOT _EXISTS

   28 CreateIndexStmt3: ColumnName
   29                 | _ID Call

   30 CreateTableStmt: _CREATE _TABLE CreateTableStmt1 TableName '(' ColumnDef CreateTableStmt2 CreateTableStmt3 ')'

   31 CreateTableStmt1: %empty
   32                 | _IF _NOT _EXISTS

   33 CreateTableStmt2: %empty
   34                 | CreateTableStmt2 ',' ColumnDef

   35 CreateTableStmt3: %empty
   36                 | ','

   37 DeleteFromStmt: _DELETE _FROM TableName DeleteFromStmt1

   38 DeleteFromStmt1: %empty
   39                | WhereClause

   40 DropIndexStmt: _DROP _INDEX DropIndexStmt1 IndexName

   41 DropIndexStmt1: %empty
   42               | _IF _EXISTS

   43 DropTableStmt: _DROP _TABLE DropTableStmt1 TableName

   44 DropTableStmt1: %empty
   45               | _IF _EXISTS

   46 EmptyStmt: %empty

   47 Expression: Term Expression1

   48 Expression1: %empty
   49            | Expression1 _OROR Term

   50 ExpressionList: Expression ExpressionList1 ExpressionList2

   51 ExpressionList1: %empty
   52                | ExpressionList1 ',' Expression

   53 ExpressionList2: %empty
   54                | ','

   55 Factor: PrimaryFactor Factor1 Factor2

   56 Factor1: %empty
   57        | Factor1 Factor11 PrimaryFactor

   58 Factor11: _GE
   59         | '>'
   60         | _LE
   61         | '<'
   62         | _NEQ
   63         | _EQ
   64         | _LIKE

   65 Factor2: %empty
   66        | Predicate

   67 Field: Expression Field1

   68 Field1: %empty
   69       | _AS _IDENTIFIER

   70 FieldList: Field FieldList1 FieldList2

   71 FieldList1: %empty
   72           | FieldList1 ',' Field

   73 FieldList2: %empty
   74           | ','

   75 GroupByClause: _GROUPBY ColumnNameList

   76 Index: '[' Expression ']'

   77 IndexName: _IDENTIFIER

   78 InsertIntoStmt: _INSERT _INTO TableName InsertIntoStmt1 InsertIntoStmt2

   79 InsertIntoStmt1: %empty
   80                | '(' ColumnNameList ')'

   81 InsertIntoStmt2: Values
   82                | SelectStmt

   83 Limit: _LIMIT Expression

   84 Literal: _FALSE
   85        | _NULL
   86        | _TRUE
   87        | _FLOAT_LIT
   88        | _IMAGINARY_LIT
   89        | _INT_LIT
   90        | _RUNE_LIT
   91        | _STRING_LIT
   92        | _QL_PARAMETER

   93 Offset: _OFFSET Expression

   94 Operand: Literal
   95        | QualifiedIdent
   96        | '(' Expression ')'

   97 OrderBy: _ORDER _BY ExpressionList OrderBy1

   98 OrderBy1: %empty
   99         | OrderBy11

  100 OrderBy11: _ASC
  101          | _DESC

  102 Predicate: Predicate1

  103 Predicate1: Predicate11 Predicate12
  104           | _IS Predicate13 _NULL

  105 Predicate11: %empty
  106            | _NOT

  107 Predicate12: _IN '(' ExpressionList ')'
  108            | _BETWEEN PrimaryFactor _AND PrimaryFactor

  109 Predicate13: %empty
  110            | _NOT

  111 PrimaryExpression: Operand
  112                  | Conversion
  113                  | PrimaryExpression Index
  114                  | PrimaryExpression Slice
  115                  | PrimaryExpression Call

  116 PrimaryFactor: PrimaryTerm PrimaryFactor1

  117 PrimaryFactor1: %empty
  118               | PrimaryFactor1 PrimaryFactor11 PrimaryTerm

  119 PrimaryFactor11: '^'
  120                | '|'
  121                | '-'
  122                | '+'

  123 PrimaryTerm: UnaryExpr PrimaryTerm1

  124 PrimaryTerm1: %empty
  125             | PrimaryTerm1 PrimaryTerm11 UnaryExpr

  126 PrimaryTerm11: _ANDNOT
  127              | '&'
  128              | _LSH
  129              | _RSH
  130              | '%'
  131              | '/'
  132              | '*'

  133 QualifiedIdent: _IDENTIFIER QualifiedIdent1

  134 QualifiedIdent1: %empty
  135                | '.' _IDENTIFIER

  136 RecordSet: RecordSet1 RecordSet2

  137 RecordSet1: TableName
  138           | '(' SelectStmt RecordSet11 ')'

  139 RecordSet11: %empty
  140            | ';'

  141 RecordSet2: %empty
  142           | _AS _IDENTIFIER

  143 RecordSetList: RecordSet RecordSetList1 RecordSetList2

  144 RecordSetList1: %empty
  145               | RecordSetList1 ',' RecordSet

  146 RecordSetList2: %empty
  147               | ','

  148 RollbackStmt: _ROLLBACK

  149 SelectStmt: _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7

  150 SelectStmt1: %empty
  151            | _DISTINCT

  152 SelectStmt2: '*'
  153            | FieldList

  154 SelectStmt3: %empty
  155            | WhereClause

  156 SelectStmt4: %empty
  157            | GroupByClause

  158 SelectStmt5: %empty
  159            | OrderBy

  160 SelectStmt6: %empty
  161            | Limit

  162 SelectStmt7: %empty
  163            | Offset

  164 Slice: '[' Slice1 ':' Slice2 ']'

  165 Slice1: %empty
  166       | Expression

  167 Slice2: %empty
  168       | Expression

  169 Start: StatementList

  170 Statement: EmptyStmt
  171          | AlterTableStmt
  172          | BeginTransactionStmt
  173          | CommitStmt
  174          | CreateIndexStmt
  175          | CreateTableStmt
  176          | DeleteFromStmt
  177          | DropIndexStmt
  178          | DropTableStmt
  179          | InsertIntoStmt
  180          | RollbackStmt
  181          | SelectStmt
  182          | TruncateTableStmt
  183          | UpdateStmt

  184 StatementList: Statement StatementList1

  185 StatementList1: %empty
  186               | StatementList1 ';' Statement

  187 TableName: _IDENTIFIER

  188 Term: Factor Term1

  189 Term1: %empty
  190      | Term1 _ANDAND Factor

  191 TruncateTableStmt: _TRUNCATE _TABLE TableName

  192 Type: _BIGINT
  193     | _BIGRAT
  194     | _BLOB
  195     | _BOOL
  196     | _BYTE
  197     | _COMPLEX128
  198     | _COMPLEX64
  199     | _DURATION
  200     | _FLOAT
  201     | _FLOAT32
  202     | _FLOAT64
  203     | _INT
  204     | _INT16
  205     | _INT32
  206     | _INT64
  207     | _INT8
  208     | _RUNE
  209     | _STRING
  210     | _TIME
  211     | _UINT
  212     | _UINT16
  213     | _UINT32
  214     | _UINT64
  215     | _UINT8

  216 UnaryExpr: UnaryExpr1 PrimaryExpression

  217 UnaryExpr1: %empty
  218           | UnaryExpr11

  219 UnaryExpr11: '^'
  220            | '!'
  221            | '-'
  222            | '+'

  223 UpdateStmt: _UPDATE TableName UpdateStmt1 AssignmentList UpdateStmt2

  224 UpdateStmt1: %empty
  225            | _SET

  226 UpdateStmt2: %empty
  227            | WhereClause

  228 Values: _VALUES '(' ExpressionList ')' Values1 Values2

  229 Values1: %empty
  230        | Values1 ',' '(' ExpressionList ')'

  231 Values2: %empty
  232        | ','

  233 WhereClause: _WHERE Expression


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'!' (33) 220
'%' (37) 130
'&' (38) 127
'(' (40) 11 22 23 30 80 96 107 138 228 230
')' (41) 11 22 23 30 80 96 107 138 228 230
'*' (42) 132 152
'+' (43) 122 222
',' (44) 7 9 18 20 34 36 52 54 72 74 145 147 230 232
'-' (45) 121 221
'.' (46) 135
'/' (47) 131
':' (58) 164
';' (59) 140 186
'<' (60) 61
'=' (61) 4
'>' (62) 59
'[' (91) 76 164
']' (93) 76 164
'^' (94) 119 219
'|' (124) 120
error (256)
_ANDAND (258) 190
_ANDNOT (259) 126
_EQ (260) 63
_FLOAT_LIT (261) 87
_GE (262) 58
_IDENTIFIER (263) 15 69 77 133 135 142 187
_IMAGINARY_LIT (264) 88
_INT_LIT (265) 89
_LE (266) 60
_LSH (267) 128
_NEQ (268) 62
_OROR (269) 49
_QL_PARAMETER (270) 92
_RSH (271) 129
_RUNE_LIT (272) 90
_STRING_LIT (273) 91
_ADD (274) 2
_ALTER (275) 1
_AND (276) 108
_AS (277) 69 142
_ASC (278) 100
_BEGIN (279) 10
_BETWEEN (280) 108
_BIGINT (281) 192
_BIGRAT (282) 193
_BLOB (283) 194
_BOOL (284) 195
_BY (285) 97
_BYTE (286) 196
_COLUMN (287) 3
_COMMIT (288) 21
_COMPLEX128 (289) 197
_COMPLEX64 (290) 198
_CREATE (291) 23 30
_DELETE (292) 37
_DESC (293) 101
_DISTINCT (294) 151
_DROP (295) 3 40 43
_DURATION (296) 199
_EXISTS (297) 27 32 42 45
_FALSE (298) 84
_FLOAT (299) 200
_FLOAT32 (300) 201
_FLOAT64 (301) 202
_FROM (302) 37 149
_GROUPBY (303) 75
_ID (304) 29
_IF (305) 27 32 42 45
_IN (306) 107
_INDEX (307) 23 40
_INSERT (308) 78
_INT (309) 203
_INT16 (310) 204
_INT32 (311) 205
_INT64 (312) 206
_INT8 (313) 207
_INTO (314) 78
_IS (315) 104
_LIKE (316) 64
_LIMIT (317) 83
_NOT (318) 27 32 106 110
_NULL (319) 85 104
_OFFSET (320) 93
_ON (321) 23
_ORDER (322) 97
_ROLLBACK (323) 148
_RUNE (324) 208
_SELECT (325) 149
_SET (326) 225
_STRING (327) 209
_TABLE (328) 1 30 43 191
_TIME (329) 210
_TRANSACTION (330) 10
_TRUE (331) 86
_TRUNCATE (332) 191
_UINT (333) 211
_UINT16 (334) 212
_UINT32 (335) 213
_UINT64 (336) 214
_UINT8 (337) 215
_UNIQUE (338) 25
_UPDATE (339) 223
_VALUES (340) 228
_WHERE (341) 233


Neterminály s pravidly, ve kterých se objevují

$accept (107)
    vlevo: 0
AlterTableStmt (108)
    vlevo: 1, vpravo: 171
AlterTableStmt1 (109)
    vlevo: 2 3, vpravo: 1
Assignment (110)
    vlevo: 4, vpravo: 5 7
AssignmentList (111)
    vlevo: 5, vpravo: 223
AssignmentList1 (112)
    vlevo: 6 7, vpravo: 5 7
AssignmentList2 (113)
    vlevo: 8 9, vpravo: 5
BeginTransactionStmt (114)
    vlevo: 10, vpravo: 172
Call (115)
    vlevo: 11, vpravo: 29 115
Call1 (116)
    vlevo: 12 13, vpravo: 11
ColumnDef (117)
    vlevo: 14, vpravo: 2 30 34
ColumnName (118)
    vlevo: 15, vpravo: 3 4 14 16 18 28
ColumnNameList (119)
    vlevo: 16, vpravo: 75 80
ColumnNameList1 (120)
    vlevo: 17 18, vpravo: 16 18
ColumnNameList2 (121)
    vlevo: 19 20, vpravo: 16
CommitStmt (122)
    vlevo: 21, vpravo: 173
Conversion (123)
    vlevo: 22, vpravo: 112
CreateIndexStmt (124)
    vlevo: 23, vpravo: 174
CreateIndexStmt1 (125)
    vlevo: 24 25, vpravo: 23
CreateIndexStmt2 (126)
    vlevo: 26 27, vpravo: 23
CreateIndexStmt3 (127)
    vlevo: 28 29, vpravo: 23
CreateTableStmt (128)
    vlevo: 30, vpravo: 175
CreateTableStmt1 (129)
    vlevo: 31 32, vpravo: 30
CreateTableStmt2 (130)
    vlevo: 33 34, vpravo: 30 34
CreateTableStmt3 (131)
    vlevo: 35 36, vpravo: 30
DeleteFromStmt (132)
    vlevo: 37, vpravo: 176
DeleteFromStmt1 (133)
    vlevo: 38 39, vpravo: 37
DropIndexStmt (134)
    vlevo: 40, vpravo: 177
DropIndexStmt1 (135)
    vlevo: 41 42, vpravo: 40
DropTableStmt (136)
    vlevo: 43, vpravo: 178
DropTableStmt1 (137)
    vlevo: 44 45, vpravo: 43
EmptyStmt (138)
    vlevo: 46, vpravo: 170
Expression (139)
    vlevo: 47, vpravo: 4 22 50 52 67 76 83 93 96 166 168 233
Expression1 (140)
    vlevo: 48 49, vpravo: 47 49
ExpressionList (141)
    vlevo: 50, vpravo: 13 97 107 228 230
ExpressionList1 (142)
    vlevo: 51 52, vpravo: 50 52
ExpressionList2 (143)
    vlevo: 53 54, vpravo: 50
Factor (144)
    vlevo: 55, vpravo: 188 190
Factor1 (145)
    vlevo: 56 57, vpravo: 55 57
Factor11 (146)
    vlevo: 58 59 60 61 62 63 64, vpravo: 57
Factor2 (147)
    vlevo: 65 66, vpravo: 55
Field (148)
    vlevo: 67, vpravo: 70 72
Field1 (149)
    vlevo: 68 69, vpravo: 67
FieldList (150)
    vlevo: 70, vpravo: 153
FieldList1 (151)
    vlevo: 71 72, vpravo: 70 72
FieldList2 (152)
    vlevo: 73 74, vpravo: 70
GroupByClause (153)
    vlevo: 75, vpravo: 157
Index (154)
    vlevo: 76, vpravo: 113
IndexName (155)
    vlevo: 77, vpravo: 23 40
InsertIntoStmt (156)
    vlevo: 78, vpravo: 179
InsertIntoStmt1 (157)
    vlevo: 79 80, vpravo: 78
InsertIntoStmt2 (158)
    vlevo: 81 82, vpravo: 78
Limit (159)
    vlevo: 83, vpravo: 161
Literal (160)
    vlevo: 84 85 86 87 88 89 90 91 92, vpravo: 94
Offset (161)
    vlevo: 93, vpravo: 163
Operand (162)
    vlevo: 94 95 96, vpravo: 111
OrderBy (163)
    vlevo: 97, vpravo: 159
OrderBy1 (164)
    vlevo: 98 99, vpravo: 97
OrderBy11 (165)
    vlevo: 100 101, vpravo: 99
Predicate (166)
    vlevo: 102, vpravo: 66
Predicate1 (167)
    vlevo: 103 104, vpravo: 102
Predicate11 (168)
    vlevo: 105 106, vpravo: 103
Predicate12 (169)
    vlevo: 107 108, vpravo: 103
Predicate13 (170)
    vlevo: 109 110, vpravo: 104
PrimaryExpression (171)
    vlevo: 111 112 113 114 115, vpravo: 113 114 115 216
PrimaryFactor (172)
    vlevo: 116, vpravo: 55 57 108
PrimaryFactor1 (173)
    vlevo: 117 118, vpravo: 116 118
PrimaryFactor11 (174)
    vlevo: 119 120 121 122, vpravo: 118
PrimaryTerm (175)
    vlevo: 123, vpravo: 116 118
PrimaryTerm1 (176)
    vlevo: 124 125, vpravo: 123 125
PrimaryTerm11 (177)
    vlevo: 126 127 128 129 130 131 132, vpravo: 125
QualifiedIdent (178)
    vlevo: 133, vpravo: 95
QualifiedIdent1 (179)
    vlevo: 134 135, vpravo: 133
RecordSet (180)
    vlevo: 136, vpravo: 143 145
RecordSet1 (181)
    vlevo: 137 138, vpravo: 136
RecordSet11 (182)
    vlevo: 139 140, vpravo: 138
RecordSet2 (183)
    vlevo: 141 142, vpravo: 136
RecordSetList (184)
    vlevo: 143, vpravo: 149
RecordSetList1 (185)
    vlevo: 144 145, vpravo: 143 145
RecordSetList2 (186)
    vlevo: 146 147, vpravo: 143
RollbackStmt (187)
    vlevo: 148, vpravo: 180
SelectStmt (188)
    vlevo: 149, vpravo: 82 138 181
SelectStmt1 (189)
    vlevo: 150 151, vpravo: 149
SelectStmt2 (190)
    vlevo: 152 153, vpravo: 149
SelectStmt3 (191)
    vlevo: 154 155, vpravo: 149
SelectStmt4 (192)
    vlevo: 156 157, vpravo: 149
SelectStmt5 (193)
    vlevo: 158 159, vpravo: 149
SelectStmt6 (194)
    vlevo: 160 161, vpravo: 149
SelectStmt7 (195)
    vlevo: 162 163, vpravo: 149
Slice (196)
    vlevo: 164, vpravo: 114
Slice1 (197)
    vlevo: 165 166, vpravo: 164
Slice2 (198)
    vlevo: 167 168, vpravo: 164
Start (199)
    vlevo: 169, vpravo: 0
Statement (200)
    vlevo: 170 171 172 173 174 175 176 177 178 179 180 181 182 183,
    vpravo: 184 186
StatementList (201)
    vlevo: 184, vpravo: 169
StatementList1 (202)
    vlevo: 185 186, vpravo: 184 186
TableName (203)
    vlevo: 187, vpravo: 1 23 30 37 43 78 137 191 223
Term (204)
    vlevo: 188, vpravo: 47 49
Term1 (205)
    vlevo: 189 190, vpravo: 188 190
TruncateTableStmt (206)
    vlevo: 191, vpravo: 182
Type (207)
    vlevo: 192 193 194 195 196 197 198 199 200 201 202 203 204 205
    206 207 208 209 210 211 212 213 214 215, vpravo: 14 22
UnaryExpr (208)
    vlevo: 216, vpravo: 123 125
UnaryExpr1 (209)
    vlevo: 217 218, vpravo: 216
UnaryExpr11 (210)
    vlevo: 219 220 221 222, vpravo: 218
UpdateStmt (211)
    vlevo: 223, vpravo: 183
UpdateStmt1 (212)
    vlevo: 224 225, vpravo: 223
UpdateStmt2 (213)
    vlevo: 226 227, vpravo: 223
Values (214)
    vlevo: 228, vpravo: 81
Values1 (215)
    vlevo: 229 230, vpravo: 228 230
Values2 (216)
    vlevo: 231 232, vpravo: 228
WhereClause (217)
    vlevo: 233, vpravo: 39 155 227


State 0

    0 $accept: . Start $end
    1 AlterTableStmt: . _ALTER _TABLE TableName AlterTableStmt1
   10 BeginTransactionStmt: . _BEGIN _TRANSACTION
   21 CommitStmt: . _COMMIT
   23 CreateIndexStmt: . _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON TableName '(' CreateIndexStmt3 ')'
   30 CreateTableStmt: . _CREATE _TABLE CreateTableStmt1 TableName '(' ColumnDef CreateTableStmt2 CreateTableStmt3 ')'
   37 DeleteFromStmt: . _DELETE _FROM TableName DeleteFromStmt1
   40 DropIndexStmt: . _DROP _INDEX DropIndexStmt1 IndexName
   43 DropTableStmt: . _DROP _TABLE DropTableStmt1 TableName
   46 EmptyStmt: . %empty  [$end, ';']
   78 InsertIntoStmt: . _INSERT _INTO TableName InsertIntoStmt1 InsertIntoStmt2
  148 RollbackStmt: . _ROLLBACK
  149 SelectStmt: . _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7
  169 Start: . StatementList
  170 Statement: . EmptyStmt
  171          | . AlterTableStmt
  172          | . BeginTransactionStmt
  173          | . CommitStmt
  174          | . CreateIndexStmt
  175          | . CreateTableStmt
  176          | . DeleteFromStmt
  177          | . DropIndexStmt
  178          | . DropTableStmt
  179          | . InsertIntoStmt
  180          | . RollbackStmt
  181          | . SelectStmt
  182          | . TruncateTableStmt
  183          | . UpdateStmt
  184 StatementList: . Statement StatementList1
  191 TruncateTableStmt: . _TRUNCATE _TABLE TableName
  223 UpdateStmt: . _UPDATE TableName UpdateStmt1 AssignmentList UpdateStmt2

    _ALTER     posunout a přejít do stavu 1
    _BEGIN     posunout a přejít do stavu 2
    _COMMIT    posunout a přejít do stavu 3
    _CREATE    posunout a přejít do stavu 4
    _DELETE    posunout a přejít do stavu 5
    _DROP      posunout a přejít do stavu 6
    _INSERT    posunout a přejít do stavu 7
    _ROLLBACK  posunout a přejít do stavu 8
    _SELECT    posunout a přejít do stavu 9
    _TRUNCATE  posunout a přejít do stavu 10
    _UPDATE    posunout a přejít do stavu 11

    $výchozí  reduce using rule 46 (EmptyStmt)

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
    Start                 přejít do stavu 24
    Statement             přejít do stavu 25
    StatementList         přejít do stavu 26
    TruncateTableStmt     přejít do stavu 27
    UpdateStmt            přejít do stavu 28


State 1

    1 AlterTableStmt: _ALTER . _TABLE TableName AlterTableStmt1

    _TABLE  posunout a přejít do stavu 29


State 2

   10 BeginTransactionStmt: _BEGIN . _TRANSACTION

    _TRANSACTION  posunout a přejít do stavu 30


State 3

   21 CommitStmt: _COMMIT .

    $výchozí  reduce using rule 21 (CommitStmt)


State 4

   23 CreateIndexStmt: _CREATE . CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON TableName '(' CreateIndexStmt3 ')'
   24 CreateIndexStmt1: . %empty  [_INDEX]
   25                 | . _UNIQUE
   30 CreateTableStmt: _CREATE . _TABLE CreateTableStmt1 TableName '(' ColumnDef CreateTableStmt2 CreateTableStmt3 ')'

    _TABLE   posunout a přejít do stavu 31
    _UNIQUE  posunout a přejít do stavu 32

    $výchozí  reduce using rule 24 (CreateIndexStmt1)

    CreateIndexStmt1  přejít do stavu 33


State 5

   37 DeleteFromStmt: _DELETE . _FROM TableName DeleteFromStmt1

    _FROM  posunout a přejít do stavu 34


State 6

   40 DropIndexStmt: _DROP . _INDEX DropIndexStmt1 IndexName
   43 DropTableStmt: _DROP . _TABLE DropTableStmt1 TableName

    _INDEX  posunout a přejít do stavu 35
    _TABLE  posunout a přejít do stavu 36


State 7

   78 InsertIntoStmt: _INSERT . _INTO TableName InsertIntoStmt1 InsertIntoStmt2

    _INTO  posunout a přejít do stavu 37


State 8

  148 RollbackStmt: _ROLLBACK .

    $výchozí  reduce using rule 148 (RollbackStmt)


State 9

  149 SelectStmt: _SELECT . SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7
  150 SelectStmt1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(', '^', '-', '+', '*', '!']
  151            | . _DISTINCT

    _DISTINCT  posunout a přejít do stavu 38

    $výchozí  reduce using rule 150 (SelectStmt1)

    SelectStmt1  přejít do stavu 39


State 10

  191 TruncateTableStmt: _TRUNCATE . _TABLE TableName

    _TABLE  posunout a přejít do stavu 40


State 11

  187 TableName: . _IDENTIFIER
  223 UpdateStmt: _UPDATE . TableName UpdateStmt1 AssignmentList UpdateStmt2

    _IDENTIFIER  posunout a přejít do stavu 41

    TableName  přejít do stavu 42


State 12

  171 Statement: AlterTableStmt .

    $výchozí  reduce using rule 171 (Statement)


State 13

  172 Statement: BeginTransactionStmt .

    $výchozí  reduce using rule 172 (Statement)


State 14

  173 Statement: CommitStmt .

    $výchozí  reduce using rule 173 (Statement)


State 15

  174 Statement: CreateIndexStmt .

    $výchozí  reduce using rule 174 (Statement)


State 16

  175 Statement: CreateTableStmt .

    $výchozí  reduce using rule 175 (Statement)


State 17

  176 Statement: DeleteFromStmt .

    $výchozí  reduce using rule 176 (Statement)


State 18

  177 Statement: DropIndexStmt .

    $výchozí  reduce using rule 177 (Statement)


State 19

  178 Statement: DropTableStmt .

    $výchozí  reduce using rule 178 (Statement)


State 20

  170 Statement: EmptyStmt .

    $výchozí  reduce using rule 170 (Statement)


State 21

  179 Statement: InsertIntoStmt .

    $výchozí  reduce using rule 179 (Statement)


State 22

  180 Statement: RollbackStmt .

    $výchozí  reduce using rule 180 (Statement)


State 23

  181 Statement: SelectStmt .

    $výchozí  reduce using rule 181 (Statement)


State 24

    0 $accept: Start . $end

    $end  posunout a přejít do stavu 43


State 25

  184 StatementList: Statement . StatementList1
  185 StatementList1: . %empty
  186               | . StatementList1 ';' Statement

    $výchozí  reduce using rule 185 (StatementList1)

    StatementList1  přejít do stavu 44


State 26

  169 Start: StatementList .

    $výchozí  reduce using rule 169 (Start)


State 27

  182 Statement: TruncateTableStmt .

    $výchozí  reduce using rule 182 (Statement)


State 28

  183 Statement: UpdateStmt .

    $výchozí  reduce using rule 183 (Statement)


State 29

    1 AlterTableStmt: _ALTER _TABLE . TableName AlterTableStmt1
  187 TableName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 41

    TableName  přejít do stavu 45


State 30

   10 BeginTransactionStmt: _BEGIN _TRANSACTION .

    $výchozí  reduce using rule 10 (BeginTransactionStmt)


State 31

   30 CreateTableStmt: _CREATE _TABLE . CreateTableStmt1 TableName '(' ColumnDef CreateTableStmt2 CreateTableStmt3 ')'
   31 CreateTableStmt1: . %empty  [_IDENTIFIER]
   32                 | . _IF _NOT _EXISTS

    _IF  posunout a přejít do stavu 46

    $výchozí  reduce using rule 31 (CreateTableStmt1)

    CreateTableStmt1  přejít do stavu 47


State 32

   25 CreateIndexStmt1: _UNIQUE .

    $výchozí  reduce using rule 25 (CreateIndexStmt1)


State 33

   23 CreateIndexStmt: _CREATE CreateIndexStmt1 . _INDEX CreateIndexStmt2 IndexName _ON TableName '(' CreateIndexStmt3 ')'

    _INDEX  posunout a přejít do stavu 48


State 34

   37 DeleteFromStmt: _DELETE _FROM . TableName DeleteFromStmt1
  187 TableName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 41

    TableName  přejít do stavu 49


State 35

   40 DropIndexStmt: _DROP _INDEX . DropIndexStmt1 IndexName
   41 DropIndexStmt1: . %empty  [_IDENTIFIER]
   42               | . _IF _EXISTS

    _IF  posunout a přejít do stavu 50

    $výchozí  reduce using rule 41 (DropIndexStmt1)

    DropIndexStmt1  přejít do stavu 51


State 36

   43 DropTableStmt: _DROP _TABLE . DropTableStmt1 TableName
   44 DropTableStmt1: . %empty  [_IDENTIFIER]
   45               | . _IF _EXISTS

    _IF  posunout a přejít do stavu 52

    $výchozí  reduce using rule 44 (DropTableStmt1)

    DropTableStmt1  přejít do stavu 53


State 37

   78 InsertIntoStmt: _INSERT _INTO . TableName InsertIntoStmt1 InsertIntoStmt2
  187 TableName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 41

    TableName  přejít do stavu 54


State 38

  151 SelectStmt1: _DISTINCT .

    $výchozí  reduce using rule 151 (SelectStmt1)


State 39

   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
   67 Field: . Expression Field1
   70 FieldList: . Field FieldList1 FieldList2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  149 SelectStmt: _SELECT SelectStmt1 . SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7
  152 SelectStmt2: . '*'
  153            | . FieldList
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '*'  posunout a přejít do stavu 58
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 60
    Factor         přejít do stavu 61
    Field          přejít do stavu 62
    FieldList      přejít do stavu 63
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    SelectStmt2    přejít do stavu 66
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 40

  187 TableName: . _IDENTIFIER
  191 TruncateTableStmt: _TRUNCATE _TABLE . TableName

    _IDENTIFIER  posunout a přejít do stavu 41

    TableName  přejít do stavu 71


State 41

  187 TableName: _IDENTIFIER .

    $výchozí  reduce using rule 187 (TableName)


State 42

  223 UpdateStmt: _UPDATE TableName . UpdateStmt1 AssignmentList UpdateStmt2
  224 UpdateStmt1: . %empty  [_IDENTIFIER]
  225            | . _SET

    _SET  posunout a přejít do stavu 72

    $výchozí  reduce using rule 224 (UpdateStmt1)

    UpdateStmt1  přejít do stavu 73


State 43

    0 $accept: Start $end .

    $výchozí  přijmout


State 44

  184 StatementList: Statement StatementList1 .  [$end]
  186 StatementList1: StatementList1 . ';' Statement

    ';'  posunout a přejít do stavu 74

    $výchozí  reduce using rule 184 (StatementList)


State 45

    1 AlterTableStmt: _ALTER _TABLE TableName . AlterTableStmt1
    2 AlterTableStmt1: . _ADD ColumnDef
    3                | . _DROP _COLUMN ColumnName

    _ADD   posunout a přejít do stavu 75
    _DROP  posunout a přejít do stavu 76

    AlterTableStmt1  přejít do stavu 77


State 46

   32 CreateTableStmt1: _IF . _NOT _EXISTS

    _NOT  posunout a přejít do stavu 78


State 47

   30 CreateTableStmt: _CREATE _TABLE CreateTableStmt1 . TableName '(' ColumnDef CreateTableStmt2 CreateTableStmt3 ')'
  187 TableName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 41

    TableName  přejít do stavu 79


State 48

   23 CreateIndexStmt: _CREATE CreateIndexStmt1 _INDEX . CreateIndexStmt2 IndexName _ON TableName '(' CreateIndexStmt3 ')'
   26 CreateIndexStmt2: . %empty  [_IDENTIFIER]
   27                 | . _IF _NOT _EXISTS

    _IF  posunout a přejít do stavu 80

    $výchozí  reduce using rule 26 (CreateIndexStmt2)

    CreateIndexStmt2  přejít do stavu 81


State 49

   37 DeleteFromStmt: _DELETE _FROM TableName . DeleteFromStmt1
   38 DeleteFromStmt1: . %empty  [$end, ';']
   39                | . WhereClause
  233 WhereClause: . _WHERE Expression

    _WHERE  posunout a přejít do stavu 82

    $výchozí  reduce using rule 38 (DeleteFromStmt1)

    DeleteFromStmt1  přejít do stavu 83
    WhereClause      přejít do stavu 84


State 50

   42 DropIndexStmt1: _IF . _EXISTS

    _EXISTS  posunout a přejít do stavu 85


State 51

   40 DropIndexStmt: _DROP _INDEX DropIndexStmt1 . IndexName
   77 IndexName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 86

    IndexName  přejít do stavu 87


State 52

   45 DropTableStmt1: _IF . _EXISTS

    _EXISTS  posunout a přejít do stavu 88


State 53

   43 DropTableStmt: _DROP _TABLE DropTableStmt1 . TableName
  187 TableName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 41

    TableName  přejít do stavu 89


State 54

   78 InsertIntoStmt: _INSERT _INTO TableName . InsertIntoStmt1 InsertIntoStmt2
   79 InsertIntoStmt1: . %empty  [_SELECT, _VALUES]
   80                | . '(' ColumnNameList ')'

    '('  posunout a přejít do stavu 90

    $výchozí  reduce using rule 79 (InsertIntoStmt1)

    InsertIntoStmt1  přejít do stavu 91


State 55

  219 UnaryExpr11: '^' .

    $výchozí  reduce using rule 219 (UnaryExpr11)


State 56

  221 UnaryExpr11: '-' .

    $výchozí  reduce using rule 221 (UnaryExpr11)


State 57

  222 UnaryExpr11: '+' .

    $výchozí  reduce using rule 222 (UnaryExpr11)


State 58

  152 SelectStmt2: '*' .

    $výchozí  reduce using rule 152 (SelectStmt2)


State 59

  220 UnaryExpr11: '!' .

    $výchozí  reduce using rule 220 (UnaryExpr11)


State 60

   67 Field: Expression . Field1
   68 Field1: . %empty  [_FROM, ',']
   69       | . _AS _IDENTIFIER

    _AS  posunout a přejít do stavu 92

    $výchozí  reduce using rule 68 (Field1)

    Field1  přejít do stavu 93


State 61

  188 Term: Factor . Term1
  189 Term1: . %empty
  190      | . Term1 _ANDAND Factor

    $výchozí  reduce using rule 189 (Term1)

    Term1  přejít do stavu 94


State 62

   70 FieldList: Field . FieldList1 FieldList2
   71 FieldList1: . %empty
   72           | . FieldList1 ',' Field

    $výchozí  reduce using rule 71 (FieldList1)

    FieldList1  přejít do stavu 95


State 63

  153 SelectStmt2: FieldList .

    $výchozí  reduce using rule 153 (SelectStmt2)


State 64

   55 Factor: PrimaryFactor . Factor1 Factor2
   56 Factor1: . %empty
   57        | . Factor1 Factor11 PrimaryFactor

    $výchozí  reduce using rule 56 (Factor1)

    Factor1  přejít do stavu 96


State 65

  116 PrimaryFactor: PrimaryTerm . PrimaryFactor1
  117 PrimaryFactor1: . %empty
  118               | . PrimaryFactor1 PrimaryFactor11 PrimaryTerm

    $výchozí  reduce using rule 117 (PrimaryFactor1)

    PrimaryFactor1  přejít do stavu 97


State 66

  149 SelectStmt: _SELECT SelectStmt1 SelectStmt2 . _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7

    _FROM  posunout a přejít do stavu 98


State 67

   47 Expression: Term . Expression1
   48 Expression1: . %empty
   49            | . Expression1 _OROR Term

    $výchozí  reduce using rule 48 (Expression1)

    Expression1  přejít do stavu 99


State 68

  123 PrimaryTerm: UnaryExpr . PrimaryTerm1
  124 PrimaryTerm1: . %empty
  125             | . PrimaryTerm1 PrimaryTerm11 UnaryExpr

    $výchozí  reduce using rule 124 (PrimaryTerm1)

    PrimaryTerm1  přejít do stavu 100


State 69

   22 Conversion: . Type '(' Expression ')'
   84 Literal: . _FALSE
   85        | . _NULL
   86        | . _TRUE
   87        | . _FLOAT_LIT
   88        | . _IMAGINARY_LIT
   89        | . _INT_LIT
   90        | . _RUNE_LIT
   91        | . _STRING_LIT
   92        | . _QL_PARAMETER
   94 Operand: . Literal
   95        | . QualifiedIdent
   96        | . '(' Expression ')'
  111 PrimaryExpression: . Operand
  112                  | . Conversion
  113                  | . PrimaryExpression Index
  114                  | . PrimaryExpression Slice
  115                  | . PrimaryExpression Call
  133 QualifiedIdent: . _IDENTIFIER QualifiedIdent1
  192 Type: . _BIGINT
  193     | . _BIGRAT
  194     | . _BLOB
  195     | . _BOOL
  196     | . _BYTE
  197     | . _COMPLEX128
  198     | . _COMPLEX64
  199     | . _DURATION
  200     | . _FLOAT
  201     | . _FLOAT32
  202     | . _FLOAT64
  203     | . _INT
  204     | . _INT16
  205     | . _INT32
  206     | . _INT64
  207     | . _INT8
  208     | . _RUNE
  209     | . _STRING
  210     | . _TIME
  211     | . _UINT
  212     | . _UINT16
  213     | . _UINT32
  214     | . _UINT64
  215     | . _UINT8
  216 UnaryExpr: UnaryExpr1 . PrimaryExpression

    _FLOAT_LIT      posunout a přejít do stavu 101
    _IDENTIFIER     posunout a přejít do stavu 102
    _IMAGINARY_LIT  posunout a přejít do stavu 103
    _INT_LIT        posunout a přejít do stavu 104
    _QL_PARAMETER   posunout a přejít do stavu 105
    _RUNE_LIT       posunout a přejít do stavu 106
    _STRING_LIT     posunout a přejít do stavu 107
    _BIGINT         posunout a přejít do stavu 108
    _BIGRAT         posunout a přejít do stavu 109
    _BLOB           posunout a přejít do stavu 110
    _BOOL           posunout a přejít do stavu 111
    _BYTE           posunout a přejít do stavu 112
    _COMPLEX128     posunout a přejít do stavu 113
    _COMPLEX64      posunout a přejít do stavu 114
    _DURATION       posunout a přejít do stavu 115
    _FALSE          posunout a přejít do stavu 116
    _FLOAT          posunout a přejít do stavu 117
    _FLOAT32        posunout a přejít do stavu 118
    _FLOAT64        posunout a přejít do stavu 119
    _INT            posunout a přejít do stavu 120
    _INT16          posunout a přejít do stavu 121
    _INT32          posunout a přejít do stavu 122
    _INT64          posunout a přejít do stavu 123
    _INT8           posunout a přejít do stavu 124
    _NULL           posunout a přejít do stavu 125
    _RUNE           posunout a přejít do stavu 126
    _STRING         posunout a přejít do stavu 127
    _TIME           posunout a přejít do stavu 128
    _TRUE           posunout a přejít do stavu 129
    _UINT           posunout a přejít do stavu 130
    _UINT16         posunout a přejít do stavu 131
    _UINT32         posunout a přejít do stavu 132
    _UINT64         posunout a přejít do stavu 133
    _UINT8          posunout a přejít do stavu 134
    '('             posunout a přejít do stavu 135

    Conversion         přejít do stavu 136
    Literal            přejít do stavu 137
    Operand            přejít do stavu 138
    PrimaryExpression  přejít do stavu 139
    QualifiedIdent     přejít do stavu 140
    Type               přejít do stavu 141


State 70

  218 UnaryExpr1: UnaryExpr11 .

    $výchozí  reduce using rule 218 (UnaryExpr1)


State 71

  191 TruncateTableStmt: _TRUNCATE _TABLE TableName .

    $výchozí  reduce using rule 191 (TruncateTableStmt)


State 72

  225 UpdateStmt1: _SET .

    $výchozí  reduce using rule 225 (UpdateStmt1)


State 73

    4 Assignment: . ColumnName '=' Expression
    5 AssignmentList: . Assignment AssignmentList1 AssignmentList2
   15 ColumnName: . _IDENTIFIER
  223 UpdateStmt: _UPDATE TableName UpdateStmt1 . AssignmentList UpdateStmt2

    _IDENTIFIER  posunout a přejít do stavu 142

    Assignment      přejít do stavu 143
    AssignmentList  přejít do stavu 144
    ColumnName      přejít do stavu 145


State 74

    1 AlterTableStmt: . _ALTER _TABLE TableName AlterTableStmt1
   10 BeginTransactionStmt: . _BEGIN _TRANSACTION
   21 CommitStmt: . _COMMIT
   23 CreateIndexStmt: . _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON TableName '(' CreateIndexStmt3 ')'
   30 CreateTableStmt: . _CREATE _TABLE CreateTableStmt1 TableName '(' ColumnDef CreateTableStmt2 CreateTableStmt3 ')'
   37 DeleteFromStmt: . _DELETE _FROM TableName DeleteFromStmt1
   40 DropIndexStmt: . _DROP _INDEX DropIndexStmt1 IndexName
   43 DropTableStmt: . _DROP _TABLE DropTableStmt1 TableName
   46 EmptyStmt: . %empty  [$end, ';']
   78 InsertIntoStmt: . _INSERT _INTO TableName InsertIntoStmt1 InsertIntoStmt2
  148 RollbackStmt: . _ROLLBACK
  149 SelectStmt: . _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7
  170 Statement: . EmptyStmt
  171          | . AlterTableStmt
  172          | . BeginTransactionStmt
  173          | . CommitStmt
  174          | . CreateIndexStmt
  175          | . CreateTableStmt
  176          | . DeleteFromStmt
  177          | . DropIndexStmt
  178          | . DropTableStmt
  179          | . InsertIntoStmt
  180          | . RollbackStmt
  181          | . SelectStmt
  182          | . TruncateTableStmt
  183          | . UpdateStmt
  186 StatementList1: StatementList1 ';' . Statement
  191 TruncateTableStmt: . _TRUNCATE _TABLE TableName
  223 UpdateStmt: . _UPDATE TableName UpdateStmt1 AssignmentList UpdateStmt2

    _ALTER     posunout a přejít do stavu 1
    _BEGIN     posunout a přejít do stavu 2
    _COMMIT    posunout a přejít do stavu 3
    _CREATE    posunout a přejít do stavu 4
    _DELETE    posunout a přejít do stavu 5
    _DROP      posunout a přejít do stavu 6
    _INSERT    posunout a přejít do stavu 7
    _ROLLBACK  posunout a přejít do stavu 8
    _SELECT    posunout a přejít do stavu 9
    _TRUNCATE  posunout a přejít do stavu 10
    _UPDATE    posunout a přejít do stavu 11

    $výchozí  reduce using rule 46 (EmptyStmt)

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
    Statement             přejít do stavu 146
    TruncateTableStmt     přejít do stavu 27
    UpdateStmt            přejít do stavu 28


State 75

    2 AlterTableStmt1: _ADD . ColumnDef
   14 ColumnDef: . ColumnName Type
   15 ColumnName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 142

    ColumnDef   přejít do stavu 147
    ColumnName  přejít do stavu 148


State 76

    3 AlterTableStmt1: _DROP . _COLUMN ColumnName

    _COLUMN  posunout a přejít do stavu 149


State 77

    1 AlterTableStmt: _ALTER _TABLE TableName AlterTableStmt1 .

    $výchozí  reduce using rule 1 (AlterTableStmt)


State 78

   32 CreateTableStmt1: _IF _NOT . _EXISTS

    _EXISTS  posunout a přejít do stavu 150


State 79

   30 CreateTableStmt: _CREATE _TABLE CreateTableStmt1 TableName . '(' ColumnDef CreateTableStmt2 CreateTableStmt3 ')'

    '('  posunout a přejít do stavu 151


State 80

   27 CreateIndexStmt2: _IF . _NOT _EXISTS

    _NOT  posunout a přejít do stavu 152


State 81

   23 CreateIndexStmt: _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 . IndexName _ON TableName '(' CreateIndexStmt3 ')'
   77 IndexName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 86

    IndexName  přejít do stavu 153


State 82

   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'
  233 WhereClause: _WHERE . Expression

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 154
    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 83

   37 DeleteFromStmt: _DELETE _FROM TableName DeleteFromStmt1 .

    $výchozí  reduce using rule 37 (DeleteFromStmt)


State 84

   39 DeleteFromStmt1: WhereClause .

    $výchozí  reduce using rule 39 (DeleteFromStmt1)


State 85

   42 DropIndexStmt1: _IF _EXISTS .

    $výchozí  reduce using rule 42 (DropIndexStmt1)


State 86

   77 IndexName: _IDENTIFIER .

    $výchozí  reduce using rule 77 (IndexName)


State 87

   40 DropIndexStmt: _DROP _INDEX DropIndexStmt1 IndexName .

    $výchozí  reduce using rule 40 (DropIndexStmt)


State 88

   45 DropTableStmt1: _IF _EXISTS .

    $výchozí  reduce using rule 45 (DropTableStmt1)


State 89

   43 DropTableStmt: _DROP _TABLE DropTableStmt1 TableName .

    $výchozí  reduce using rule 43 (DropTableStmt)


State 90

   15 ColumnName: . _IDENTIFIER
   16 ColumnNameList: . ColumnName ColumnNameList1 ColumnNameList2
   80 InsertIntoStmt1: '(' . ColumnNameList ')'

    _IDENTIFIER  posunout a přejít do stavu 142

    ColumnName      přejít do stavu 155
    ColumnNameList  přejít do stavu 156


State 91

   78 InsertIntoStmt: _INSERT _INTO TableName InsertIntoStmt1 . InsertIntoStmt2
   81 InsertIntoStmt2: . Values
   82                | . SelectStmt
  149 SelectStmt: . _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7
  228 Values: . _VALUES '(' ExpressionList ')' Values1 Values2

    _SELECT  posunout a přejít do stavu 9
    _VALUES  posunout a přejít do stavu 157

    InsertIntoStmt2  přejít do stavu 158
    SelectStmt       přejít do stavu 159
    Values           přejít do stavu 160


State 92

   69 Field1: _AS . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 161


State 93

   67 Field: Expression Field1 .

    $výchozí  reduce using rule 67 (Field)


State 94

  188 Term: Factor Term1 .  [$end, _OROR, _AS, _ASC, _DESC, _FROM, _GROUPBY, _LIMIT, _OFFSET, _ORDER, _WHERE, ',', ')', ']', ';', ':']
  190 Term1: Term1 . _ANDAND Factor

    _ANDAND  posunout a přejít do stavu 162

    $výchozí  reduce using rule 188 (Term)


State 95

   70 FieldList: Field FieldList1 . FieldList2
   72 FieldList1: FieldList1 . ',' Field
   73 FieldList2: . %empty  [_FROM]
   74           | . ','

    ','  posunout a přejít do stavu 163

    $výchozí  reduce using rule 73 (FieldList2)

    FieldList2  přejít do stavu 164


State 96

   55 Factor: PrimaryFactor Factor1 . Factor2
   57 Factor1: Factor1 . Factor11 PrimaryFactor
   58 Factor11: . _GE
   59         | . '>'
   60         | . _LE
   61         | . '<'
   62         | . _NEQ
   63         | . _EQ
   64         | . _LIKE
   65 Factor2: . %empty  [$end, _ANDAND, _OROR, _AS, _ASC, _DESC, _FROM, _GROUPBY, _LIMIT, _OFFSET, _ORDER, _WHERE, ',', ')', ']', ';', ':']
   66        | . Predicate
  102 Predicate: . Predicate1
  103 Predicate1: . Predicate11 Predicate12
  104           | . _IS Predicate13 _NULL
  105 Predicate11: . %empty  [_BETWEEN, _IN]
  106            | . _NOT

    _EQ    posunout a přejít do stavu 165
    _GE    posunout a přejít do stavu 166
    _LE    posunout a přejít do stavu 167
    _NEQ   posunout a přejít do stavu 168
    _IS    posunout a přejít do stavu 169
    _LIKE  posunout a přejít do stavu 170
    _NOT   posunout a přejít do stavu 171
    '>'    posunout a přejít do stavu 172
    '<'    posunout a přejít do stavu 173

    _BETWEEN    reduce using rule 105 (Predicate11)
    _IN         reduce using rule 105 (Predicate11)
    $výchozí  reduce using rule 65 (Factor2)

    Factor11     přejít do stavu 174
    Factor2      přejít do stavu 175
    Predicate    přejít do stavu 176
    Predicate1   přejít do stavu 177
    Predicate11  přejít do stavu 178


State 97

  116 PrimaryFactor: PrimaryTerm PrimaryFactor1 .  [$end, _ANDAND, _EQ, _GE, _LE, _NEQ, _OROR, _AND, _AS, _ASC, _BETWEEN, _DESC, _FROM, _GROUPBY, _IN, _IS, _LIKE, _LIMIT, _NOT, _OFFSET, _ORDER, _WHERE, ',', ')', '>', '<', ']', ';', ':']
  118 PrimaryFactor1: PrimaryFactor1 . PrimaryFactor11 PrimaryTerm
  119 PrimaryFactor11: . '^'
  120                | . '|'
  121                | . '-'
  122                | . '+'

    '^'  posunout a přejít do stavu 179
    '|'  posunout a přejít do stavu 180
    '-'  posunout a přejít do stavu 181
    '+'  posunout a přejít do stavu 182

    $výchozí  reduce using rule 116 (PrimaryFactor)

    PrimaryFactor11  přejít do stavu 183


State 98

  136 RecordSet: . RecordSet1 RecordSet2
  137 RecordSet1: . TableName
  138           | . '(' SelectStmt RecordSet11 ')'
  143 RecordSetList: . RecordSet RecordSetList1 RecordSetList2
  149 SelectStmt: _SELECT SelectStmt1 SelectStmt2 _FROM . RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7
  187 TableName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 41
    '('          posunout a přejít do stavu 184

    RecordSet      přejít do stavu 185
    RecordSet1     přejít do stavu 186
    RecordSetList  přejít do stavu 187
    TableName      přejít do stavu 188


State 99

   47 Expression: Term Expression1 .  [$end, _AS, _ASC, _DESC, _FROM, _GROUPBY, _LIMIT, _OFFSET, _ORDER, _WHERE, ',', ')', ']', ';', ':']
   49 Expression1: Expression1 . _OROR Term

    _OROR  posunout a přejít do stavu 189

    $výchozí  reduce using rule 47 (Expression)


State 100

  123 PrimaryTerm: UnaryExpr PrimaryTerm1 .  [$end, _ANDAND, _EQ, _GE, _LE, _NEQ, _OROR, _AND, _AS, _ASC, _BETWEEN, _DESC, _FROM, _GROUPBY, _IN, _IS, _LIKE, _LIMIT, _NOT, _OFFSET, _ORDER, _WHERE, ',', ')', '>', '<', ']', '^', '|', '-', '+', ';', ':']
  125 PrimaryTerm1: PrimaryTerm1 . PrimaryTerm11 UnaryExpr
  126 PrimaryTerm11: . _ANDNOT
  127              | . '&'
  128              | . _LSH
  129              | . _RSH
  130              | . '%'
  131              | . '/'
  132              | . '*'

    _ANDNOT  posunout a přejít do stavu 190
    _LSH     posunout a přejít do stavu 191
    _RSH     posunout a přejít do stavu 192
    '&'      posunout a přejít do stavu 193
    '%'      posunout a přejít do stavu 194
    '/'      posunout a přejít do stavu 195
    '*'      posunout a přejít do stavu 196

    $výchozí  reduce using rule 123 (PrimaryTerm)

    PrimaryTerm11  přejít do stavu 197


State 101

   87 Literal: _FLOAT_LIT .

    $výchozí  reduce using rule 87 (Literal)


State 102

  133 QualifiedIdent: _IDENTIFIER . QualifiedIdent1
  134 QualifiedIdent1: . %empty  [$end, _ANDAND, _ANDNOT, _EQ, _GE, _LE, _LSH, _NEQ, _OROR, _RSH, _AND, _AS, _ASC, _BETWEEN, _DESC, _FROM, _GROUPBY, _IN, _IS, _LIKE, _LIMIT, _NOT, _OFFSET, _ORDER, _WHERE, ',', '(', ')', '>', '<', '[', ']', '^', '|', '-', '+', '&', '%', '/', '*', ';', ':']
  135                | . '.' _IDENTIFIER

    '.'  posunout a přejít do stavu 198

    $výchozí  reduce using rule 134 (QualifiedIdent1)

    QualifiedIdent1  přejít do stavu 199


State 103

   88 Literal: _IMAGINARY_LIT .

    $výchozí  reduce using rule 88 (Literal)


State 104

   89 Literal: _INT_LIT .

    $výchozí  reduce using rule 89 (Literal)


State 105

   92 Literal: _QL_PARAMETER .

    $výchozí  reduce using rule 92 (Literal)


State 106

   90 Literal: _RUNE_LIT .

    $výchozí  reduce using rule 90 (Literal)


State 107

   91 Literal: _STRING_LIT .

    $výchozí  reduce using rule 91 (Literal)


State 108

  192 Type: _BIGINT .

    $výchozí  reduce using rule 192 (Type)


State 109

  193 Type: _BIGRAT .

    $výchozí  reduce using rule 193 (Type)


State 110

  194 Type: _BLOB .

    $výchozí  reduce using rule 194 (Type)


State 111

  195 Type: _BOOL .

    $výchozí  reduce using rule 195 (Type)


State 112

  196 Type: _BYTE .

    $výchozí  reduce using rule 196 (Type)


State 113

  197 Type: _COMPLEX128 .

    $výchozí  reduce using rule 197 (Type)


State 114

  198 Type: _COMPLEX64 .

    $výchozí  reduce using rule 198 (Type)


State 115

  199 Type: _DURATION .

    $výchozí  reduce using rule 199 (Type)


State 116

   84 Literal: _FALSE .

    $výchozí  reduce using rule 84 (Literal)


State 117

  200 Type: _FLOAT .

    $výchozí  reduce using rule 200 (Type)


State 118

  201 Type: _FLOAT32 .

    $výchozí  reduce using rule 201 (Type)


State 119

  202 Type: _FLOAT64 .

    $výchozí  reduce using rule 202 (Type)


State 120

  203 Type: _INT .

    $výchozí  reduce using rule 203 (Type)


State 121

  204 Type: _INT16 .

    $výchozí  reduce using rule 204 (Type)


State 122

  205 Type: _INT32 .

    $výchozí  reduce using rule 205 (Type)


State 123

  206 Type: _INT64 .

    $výchozí  reduce using rule 206 (Type)


State 124

  207 Type: _INT8 .

    $výchozí  reduce using rule 207 (Type)


State 125

   85 Literal: _NULL .

    $výchozí  reduce using rule 85 (Literal)


State 126

  208 Type: _RUNE .

    $výchozí  reduce using rule 208 (Type)


State 127

  209 Type: _STRING .

    $výchozí  reduce using rule 209 (Type)


State 128

  210 Type: _TIME .

    $výchozí  reduce using rule 210 (Type)


State 129

   86 Literal: _TRUE .

    $výchozí  reduce using rule 86 (Literal)


State 130

  211 Type: _UINT .

    $výchozí  reduce using rule 211 (Type)


State 131

  212 Type: _UINT16 .

    $výchozí  reduce using rule 212 (Type)


State 132

  213 Type: _UINT32 .

    $výchozí  reduce using rule 213 (Type)


State 133

  214 Type: _UINT64 .

    $výchozí  reduce using rule 214 (Type)


State 134

  215 Type: _UINT8 .

    $výchozí  reduce using rule 215 (Type)


State 135

   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
   96 Operand: '(' . Expression ')'
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 200
    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 136

  112 PrimaryExpression: Conversion .

    $výchozí  reduce using rule 112 (PrimaryExpression)


State 137

   94 Operand: Literal .

    $výchozí  reduce using rule 94 (Operand)


State 138

  111 PrimaryExpression: Operand .

    $výchozí  reduce using rule 111 (PrimaryExpression)


State 139

   11 Call: . '(' Call1 ')'
   76 Index: . '[' Expression ']'
  113 PrimaryExpression: PrimaryExpression . Index
  114                  | PrimaryExpression . Slice
  115                  | PrimaryExpression . Call
  164 Slice: . '[' Slice1 ':' Slice2 ']'
  216 UnaryExpr: UnaryExpr1 PrimaryExpression .  [$end, _ANDAND, _ANDNOT, _EQ, _GE, _LE, _LSH, _NEQ, _OROR, _RSH, _AND, _AS, _ASC, _BETWEEN, _DESC, _FROM, _GROUPBY, _IN, _IS, _LIKE, _LIMIT, _NOT, _OFFSET, _ORDER, _WHERE, ',', ')', '>', '<', ']', '^', '|', '-', '+', '&', '%', '/', '*', ';', ':']

    '('  posunout a přejít do stavu 201
    '['  posunout a přejít do stavu 202

    $výchozí  reduce using rule 216 (UnaryExpr)

    Call   přejít do stavu 203
    Index  přejít do stavu 204
    Slice  přejít do stavu 205


State 140

   95 Operand: QualifiedIdent .

    $výchozí  reduce using rule 95 (Operand)


State 141

   22 Conversion: Type . '(' Expression ')'

    '('  posunout a přejít do stavu 206


State 142

   15 ColumnName: _IDENTIFIER .

    $výchozí  reduce using rule 15 (ColumnName)


State 143

    5 AssignmentList: Assignment . AssignmentList1 AssignmentList2
    6 AssignmentList1: . %empty
    7                | . AssignmentList1 ',' Assignment

    $výchozí  reduce using rule 6 (AssignmentList1)

    AssignmentList1  přejít do stavu 207


State 144

  223 UpdateStmt: _UPDATE TableName UpdateStmt1 AssignmentList . UpdateStmt2
  226 UpdateStmt2: . %empty  [$end, ';']
  227            | . WhereClause
  233 WhereClause: . _WHERE Expression

    _WHERE  posunout a přejít do stavu 82

    $výchozí  reduce using rule 226 (UpdateStmt2)

    UpdateStmt2  přejít do stavu 208
    WhereClause  přejít do stavu 209


State 145

    4 Assignment: ColumnName . '=' Expression

    '='  posunout a přejít do stavu 210


State 146

  186 StatementList1: StatementList1 ';' Statement .

    $výchozí  reduce using rule 186 (StatementList1)


State 147

    2 AlterTableStmt1: _ADD ColumnDef .

    $výchozí  reduce using rule 2 (AlterTableStmt1)


State 148

   14 ColumnDef: ColumnName . Type
  192 Type: . _BIGINT
  193     | . _BIGRAT
  194     | . _BLOB
  195     | . _BOOL
  196     | . _BYTE
  197     | . _COMPLEX128
  198     | . _COMPLEX64
  199     | . _DURATION
  200     | . _FLOAT
  201     | . _FLOAT32
  202     | . _FLOAT64
  203     | . _INT
  204     | . _INT16
  205     | . _INT32
  206     | . _INT64
  207     | . _INT8
  208     | . _RUNE
  209     | . _STRING
  210     | . _TIME
  211     | . _UINT
  212     | . _UINT16
  213     | . _UINT32
  214     | . _UINT64
  215     | . _UINT8

    _BIGINT      posunout a přejít do stavu 108
    _BIGRAT      posunout a přejít do stavu 109
    _BLOB        posunout a přejít do stavu 110
    _BOOL        posunout a přejít do stavu 111
    _BYTE        posunout a přejít do stavu 112
    _COMPLEX128  posunout a přejít do stavu 113
    _COMPLEX64   posunout a přejít do stavu 114
    _DURATION    posunout a přejít do stavu 115
    _FLOAT       posunout a přejít do stavu 117
    _FLOAT32     posunout a přejít do stavu 118
    _FLOAT64     posunout a přejít do stavu 119
    _INT         posunout a přejít do stavu 120
    _INT16       posunout a přejít do stavu 121
    _INT32       posunout a přejít do stavu 122
    _INT64       posunout a přejít do stavu 123
    _INT8        posunout a přejít do stavu 124
    _RUNE        posunout a přejít do stavu 126
    _STRING      posunout a přejít do stavu 127
    _TIME        posunout a přejít do stavu 128
    _UINT        posunout a přejít do stavu 130
    _UINT16      posunout a přejít do stavu 131
    _UINT32      posunout a přejít do stavu 132
    _UINT64      posunout a přejít do stavu 133
    _UINT8       posunout a přejít do stavu 134

    Type  přejít do stavu 211


State 149

    3 AlterTableStmt1: _DROP _COLUMN . ColumnName
   15 ColumnName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 142

    ColumnName  přejít do stavu 212


State 150

   32 CreateTableStmt1: _IF _NOT _EXISTS .

    $výchozí  reduce using rule 32 (CreateTableStmt1)


State 151

   14 ColumnDef: . ColumnName Type
   15 ColumnName: . _IDENTIFIER
   30 CreateTableStmt: _CREATE _TABLE CreateTableStmt1 TableName '(' . ColumnDef CreateTableStmt2 CreateTableStmt3 ')'

    _IDENTIFIER  posunout a přejít do stavu 142

    ColumnDef   přejít do stavu 213
    ColumnName  přejít do stavu 148


State 152

   27 CreateIndexStmt2: _IF _NOT . _EXISTS

    _EXISTS  posunout a přejít do stavu 214


State 153

   23 CreateIndexStmt: _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName . _ON TableName '(' CreateIndexStmt3 ')'

    _ON  posunout a přejít do stavu 215


State 154

  233 WhereClause: _WHERE Expression .

    $výchozí  reduce using rule 233 (WhereClause)


State 155

   16 ColumnNameList: ColumnName . ColumnNameList1 ColumnNameList2
   17 ColumnNameList1: . %empty
   18                | . ColumnNameList1 ',' ColumnName

    $výchozí  reduce using rule 17 (ColumnNameList1)

    ColumnNameList1  přejít do stavu 216


State 156

   80 InsertIntoStmt1: '(' ColumnNameList . ')'

    ')'  posunout a přejít do stavu 217


State 157

  228 Values: _VALUES . '(' ExpressionList ')' Values1 Values2

    '('  posunout a přejít do stavu 218


State 158

   78 InsertIntoStmt: _INSERT _INTO TableName InsertIntoStmt1 InsertIntoStmt2 .

    $výchozí  reduce using rule 78 (InsertIntoStmt)


State 159

   82 InsertIntoStmt2: SelectStmt .

    $výchozí  reduce using rule 82 (InsertIntoStmt2)


State 160

   81 InsertIntoStmt2: Values .

    $výchozí  reduce using rule 81 (InsertIntoStmt2)


State 161

   69 Field1: _AS _IDENTIFIER .

    $výchozí  reduce using rule 69 (Field1)


State 162

   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  190 Term1: Term1 _ANDAND . Factor
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Factor         přejít do stavu 219
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 163

   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
   67 Field: . Expression Field1
   72 FieldList1: FieldList1 ',' . Field
   74 FieldList2: ',' .  [_FROM]
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    _FROM       reduce using rule 74 (FieldList2)
    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 60
    Factor         přejít do stavu 61
    Field          přejít do stavu 220
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 164

   70 FieldList: Field FieldList1 FieldList2 .

    $výchozí  reduce using rule 70 (FieldList)


State 165

   63 Factor11: _EQ .

    $výchozí  reduce using rule 63 (Factor11)


State 166

   58 Factor11: _GE .

    $výchozí  reduce using rule 58 (Factor11)


State 167

   60 Factor11: _LE .

    $výchozí  reduce using rule 60 (Factor11)


State 168

   62 Factor11: _NEQ .

    $výchozí  reduce using rule 62 (Factor11)


State 169

  104 Predicate1: _IS . Predicate13 _NULL
  109 Predicate13: . %empty  [_NULL]
  110            | . _NOT

    _NOT  posunout a přejít do stavu 221

    $výchozí  reduce using rule 109 (Predicate13)

    Predicate13  přejít do stavu 222


State 170

   64 Factor11: _LIKE .

    $výchozí  reduce using rule 64 (Factor11)


State 171

  106 Predicate11: _NOT .

    $výchozí  reduce using rule 106 (Predicate11)


State 172

   59 Factor11: '>' .

    $výchozí  reduce using rule 59 (Factor11)


State 173

   61 Factor11: '<' .

    $výchozí  reduce using rule 61 (Factor11)


State 174

   57 Factor1: Factor1 Factor11 . PrimaryFactor
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    PrimaryFactor  přejít do stavu 223
    PrimaryTerm    přejít do stavu 65
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 175

   55 Factor: PrimaryFactor Factor1 Factor2 .

    $výchozí  reduce using rule 55 (Factor)


State 176

   66 Factor2: Predicate .

    $výchozí  reduce using rule 66 (Factor2)


State 177

  102 Predicate: Predicate1 .

    $výchozí  reduce using rule 102 (Predicate)


State 178

  103 Predicate1: Predicate11 . Predicate12
  107 Predicate12: . _IN '(' ExpressionList ')'
  108            | . _BETWEEN PrimaryFactor _AND PrimaryFactor

    _BETWEEN  posunout a přejít do stavu 224
    _IN       posunout a přejít do stavu 225

    Predicate12  přejít do stavu 226


State 179

  119 PrimaryFactor11: '^' .

    $výchozí  reduce using rule 119 (PrimaryFactor11)


State 180

  120 PrimaryFactor11: '|' .

    $výchozí  reduce using rule 120 (PrimaryFactor11)


State 181

  121 PrimaryFactor11: '-' .

    $výchozí  reduce using rule 121 (PrimaryFactor11)


State 182

  122 PrimaryFactor11: '+' .

    $výchozí  reduce using rule 122 (PrimaryFactor11)


State 183

  118 PrimaryFactor1: PrimaryFactor1 PrimaryFactor11 . PrimaryTerm
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    PrimaryTerm  přejít do stavu 227
    UnaryExpr    přejít do stavu 68
    UnaryExpr1   přejít do stavu 69
    UnaryExpr11  přejít do stavu 70


State 184

  138 RecordSet1: '(' . SelectStmt RecordSet11 ')'
  149 SelectStmt: . _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7

    _SELECT  posunout a přejít do stavu 9

    SelectStmt  přejít do stavu 228


State 185

  143 RecordSetList: RecordSet . RecordSetList1 RecordSetList2
  144 RecordSetList1: . %empty
  145               | . RecordSetList1 ',' RecordSet

    $výchozí  reduce using rule 144 (RecordSetList1)

    RecordSetList1  přejít do stavu 229


State 186

  136 RecordSet: RecordSet1 . RecordSet2
  141 RecordSet2: . %empty  [$end, _GROUPBY, _LIMIT, _OFFSET, _ORDER, _WHERE, ',', ')', ';']
  142           | . _AS _IDENTIFIER

    _AS  posunout a přejít do stavu 230

    $výchozí  reduce using rule 141 (RecordSet2)

    RecordSet2  přejít do stavu 231


State 187

  149 SelectStmt: _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList . SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7
  154 SelectStmt3: . %empty  [$end, _GROUPBY, _LIMIT, _OFFSET, _ORDER, ')', ';']
  155            | . WhereClause
  233 WhereClause: . _WHERE Expression

    _WHERE  posunout a přejít do stavu 82

    $výchozí  reduce using rule 154 (SelectStmt3)

    SelectStmt3  přejít do stavu 232
    WhereClause  přejít do stavu 233


State 188

  137 RecordSet1: TableName .

    $výchozí  reduce using rule 137 (RecordSet1)


State 189

   49 Expression1: Expression1 _OROR . Term
   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Term           přejít do stavu 234
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 190

  126 PrimaryTerm11: _ANDNOT .

    $výchozí  reduce using rule 126 (PrimaryTerm11)


State 191

  128 PrimaryTerm11: _LSH .

    $výchozí  reduce using rule 128 (PrimaryTerm11)


State 192

  129 PrimaryTerm11: _RSH .

    $výchozí  reduce using rule 129 (PrimaryTerm11)


State 193

  127 PrimaryTerm11: '&' .

    $výchozí  reduce using rule 127 (PrimaryTerm11)


State 194

  130 PrimaryTerm11: '%' .

    $výchozí  reduce using rule 130 (PrimaryTerm11)


State 195

  131 PrimaryTerm11: '/' .

    $výchozí  reduce using rule 131 (PrimaryTerm11)


State 196

  132 PrimaryTerm11: '*' .

    $výchozí  reduce using rule 132 (PrimaryTerm11)


State 197

  125 PrimaryTerm1: PrimaryTerm1 PrimaryTerm11 . UnaryExpr
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    UnaryExpr    přejít do stavu 235
    UnaryExpr1   přejít do stavu 69
    UnaryExpr11  přejít do stavu 70


State 198

  135 QualifiedIdent1: '.' . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 236


State 199

  133 QualifiedIdent: _IDENTIFIER QualifiedIdent1 .

    $výchozí  reduce using rule 133 (QualifiedIdent)


State 200

   96 Operand: '(' Expression . ')'

    ')'  posunout a přejít do stavu 237


State 201

   11 Call: '(' . Call1 ')'
   12 Call1: . %empty  [')']
   13      | . ExpressionList
   47 Expression: . Term Expression1
   50 ExpressionList: . Expression ExpressionList1 ExpressionList2
   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    ')'         reduce using rule 12 (Call1)
    $výchozí  reduce using rule 217 (UnaryExpr1)

    Call1           přejít do stavu 238
    Expression      přejít do stavu 239
    ExpressionList  přejít do stavu 240
    Factor          přejít do stavu 61
    PrimaryFactor   přejít do stavu 64
    PrimaryTerm     přejít do stavu 65
    Term            přejít do stavu 67
    UnaryExpr       přejít do stavu 68
    UnaryExpr1      přejít do stavu 69
    UnaryExpr11     přejít do stavu 70


State 202

   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
   76 Index: '[' . Expression ']'
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  164 Slice: '[' . Slice1 ':' Slice2 ']'
  165 Slice1: . %empty  [':']
  166       | . Expression
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    ':'         reduce using rule 165 (Slice1)
    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 241
    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Slice1         přejít do stavu 242
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 203

  115 PrimaryExpression: PrimaryExpression Call .

    $výchozí  reduce using rule 115 (PrimaryExpression)


State 204

  113 PrimaryExpression: PrimaryExpression Index .

    $výchozí  reduce using rule 113 (PrimaryExpression)


State 205

  114 PrimaryExpression: PrimaryExpression Slice .

    $výchozí  reduce using rule 114 (PrimaryExpression)


State 206

   22 Conversion: Type '(' . Expression ')'
   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 243
    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 207

    5 AssignmentList: Assignment AssignmentList1 . AssignmentList2
    7 AssignmentList1: AssignmentList1 . ',' Assignment
    8 AssignmentList2: . %empty  [$end, _WHERE, ';']
    9                | . ','

    ','  posunout a přejít do stavu 244

    $výchozí  reduce using rule 8 (AssignmentList2)

    AssignmentList2  přejít do stavu 245


State 208

  223 UpdateStmt: _UPDATE TableName UpdateStmt1 AssignmentList UpdateStmt2 .

    $výchozí  reduce using rule 223 (UpdateStmt)


State 209

  227 UpdateStmt2: WhereClause .

    $výchozí  reduce using rule 227 (UpdateStmt2)


State 210

    4 Assignment: ColumnName '=' . Expression
   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 246
    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 211

   14 ColumnDef: ColumnName Type .

    $výchozí  reduce using rule 14 (ColumnDef)


State 212

    3 AlterTableStmt1: _DROP _COLUMN ColumnName .

    $výchozí  reduce using rule 3 (AlterTableStmt1)


State 213

   30 CreateTableStmt: _CREATE _TABLE CreateTableStmt1 TableName '(' ColumnDef . CreateTableStmt2 CreateTableStmt3 ')'
   33 CreateTableStmt2: . %empty
   34                 | . CreateTableStmt2 ',' ColumnDef

    $výchozí  reduce using rule 33 (CreateTableStmt2)

    CreateTableStmt2  přejít do stavu 247


State 214

   27 CreateIndexStmt2: _IF _NOT _EXISTS .

    $výchozí  reduce using rule 27 (CreateIndexStmt2)


State 215

   23 CreateIndexStmt: _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON . TableName '(' CreateIndexStmt3 ')'
  187 TableName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 41

    TableName  přejít do stavu 248


State 216

   16 ColumnNameList: ColumnName ColumnNameList1 . ColumnNameList2
   18 ColumnNameList1: ColumnNameList1 . ',' ColumnName
   19 ColumnNameList2: . %empty  [$end, _LIMIT, _OFFSET, _ORDER, ')', ';']
   20                | . ','

    ','  posunout a přejít do stavu 249

    $výchozí  reduce using rule 19 (ColumnNameList2)

    ColumnNameList2  přejít do stavu 250


State 217

   80 InsertIntoStmt1: '(' ColumnNameList ')' .

    $výchozí  reduce using rule 80 (InsertIntoStmt1)


State 218

   47 Expression: . Term Expression1
   50 ExpressionList: . Expression ExpressionList1 ExpressionList2
   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'
  228 Values: _VALUES '(' . ExpressionList ')' Values1 Values2

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression      přejít do stavu 239
    ExpressionList  přejít do stavu 251
    Factor          přejít do stavu 61
    PrimaryFactor   přejít do stavu 64
    PrimaryTerm     přejít do stavu 65
    Term            přejít do stavu 67
    UnaryExpr       přejít do stavu 68
    UnaryExpr1      přejít do stavu 69
    UnaryExpr11     přejít do stavu 70


State 219

  190 Term1: Term1 _ANDAND Factor .

    $výchozí  reduce using rule 190 (Term1)


State 220

   72 FieldList1: FieldList1 ',' Field .

    $výchozí  reduce using rule 72 (FieldList1)


State 221

  110 Predicate13: _NOT .

    $výchozí  reduce using rule 110 (Predicate13)


State 222

  104 Predicate1: _IS Predicate13 . _NULL

    _NULL  posunout a přejít do stavu 252


State 223

   57 Factor1: Factor1 Factor11 PrimaryFactor .

    $výchozí  reduce using rule 57 (Factor1)


State 224

  108 Predicate12: _BETWEEN . PrimaryFactor _AND PrimaryFactor
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    PrimaryFactor  přejít do stavu 253
    PrimaryTerm    přejít do stavu 65
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 225

  107 Predicate12: _IN . '(' ExpressionList ')'

    '('  posunout a přejít do stavu 254


State 226

  103 Predicate1: Predicate11 Predicate12 .

    $výchozí  reduce using rule 103 (Predicate1)


State 227

  118 PrimaryFactor1: PrimaryFactor1 PrimaryFactor11 PrimaryTerm .

    $výchozí  reduce using rule 118 (PrimaryFactor1)


State 228

  138 RecordSet1: '(' SelectStmt . RecordSet11 ')'
  139 RecordSet11: . %empty  [')']
  140            | . ';'

    ';'  posunout a přejít do stavu 255

    $výchozí  reduce using rule 139 (RecordSet11)

    RecordSet11  přejít do stavu 256


State 229

  143 RecordSetList: RecordSet RecordSetList1 . RecordSetList2
  145 RecordSetList1: RecordSetList1 . ',' RecordSet
  146 RecordSetList2: . %empty  [$end, _GROUPBY, _LIMIT, _OFFSET, _ORDER, _WHERE, ')', ';']
  147               | . ','

    ','  posunout a přejít do stavu 257

    $výchozí  reduce using rule 146 (RecordSetList2)

    RecordSetList2  přejít do stavu 258


State 230

  142 RecordSet2: _AS . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 259


State 231

  136 RecordSet: RecordSet1 RecordSet2 .

    $výchozí  reduce using rule 136 (RecordSet)


State 232

   75 GroupByClause: . _GROUPBY ColumnNameList
  149 SelectStmt: _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 . SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7
  156 SelectStmt4: . %empty  [$end, _LIMIT, _OFFSET, _ORDER, ')', ';']
  157            | . GroupByClause

    _GROUPBY  posunout a přejít do stavu 260

    $výchozí  reduce using rule 156 (SelectStmt4)

    GroupByClause  přejít do stavu 261
    SelectStmt4    přejít do stavu 262


State 233

  155 SelectStmt3: WhereClause .

    $výchozí  reduce using rule 155 (SelectStmt3)


State 234

   49 Expression1: Expression1 _OROR Term .

    $výchozí  reduce using rule 49 (Expression1)


State 235

  125 PrimaryTerm1: PrimaryTerm1 PrimaryTerm11 UnaryExpr .

    $výchozí  reduce using rule 125 (PrimaryTerm1)


State 236

  135 QualifiedIdent1: '.' _IDENTIFIER .

    $výchozí  reduce using rule 135 (QualifiedIdent1)


State 237

   96 Operand: '(' Expression ')' .

    $výchozí  reduce using rule 96 (Operand)


State 238

   11 Call: '(' Call1 . ')'

    ')'  posunout a přejít do stavu 263


State 239

   50 ExpressionList: Expression . ExpressionList1 ExpressionList2
   51 ExpressionList1: . %empty
   52                | . ExpressionList1 ',' Expression

    $výchozí  reduce using rule 51 (ExpressionList1)

    ExpressionList1  přejít do stavu 264


State 240

   13 Call1: ExpressionList .

    $výchozí  reduce using rule 13 (Call1)


State 241

   76 Index: '[' Expression . ']'
  166 Slice1: Expression .  [':']

    ']'  posunout a přejít do stavu 265

    $výchozí  reduce using rule 166 (Slice1)


State 242

  164 Slice: '[' Slice1 . ':' Slice2 ']'

    ':'  posunout a přejít do stavu 266


State 243

   22 Conversion: Type '(' Expression . ')'

    ')'  posunout a přejít do stavu 267


State 244

    4 Assignment: . ColumnName '=' Expression
    7 AssignmentList1: AssignmentList1 ',' . Assignment
    9 AssignmentList2: ',' .  [$end, _WHERE, ';']
   15 ColumnName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 142

    $výchozí  reduce using rule 9 (AssignmentList2)

    Assignment  přejít do stavu 268
    ColumnName  přejít do stavu 145


State 245

    5 AssignmentList: Assignment AssignmentList1 AssignmentList2 .

    $výchozí  reduce using rule 5 (AssignmentList)


State 246

    4 Assignment: ColumnName '=' Expression .

    $výchozí  reduce using rule 4 (Assignment)


State 247

   30 CreateTableStmt: _CREATE _TABLE CreateTableStmt1 TableName '(' ColumnDef CreateTableStmt2 . CreateTableStmt3 ')'
   34 CreateTableStmt2: CreateTableStmt2 . ',' ColumnDef
   35 CreateTableStmt3: . %empty  [')']
   36                 | . ','

    ','  posunout a přejít do stavu 269

    $výchozí  reduce using rule 35 (CreateTableStmt3)

    CreateTableStmt3  přejít do stavu 270


State 248

   23 CreateIndexStmt: _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON TableName . '(' CreateIndexStmt3 ')'

    '('  posunout a přejít do stavu 271


State 249

   15 ColumnName: . _IDENTIFIER
   18 ColumnNameList1: ColumnNameList1 ',' . ColumnName
   20 ColumnNameList2: ',' .  [$end, _LIMIT, _OFFSET, _ORDER, ')', ';']

    _IDENTIFIER  posunout a přejít do stavu 142

    $výchozí  reduce using rule 20 (ColumnNameList2)

    ColumnName  přejít do stavu 272


State 250

   16 ColumnNameList: ColumnName ColumnNameList1 ColumnNameList2 .

    $výchozí  reduce using rule 16 (ColumnNameList)


State 251

  228 Values: _VALUES '(' ExpressionList . ')' Values1 Values2

    ')'  posunout a přejít do stavu 273


State 252

  104 Predicate1: _IS Predicate13 _NULL .

    $výchozí  reduce using rule 104 (Predicate1)


State 253

  108 Predicate12: _BETWEEN PrimaryFactor . _AND PrimaryFactor

    _AND  posunout a přejít do stavu 274


State 254

   47 Expression: . Term Expression1
   50 ExpressionList: . Expression ExpressionList1 ExpressionList2
   55 Factor: . PrimaryFactor Factor1 Factor2
  107 Predicate12: _IN '(' . ExpressionList ')'
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression      přejít do stavu 239
    ExpressionList  přejít do stavu 275
    Factor          přejít do stavu 61
    PrimaryFactor   přejít do stavu 64
    PrimaryTerm     přejít do stavu 65
    Term            přejít do stavu 67
    UnaryExpr       přejít do stavu 68
    UnaryExpr1      přejít do stavu 69
    UnaryExpr11     přejít do stavu 70


State 255

  140 RecordSet11: ';' .

    $výchozí  reduce using rule 140 (RecordSet11)


State 256

  138 RecordSet1: '(' SelectStmt RecordSet11 . ')'

    ')'  posunout a přejít do stavu 276


State 257

  136 RecordSet: . RecordSet1 RecordSet2
  137 RecordSet1: . TableName
  138           | . '(' SelectStmt RecordSet11 ')'
  145 RecordSetList1: RecordSetList1 ',' . RecordSet
  147 RecordSetList2: ',' .  [$end, _GROUPBY, _LIMIT, _OFFSET, _ORDER, _WHERE, ')', ';']
  187 TableName: . _IDENTIFIER

    _IDENTIFIER  posunout a přejít do stavu 41
    '('          posunout a přejít do stavu 184

    $výchozí  reduce using rule 147 (RecordSetList2)

    RecordSet   přejít do stavu 277
    RecordSet1  přejít do stavu 186
    TableName   přejít do stavu 188


State 258

  143 RecordSetList: RecordSet RecordSetList1 RecordSetList2 .

    $výchozí  reduce using rule 143 (RecordSetList)


State 259

  142 RecordSet2: _AS _IDENTIFIER .

    $výchozí  reduce using rule 142 (RecordSet2)


State 260

   15 ColumnName: . _IDENTIFIER
   16 ColumnNameList: . ColumnName ColumnNameList1 ColumnNameList2
   75 GroupByClause: _GROUPBY . ColumnNameList

    _IDENTIFIER  posunout a přejít do stavu 142

    ColumnName      přejít do stavu 155
    ColumnNameList  přejít do stavu 278


State 261

  157 SelectStmt4: GroupByClause .

    $výchozí  reduce using rule 157 (SelectStmt4)


State 262

   97 OrderBy: . _ORDER _BY ExpressionList OrderBy1
  149 SelectStmt: _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 . SelectStmt5 SelectStmt6 SelectStmt7
  158 SelectStmt5: . %empty  [$end, _LIMIT, _OFFSET, ')', ';']
  159            | . OrderBy

    _ORDER  posunout a přejít do stavu 279

    $výchozí  reduce using rule 158 (SelectStmt5)

    OrderBy      přejít do stavu 280
    SelectStmt5  přejít do stavu 281


State 263

   11 Call: '(' Call1 ')' .

    $výchozí  reduce using rule 11 (Call)


State 264

   50 ExpressionList: Expression ExpressionList1 . ExpressionList2
   52 ExpressionList1: ExpressionList1 . ',' Expression
   53 ExpressionList2: . %empty  [$end, _ASC, _DESC, _LIMIT, _OFFSET, ')', ';']
   54                | . ','

    ','  posunout a přejít do stavu 282

    $výchozí  reduce using rule 53 (ExpressionList2)

    ExpressionList2  přejít do stavu 283


State 265

   76 Index: '[' Expression ']' .

    $výchozí  reduce using rule 76 (Index)


State 266

   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  164 Slice: '[' Slice1 ':' . Slice2 ']'
  167 Slice2: . %empty  [']']
  168       | . Expression
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    ']'         reduce using rule 167 (Slice2)
    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 284
    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Slice2         přejít do stavu 285
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 267

   22 Conversion: Type '(' Expression ')' .

    $výchozí  reduce using rule 22 (Conversion)


State 268

    7 AssignmentList1: AssignmentList1 ',' Assignment .

    $výchozí  reduce using rule 7 (AssignmentList1)


State 269

   14 ColumnDef: . ColumnName Type
   15 ColumnName: . _IDENTIFIER
   34 CreateTableStmt2: CreateTableStmt2 ',' . ColumnDef
   36 CreateTableStmt3: ',' .  [')']

    _IDENTIFIER  posunout a přejít do stavu 142

    $výchozí  reduce using rule 36 (CreateTableStmt3)

    ColumnDef   přejít do stavu 286
    ColumnName  přejít do stavu 148


State 270

   30 CreateTableStmt: _CREATE _TABLE CreateTableStmt1 TableName '(' ColumnDef CreateTableStmt2 CreateTableStmt3 . ')'

    ')'  posunout a přejít do stavu 287


State 271

   15 ColumnName: . _IDENTIFIER
   23 CreateIndexStmt: _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON TableName '(' . CreateIndexStmt3 ')'
   28 CreateIndexStmt3: . ColumnName
   29                 | . _ID Call

    _IDENTIFIER  posunout a přejít do stavu 142
    _ID          posunout a přejít do stavu 288

    ColumnName        přejít do stavu 289
    CreateIndexStmt3  přejít do stavu 290


State 272

   18 ColumnNameList1: ColumnNameList1 ',' ColumnName .

    $výchozí  reduce using rule 18 (ColumnNameList1)


State 273

  228 Values: _VALUES '(' ExpressionList ')' . Values1 Values2
  229 Values1: . %empty
  230        | . Values1 ',' '(' ExpressionList ')'

    $výchozí  reduce using rule 229 (Values1)

    Values1  přejít do stavu 291


State 274

  108 Predicate12: _BETWEEN PrimaryFactor _AND . PrimaryFactor
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    PrimaryFactor  přejít do stavu 292
    PrimaryTerm    přejít do stavu 65
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 275

  107 Predicate12: _IN '(' ExpressionList . ')'

    ')'  posunout a přejít do stavu 293


State 276

  138 RecordSet1: '(' SelectStmt RecordSet11 ')' .

    $výchozí  reduce using rule 138 (RecordSet1)


State 277

  145 RecordSetList1: RecordSetList1 ',' RecordSet .

    $výchozí  reduce using rule 145 (RecordSetList1)


State 278

   75 GroupByClause: _GROUPBY ColumnNameList .

    $výchozí  reduce using rule 75 (GroupByClause)


State 279

   97 OrderBy: _ORDER . _BY ExpressionList OrderBy1

    _BY  posunout a přejít do stavu 294


State 280

  159 SelectStmt5: OrderBy .

    $výchozí  reduce using rule 159 (SelectStmt5)


State 281

   83 Limit: . _LIMIT Expression
  149 SelectStmt: _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 . SelectStmt6 SelectStmt7
  160 SelectStmt6: . %empty  [$end, _OFFSET, ')', ';']
  161            | . Limit

    _LIMIT  posunout a přejít do stavu 295

    $výchozí  reduce using rule 160 (SelectStmt6)

    Limit        přejít do stavu 296
    SelectStmt6  přejít do stavu 297


State 282

   47 Expression: . Term Expression1
   52 ExpressionList1: ExpressionList1 ',' . Expression
   54 ExpressionList2: ',' .  [$end, _ASC, _DESC, _LIMIT, _OFFSET, ')', ';']
   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $end        reduce using rule 54 (ExpressionList2)
    _ASC        reduce using rule 54 (ExpressionList2)
    _DESC       reduce using rule 54 (ExpressionList2)
    _LIMIT      reduce using rule 54 (ExpressionList2)
    _OFFSET     reduce using rule 54 (ExpressionList2)
    ')'         reduce using rule 54 (ExpressionList2)
    ';'         reduce using rule 54 (ExpressionList2)
    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 298
    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 283

   50 ExpressionList: Expression ExpressionList1 ExpressionList2 .

    $výchozí  reduce using rule 50 (ExpressionList)


State 284

  168 Slice2: Expression .

    $výchozí  reduce using rule 168 (Slice2)


State 285

  164 Slice: '[' Slice1 ':' Slice2 . ']'

    ']'  posunout a přejít do stavu 299


State 286

   34 CreateTableStmt2: CreateTableStmt2 ',' ColumnDef .

    $výchozí  reduce using rule 34 (CreateTableStmt2)


State 287

   30 CreateTableStmt: _CREATE _TABLE CreateTableStmt1 TableName '(' ColumnDef CreateTableStmt2 CreateTableStmt3 ')' .

    $výchozí  reduce using rule 30 (CreateTableStmt)


State 288

   11 Call: . '(' Call1 ')'
   29 CreateIndexStmt3: _ID . Call

    '('  posunout a přejít do stavu 201

    Call  přejít do stavu 300


State 289

   28 CreateIndexStmt3: ColumnName .

    $výchozí  reduce using rule 28 (CreateIndexStmt3)


State 290

   23 CreateIndexStmt: _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON TableName '(' CreateIndexStmt3 . ')'

    ')'  posunout a přejít do stavu 301


State 291

  228 Values: _VALUES '(' ExpressionList ')' Values1 . Values2
  230 Values1: Values1 . ',' '(' ExpressionList ')'
  231 Values2: . %empty  [$end, ';']
  232        | . ','

    ','  posunout a přejít do stavu 302

    $výchozí  reduce using rule 231 (Values2)

    Values2  přejít do stavu 303


State 292

  108 Predicate12: _BETWEEN PrimaryFactor _AND PrimaryFactor .

    $výchozí  reduce using rule 108 (Predicate12)


State 293

  107 Predicate12: _IN '(' ExpressionList ')' .

    $výchozí  reduce using rule 107 (Predicate12)


State 294

   47 Expression: . Term Expression1
   50 ExpressionList: . Expression ExpressionList1 ExpressionList2
   55 Factor: . PrimaryFactor Factor1 Factor2
   97 OrderBy: _ORDER _BY . ExpressionList OrderBy1
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression      přejít do stavu 239
    ExpressionList  přejít do stavu 304
    Factor          přejít do stavu 61
    PrimaryFactor   přejít do stavu 64
    PrimaryTerm     přejít do stavu 65
    Term            přejít do stavu 67
    UnaryExpr       přejít do stavu 68
    UnaryExpr1      přejít do stavu 69
    UnaryExpr11     přejít do stavu 70


State 295

   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
   83 Limit: _LIMIT . Expression
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 305
    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 296

  161 SelectStmt6: Limit .

    $výchozí  reduce using rule 161 (SelectStmt6)


State 297

   93 Offset: . _OFFSET Expression
  149 SelectStmt: _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 . SelectStmt7
  162 SelectStmt7: . %empty  [$end, ')', ';']
  163            | . Offset

    _OFFSET  posunout a přejít do stavu 306

    $výchozí  reduce using rule 162 (SelectStmt7)

    Offset       přejít do stavu 307
    SelectStmt7  přejít do stavu 308


State 298

   52 ExpressionList1: ExpressionList1 ',' Expression .

    $výchozí  reduce using rule 52 (ExpressionList1)


State 299

  164 Slice: '[' Slice1 ':' Slice2 ']' .

    $výchozí  reduce using rule 164 (Slice)


State 300

   29 CreateIndexStmt3: _ID Call .

    $výchozí  reduce using rule 29 (CreateIndexStmt3)


State 301

   23 CreateIndexStmt: _CREATE CreateIndexStmt1 _INDEX CreateIndexStmt2 IndexName _ON TableName '(' CreateIndexStmt3 ')' .

    $výchozí  reduce using rule 23 (CreateIndexStmt)


State 302

  230 Values1: Values1 ',' . '(' ExpressionList ')'
  232 Values2: ',' .  [$end, ';']

    '('  posunout a přejít do stavu 309

    $výchozí  reduce using rule 232 (Values2)


State 303

  228 Values: _VALUES '(' ExpressionList ')' Values1 Values2 .

    $výchozí  reduce using rule 228 (Values)


State 304

   97 OrderBy: _ORDER _BY ExpressionList . OrderBy1
   98 OrderBy1: . %empty  [$end, _LIMIT, _OFFSET, ')', ';']
   99         | . OrderBy11
  100 OrderBy11: . _ASC
  101          | . _DESC

    _ASC   posunout a přejít do stavu 310
    _DESC  posunout a přejít do stavu 311

    $výchozí  reduce using rule 98 (OrderBy1)

    OrderBy1   přejít do stavu 312
    OrderBy11  přejít do stavu 313


State 305

   83 Limit: _LIMIT Expression .

    $výchozí  reduce using rule 83 (Limit)


State 306

   47 Expression: . Term Expression1
   55 Factor: . PrimaryFactor Factor1 Factor2
   93 Offset: _OFFSET . Expression
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression     přejít do stavu 314
    Factor         přejít do stavu 61
    PrimaryFactor  přejít do stavu 64
    PrimaryTerm    přejít do stavu 65
    Term           přejít do stavu 67
    UnaryExpr      přejít do stavu 68
    UnaryExpr1     přejít do stavu 69
    UnaryExpr11    přejít do stavu 70


State 307

  163 SelectStmt7: Offset .

    $výchozí  reduce using rule 163 (SelectStmt7)


State 308

  149 SelectStmt: _SELECT SelectStmt1 SelectStmt2 _FROM RecordSetList SelectStmt3 SelectStmt4 SelectStmt5 SelectStmt6 SelectStmt7 .

    $výchozí  reduce using rule 149 (SelectStmt)


State 309

   47 Expression: . Term Expression1
   50 ExpressionList: . Expression ExpressionList1 ExpressionList2
   55 Factor: . PrimaryFactor Factor1 Factor2
  116 PrimaryFactor: . PrimaryTerm PrimaryFactor1
  123 PrimaryTerm: . UnaryExpr PrimaryTerm1
  188 Term: . Factor Term1
  216 UnaryExpr: . UnaryExpr1 PrimaryExpression
  217 UnaryExpr1: . %empty  [_FLOAT_LIT, _IDENTIFIER, _IMAGINARY_LIT, _INT_LIT, _QL_PARAMETER, _RUNE_LIT, _STRING_LIT, _BIGINT, _BIGRAT, _BLOB, _BOOL, _BYTE, _COMPLEX128, _COMPLEX64, _DURATION, _FALSE, _FLOAT, _FLOAT32, _FLOAT64, _INT, _INT16, _INT32, _INT64, _INT8, _NULL, _RUNE, _STRING, _TIME, _TRUE, _UINT, _UINT16, _UINT32, _UINT64, _UINT8, '(']
  218           | . UnaryExpr11
  219 UnaryExpr11: . '^'
  220            | . '!'
  221            | . '-'
  222            | . '+'
  230 Values1: Values1 ',' '(' . ExpressionList ')'

    '^'  posunout a přejít do stavu 55
    '-'  posunout a přejít do stavu 56
    '+'  posunout a přejít do stavu 57
    '!'  posunout a přejít do stavu 59

    $výchozí  reduce using rule 217 (UnaryExpr1)

    Expression      přejít do stavu 239
    ExpressionList  přejít do stavu 315
    Factor          přejít do stavu 61
    PrimaryFactor   přejít do stavu 64
    PrimaryTerm     přejít do stavu 65
    Term            přejít do stavu 67
    UnaryExpr       přejít do stavu 68
    UnaryExpr1      přejít do stavu 69
    UnaryExpr11     přejít do stavu 70


State 310

  100 OrderBy11: _ASC .

    $výchozí  reduce using rule 100 (OrderBy11)


State 311

  101 OrderBy11: _DESC .

    $výchozí  reduce using rule 101 (OrderBy11)


State 312

   97 OrderBy: _ORDER _BY ExpressionList OrderBy1 .

    $výchozí  reduce using rule 97 (OrderBy)


State 313

   99 OrderBy1: OrderBy11 .

    $výchozí  reduce using rule 99 (OrderBy1)


State 314

   93 Offset: _OFFSET Expression .

    $výchozí  reduce using rule 93 (Offset)


State 315

  230 Values1: Values1 ',' '(' ExpressionList . ')'

    ')'  posunout a přejít do stavu 316


State 316

  230 Values1: Values1 ',' '(' ExpressionList ')' .

    $výchozí  reduce using rule 230 (Values1)
