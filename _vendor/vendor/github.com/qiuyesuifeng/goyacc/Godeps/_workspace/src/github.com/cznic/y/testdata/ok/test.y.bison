Terminals unused in grammar

   IGNORE
   XLEFT
   XRIGHT
   TEST1
   TEST2


Gramatika

    0 $accept: File $end

    1 $@1: %empty

    2 File: PackageDecl $@1 Imports TopLevelDeclList

    3 PackageDecl: %empty
    4            | PACKAGE Symbol ';'

    5 Imports: %empty
    6        | Imports Import ';'

    7 Import: IMPORT ImportDecl
    8       | IMPORT '(' ImportDeclList oSemi ')'
    9       | IMPORT '(' ')'

   10 ImportDecl: LITERAL
   11           | Symbol LITERAL
   12           | '.' LITERAL

   13 ImportDeclList: ImportDecl
   14               | ImportDeclList ';' ImportDecl

   15 TopLevelDecl: %empty
   16             | CommonDecl
   17             | FuncDecl
   18             | NonDclStmt
   19             | error

   20 CommonDecl: VAR VarDecl
   21           | VAR '(' VarDeclList oSemi ')'
   22           | VAR '(' ')'
   23           | Const ConstDecl
   24           | Const '(' ConstDecl oSemi ')'
   25           | Const '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | Const '(' ')'
   27           | TYPE TypeDecl
   28           | TYPE '(' TypeDeclList oSemi ')'
   29           | TYPE '(' ')'

   30 Const: CONST

   31 VarDecl: DeclNameList Type
   32        | DeclNameList Type '=' ExprList
   33        | DeclNameList '=' ExprList

   34 ConstDecl: DeclNameList Type '=' ExprList
   35          | DeclNameList '=' ExprList

   36 ConstDecl1: ConstDecl
   37           | DeclNameList Type
   38           | DeclNameList

   39 TypeDeclName: Symbol

   40 TypeDecl: TypeDeclName Type

   41 SimpleStmt: Expr
   42           | Expr ASOP Expr
   43           | ExprList '=' ExprList
   44           | ExprList COLAS ExprList
   45           | Expr INC
   46           | Expr DEC

   47 Case: CASE ExprOrTypeList ':'
   48     | CASE ExprOrTypeList '=' Expr ':'
   49     | CASE ExprOrTypeList COLAS Expr ':'
   50     | DEFAULT ':'

   51 $@2: %empty

   52 CompoundStmt: '{' $@2 StmtList '}'

   53 $@3: %empty

   54 CaseBlock: Case $@3 StmtList

   55 CaseBlockList: %empty
   56              | CaseBlockList CaseBlock

   57 $@4: %empty

   58 LoopBody: BODY $@4 StmtList '}'

   59 RangeStmt: ExprList '=' RANGE Expr
   60          | ExprList COLAS RANGE Expr

   61 ForHeader: oSimpleStmt ';' oSimpleStmt ';' oSimpleStmt
   62          | oSimpleStmt
   63          | RangeStmt

   64 ForBody: ForHeader LoopBody

   65 $@5: %empty

   66 ForStmt: FOR $@5 ForBody

   67 IfHeader: oSimpleStmt
   68         | oSimpleStmt ';' oSimpleStmt

   69 $@6: %empty

   70 $@7: %empty

   71 IfStmt: IF $@6 IfHeader LoopBody $@7 ElseIfList Else

   72 $@8: %empty

   73 ElseIf: ELSE IF IfHeader $@8 LoopBody

   74 ElseIfList: %empty
   75           | ElseIfList ElseIf

   76 Else: %empty

   77 $@9: %empty

   78 Else: ELSE $@9 CompoundStmt

   79 $@10: %empty

   80 SwitchStmt: SWITCH $@10 IfHeader BODY CaseBlockList '}'

   81 $@11: %empty

   82 SelectStmt: SELECT $@11 BODY CaseBlockList '}'

   83 Expr: UnaryExpr
   84     | Expr OROR Expr
   85     | Expr ANDAND Expr
   86     | Expr EQ Expr
   87     | Expr NE Expr
   88     | Expr '<' Expr
   89     | Expr LE Expr
   90     | Expr GE Expr
   91     | Expr '>' Expr
   92     | Expr '+' Expr
   93     | Expr '-' Expr
   94     | Expr '|' Expr
   95     | Expr '^' Expr
   96     | Expr '*' Expr
   97     | Expr '/' Expr
   98     | Expr '%' Expr
   99     | Expr '&' Expr
  100     | Expr ANDNOT Expr
  101     | Expr LSH Expr
  102     | Expr RSH Expr
  103     | Expr COMM Expr

  104 UnaryExpr: PrimaryExpr
  105          | '*' UnaryExpr
  106          | '&' UnaryExpr
  107          | '+' UnaryExpr
  108          | '-' UnaryExpr
  109          | '!' UnaryExpr
  110          | '~' UnaryExpr
  111          | '^' UnaryExpr
  112          | COMM UnaryExpr

  113 PseudoCall: PrimaryExpr '(' ')'
  114           | PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | PrimaryExpr '(' ExprOrTypeList DDD oComma ')'

  116 PrimaryExprNoParen: LITERAL
  117                   | Name
  118                   | PrimaryExpr '.' Symbol
  119                   | PrimaryExpr '.' '(' ExprOrType ')'
  120                   | PrimaryExpr '.' '(' TYPE ')'
  121                   | PrimaryExpr '[' Expr ']'
  122                   | PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | PseudoCall
  125                   | ConvType '(' ExprList oComma ')'
  126                   | ConvType '(' ')'
  127                   | CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | FuncLit

  131 StartCompLit: %empty

  132 Keyval: Expr ':' CompLitExpr

  133 BareCompLitExpr: Expr
  134                | '{' StartCompLit BracedKeyvalList '}'

  135 CompLitExpr: Expr
  136            | '{' StartCompLit BracedKeyvalList '}'

  137 PrimaryExpr: PrimaryExprNoParen
  138            | '(' ExprOrType ')'

  139 ExprOrType: Expr
  140           | NonExprType

  141 NameOrType: Type

  142 LBrace: BODY
  143       | '{'

  144 NewName: Symbol

  145 DeclName: Symbol

  146 oNewName: %empty
  147         | NewName

  148 Symbol: IDENT

  149 Name: Symbol

  150 LabelName: NewName

  151 Ddd: DDD
  152    | DDD Type

  153 Type: RecvChanType
  154     | FuncType
  155     | OtherType
  156     | PtrType
  157     | TypeName
  158     | '(' Type ')'

  159 NonExprType: RecvChanType
  160            | FuncType
  161            | OtherType
  162            | '*' NonExprType

  163 NonRecvChanType: FuncType
  164                | OtherType
  165                | PtrType
  166                | TypeName
  167                | '(' Type ')'

  168 ConvType: FuncType
  169         | OtherType

  170 CompLitType: OtherType

  171 FuncRetType: RecvChanType
  172            | FuncType
  173            | OtherType
  174            | PtrType
  175            | TypeName

  176 TypeName: Name
  177         | Name '.' Symbol

  178 OtherType: '[' oExpr ']' Type
  179          | '[' DDD ']' Type
  180          | CHAN NonRecvChanType
  181          | CHAN COMM Type
  182          | MAP '[' Type ']' Type
  183          | StructType
  184          | UnionType
  185          | VariantType
  186          | InterfaceType

  187 PtrType: '*' Type

  188 RecvChanType: COMM CHAN Type

  189 StructType: STRUCT LBrace StructDeclList oSemi '}'
  190           | STRUCT LBrace '}'

  191 UnionType: UNION LBrace StructDeclList oSemi '}'
  192          | UNION LBrace '}'

  193 VariantType: VARIANT LBrace StructDeclList oSemi '}'
  194            | VARIANT LBrace '}'

  195 InterfaceType: INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | INTERFACE LBrace '}'

  197 FuncDecl: FUNC FuncDecl1 FuncBody

  198 FuncDecl1: Symbol '(' oArgTypeListOComma ')' FuncResult
  199          | '(' oArgTypeListOComma ')' Symbol '(' oArgTypeListOComma ')' FuncResult

  200 FuncType: FUNC '(' oArgTypeListOComma ')' FuncResult

  201 FuncBody: %empty
  202         | '{' StmtList '}'

  203 FuncResult: %empty
  204           | FuncRetType
  205           | '(' oArgTypeListOComma ')'

  206 FuncLitDecl: FuncType

  207 FuncLit: FuncLitDecl LBrace StmtList '}'
  208        | FuncLitDecl error

  209 TopLevelDeclList: %empty
  210                 | TopLevelDeclList TopLevelDecl ';'

  211 VarDeclList: VarDecl
  212            | VarDeclList ';' VarDecl

  213 ConstDeclList: ConstDecl1
  214              | ConstDeclList ';' ConstDecl1

  215 TypeDeclList: TypeDecl
  216             | TypeDeclList ';' TypeDecl

  217 StructDeclList: StructDecl
  218               | StructDeclList ';' StructDecl

  219 InterfaceDeclList: InterfaceDecl
  220                  | InterfaceDeclList ';' InterfaceDecl

  221 StructDecl: NewNameList Type oLiteral
  222           | Embedded oLiteral
  223           | '(' Embedded ')' oLiteral
  224           | '*' Embedded oLiteral
  225           | '(' '*' Embedded ')' oLiteral
  226           | '*' '(' Embedded ')' oLiteral

  227 Qualident: IDENT
  228          | IDENT '.' Symbol

  229 Embedded: Qualident

  230 InterfaceDecl: NewName InterfaceMethodDecl
  231              | Qualident
  232              | '(' Qualident ')'

  233 InterfaceMethodDecl: '(' oArgTypeListOComma ')' FuncResult

  234 ArgType: NameOrType
  235        | Symbol NameOrType
  236        | Symbol Ddd
  237        | Ddd

  238 ArgTypeList: ArgType
  239            | ArgTypeList ',' ArgType

  240 oArgTypeListOComma: %empty
  241                   | ArgTypeList oComma

  242 Statement: %empty
  243          | CompoundStmt
  244          | CommonDecl
  245          | NonDclStmt
  246          | error

  247 NonDclStmt: SimpleStmt
  248           | ForStmt
  249           | SwitchStmt
  250           | SelectStmt
  251           | IfStmt
  252           | LabelName ':' Statement
  253           | FALL
  254           | BREAK oNewName
  255           | CONTINUE oNewName
  256           | GO PseudoCall
  257           | DEFER PseudoCall
  258           | GOTO NewName
  259           | GOTO
  260           | RETURN oExprList

  261 StmtList: Statement
  262         | StmtList ';' Statement

  263 NewNameList: NewName
  264            | NewNameList ',' NewName

  265 DeclNameList: DeclName
  266             | DeclNameList ',' DeclName

  267 ExprList: Expr
  268         | ExprList ',' Expr

  269 ExprOrTypeList: ExprOrType
  270               | ExprOrTypeList ',' ExprOrType

  271 KeyvalList: Keyval
  272           | BareCompLitExpr
  273           | KeyvalList ',' Keyval
  274           | KeyvalList ',' BareCompLitExpr

  275 BracedKeyvalList: %empty
  276                 | KeyvalList oComma

  277 oSemi: %empty
  278      | ';'

  279 oComma: %empty
  280       | ','

  281 oExpr: %empty
  282      | Expr

  283 oExprList: %empty
  284          | ExprList

  285 oSimpleStmt: %empty
  286            | SimpleStmt

  287 oLiteral: %empty
  288         | LITERAL


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'!' (33) 109
'%' (37) 98
'&' (38) 99 106
'(' (40) 8 9 21 22 24 25 26 28 29 113 114 115 119 120 125 126 129 138
    158 167 198 199 200 205 223 225 226 232 233
')' (41) 8 9 21 22 24 25 26 28 29 113 114 115 119 120 125 126 129 138
    158 167 198 199 200 205 223 225 226 232 233
'*' (42) 96 105 162 187 224 225 226
'+' (43) 92 107
',' (44) 239 264 266 268 270 273 274 280
'-' (45) 93 108
'.' (46) 12 118 119 120 177 228
'/' (47) 97
':' (58) 47 48 49 50 122 123 132 252
';' (59) 4 6 14 25 61 68 210 212 214 216 218 220 262 278
'<' (60) 88
'=' (61) 32 33 34 35 43 48 59
'>' (62) 91
'[' (91) 121 122 123 178 179 182
']' (93) 121 122 123 178 179 182
'^' (94) 95 111
'{' (123) 52 128 129 134 136 143 202
'|' (124) 94
'}' (125) 52 58 80 82 127 128 129 134 136 189 190 191 192 193 194 195
    196 202 207
'~' (126) 110
error (256) 19 208 246
ANDAND (258) 85
ANDNOT (259) 100
ASOP (260) 42
BODY (261) 58 80 82 142
BREAK (262) 254
CASE (263) 47 48 49
CHAN (264) 180 181 188
COLAS (265) 44 49 60
COMM (266) 103 112 181 188
CONST (267) 30
CONTINUE (268) 255
DDD (269) 115 151 152 179
DEC (270) 46
DEFAULT (271) 50
DEFER (272) 257
ELSE (273) 73 78
EQ (274) 86
FALL (275) 253
FOR (276) 66
FUNC (277) 197 200
GE (278) 90
GO (279) 256
GOTO (280) 258 259
IDENT (281) 148 227 228
IF (282) 71 73
IGNORE (283)
IMPORT (284) 7 8 9
INC (285) 45
INTERFACE (286) 195 196
LE (287) 89
LITERAL (288) 10 11 12 116 288
LSH (289) 101
MAP (290) 182
NE (291) 87
OROR (292) 84
PACKAGE (293) 4
RANGE (294) 59 60
RETURN (295) 260
RSH (296) 102
SELECT (297) 82
STRUCT (298) 189 190
SWITCH (299) 80
TYPE (300) 27 28 29 120
UNION (301) 191 192
VAR (302) 20 21 22
VARIANT (303) 193 194
XLEFT (304)
XRIGHT (305)
TEST1 (306)
TEST2 (307)
notPackage (308)
notParen (309)
preferToRightParen (310)


Neterminály s pravidly, ve kterých se objevují

$accept (79)
    vlevo: 0
File (80)
    vlevo: 2, vpravo: 0
$@1 (81)
    vlevo: 1, vpravo: 2
PackageDecl (82)
    vlevo: 3 4, vpravo: 2
Imports (83)
    vlevo: 5 6, vpravo: 2 6
Import (84)
    vlevo: 7 8 9, vpravo: 6
ImportDecl (85)
    vlevo: 10 11 12, vpravo: 7 13 14
ImportDeclList (86)
    vlevo: 13 14, vpravo: 8 14
TopLevelDecl (87)
    vlevo: 15 16 17 18 19, vpravo: 210
CommonDecl (88)
    vlevo: 20 21 22 23 24 25 26 27 28 29, vpravo: 16 244
Const (89)
    vlevo: 30, vpravo: 23 24 25 26
VarDecl (90)
    vlevo: 31 32 33, vpravo: 20 211 212
ConstDecl (91)
    vlevo: 34 35, vpravo: 23 24 25 36
ConstDecl1 (92)
    vlevo: 36 37 38, vpravo: 213 214
TypeDeclName (93)
    vlevo: 39, vpravo: 40
TypeDecl (94)
    vlevo: 40, vpravo: 27 215 216
SimpleStmt (95)
    vlevo: 41 42 43 44 45 46, vpravo: 247 286
Case (96)
    vlevo: 47 48 49 50, vpravo: 54
CompoundStmt (97)
    vlevo: 52, vpravo: 78 243
$@2 (98)
    vlevo: 51, vpravo: 52
CaseBlock (99)
    vlevo: 54, vpravo: 56
$@3 (100)
    vlevo: 53, vpravo: 54
CaseBlockList (101)
    vlevo: 55 56, vpravo: 56 80 82
LoopBody (102)
    vlevo: 58, vpravo: 64 71 73
$@4 (103)
    vlevo: 57, vpravo: 58
RangeStmt (104)
    vlevo: 59 60, vpravo: 63
ForHeader (105)
    vlevo: 61 62 63, vpravo: 64
ForBody (106)
    vlevo: 64, vpravo: 66
ForStmt (107)
    vlevo: 66, vpravo: 248
$@5 (108)
    vlevo: 65, vpravo: 66
IfHeader (109)
    vlevo: 67 68, vpravo: 71 73 80
IfStmt (110)
    vlevo: 71, vpravo: 251
$@6 (111)
    vlevo: 69, vpravo: 71
$@7 (112)
    vlevo: 70, vpravo: 71
ElseIf (113)
    vlevo: 73, vpravo: 75
$@8 (114)
    vlevo: 72, vpravo: 73
ElseIfList (115)
    vlevo: 74 75, vpravo: 71 75
Else (116)
    vlevo: 76 78, vpravo: 71
$@9 (117)
    vlevo: 77, vpravo: 78
SwitchStmt (118)
    vlevo: 80, vpravo: 249
$@10 (119)
    vlevo: 79, vpravo: 80
SelectStmt (120)
    vlevo: 82, vpravo: 250
$@11 (121)
    vlevo: 81, vpravo: 82
Expr (122)
    vlevo: 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101
    102 103, vpravo: 41 42 45 46 48 49 59 60 84 85 86 87 88 89 90 91
    92 93 94 95 96 97 98 99 100 101 102 103 121 132 133 135 139 267
    268 282
UnaryExpr (123)
    vlevo: 104 105 106 107 108 109 110 111 112, vpravo: 83 105 106
    107 108 109 110 111 112
PseudoCall (124)
    vlevo: 113 114 115, vpravo: 124 256 257
PrimaryExprNoParen (125)
    vlevo: 116 117 118 119 120 121 122 123 124 125 126 127 128 129
    130, vpravo: 128 137
StartCompLit (126)
    vlevo: 131, vpravo: 127 128 129 134 136
Keyval (127)
    vlevo: 132, vpravo: 271 273
BareCompLitExpr (128)
    vlevo: 133 134, vpravo: 272 274
CompLitExpr (129)
    vlevo: 135 136, vpravo: 132
PrimaryExpr (130)
    vlevo: 137 138, vpravo: 104 113 114 115 118 119 120 121 122 123
ExprOrType (131)
    vlevo: 139 140, vpravo: 119 129 138 269 270
NameOrType (132)
    vlevo: 141, vpravo: 234 235
LBrace (133)
    vlevo: 142 143, vpravo: 127 189 190 191 192 193 194 195 196 207
NewName (134)
    vlevo: 144, vpravo: 147 150 230 258 263 264
DeclName (135)
    vlevo: 145, vpravo: 265 266
oNewName (136)
    vlevo: 146 147, vpravo: 254 255
Symbol (137)
    vlevo: 148, vpravo: 4 11 39 118 144 145 149 177 198 199 228 235
    236
Name (138)
    vlevo: 149, vpravo: 117 176 177
LabelName (139)
    vlevo: 150, vpravo: 252
Ddd (140)
    vlevo: 151 152, vpravo: 236 237
Type (141)
    vlevo: 153 154 155 156 157 158, vpravo: 31 32 34 37 40 141 152
    158 167 178 179 181 182 187 188 221
NonExprType (142)
    vlevo: 159 160 161 162, vpravo: 140 162
NonRecvChanType (143)
    vlevo: 163 164 165 166 167, vpravo: 180
ConvType (144)
    vlevo: 168 169, vpravo: 125 126
CompLitType (145)
    vlevo: 170, vpravo: 127
FuncRetType (146)
    vlevo: 171 172 173 174 175, vpravo: 204
TypeName (147)
    vlevo: 176 177, vpravo: 157 166 175
OtherType (148)
    vlevo: 178 179 180 181 182 183 184 185 186, vpravo: 155 161 164
    169 170 173
PtrType (149)
    vlevo: 187, vpravo: 156 165 174
RecvChanType (150)
    vlevo: 188, vpravo: 153 159 171
StructType (151)
    vlevo: 189 190, vpravo: 183
UnionType (152)
    vlevo: 191 192, vpravo: 184
VariantType (153)
    vlevo: 193 194, vpravo: 185
InterfaceType (154)
    vlevo: 195 196, vpravo: 186
FuncDecl (155)
    vlevo: 197, vpravo: 17
FuncDecl1 (156)
    vlevo: 198 199, vpravo: 197
FuncType (157)
    vlevo: 200, vpravo: 154 160 163 168 172 206
FuncBody (158)
    vlevo: 201 202, vpravo: 197
FuncResult (159)
    vlevo: 203 204 205, vpravo: 198 199 200 233
FuncLitDecl (160)
    vlevo: 206, vpravo: 207 208
FuncLit (161)
    vlevo: 207 208, vpravo: 130
TopLevelDeclList (162)
    vlevo: 209 210, vpravo: 2 210
VarDeclList (163)
    vlevo: 211 212, vpravo: 21 212
ConstDeclList (164)
    vlevo: 213 214, vpravo: 25 214
TypeDeclList (165)
    vlevo: 215 216, vpravo: 28 216
StructDeclList (166)
    vlevo: 217 218, vpravo: 189 191 193 218
InterfaceDeclList (167)
    vlevo: 219 220, vpravo: 195 220
StructDecl (168)
    vlevo: 221 222 223 224 225 226, vpravo: 217 218
Qualident (169)
    vlevo: 227 228, vpravo: 229 231 232
Embedded (170)
    vlevo: 229, vpravo: 222 223 224 225 226
InterfaceDecl (171)
    vlevo: 230 231 232, vpravo: 219 220
InterfaceMethodDecl (172)
    vlevo: 233, vpravo: 230
ArgType (173)
    vlevo: 234 235 236 237, vpravo: 238 239
ArgTypeList (174)
    vlevo: 238 239, vpravo: 239 241
oArgTypeListOComma (175)
    vlevo: 240 241, vpravo: 198 199 200 205 233
Statement (176)
    vlevo: 242 243 244 245 246, vpravo: 252 261 262
NonDclStmt (177)
    vlevo: 247 248 249 250 251 252 253 254 255 256 257 258 259 260,
    vpravo: 18 245
StmtList (178)
    vlevo: 261 262, vpravo: 52 54 58 202 207 262
NewNameList (179)
    vlevo: 263 264, vpravo: 221 264
DeclNameList (180)
    vlevo: 265 266, vpravo: 31 32 33 34 35 37 38 266
ExprList (181)
    vlevo: 267 268, vpravo: 32 33 34 35 43 44 59 60 125 268 284
ExprOrTypeList (182)
    vlevo: 269 270, vpravo: 47 48 49 114 115 270
KeyvalList (183)
    vlevo: 271 272 273 274, vpravo: 273 274 276
BracedKeyvalList (184)
    vlevo: 275 276, vpravo: 127 128 129 134 136
oSemi (185)
    vlevo: 277 278, vpravo: 8 21 24 25 28 189 191 193 195
oComma (186)
    vlevo: 279 280, vpravo: 114 115 125 241 276
oExpr (187)
    vlevo: 281 282, vpravo: 122 123 178
oExprList (188)
    vlevo: 283 284, vpravo: 260
oSimpleStmt (189)
    vlevo: 285 286, vpravo: 61 62 67 68
oLiteral (190)
    vlevo: 287 288, vpravo: 221 222 223 224 225 226


State 0

    0 $accept: . File $end
    2 File: . PackageDecl $@1 Imports TopLevelDeclList
    3 PackageDecl: . %empty  [$end, error, BREAK, CHAN, COMM, CONST, CONTINUE, DEFER, FALL, FOR, FUNC, GO, GOTO, IDENT, IF, IMPORT, INTERFACE, LITERAL, MAP, RETURN, SELECT, STRUCT, SWITCH, TYPE, UNION, VAR, VARIANT, '!', '&', '(', '*', '+', '-', ';', '[', '^', '~']
    4            | . PACKAGE Symbol ';'

    PACKAGE  posunout a přejít do stavu 1

    $výchozí  reduce using rule 3 (PackageDecl)

    File         přejít do stavu 2
    PackageDecl  přejít do stavu 3


State 1

    4 PackageDecl: PACKAGE . Symbol ';'
  148 Symbol: . IDENT

    IDENT  posunout a přejít do stavu 4

    Symbol  přejít do stavu 5


State 2

    0 $accept: File . $end

    $end  posunout a přejít do stavu 6


State 3

    1 $@1: . %empty
    2 File: PackageDecl . $@1 Imports TopLevelDeclList

    $výchozí  reduce using rule 1 ($@1)

    $@1  přejít do stavu 7


State 4

  148 Symbol: IDENT .

    $výchozí  reduce using rule 148 (Symbol)


State 5

    4 PackageDecl: PACKAGE Symbol . ';'

    ';'  posunout a přejít do stavu 8


State 6

    0 $accept: File $end .

    $výchozí  přijmout


State 7

    2 File: PackageDecl $@1 . Imports TopLevelDeclList
    5 Imports: . %empty
    6        | . Imports Import ';'

    $výchozí  reduce using rule 5 (Imports)

    Imports  přejít do stavu 9


State 8

    4 PackageDecl: PACKAGE Symbol ';' .

    $výchozí  reduce using rule 4 (PackageDecl)


State 9

    2 File: PackageDecl $@1 Imports . TopLevelDeclList
    6 Imports: Imports . Import ';'
    7 Import: . IMPORT ImportDecl
    8       | . IMPORT '(' ImportDeclList oSemi ')'
    9       | . IMPORT '(' ')'
  209 TopLevelDeclList: . %empty  [$end, error, BREAK, CHAN, COMM, CONST, CONTINUE, DEFER, FALL, FOR, FUNC, GO, GOTO, IDENT, IF, INTERFACE, LITERAL, MAP, RETURN, SELECT, STRUCT, SWITCH, TYPE, UNION, VAR, VARIANT, '!', '&', '(', '*', '+', '-', ';', '[', '^', '~']
  210                 | . TopLevelDeclList TopLevelDecl ';'

    IMPORT  posunout a přejít do stavu 10

    $výchozí  reduce using rule 209 (TopLevelDeclList)

    Import            přejít do stavu 11
    TopLevelDeclList  přejít do stavu 12


State 10

    7 Import: IMPORT . ImportDecl
    8       | IMPORT . '(' ImportDeclList oSemi ')'
    9       | IMPORT . '(' ')'
   10 ImportDecl: . LITERAL
   11           | . Symbol LITERAL
   12           | . '.' LITERAL
  148 Symbol: . IDENT

    IDENT    posunout a přejít do stavu 4
    LITERAL  posunout a přejít do stavu 13
    '('      posunout a přejít do stavu 14
    '.'      posunout a přejít do stavu 15

    ImportDecl  přejít do stavu 16
    Symbol      přejít do stavu 17


State 11

    6 Imports: Imports Import . ';'

    ';'  posunout a přejít do stavu 18


State 12

    2 File: PackageDecl $@1 Imports TopLevelDeclList .  [$end]
   15 TopLevelDecl: . %empty  [';']
   16             | . CommonDecl
   17             | . FuncDecl
   18             | . NonDclStmt
   19             | . error
   20 CommonDecl: . VAR VarDecl
   21           | . VAR '(' VarDeclList oSemi ')'
   22           | . VAR '(' ')'
   23           | . Const ConstDecl
   24           | . Const '(' ConstDecl oSemi ')'
   25           | . Const '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | . Const '(' ')'
   27           | . TYPE TypeDecl
   28           | . TYPE '(' TypeDeclList oSemi ')'
   29           | . TYPE '(' ')'
   30 Const: . CONST
   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   66 ForStmt: . FOR $@5 ForBody
   71 IfStmt: . IF $@6 IfHeader LoopBody $@7 ElseIfList Else
   80 SwitchStmt: . SWITCH $@10 IfHeader BODY CaseBlockList '}'
   82 SelectStmt: . SELECT $@11 BODY CaseBlockList '}'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  144 NewName: . Symbol
  148 Symbol: . IDENT
  149 Name: . Symbol
  150 LabelName: . NewName
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  197 FuncDecl: . FUNC FuncDecl1 FuncBody
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  210 TopLevelDeclList: TopLevelDeclList . TopLevelDecl ';'
  247 NonDclStmt: . SimpleStmt
  248           | . ForStmt
  249           | . SwitchStmt
  250           | . SelectStmt
  251           | . IfStmt
  252           | . LabelName ':' Statement
  253           | . FALL
  254           | . BREAK oNewName
  255           | . CONTINUE oNewName
  256           | . GO PseudoCall
  257           | . DEFER PseudoCall
  258           | . GOTO NewName
  259           | . GOTO
  260           | . RETURN oExprList
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    error      posunout a přejít do stavu 19
    BREAK      posunout a přejít do stavu 20
    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    CONST      posunout a přejít do stavu 23
    CONTINUE   posunout a přejít do stavu 24
    DEFER      posunout a přejít do stavu 25
    FALL       posunout a přejít do stavu 26
    FOR        posunout a přejít do stavu 27
    FUNC       posunout a přejít do stavu 28
    GO         posunout a přejít do stavu 29
    GOTO       posunout a přejít do stavu 30
    IDENT      posunout a přejít do stavu 4
    IF         posunout a přejít do stavu 31
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RETURN     posunout a přejít do stavu 35
    SELECT     posunout a přejít do stavu 36
    STRUCT     posunout a přejít do stavu 37
    SWITCH     posunout a přejít do stavu 38
    TYPE       posunout a přejít do stavu 39
    UNION      posunout a přejít do stavu 40
    VAR        posunout a přejít do stavu 41
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $end  reduce using rule 2 (File)
    ';'   reduce using rule 15 (TopLevelDecl)

    TopLevelDecl        přejít do stavu 52
    CommonDecl          přejít do stavu 53
    Const               přejít do stavu 54
    SimpleStmt          přejít do stavu 55
    ForStmt             přejít do stavu 56
    IfStmt              přejít do stavu 57
    SwitchStmt          přejít do stavu 58
    SelectStmt          přejít do stavu 59
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    NewName             přejít do stavu 65
    Symbol              přejít do stavu 66
    Name                přejít do stavu 67
    LabelName           přejít do stavu 68
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncDecl            přejít do stavu 76
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    NonDclStmt          přejít do stavu 80
    ExprList            přejít do stavu 81


State 13

   10 ImportDecl: LITERAL .

    $výchozí  reduce using rule 10 (ImportDecl)


State 14

    8 Import: IMPORT '(' . ImportDeclList oSemi ')'
    9       | IMPORT '(' . ')'
   10 ImportDecl: . LITERAL
   11           | . Symbol LITERAL
   12           | . '.' LITERAL
   13 ImportDeclList: . ImportDecl
   14               | . ImportDeclList ';' ImportDecl
  148 Symbol: . IDENT

    IDENT    posunout a přejít do stavu 4
    LITERAL  posunout a přejít do stavu 13
    ')'      posunout a přejít do stavu 82
    '.'      posunout a přejít do stavu 15

    ImportDecl      přejít do stavu 83
    ImportDeclList  přejít do stavu 84
    Symbol          přejít do stavu 17


State 15

   12 ImportDecl: '.' . LITERAL

    LITERAL  posunout a přejít do stavu 85


State 16

    7 Import: IMPORT ImportDecl .

    $výchozí  reduce using rule 7 (Import)


State 17

   11 ImportDecl: Symbol . LITERAL

    LITERAL  posunout a přejít do stavu 86


State 18

    6 Imports: Imports Import ';' .

    $výchozí  reduce using rule 6 (Imports)


State 19

   19 TopLevelDecl: error .

    $výchozí  reduce using rule 19 (TopLevelDecl)


State 20

  144 NewName: . Symbol
  146 oNewName: . %empty  [CASE, DEFAULT, ';', '}']
  147         | . NewName
  148 Symbol: . IDENT
  254 NonDclStmt: BREAK . oNewName

    IDENT  posunout a přejít do stavu 4

    $výchozí  reduce using rule 146 (oNewName)

    NewName   přejít do stavu 87
    oNewName  přejít do stavu 88
    Symbol    přejít do stavu 89


State 21

  148 Symbol: . IDENT
  149 Name: . Symbol
  163 NonRecvChanType: . FuncType
  164                | . OtherType
  165                | . PtrType
  166                | . TypeName
  167                | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  180          | CHAN . NonRecvChanType
  181          | . CHAN COMM Type
  181          | CHAN . COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 90
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 92
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol           přejít do stavu 94
    Name             přejít do stavu 95
    NonRecvChanType  přejít do stavu 96
    TypeName         přejít do stavu 97
    OtherType        přejít do stavu 98
    PtrType          přejít do stavu 99
    StructType       přejít do stavu 72
    UnionType        přejít do stavu 73
    VariantType      přejít do stavu 74
    InterfaceType    přejít do stavu 75
    FuncType         přejít do stavu 100


State 22

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  112          | COMM . UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 101
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 23

   30 Const: CONST .

    $výchozí  reduce using rule 30 (Const)


State 24

  144 NewName: . Symbol
  146 oNewName: . %empty  [CASE, DEFAULT, ';', '}']
  147         | . NewName
  148 Symbol: . IDENT
  255 NonDclStmt: CONTINUE . oNewName

    IDENT  posunout a přejít do stavu 4

    $výchozí  reduce using rule 146 (oNewName)

    NewName   přejít do stavu 87
    oNewName  přejít do stavu 102
    Symbol    přejít do stavu 89


State 25

  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  257 NonDclStmt: DEFER . PseudoCall

    CHAN       posunout a přejít do stavu 21
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 45
    '['        posunout a přejít do stavu 49

    PseudoCall          přejít do stavu 103
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 104
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 26

  253 NonDclStmt: FALL .

    $výchozí  reduce using rule 253 (NonDclStmt)


State 27

   65 $@5: . %empty
   66 ForStmt: FOR . $@5 ForBody

    $výchozí  reduce using rule 65 ($@5)

    $@5  přejít do stavu 105


State 28

  148 Symbol: . IDENT
  197 FuncDecl: FUNC . FuncDecl1 FuncBody
  198 FuncDecl1: . Symbol '(' oArgTypeListOComma ')' FuncResult
  199          | . '(' oArgTypeListOComma ')' Symbol '(' oArgTypeListOComma ')' FuncResult
  200 FuncType: FUNC . '(' oArgTypeListOComma ')' FuncResult

    IDENT  posunout a přejít do stavu 4
    '('    posunout a přejít do stavu 106

    Symbol     přejít do stavu 107
    FuncDecl1  přejít do stavu 108


State 29

  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  256 NonDclStmt: GO . PseudoCall

    CHAN       posunout a přejít do stavu 21
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 45
    '['        posunout a přejít do stavu 49

    PseudoCall          přejít do stavu 109
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 104
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 30

  144 NewName: . Symbol
  148 Symbol: . IDENT
  258 NonDclStmt: GOTO . NewName
  259           | GOTO .  [CASE, DEFAULT, ';', '}']

    IDENT  posunout a přejít do stavu 4

    $výchozí  reduce using rule 259 (NonDclStmt)

    NewName  přejít do stavu 110
    Symbol   přejít do stavu 89


State 31

   69 $@6: . %empty
   71 IfStmt: IF . $@6 IfHeader LoopBody $@7 ElseIfList Else

    $výchozí  reduce using rule 69 ($@6)

    $@6  přejít do stavu 111


State 32

  142 LBrace: . BODY
  143       | . '{'
  195 InterfaceType: INTERFACE . LBrace InterfaceDeclList oSemi '}'
  196              | INTERFACE . LBrace '}'

    BODY  posunout a přejít do stavu 112
    '{'   posunout a přejít do stavu 113

    LBrace  přejít do stavu 114


State 33

  116 PrimaryExprNoParen: LITERAL .

    $výchozí  reduce using rule 116 (PrimaryExprNoParen)


State 34

  182 OtherType: MAP . '[' Type ']' Type

    '['  posunout a přejít do stavu 115


State 35

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  260 NonDclStmt: RETURN . oExprList
  267 ExprList: . Expr
  268         | . ExprList ',' Expr
  283 oExprList: . %empty  [CASE, DEFAULT, ';', '}']
  284          | . ExprList

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 283 (oExprList)

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 117
    oExprList           přejít do stavu 118


State 36

   81 $@11: . %empty
   82 SelectStmt: SELECT . $@11 BODY CaseBlockList '}'

    $výchozí  reduce using rule 81 ($@11)

    $@11  přejít do stavu 119


State 37

  142 LBrace: . BODY
  143       | . '{'
  189 StructType: STRUCT . LBrace StructDeclList oSemi '}'
  190           | STRUCT . LBrace '}'

    BODY  posunout a přejít do stavu 112
    '{'   posunout a přejít do stavu 113

    LBrace  přejít do stavu 120


State 38

   79 $@10: . %empty
   80 SwitchStmt: SWITCH . $@10 IfHeader BODY CaseBlockList '}'

    $výchozí  reduce using rule 79 ($@10)

    $@10  přejít do stavu 121


State 39

   27 CommonDecl: TYPE . TypeDecl
   28           | TYPE . '(' TypeDeclList oSemi ')'
   29           | TYPE . '(' ')'
   39 TypeDeclName: . Symbol
   40 TypeDecl: . TypeDeclName Type
  148 Symbol: . IDENT

    IDENT  posunout a přejít do stavu 4
    '('    posunout a přejít do stavu 122

    TypeDeclName  přejít do stavu 123
    TypeDecl      přejít do stavu 124
    Symbol        přejít do stavu 125


State 40

  142 LBrace: . BODY
  143       | . '{'
  191 UnionType: UNION . LBrace StructDeclList oSemi '}'
  192          | UNION . LBrace '}'

    BODY  posunout a přejít do stavu 112
    '{'   posunout a přejít do stavu 113

    LBrace  přejít do stavu 126


State 41

   20 CommonDecl: VAR . VarDecl
   21           | VAR . '(' VarDeclList oSemi ')'
   22           | VAR . '(' ')'
   31 VarDecl: . DeclNameList Type
   32        | . DeclNameList Type '=' ExprList
   33        | . DeclNameList '=' ExprList
  145 DeclName: . Symbol
  148 Symbol: . IDENT
  265 DeclNameList: . DeclName
  266             | . DeclNameList ',' DeclName

    IDENT  posunout a přejít do stavu 4
    '('    posunout a přejít do stavu 127

    VarDecl       přejít do stavu 128
    DeclName      přejít do stavu 129
    Symbol        přejít do stavu 130
    DeclNameList  přejít do stavu 131


State 42

  142 LBrace: . BODY
  143       | . '{'
  193 VariantType: VARIANT . LBrace StructDeclList oSemi '}'
  194            | VARIANT . LBrace '}'

    BODY  posunout a přejít do stavu 112
    '{'   posunout a přejít do stavu 113

    LBrace  přejít do stavu 132


State 43

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  109          | '!' . UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 133
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 44

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  106          | '&' . UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 134
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 45

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  129                   | '(' . ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  138            | '(' . ExprOrType ')'
  139 ExprOrType: . Expr
  140           | . NonExprType
  148 Symbol: . IDENT
  149 Name: . Symbol
  159 NonExprType: . RecvChanType
  160            | . FuncType
  161            | . OtherType
  162            | . '*' NonExprType
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 135
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 136
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 137
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    ExprOrType          přejít do stavu 138
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    NonExprType         přejít do stavu 139
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 140
    RecvChanType        přejít do stavu 141
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 142
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 46

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  105          | '*' . UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 143
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 47

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  107          | '+' . UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 144
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 48

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  108          | '-' . UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 145
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 49

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  178          | '[' . oExpr ']' Type
  179          | . '[' DDD ']' Type
  179          | '[' . DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  281 oExpr: . %empty  [']']
  282      | . Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    DDD        posunout a přejít do stavu 146
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 281 (oExpr)

    Expr                přejít do stavu 147
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    oExpr               přejít do stavu 148


State 50

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  111          | '^' . UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 149
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 51

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  110          | '~' . UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 150
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 52

  210 TopLevelDeclList: TopLevelDeclList TopLevelDecl . ';'

    ';'  posunout a přejít do stavu 151


State 53

   16 TopLevelDecl: CommonDecl .

    $výchozí  reduce using rule 16 (TopLevelDecl)


State 54

   23 CommonDecl: Const . ConstDecl
   24           | Const . '(' ConstDecl oSemi ')'
   25           | Const . '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | Const . '(' ')'
   34 ConstDecl: . DeclNameList Type '=' ExprList
   35          | . DeclNameList '=' ExprList
  145 DeclName: . Symbol
  148 Symbol: . IDENT
  265 DeclNameList: . DeclName
  266             | . DeclNameList ',' DeclName

    IDENT  posunout a přejít do stavu 4
    '('    posunout a přejít do stavu 152

    ConstDecl     přejít do stavu 153
    DeclName      přejít do stavu 129
    Symbol        přejít do stavu 130
    DeclNameList  přejít do stavu 154


State 55

  247 NonDclStmt: SimpleStmt .

    $výchozí  reduce using rule 247 (NonDclStmt)


State 56

  248 NonDclStmt: ForStmt .

    $výchozí  reduce using rule 248 (NonDclStmt)


State 57

  251 NonDclStmt: IfStmt .

    $výchozí  reduce using rule 251 (NonDclStmt)


State 58

  249 NonDclStmt: SwitchStmt .

    $výchozí  reduce using rule 249 (NonDclStmt)


State 59

  250 NonDclStmt: SelectStmt .

    $výchozí  reduce using rule 250 (NonDclStmt)


State 60

   41 SimpleStmt: Expr .  [BODY, CASE, DEFAULT, ';', '}']
   42           | Expr . ASOP Expr
   45           | Expr . INC
   46           | Expr . DEC
   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr
  267 ExprList: Expr .  [COLAS, '=', ',']

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    ASOP    posunout a přejít do stavu 157
    COMM    posunout a přejít do stavu 158
    DEC     posunout a přejít do stavu 159
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    INC     posunout a přejít do stavu 162
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    COLAS       reduce using rule 267 (ExprList)
    '='         reduce using rule 267 (ExprList)
    ','         reduce using rule 267 (ExprList)
    $výchozí  reduce using rule 41 (SimpleStmt)


State 61

   83 Expr: UnaryExpr .

    $výchozí  reduce using rule 83 (Expr)


State 62

  124 PrimaryExprNoParen: PseudoCall .

    $výchozí  reduce using rule 124 (PrimaryExprNoParen)


State 63

  128 PrimaryExprNoParen: PrimaryExprNoParen . '{' StartCompLit BracedKeyvalList '}'
  137 PrimaryExpr: PrimaryExprNoParen .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', '(', ')', '*', '+', '-', '.', '/', ':', ';', '<', '=', '>', '[', '^', '|', '}', ']', ',']

    '{'  posunout a přejít do stavu 178

    $výchozí  reduce using rule 137 (PrimaryExpr)


State 64

  104 UnaryExpr: PrimaryExpr .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', ')', '*', '+', '-', '/', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
  113 PseudoCall: PrimaryExpr . '(' ')'
  114           | PrimaryExpr . '(' ExprOrTypeList oComma ')'
  115           | PrimaryExpr . '(' ExprOrTypeList DDD oComma ')'
  118 PrimaryExprNoParen: PrimaryExpr . '.' Symbol
  119                   | PrimaryExpr . '.' '(' ExprOrType ')'
  120                   | PrimaryExpr . '.' '(' TYPE ')'
  121                   | PrimaryExpr . '[' Expr ']'
  122                   | PrimaryExpr . '[' oExpr ':' oExpr ']'
  123                   | PrimaryExpr . '[' oExpr ':' oExpr ':' oExpr ']'

    '('  posunout a přejít do stavu 179
    '.'  posunout a přejít do stavu 180
    '['  posunout a přejít do stavu 181

    $výchozí  reduce using rule 104 (UnaryExpr)


State 65

  150 LabelName: NewName .

    $výchozí  reduce using rule 150 (LabelName)


State 66

  144 NewName: Symbol .  [':']
  149 Name: Symbol .  [ANDAND, ANDNOT, ASOP, CASE, COLAS, COMM, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', '(', '*', '+', '-', '.', '/', ';', '<', '=', '>', '[', '^', '{', '|', '}', ',']

    ':'         reduce using rule 144 (NewName)
    $výchozí  reduce using rule 149 (Name)


State 67

  117 PrimaryExprNoParen: Name .

    $výchozí  reduce using rule 117 (PrimaryExprNoParen)


State 68

  252 NonDclStmt: LabelName . ':' Statement

    ':'  posunout a přejít do stavu 182


State 69

  125 PrimaryExprNoParen: ConvType . '(' ExprList oComma ')'
  126                   | ConvType . '(' ')'

    '('  posunout a přejít do stavu 183


State 70

  127 PrimaryExprNoParen: CompLitType . LBrace StartCompLit BracedKeyvalList '}'
  142 LBrace: . BODY
  143       | . '{'

    BODY  posunout a přejít do stavu 112
    '{'   posunout a přejít do stavu 113

    LBrace  přejít do stavu 184


State 71

  169 ConvType: OtherType .  ['(']
  170 CompLitType: OtherType .  [BODY, '{']

    '('         reduce using rule 169 (ConvType)
    $výchozí  reduce using rule 170 (CompLitType)


State 72

  183 OtherType: StructType .

    $výchozí  reduce using rule 183 (OtherType)


State 73

  184 OtherType: UnionType .

    $výchozí  reduce using rule 184 (OtherType)


State 74

  185 OtherType: VariantType .

    $výchozí  reduce using rule 185 (OtherType)


State 75

  186 OtherType: InterfaceType .

    $výchozí  reduce using rule 186 (OtherType)


State 76

   17 TopLevelDecl: FuncDecl .

    $výchozí  reduce using rule 17 (TopLevelDecl)


State 77

  168 ConvType: FuncType .  ['(']
  206 FuncLitDecl: FuncType .  [error, BODY, '{']

    '('         reduce using rule 168 (ConvType)
    $výchozí  reduce using rule 206 (FuncLitDecl)


State 78

  142 LBrace: . BODY
  143       | . '{'
  207 FuncLit: FuncLitDecl . LBrace StmtList '}'
  208        | FuncLitDecl . error

    error  posunout a přejít do stavu 185
    BODY   posunout a přejít do stavu 112
    '{'    posunout a přejít do stavu 113

    LBrace  přejít do stavu 186


State 79

  130 PrimaryExprNoParen: FuncLit .

    $výchozí  reduce using rule 130 (PrimaryExprNoParen)


State 80

   18 TopLevelDecl: NonDclStmt .

    $výchozí  reduce using rule 18 (TopLevelDecl)


State 81

   43 SimpleStmt: ExprList . '=' ExprList
   44           | ExprList . COLAS ExprList
  268 ExprList: ExprList . ',' Expr

    COLAS  posunout a přejít do stavu 187
    '='    posunout a přejít do stavu 188
    ','    posunout a přejít do stavu 189


State 82

    9 Import: IMPORT '(' ')' .

    $výchozí  reduce using rule 9 (Import)


State 83

   13 ImportDeclList: ImportDecl .

    $výchozí  reduce using rule 13 (ImportDeclList)


State 84

    8 Import: IMPORT '(' ImportDeclList . oSemi ')'
   14 ImportDeclList: ImportDeclList . ';' ImportDecl
  277 oSemi: . %empty  [')']
  278      | . ';'

    ';'  posunout a přejít do stavu 190

    $výchozí  reduce using rule 277 (oSemi)

    oSemi  přejít do stavu 191


State 85

   12 ImportDecl: '.' LITERAL .

    $výchozí  reduce using rule 12 (ImportDecl)


State 86

   11 ImportDecl: Symbol LITERAL .

    $výchozí  reduce using rule 11 (ImportDecl)


State 87

  147 oNewName: NewName .

    $výchozí  reduce using rule 147 (oNewName)


State 88

  254 NonDclStmt: BREAK oNewName .

    $výchozí  reduce using rule 254 (NonDclStmt)


State 89

  144 NewName: Symbol .

    $výchozí  reduce using rule 144 (NewName)


State 90

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  181          | CHAN COMM . Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 194
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 91

  200 FuncType: FUNC . '(' oArgTypeListOComma ')' FuncResult

    '('  posunout a přejít do stavu 200


State 92

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  167 NonRecvChanType: '(' . Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 201
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 93

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  187        | '*' . Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 202
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 94

  149 Name: Symbol .

    $výchozí  reduce using rule 149 (Name)


State 95

  176 TypeName: Name .  [error, BODY, CASE, COLAS, DDD, DEFAULT, LITERAL, '(', ')', ':', ';', '=', '{', '}', ']', ',']
  177         | Name . '.' Symbol

    '.'  posunout a přejít do stavu 203

    $výchozí  reduce using rule 176 (TypeName)


State 96

  180 OtherType: CHAN NonRecvChanType .

    $výchozí  reduce using rule 180 (OtherType)


State 97

  166 NonRecvChanType: TypeName .

    $výchozí  reduce using rule 166 (NonRecvChanType)


State 98

  164 NonRecvChanType: OtherType .

    $výchozí  reduce using rule 164 (NonRecvChanType)


State 99

  165 NonRecvChanType: PtrType .

    $výchozí  reduce using rule 165 (NonRecvChanType)


State 100

  163 NonRecvChanType: FuncType .

    $výchozí  reduce using rule 163 (NonRecvChanType)


State 101

  112 UnaryExpr: COMM UnaryExpr .

    $výchozí  reduce using rule 112 (UnaryExpr)


State 102

  255 NonDclStmt: CONTINUE oNewName .

    $výchozí  reduce using rule 255 (NonDclStmt)


State 103

  124 PrimaryExprNoParen: PseudoCall .  ['(', '.', '[', '{']
  257 NonDclStmt: DEFER PseudoCall .  [CASE, DEFAULT, ';', '}']

    CASE        reduce using rule 257 (NonDclStmt)
    DEFAULT     reduce using rule 257 (NonDclStmt)
    ';'         reduce using rule 257 (NonDclStmt)
    '}'         reduce using rule 257 (NonDclStmt)
    $výchozí  reduce using rule 124 (PrimaryExprNoParen)


State 104

  113 PseudoCall: PrimaryExpr . '(' ')'
  114           | PrimaryExpr . '(' ExprOrTypeList oComma ')'
  115           | PrimaryExpr . '(' ExprOrTypeList DDD oComma ')'
  118 PrimaryExprNoParen: PrimaryExpr . '.' Symbol
  119                   | PrimaryExpr . '.' '(' ExprOrType ')'
  120                   | PrimaryExpr . '.' '(' TYPE ')'
  121                   | PrimaryExpr . '[' Expr ']'
  122                   | PrimaryExpr . '[' oExpr ':' oExpr ']'
  123                   | PrimaryExpr . '[' oExpr ':' oExpr ':' oExpr ']'

    '('  posunout a přejít do stavu 179
    '.'  posunout a přejít do stavu 180
    '['  posunout a přejít do stavu 181


State 105

   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   59 RangeStmt: . ExprList '=' RANGE Expr
   60          | . ExprList COLAS RANGE Expr
   61 ForHeader: . oSimpleStmt ';' oSimpleStmt ';' oSimpleStmt
   62          | . oSimpleStmt
   63          | . RangeStmt
   64 ForBody: . ForHeader LoopBody
   66 ForStmt: FOR $@5 . ForBody
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr
  285 oSimpleStmt: . %empty  [BODY, ';']
  286            | . SimpleStmt

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 285 (oSimpleStmt)

    SimpleStmt          přejít do stavu 204
    RangeStmt           přejít do stavu 205
    ForHeader           přejít do stavu 206
    ForBody             přejít do stavu 207
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 208
    oSimpleStmt         přejít do stavu 209


State 106

  141 NameOrType: . Type
  148 Symbol: . IDENT
  149 Name: . Symbol
  151 Ddd: . DDD
  152    | . DDD Type
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  199 FuncDecl1: '(' . oArgTypeListOComma ')' Symbol '(' oArgTypeListOComma ')' FuncResult
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  200         | FUNC '(' . oArgTypeListOComma ')' FuncResult
  234 ArgType: . NameOrType
  235        | . Symbol NameOrType
  236        | . Symbol Ddd
  237        | . Ddd
  238 ArgTypeList: . ArgType
  239            | . ArgTypeList ',' ArgType
  240 oArgTypeListOComma: . %empty  [')']
  241                   | . ArgTypeList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    DDD        posunout a přejít do stavu 210
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 240 (oArgTypeListOComma)

    NameOrType          přejít do stavu 211
    Symbol              přejít do stavu 212
    Name                přejít do stavu 95
    Ddd                 přejít do stavu 213
    Type                přejít do stavu 214
    TypeName            přejít do stavu 195
    OtherType           přejít do stavu 196
    PtrType             přejít do stavu 197
    RecvChanType        přejít do stavu 198
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 199
    ArgType             přejít do stavu 215
    ArgTypeList         přejít do stavu 216
    oArgTypeListOComma  přejít do stavu 217


State 107

  198 FuncDecl1: Symbol . '(' oArgTypeListOComma ')' FuncResult

    '('  posunout a přejít do stavu 218


State 108

  197 FuncDecl: FUNC FuncDecl1 . FuncBody
  201 FuncBody: . %empty  [';']
  202         | . '{' StmtList '}'

    '{'  posunout a přejít do stavu 219

    $výchozí  reduce using rule 201 (FuncBody)

    FuncBody  přejít do stavu 220


State 109

  124 PrimaryExprNoParen: PseudoCall .  ['(', '.', '[', '{']
  256 NonDclStmt: GO PseudoCall .  [CASE, DEFAULT, ';', '}']

    CASE        reduce using rule 256 (NonDclStmt)
    DEFAULT     reduce using rule 256 (NonDclStmt)
    ';'         reduce using rule 256 (NonDclStmt)
    '}'         reduce using rule 256 (NonDclStmt)
    $výchozí  reduce using rule 124 (PrimaryExprNoParen)


State 110

  258 NonDclStmt: GOTO NewName .

    $výchozí  reduce using rule 258 (NonDclStmt)


State 111

   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   67 IfHeader: . oSimpleStmt
   68         | . oSimpleStmt ';' oSimpleStmt
   71 IfStmt: IF $@6 . IfHeader LoopBody $@7 ElseIfList Else
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr
  285 oSimpleStmt: . %empty  [BODY, ';']
  286            | . SimpleStmt

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 285 (oSimpleStmt)

    SimpleStmt          přejít do stavu 204
    IfHeader            přejít do stavu 221
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 81
    oSimpleStmt         přejít do stavu 222


State 112

  142 LBrace: BODY .

    $výchozí  reduce using rule 142 (LBrace)


State 113

  143 LBrace: '{' .

    $výchozí  reduce using rule 143 (LBrace)


State 114

  144 NewName: . Symbol
  148 Symbol: . IDENT
  195 InterfaceType: INTERFACE LBrace . InterfaceDeclList oSemi '}'
  196              | INTERFACE LBrace . '}'
  219 InterfaceDeclList: . InterfaceDecl
  220                  | . InterfaceDeclList ';' InterfaceDecl
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  230 InterfaceDecl: . NewName InterfaceMethodDecl
  231              | . Qualident
  232              | . '(' Qualident ')'

    IDENT  posunout a přejít do stavu 223
    '('    posunout a přejít do stavu 224
    '}'    posunout a přejít do stavu 225

    NewName            přejít do stavu 226
    Symbol             přejít do stavu 89
    InterfaceDeclList  přejít do stavu 227
    Qualident          přejít do stavu 228
    InterfaceDecl      přejít do stavu 229


State 115

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  182          | MAP '[' . Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 230
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 116

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr
  267 ExprList: Expr .  [BODY, CASE, DEFAULT, ')', ';', '}', ',']

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 267 (ExprList)


State 117

  268 ExprList: ExprList . ',' Expr
  284 oExprList: ExprList .  [CASE, DEFAULT, ';', '}']

    ','  posunout a přejít do stavu 189

    $výchozí  reduce using rule 284 (oExprList)


State 118

  260 NonDclStmt: RETURN oExprList .

    $výchozí  reduce using rule 260 (NonDclStmt)


State 119

   82 SelectStmt: SELECT $@11 . BODY CaseBlockList '}'

    BODY  posunout a přejít do stavu 231


State 120

  144 NewName: . Symbol
  148 Symbol: . IDENT
  189 StructType: STRUCT LBrace . StructDeclList oSemi '}'
  190           | STRUCT LBrace . '}'
  217 StructDeclList: . StructDecl
  218               | . StructDeclList ';' StructDecl
  221 StructDecl: . NewNameList Type oLiteral
  222           | . Embedded oLiteral
  223           | . '(' Embedded ')' oLiteral
  224           | . '*' Embedded oLiteral
  225           | . '(' '*' Embedded ')' oLiteral
  226           | . '*' '(' Embedded ')' oLiteral
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  229 Embedded: . Qualident
  263 NewNameList: . NewName
  264            | . NewNameList ',' NewName

    IDENT  posunout a přejít do stavu 223
    '('    posunout a přejít do stavu 232
    '*'    posunout a přejít do stavu 233
    '}'    posunout a přejít do stavu 234

    NewName         přejít do stavu 235
    Symbol          přejít do stavu 89
    StructDeclList  přejít do stavu 236
    StructDecl      přejít do stavu 237
    Qualident       přejít do stavu 238
    Embedded        přejít do stavu 239
    NewNameList     přejít do stavu 240


State 121

   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   67 IfHeader: . oSimpleStmt
   68         | . oSimpleStmt ';' oSimpleStmt
   80 SwitchStmt: SWITCH $@10 . IfHeader BODY CaseBlockList '}'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr
  285 oSimpleStmt: . %empty  [BODY, ';']
  286            | . SimpleStmt

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 285 (oSimpleStmt)

    SimpleStmt          přejít do stavu 204
    IfHeader            přejít do stavu 241
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 81
    oSimpleStmt         přejít do stavu 222


State 122

   28 CommonDecl: TYPE '(' . TypeDeclList oSemi ')'
   29           | TYPE '(' . ')'
   39 TypeDeclName: . Symbol
   40 TypeDecl: . TypeDeclName Type
  148 Symbol: . IDENT
  215 TypeDeclList: . TypeDecl
  216             | . TypeDeclList ';' TypeDecl

    IDENT  posunout a přejít do stavu 4
    ')'    posunout a přejít do stavu 242

    TypeDeclName  přejít do stavu 123
    TypeDecl      přejít do stavu 243
    Symbol        přejít do stavu 125
    TypeDeclList  přejít do stavu 244


State 123

   40 TypeDecl: TypeDeclName . Type
  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 245
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 124

   27 CommonDecl: TYPE TypeDecl .

    $výchozí  reduce using rule 27 (CommonDecl)


State 125

   39 TypeDeclName: Symbol .

    $výchozí  reduce using rule 39 (TypeDeclName)


State 126

  144 NewName: . Symbol
  148 Symbol: . IDENT
  191 UnionType: UNION LBrace . StructDeclList oSemi '}'
  192          | UNION LBrace . '}'
  217 StructDeclList: . StructDecl
  218               | . StructDeclList ';' StructDecl
  221 StructDecl: . NewNameList Type oLiteral
  222           | . Embedded oLiteral
  223           | . '(' Embedded ')' oLiteral
  224           | . '*' Embedded oLiteral
  225           | . '(' '*' Embedded ')' oLiteral
  226           | . '*' '(' Embedded ')' oLiteral
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  229 Embedded: . Qualident
  263 NewNameList: . NewName
  264            | . NewNameList ',' NewName

    IDENT  posunout a přejít do stavu 223
    '('    posunout a přejít do stavu 232
    '*'    posunout a přejít do stavu 233
    '}'    posunout a přejít do stavu 246

    NewName         přejít do stavu 235
    Symbol          přejít do stavu 89
    StructDeclList  přejít do stavu 247
    StructDecl      přejít do stavu 237
    Qualident       přejít do stavu 238
    Embedded        přejít do stavu 239
    NewNameList     přejít do stavu 240


State 127

   21 CommonDecl: VAR '(' . VarDeclList oSemi ')'
   22           | VAR '(' . ')'
   31 VarDecl: . DeclNameList Type
   32        | . DeclNameList Type '=' ExprList
   33        | . DeclNameList '=' ExprList
  145 DeclName: . Symbol
  148 Symbol: . IDENT
  211 VarDeclList: . VarDecl
  212            | . VarDeclList ';' VarDecl
  265 DeclNameList: . DeclName
  266             | . DeclNameList ',' DeclName

    IDENT  posunout a přejít do stavu 4
    ')'    posunout a přejít do stavu 248

    VarDecl       přejít do stavu 249
    DeclName      přejít do stavu 129
    Symbol        přejít do stavu 130
    VarDeclList   přejít do stavu 250
    DeclNameList  přejít do stavu 131


State 128

   20 CommonDecl: VAR VarDecl .

    $výchozí  reduce using rule 20 (CommonDecl)


State 129

  265 DeclNameList: DeclName .

    $výchozí  reduce using rule 265 (DeclNameList)


State 130

  145 DeclName: Symbol .

    $výchozí  reduce using rule 145 (DeclName)


State 131

   31 VarDecl: DeclNameList . Type
   32        | DeclNameList . Type '=' ExprList
   33        | DeclNameList . '=' ExprList
  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  266 DeclNameList: DeclNameList . ',' DeclName

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '='        posunout a přejít do stavu 251
    '['        posunout a přejít do stavu 49
    ','        posunout a přejít do stavu 252

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 253
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 132

  144 NewName: . Symbol
  148 Symbol: . IDENT
  193 VariantType: VARIANT LBrace . StructDeclList oSemi '}'
  194            | VARIANT LBrace . '}'
  217 StructDeclList: . StructDecl
  218               | . StructDeclList ';' StructDecl
  221 StructDecl: . NewNameList Type oLiteral
  222           | . Embedded oLiteral
  223           | . '(' Embedded ')' oLiteral
  224           | . '*' Embedded oLiteral
  225           | . '(' '*' Embedded ')' oLiteral
  226           | . '*' '(' Embedded ')' oLiteral
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  229 Embedded: . Qualident
  263 NewNameList: . NewName
  264            | . NewNameList ',' NewName

    IDENT  posunout a přejít do stavu 223
    '('    posunout a přejít do stavu 232
    '*'    posunout a přejít do stavu 233
    '}'    posunout a přejít do stavu 254

    NewName         přejít do stavu 235
    Symbol          přejít do stavu 89
    StructDeclList  přejít do stavu 255
    StructDecl      přejít do stavu 237
    Qualident       přejít do stavu 238
    Embedded        přejít do stavu 239
    NewNameList     přejít do stavu 240


State 133

  109 UnaryExpr: '!' UnaryExpr .

    $výchozí  reduce using rule 109 (UnaryExpr)


State 134

  106 UnaryExpr: '&' UnaryExpr .

    $výchozí  reduce using rule 106 (UnaryExpr)


State 135

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  112          | COMM . UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  188 RecvChanType: COMM . CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 256
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 101
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 136

  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  105          | '*' . UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  159 NonExprType: . RecvChanType
  160            | . FuncType
  161            | . OtherType
  162            | . '*' NonExprType
  162            | '*' . NonExprType
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 135
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 136
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    UnaryExpr           přejít do stavu 143
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    NonExprType         přejít do stavu 257
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 140
    RecvChanType        přejít do stavu 141
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 142
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 137

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr
  139 ExprOrType: Expr .  [COLAS, DDD, ')', ':', '=', ',']

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 139 (ExprOrType)


State 138

  129 PrimaryExprNoParen: '(' ExprOrType . ')' '{' StartCompLit BracedKeyvalList '}'
  138 PrimaryExpr: '(' ExprOrType . ')'

    ')'  posunout a přejít do stavu 258


State 139

  140 ExprOrType: NonExprType .

    $výchozí  reduce using rule 140 (ExprOrType)


State 140

  161 NonExprType: OtherType .  [COLAS, DDD, ')', ':', '=', ',']
  169 ConvType: OtherType .  ['(']
  170 CompLitType: OtherType .  [BODY, '{']

    BODY        reduce using rule 170 (CompLitType)
    '('         reduce using rule 169 (ConvType)
    '{'         reduce using rule 170 (CompLitType)
    $výchozí  reduce using rule 161 (NonExprType)


State 141

  159 NonExprType: RecvChanType .

    $výchozí  reduce using rule 159 (NonExprType)


State 142

  160 NonExprType: FuncType .  [COLAS, DDD, ')', ':', '=', ',']
  168 ConvType: FuncType .  ['(']
  206 FuncLitDecl: FuncType .  [error, BODY, '{']

    error       reduce using rule 206 (FuncLitDecl)
    BODY        reduce using rule 206 (FuncLitDecl)
    '('         reduce using rule 168 (ConvType)
    '{'         reduce using rule 206 (FuncLitDecl)
    $výchozí  reduce using rule 160 (NonExprType)


State 143

  105 UnaryExpr: '*' UnaryExpr .

    $výchozí  reduce using rule 105 (UnaryExpr)


State 144

  107 UnaryExpr: '+' UnaryExpr .

    $výchozí  reduce using rule 107 (UnaryExpr)


State 145

  108 UnaryExpr: '-' UnaryExpr .

    $výchozí  reduce using rule 108 (UnaryExpr)


State 146

  179 OtherType: '[' DDD . ']' Type

    ']'  posunout a přejít do stavu 259


State 147

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr
  282 oExpr: Expr .  [':', ']']

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 282 (oExpr)


State 148

  178 OtherType: '[' oExpr . ']' Type

    ']'  posunout a přejít do stavu 260


State 149

  111 UnaryExpr: '^' UnaryExpr .

    $výchozí  reduce using rule 111 (UnaryExpr)


State 150

  110 UnaryExpr: '~' UnaryExpr .

    $výchozí  reduce using rule 110 (UnaryExpr)


State 151

  210 TopLevelDeclList: TopLevelDeclList TopLevelDecl ';' .

    $výchozí  reduce using rule 210 (TopLevelDeclList)


State 152

   24 CommonDecl: Const '(' . ConstDecl oSemi ')'
   25           | Const '(' . ConstDecl ';' ConstDeclList oSemi ')'
   26           | Const '(' . ')'
   34 ConstDecl: . DeclNameList Type '=' ExprList
   35          | . DeclNameList '=' ExprList
  145 DeclName: . Symbol
  148 Symbol: . IDENT
  265 DeclNameList: . DeclName
  266             | . DeclNameList ',' DeclName

    IDENT  posunout a přejít do stavu 4
    ')'    posunout a přejít do stavu 261

    ConstDecl     přejít do stavu 262
    DeclName      přejít do stavu 129
    Symbol        přejít do stavu 130
    DeclNameList  přejít do stavu 154


State 153

   23 CommonDecl: Const ConstDecl .

    $výchozí  reduce using rule 23 (CommonDecl)


State 154

   34 ConstDecl: DeclNameList . Type '=' ExprList
   35          | DeclNameList . '=' ExprList
  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  266 DeclNameList: DeclNameList . ',' DeclName

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '='        posunout a přejít do stavu 263
    '['        posunout a přejít do stavu 49
    ','        posunout a přejít do stavu 252

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 264
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 155

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   85     | Expr ANDAND . Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 265
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 156

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  100     | Expr ANDNOT . Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 266
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 157

   42 SimpleStmt: Expr ASOP . Expr
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 267
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 158

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  103     | Expr COMM . Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 268
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 159

   46 SimpleStmt: Expr DEC .

    $výchozí  reduce using rule 46 (SimpleStmt)


State 160

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   86     | Expr EQ . Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 269
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 161

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   90     | Expr GE . Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 270
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 162

   45 SimpleStmt: Expr INC .

    $výchozí  reduce using rule 45 (SimpleStmt)


State 163

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   89     | Expr LE . Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 271
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 164

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  101     | Expr LSH . Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 272
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 165

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   87     | Expr NE . Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 273
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 166

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   84     | Expr OROR . Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 274
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 167

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  102     | Expr RSH . Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 275
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 168

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   98     | Expr '%' . Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 276
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 169

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
   99     | Expr '&' . Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 277
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 170

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   96     | Expr '*' . Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 278
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 171

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   92     | Expr '+' . Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 279
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 172

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   93     | Expr '-' . Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 280
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 173

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   97     | Expr '/' . Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 281
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 174

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   88     | Expr '<' . Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 282
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 175

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   91     | Expr '>' . Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 283
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 176

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   95     | Expr '^' . Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 284
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 177

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   94     | Expr '|' . Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 285
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 178

  128 PrimaryExprNoParen: PrimaryExprNoParen '{' . StartCompLit BracedKeyvalList '}'
  131 StartCompLit: . %empty

    $výchozí  reduce using rule 131 (StartCompLit)

    StartCompLit  přejít do stavu 286


State 179

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  113           | PrimaryExpr '(' . ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  114           | PrimaryExpr '(' . ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  115           | PrimaryExpr '(' . ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  139 ExprOrType: . Expr
  140           | . NonExprType
  148 Symbol: . IDENT
  149 Name: . Symbol
  159 NonExprType: . RecvChanType
  160            | . FuncType
  161            | . OtherType
  162            | . '*' NonExprType
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  269 ExprOrTypeList: . ExprOrType
  270               | . ExprOrTypeList ',' ExprOrType

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 135
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    ')'        posunout a přejít do stavu 287
    '*'        posunout a přejít do stavu 136
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 137
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    ExprOrType          přejít do stavu 288
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    NonExprType         přejít do stavu 139
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 140
    RecvChanType        přejít do stavu 141
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 142
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprOrTypeList      přejít do stavu 289


State 180

  118 PrimaryExprNoParen: PrimaryExpr '.' . Symbol
  119                   | PrimaryExpr '.' . '(' ExprOrType ')'
  120                   | PrimaryExpr '.' . '(' TYPE ')'
  148 Symbol: . IDENT

    IDENT  posunout a přejít do stavu 4
    '('    posunout a přejít do stavu 290

    Symbol  přejít do stavu 291


State 181

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  121                   | PrimaryExpr '[' . Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  122                   | PrimaryExpr '[' . oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  123                   | PrimaryExpr '[' . oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  281 oExpr: . %empty  [':']
  282      | . Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 281 (oExpr)

    Expr                přejít do stavu 292
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    oExpr               přejít do stavu 293


State 182

   20 CommonDecl: . VAR VarDecl
   21           | . VAR '(' VarDeclList oSemi ')'
   22           | . VAR '(' ')'
   23           | . Const ConstDecl
   24           | . Const '(' ConstDecl oSemi ')'
   25           | . Const '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | . Const '(' ')'
   27           | . TYPE TypeDecl
   28           | . TYPE '(' TypeDeclList oSemi ')'
   29           | . TYPE '(' ')'
   30 Const: . CONST
   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   52 CompoundStmt: . '{' $@2 StmtList '}'
   66 ForStmt: . FOR $@5 ForBody
   71 IfStmt: . IF $@6 IfHeader LoopBody $@7 ElseIfList Else
   80 SwitchStmt: . SWITCH $@10 IfHeader BODY CaseBlockList '}'
   82 SelectStmt: . SELECT $@11 BODY CaseBlockList '}'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  144 NewName: . Symbol
  148 Symbol: . IDENT
  149 Name: . Symbol
  150 LabelName: . NewName
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  242 Statement: . %empty  [CASE, DEFAULT, ';', '}']
  243          | . CompoundStmt
  244          | . CommonDecl
  245          | . NonDclStmt
  246          | . error
  247 NonDclStmt: . SimpleStmt
  248           | . ForStmt
  249           | . SwitchStmt
  250           | . SelectStmt
  251           | . IfStmt
  252           | . LabelName ':' Statement
  252           | LabelName ':' . Statement
  253           | . FALL
  254           | . BREAK oNewName
  255           | . CONTINUE oNewName
  256           | . GO PseudoCall
  257           | . DEFER PseudoCall
  258           | . GOTO NewName
  259           | . GOTO
  260           | . RETURN oExprList
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    error      posunout a přejít do stavu 294
    BREAK      posunout a přejít do stavu 20
    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    CONST      posunout a přejít do stavu 23
    CONTINUE   posunout a přejít do stavu 24
    DEFER      posunout a přejít do stavu 25
    FALL       posunout a přejít do stavu 26
    FOR        posunout a přejít do stavu 27
    FUNC       posunout a přejít do stavu 91
    GO         posunout a přejít do stavu 29
    GOTO       posunout a přejít do stavu 30
    IDENT      posunout a přejít do stavu 4
    IF         posunout a přejít do stavu 31
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RETURN     posunout a přejít do stavu 35
    SELECT     posunout a přejít do stavu 36
    STRUCT     posunout a přejít do stavu 37
    SWITCH     posunout a přejít do stavu 38
    TYPE       posunout a přejít do stavu 39
    UNION      posunout a přejít do stavu 40
    VAR        posunout a přejít do stavu 41
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 295
    '~'        posunout a přejít do stavu 51

    CASE     reduce using rule 242 (Statement)
    DEFAULT  reduce using rule 242 (Statement)
    ';'      reduce using rule 242 (Statement)
    '}'      reduce using rule 242 (Statement)

    CommonDecl          přejít do stavu 296
    Const               přejít do stavu 54
    SimpleStmt          přejít do stavu 55
    CompoundStmt        přejít do stavu 297
    ForStmt             přejít do stavu 56
    IfStmt              přejít do stavu 57
    SwitchStmt          přejít do stavu 58
    SelectStmt          přejít do stavu 59
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    NewName             přejít do stavu 65
    Symbol              přejít do stavu 66
    Name                přejít do stavu 67
    LabelName           přejít do stavu 68
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    Statement           přejít do stavu 298
    NonDclStmt          přejít do stavu 299
    ExprList            přejít do stavu 81


State 183

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  125                   | ConvType '(' . ExprList oComma ')'
  126                   | . ConvType '(' ')'
  126                   | ConvType '(' . ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    ')'        posunout a přejít do stavu 300
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 301


State 184

  127 PrimaryExprNoParen: CompLitType LBrace . StartCompLit BracedKeyvalList '}'
  131 StartCompLit: . %empty

    $výchozí  reduce using rule 131 (StartCompLit)

    StartCompLit  přejít do stavu 302


State 185

  208 FuncLit: FuncLitDecl error .

    $výchozí  reduce using rule 208 (FuncLit)


State 186

   20 CommonDecl: . VAR VarDecl
   21           | . VAR '(' VarDeclList oSemi ')'
   22           | . VAR '(' ')'
   23           | . Const ConstDecl
   24           | . Const '(' ConstDecl oSemi ')'
   25           | . Const '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | . Const '(' ')'
   27           | . TYPE TypeDecl
   28           | . TYPE '(' TypeDeclList oSemi ')'
   29           | . TYPE '(' ')'
   30 Const: . CONST
   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   52 CompoundStmt: . '{' $@2 StmtList '}'
   66 ForStmt: . FOR $@5 ForBody
   71 IfStmt: . IF $@6 IfHeader LoopBody $@7 ElseIfList Else
   80 SwitchStmt: . SWITCH $@10 IfHeader BODY CaseBlockList '}'
   82 SelectStmt: . SELECT $@11 BODY CaseBlockList '}'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  144 NewName: . Symbol
  148 Symbol: . IDENT
  149 Name: . Symbol
  150 LabelName: . NewName
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  207        | FuncLitDecl LBrace . StmtList '}'
  208        | . FuncLitDecl error
  242 Statement: . %empty  [';', '}']
  243          | . CompoundStmt
  244          | . CommonDecl
  245          | . NonDclStmt
  246          | . error
  247 NonDclStmt: . SimpleStmt
  248           | . ForStmt
  249           | . SwitchStmt
  250           | . SelectStmt
  251           | . IfStmt
  252           | . LabelName ':' Statement
  253           | . FALL
  254           | . BREAK oNewName
  255           | . CONTINUE oNewName
  256           | . GO PseudoCall
  257           | . DEFER PseudoCall
  258           | . GOTO NewName
  259           | . GOTO
  260           | . RETURN oExprList
  261 StmtList: . Statement
  262         | . StmtList ';' Statement
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    error      posunout a přejít do stavu 294
    BREAK      posunout a přejít do stavu 20
    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    CONST      posunout a přejít do stavu 23
    CONTINUE   posunout a přejít do stavu 24
    DEFER      posunout a přejít do stavu 25
    FALL       posunout a přejít do stavu 26
    FOR        posunout a přejít do stavu 27
    FUNC       posunout a přejít do stavu 91
    GO         posunout a přejít do stavu 29
    GOTO       posunout a přejít do stavu 30
    IDENT      posunout a přejít do stavu 4
    IF         posunout a přejít do stavu 31
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RETURN     posunout a přejít do stavu 35
    SELECT     posunout a přejít do stavu 36
    STRUCT     posunout a přejít do stavu 37
    SWITCH     posunout a přejít do stavu 38
    TYPE       posunout a přejít do stavu 39
    UNION      posunout a přejít do stavu 40
    VAR        posunout a přejít do stavu 41
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 295
    '~'        posunout a přejít do stavu 51

    ';'  reduce using rule 242 (Statement)
    '}'  reduce using rule 242 (Statement)

    CommonDecl          přejít do stavu 296
    Const               přejít do stavu 54
    SimpleStmt          přejít do stavu 55
    CompoundStmt        přejít do stavu 297
    ForStmt             přejít do stavu 56
    IfStmt              přejít do stavu 57
    SwitchStmt          přejít do stavu 58
    SelectStmt          přejít do stavu 59
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    NewName             přejít do stavu 65
    Symbol              přejít do stavu 66
    Name                přejít do stavu 67
    LabelName           přejít do stavu 68
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    Statement           přejít do stavu 303
    NonDclStmt          přejít do stavu 299
    StmtList            přejít do stavu 304
    ExprList            přejít do stavu 81


State 187

   44 SimpleStmt: ExprList COLAS . ExprList
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 305


State 188

   43 SimpleStmt: ExprList '=' . ExprList
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 306


State 189

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  268 ExprList: ExprList ',' . Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 307
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 190

   10 ImportDecl: . LITERAL
   11           | . Symbol LITERAL
   12           | . '.' LITERAL
   14 ImportDeclList: ImportDeclList ';' . ImportDecl
  148 Symbol: . IDENT
  278 oSemi: ';' .  [')']

    IDENT    posunout a přejít do stavu 4
    LITERAL  posunout a přejít do stavu 13
    '.'      posunout a přejít do stavu 15

    $výchozí  reduce using rule 278 (oSemi)

    ImportDecl  přejít do stavu 308
    Symbol      přejít do stavu 17


State 191

    8 Import: IMPORT '(' ImportDeclList oSemi . ')'

    ')'  posunout a přejít do stavu 309


State 192

  188 RecvChanType: COMM . CHAN Type

    CHAN  posunout a přejít do stavu 310


State 193

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  158     | '(' . Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 311
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 194

  181 OtherType: CHAN COMM Type .

    $výchozí  reduce using rule 181 (OtherType)


State 195

  157 Type: TypeName .

    $výchozí  reduce using rule 157 (Type)


State 196

  155 Type: OtherType .

    $výchozí  reduce using rule 155 (Type)


State 197

  156 Type: PtrType .

    $výchozí  reduce using rule 156 (Type)


State 198

  153 Type: RecvChanType .

    $výchozí  reduce using rule 153 (Type)


State 199

  154 Type: FuncType .

    $výchozí  reduce using rule 154 (Type)


State 200

  141 NameOrType: . Type
  148 Symbol: . IDENT
  149 Name: . Symbol
  151 Ddd: . DDD
  152    | . DDD Type
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  200         | FUNC '(' . oArgTypeListOComma ')' FuncResult
  234 ArgType: . NameOrType
  235        | . Symbol NameOrType
  236        | . Symbol Ddd
  237        | . Ddd
  238 ArgTypeList: . ArgType
  239            | . ArgTypeList ',' ArgType
  240 oArgTypeListOComma: . %empty  [')']
  241                   | . ArgTypeList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    DDD        posunout a přejít do stavu 210
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 240 (oArgTypeListOComma)

    NameOrType          přejít do stavu 211
    Symbol              přejít do stavu 212
    Name                přejít do stavu 95
    Ddd                 přejít do stavu 213
    Type                přejít do stavu 214
    TypeName            přejít do stavu 195
    OtherType           přejít do stavu 196
    PtrType             přejít do stavu 197
    RecvChanType        přejít do stavu 198
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 199
    ArgType             přejít do stavu 215
    ArgTypeList         přejít do stavu 216
    oArgTypeListOComma  přejít do stavu 312


State 201

  167 NonRecvChanType: '(' Type . ')'

    ')'  posunout a přejít do stavu 313


State 202

  187 PtrType: '*' Type .

    $výchozí  reduce using rule 187 (PtrType)


State 203

  148 Symbol: . IDENT
  177 TypeName: Name '.' . Symbol

    IDENT  posunout a přejít do stavu 4

    Symbol  přejít do stavu 314


State 204

  286 oSimpleStmt: SimpleStmt .

    $výchozí  reduce using rule 286 (oSimpleStmt)


State 205

   63 ForHeader: RangeStmt .

    $výchozí  reduce using rule 63 (ForHeader)


State 206

   58 LoopBody: . BODY $@4 StmtList '}'
   64 ForBody: ForHeader . LoopBody

    BODY  posunout a přejít do stavu 315

    LoopBody  přejít do stavu 316


State 207

   66 ForStmt: FOR $@5 ForBody .

    $výchozí  reduce using rule 66 (ForStmt)


State 208

   43 SimpleStmt: ExprList . '=' ExprList
   44           | ExprList . COLAS ExprList
   59 RangeStmt: ExprList . '=' RANGE Expr
   60          | ExprList . COLAS RANGE Expr
  268 ExprList: ExprList . ',' Expr

    COLAS  posunout a přejít do stavu 317
    '='    posunout a přejít do stavu 318
    ','    posunout a přejít do stavu 189


State 209

   61 ForHeader: oSimpleStmt . ';' oSimpleStmt ';' oSimpleStmt
   62          | oSimpleStmt .  [BODY]

    ';'  posunout a přejít do stavu 319

    $výchozí  reduce using rule 62 (ForHeader)


State 210

  148 Symbol: . IDENT
  149 Name: . Symbol
  151 Ddd: DDD .  [')', ',']
  152    | DDD . Type
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 151 (Ddd)

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 320
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 211

  234 ArgType: NameOrType .

    $výchozí  reduce using rule 234 (ArgType)


State 212

  141 NameOrType: . Type
  148 Symbol: . IDENT
  149 Name: . Symbol
  149     | Symbol .  [')', '.', ',']
  151 Ddd: . DDD
  152    | . DDD Type
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  235 ArgType: Symbol . NameOrType
  236        | Symbol . Ddd

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    DDD        posunout a přejít do stavu 210
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 149 (Name)

    NameOrType     přejít do stavu 321
    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Ddd            přejít do stavu 322
    Type           přejít do stavu 214
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 213

  237 ArgType: Ddd .

    $výchozí  reduce using rule 237 (ArgType)


State 214

  141 NameOrType: Type .

    $výchozí  reduce using rule 141 (NameOrType)


State 215

  238 ArgTypeList: ArgType .

    $výchozí  reduce using rule 238 (ArgTypeList)


State 216

  239 ArgTypeList: ArgTypeList . ',' ArgType
  241 oArgTypeListOComma: ArgTypeList . oComma
  279 oComma: . %empty  [')']
  280       | . ','

    ','  posunout a přejít do stavu 323

    $výchozí  reduce using rule 279 (oComma)

    oComma  přejít do stavu 324


State 217

  199 FuncDecl1: '(' oArgTypeListOComma . ')' Symbol '(' oArgTypeListOComma ')' FuncResult
  200 FuncType: FUNC '(' oArgTypeListOComma . ')' FuncResult

    ')'  posunout a přejít do stavu 325


State 218

  141 NameOrType: . Type
  148 Symbol: . IDENT
  149 Name: . Symbol
  151 Ddd: . DDD
  152    | . DDD Type
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  198 FuncDecl1: Symbol '(' . oArgTypeListOComma ')' FuncResult
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  234 ArgType: . NameOrType
  235        | . Symbol NameOrType
  236        | . Symbol Ddd
  237        | . Ddd
  238 ArgTypeList: . ArgType
  239            | . ArgTypeList ',' ArgType
  240 oArgTypeListOComma: . %empty  [')']
  241                   | . ArgTypeList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    DDD        posunout a přejít do stavu 210
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 240 (oArgTypeListOComma)

    NameOrType          přejít do stavu 211
    Symbol              přejít do stavu 212
    Name                přejít do stavu 95
    Ddd                 přejít do stavu 213
    Type                přejít do stavu 214
    TypeName            přejít do stavu 195
    OtherType           přejít do stavu 196
    PtrType             přejít do stavu 197
    RecvChanType        přejít do stavu 198
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 199
    ArgType             přejít do stavu 215
    ArgTypeList         přejít do stavu 216
    oArgTypeListOComma  přejít do stavu 326


State 219

   20 CommonDecl: . VAR VarDecl
   21           | . VAR '(' VarDeclList oSemi ')'
   22           | . VAR '(' ')'
   23           | . Const ConstDecl
   24           | . Const '(' ConstDecl oSemi ')'
   25           | . Const '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | . Const '(' ')'
   27           | . TYPE TypeDecl
   28           | . TYPE '(' TypeDeclList oSemi ')'
   29           | . TYPE '(' ')'
   30 Const: . CONST
   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   52 CompoundStmt: . '{' $@2 StmtList '}'
   66 ForStmt: . FOR $@5 ForBody
   71 IfStmt: . IF $@6 IfHeader LoopBody $@7 ElseIfList Else
   80 SwitchStmt: . SWITCH $@10 IfHeader BODY CaseBlockList '}'
   82 SelectStmt: . SELECT $@11 BODY CaseBlockList '}'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  144 NewName: . Symbol
  148 Symbol: . IDENT
  149 Name: . Symbol
  150 LabelName: . NewName
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  202 FuncBody: '{' . StmtList '}'
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  242 Statement: . %empty  [';', '}']
  243          | . CompoundStmt
  244          | . CommonDecl
  245          | . NonDclStmt
  246          | . error
  247 NonDclStmt: . SimpleStmt
  248           | . ForStmt
  249           | . SwitchStmt
  250           | . SelectStmt
  251           | . IfStmt
  252           | . LabelName ':' Statement
  253           | . FALL
  254           | . BREAK oNewName
  255           | . CONTINUE oNewName
  256           | . GO PseudoCall
  257           | . DEFER PseudoCall
  258           | . GOTO NewName
  259           | . GOTO
  260           | . RETURN oExprList
  261 StmtList: . Statement
  262         | . StmtList ';' Statement
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    error      posunout a přejít do stavu 294
    BREAK      posunout a přejít do stavu 20
    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    CONST      posunout a přejít do stavu 23
    CONTINUE   posunout a přejít do stavu 24
    DEFER      posunout a přejít do stavu 25
    FALL       posunout a přejít do stavu 26
    FOR        posunout a přejít do stavu 27
    FUNC       posunout a přejít do stavu 91
    GO         posunout a přejít do stavu 29
    GOTO       posunout a přejít do stavu 30
    IDENT      posunout a přejít do stavu 4
    IF         posunout a přejít do stavu 31
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RETURN     posunout a přejít do stavu 35
    SELECT     posunout a přejít do stavu 36
    STRUCT     posunout a přejít do stavu 37
    SWITCH     posunout a přejít do stavu 38
    TYPE       posunout a přejít do stavu 39
    UNION      posunout a přejít do stavu 40
    VAR        posunout a přejít do stavu 41
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 295
    '~'        posunout a přejít do stavu 51

    ';'  reduce using rule 242 (Statement)
    '}'  reduce using rule 242 (Statement)

    CommonDecl          přejít do stavu 296
    Const               přejít do stavu 54
    SimpleStmt          přejít do stavu 55
    CompoundStmt        přejít do stavu 297
    ForStmt             přejít do stavu 56
    IfStmt              přejít do stavu 57
    SwitchStmt          přejít do stavu 58
    SelectStmt          přejít do stavu 59
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    NewName             přejít do stavu 65
    Symbol              přejít do stavu 66
    Name                přejít do stavu 67
    LabelName           přejít do stavu 68
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    Statement           přejít do stavu 303
    NonDclStmt          přejít do stavu 299
    StmtList            přejít do stavu 327
    ExprList            přejít do stavu 81


State 220

  197 FuncDecl: FUNC FuncDecl1 FuncBody .

    $výchozí  reduce using rule 197 (FuncDecl)


State 221

   58 LoopBody: . BODY $@4 StmtList '}'
   71 IfStmt: IF $@6 IfHeader . LoopBody $@7 ElseIfList Else

    BODY  posunout a přejít do stavu 315

    LoopBody  přejít do stavu 328


State 222

   67 IfHeader: oSimpleStmt .  [BODY]
   68         | oSimpleStmt . ';' oSimpleStmt

    ';'  posunout a přejít do stavu 329

    $výchozí  reduce using rule 67 (IfHeader)


State 223

  148 Symbol: IDENT .  [CHAN, COMM, FUNC, IDENT, INTERFACE, MAP, STRUCT, UNION, VARIANT, '(', '*', '[', ',']
  227 Qualident: IDENT .  [LITERAL, ';', '}']
  228          | IDENT . '.' Symbol

    '.'  posunout a přejít do stavu 330

    LITERAL     reduce using rule 227 (Qualident)
    ';'         reduce using rule 227 (Qualident)
    '}'         reduce using rule 227 (Qualident)
    $výchozí  reduce using rule 148 (Symbol)


State 224

  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  232 InterfaceDecl: '(' . Qualident ')'

    IDENT  posunout a přejít do stavu 331

    Qualident  přejít do stavu 332


State 225

  196 InterfaceType: INTERFACE LBrace '}' .

    $výchozí  reduce using rule 196 (InterfaceType)


State 226

  230 InterfaceDecl: NewName . InterfaceMethodDecl
  233 InterfaceMethodDecl: . '(' oArgTypeListOComma ')' FuncResult

    '('  posunout a přejít do stavu 333

    InterfaceMethodDecl  přejít do stavu 334


State 227

  195 InterfaceType: INTERFACE LBrace InterfaceDeclList . oSemi '}'
  220 InterfaceDeclList: InterfaceDeclList . ';' InterfaceDecl
  277 oSemi: . %empty  ['}']
  278      | . ';'

    ';'  posunout a přejít do stavu 335

    $výchozí  reduce using rule 277 (oSemi)

    oSemi  přejít do stavu 336


State 228

  231 InterfaceDecl: Qualident .

    $výchozí  reduce using rule 231 (InterfaceDecl)


State 229

  219 InterfaceDeclList: InterfaceDecl .

    $výchozí  reduce using rule 219 (InterfaceDeclList)


State 230

  182 OtherType: MAP '[' Type . ']' Type

    ']'  posunout a přejít do stavu 337


State 231

   55 CaseBlockList: . %empty
   56              | . CaseBlockList CaseBlock
   82 SelectStmt: SELECT $@11 BODY . CaseBlockList '}'

    $výchozí  reduce using rule 55 (CaseBlockList)

    CaseBlockList  přejít do stavu 338


State 232

  223 StructDecl: '(' . Embedded ')' oLiteral
  225           | '(' . '*' Embedded ')' oLiteral
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  229 Embedded: . Qualident

    IDENT  posunout a přejít do stavu 331
    '*'    posunout a přejít do stavu 339

    Qualident  přejít do stavu 238
    Embedded   přejít do stavu 340


State 233

  224 StructDecl: '*' . Embedded oLiteral
  226           | '*' . '(' Embedded ')' oLiteral
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  229 Embedded: . Qualident

    IDENT  posunout a přejít do stavu 331
    '('    posunout a přejít do stavu 341

    Qualident  přejít do stavu 238
    Embedded   přejít do stavu 342


State 234

  190 StructType: STRUCT LBrace '}' .

    $výchozí  reduce using rule 190 (StructType)


State 235

  263 NewNameList: NewName .

    $výchozí  reduce using rule 263 (NewNameList)


State 236

  189 StructType: STRUCT LBrace StructDeclList . oSemi '}'
  218 StructDeclList: StructDeclList . ';' StructDecl
  277 oSemi: . %empty  ['}']
  278      | . ';'

    ';'  posunout a přejít do stavu 343

    $výchozí  reduce using rule 277 (oSemi)

    oSemi  přejít do stavu 344


State 237

  217 StructDeclList: StructDecl .

    $výchozí  reduce using rule 217 (StructDeclList)


State 238

  229 Embedded: Qualident .

    $výchozí  reduce using rule 229 (Embedded)


State 239

  222 StructDecl: Embedded . oLiteral
  287 oLiteral: . %empty  [';', '}']
  288         | . LITERAL

    LITERAL  posunout a přejít do stavu 345

    $výchozí  reduce using rule 287 (oLiteral)

    oLiteral  přejít do stavu 346


State 240

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  221 StructDecl: NewNameList . Type oLiteral
  264 NewNameList: NewNameList . ',' NewName

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49
    ','        posunout a přejít do stavu 347

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 348
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 241

   80 SwitchStmt: SWITCH $@10 IfHeader . BODY CaseBlockList '}'

    BODY  posunout a přejít do stavu 349


State 242

   29 CommonDecl: TYPE '(' ')' .

    $výchozí  reduce using rule 29 (CommonDecl)


State 243

  215 TypeDeclList: TypeDecl .

    $výchozí  reduce using rule 215 (TypeDeclList)


State 244

   28 CommonDecl: TYPE '(' TypeDeclList . oSemi ')'
  216 TypeDeclList: TypeDeclList . ';' TypeDecl
  277 oSemi: . %empty  [')']
  278      | . ';'

    ';'  posunout a přejít do stavu 350

    $výchozí  reduce using rule 277 (oSemi)

    oSemi  přejít do stavu 351


State 245

   40 TypeDecl: TypeDeclName Type .

    $výchozí  reduce using rule 40 (TypeDecl)


State 246

  192 UnionType: UNION LBrace '}' .

    $výchozí  reduce using rule 192 (UnionType)


State 247

  191 UnionType: UNION LBrace StructDeclList . oSemi '}'
  218 StructDeclList: StructDeclList . ';' StructDecl
  277 oSemi: . %empty  ['}']
  278      | . ';'

    ';'  posunout a přejít do stavu 343

    $výchozí  reduce using rule 277 (oSemi)

    oSemi  přejít do stavu 352


State 248

   22 CommonDecl: VAR '(' ')' .

    $výchozí  reduce using rule 22 (CommonDecl)


State 249

  211 VarDeclList: VarDecl .

    $výchozí  reduce using rule 211 (VarDeclList)


State 250

   21 CommonDecl: VAR '(' VarDeclList . oSemi ')'
  212 VarDeclList: VarDeclList . ';' VarDecl
  277 oSemi: . %empty  [')']
  278      | . ';'

    ';'  posunout a přejít do stavu 353

    $výchozí  reduce using rule 277 (oSemi)

    oSemi  přejít do stavu 354


State 251

   33 VarDecl: DeclNameList '=' . ExprList
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 355


State 252

  145 DeclName: . Symbol
  148 Symbol: . IDENT
  266 DeclNameList: DeclNameList ',' . DeclName

    IDENT  posunout a přejít do stavu 4

    DeclName  přejít do stavu 356
    Symbol    přejít do stavu 130


State 253

   31 VarDecl: DeclNameList Type .  [CASE, DEFAULT, ')', ';', '}']
   32        | DeclNameList Type . '=' ExprList

    '='  posunout a přejít do stavu 357

    $výchozí  reduce using rule 31 (VarDecl)


State 254

  194 VariantType: VARIANT LBrace '}' .

    $výchozí  reduce using rule 194 (VariantType)


State 255

  193 VariantType: VARIANT LBrace StructDeclList . oSemi '}'
  218 StructDeclList: StructDeclList . ';' StructDecl
  277 oSemi: . %empty  ['}']
  278      | . ';'

    ';'  posunout a přejít do stavu 343

    $výchozí  reduce using rule 277 (oSemi)

    oSemi  přejít do stavu 358


State 256

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  163 NonRecvChanType: . FuncType
  164                | . OtherType
  165                | . PtrType
  166                | . TypeName
  167                | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  180          | CHAN . NonRecvChanType
  181          | . CHAN COMM Type
  181          | CHAN . COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  188             | COMM CHAN . Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 359
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 360
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol           přejít do stavu 94
    Name             přejít do stavu 95
    Type             přejít do stavu 361
    NonRecvChanType  přejít do stavu 96
    TypeName         přejít do stavu 362
    OtherType        přejít do stavu 363
    PtrType          přejít do stavu 364
    RecvChanType     přejít do stavu 198
    StructType       přejít do stavu 72
    UnionType        přejít do stavu 73
    VariantType      přejít do stavu 74
    InterfaceType    přejít do stavu 75
    FuncType         přejít do stavu 365


State 257

  162 NonExprType: '*' NonExprType .

    $výchozí  reduce using rule 162 (NonExprType)


State 258

  129 PrimaryExprNoParen: '(' ExprOrType ')' . '{' StartCompLit BracedKeyvalList '}'
  138 PrimaryExpr: '(' ExprOrType ')' .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', '(', ')', '*', '+', '-', '.', '/', ':', ';', '<', '=', '>', '[', '^', '|', '}', ']', ',']

    '{'  posunout a přejít do stavu 366

    $výchozí  reduce using rule 138 (PrimaryExpr)


State 259

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  179          | '[' DDD ']' . Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 367
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 260

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  178          | '[' oExpr ']' . Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 368
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 261

   26 CommonDecl: Const '(' ')' .

    $výchozí  reduce using rule 26 (CommonDecl)


State 262

   24 CommonDecl: Const '(' ConstDecl . oSemi ')'
   25           | Const '(' ConstDecl . ';' ConstDeclList oSemi ')'
  277 oSemi: . %empty  [')']
  278      | . ';'

    ';'  posunout a přejít do stavu 369

    $výchozí  reduce using rule 277 (oSemi)

    oSemi  přejít do stavu 370


State 263

   35 ConstDecl: DeclNameList '=' . ExprList
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 371


State 264

   34 ConstDecl: DeclNameList Type . '=' ExprList

    '='  posunout a přejít do stavu 372


State 265

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   85     | Expr ANDAND Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, INC, OROR, ')', ':', ';', '=', '}', ']', ',']
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 85 (Expr)

    Conflict between rule 85 and token ANDAND resolved as reduce (%left ANDAND).
    Conflict between rule 85 and token ANDNOT resolved as shift (ANDAND < ANDNOT).
    Conflict between rule 85 and token COMM resolved as reduce (COMM < ANDAND).
    Conflict between rule 85 and token EQ resolved as shift (ANDAND < EQ).
    Conflict between rule 85 and token GE resolved as shift (ANDAND < GE).
    Conflict between rule 85 and token LE resolved as shift (ANDAND < LE).
    Conflict between rule 85 and token LSH resolved as shift (ANDAND < LSH).
    Conflict between rule 85 and token NE resolved as shift (ANDAND < NE).
    Conflict between rule 85 and token OROR resolved as reduce (OROR < ANDAND).
    Conflict between rule 85 and token RSH resolved as shift (ANDAND < RSH).
    Conflict between rule 85 and token '%' resolved as shift (ANDAND < '%').
    Conflict between rule 85 and token '&' resolved as shift (ANDAND < '&').
    Conflict between rule 85 and token '*' resolved as shift (ANDAND < '*').
    Conflict between rule 85 and token '+' resolved as shift (ANDAND < '+').
    Conflict between rule 85 and token '-' resolved as shift (ANDAND < '-').
    Conflict between rule 85 and token '/' resolved as shift (ANDAND < '/').
    Conflict between rule 85 and token '<' resolved as shift (ANDAND < '<').
    Conflict between rule 85 and token '>' resolved as shift (ANDAND < '>').
    Conflict between rule 85 and token '^' resolved as shift (ANDAND < '^').
    Conflict between rule 85 and token '|' resolved as shift (ANDAND < '|').


State 266

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  100     | Expr ANDNOT Expr .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', ')', '*', '+', '-', '/', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    $výchozí  reduce using rule 100 (Expr)

    Conflict between rule 100 and token ANDAND resolved as reduce (ANDAND < ANDNOT).
    Conflict between rule 100 and token ANDNOT resolved as reduce (%left ANDNOT).
    Conflict between rule 100 and token COMM resolved as reduce (COMM < ANDNOT).
    Conflict between rule 100 and token EQ resolved as reduce (EQ < ANDNOT).
    Conflict between rule 100 and token GE resolved as reduce (GE < ANDNOT).
    Conflict between rule 100 and token LE resolved as reduce (LE < ANDNOT).
    Conflict between rule 100 and token LSH resolved as reduce (%left LSH).
    Conflict between rule 100 and token NE resolved as reduce (NE < ANDNOT).
    Conflict between rule 100 and token OROR resolved as reduce (OROR < ANDNOT).
    Conflict between rule 100 and token RSH resolved as reduce (%left RSH).
    Conflict between rule 100 and token '%' resolved as reduce (%left '%').
    Conflict between rule 100 and token '&' resolved as reduce (%left '&').
    Conflict between rule 100 and token '*' resolved as reduce (%left '*').
    Conflict between rule 100 and token '+' resolved as reduce ('+' < ANDNOT).
    Conflict between rule 100 and token '-' resolved as reduce ('-' < ANDNOT).
    Conflict between rule 100 and token '/' resolved as reduce (%left '/').
    Conflict between rule 100 and token '<' resolved as reduce ('<' < ANDNOT).
    Conflict between rule 100 and token '>' resolved as reduce ('>' < ANDNOT).
    Conflict between rule 100 and token '^' resolved as reduce ('^' < ANDNOT).
    Conflict between rule 100 and token '|' resolved as reduce ('|' < ANDNOT).


State 267

   42 SimpleStmt: Expr ASOP Expr .  [BODY, CASE, DEFAULT, ';', '}']
   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 42 (SimpleStmt)


State 268

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr
  103     | Expr COMM Expr .  [ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, INC, ')', ':', ';', '=', '}', ']', ',']

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 103 (Expr)

    Conflict between rule 103 and token ANDAND resolved as shift (COMM < ANDAND).
    Conflict between rule 103 and token ANDNOT resolved as shift (COMM < ANDNOT).
    Conflict between rule 103 and token COMM resolved as reduce (%left COMM).
    Conflict between rule 103 and token EQ resolved as shift (COMM < EQ).
    Conflict between rule 103 and token GE resolved as shift (COMM < GE).
    Conflict between rule 103 and token LE resolved as shift (COMM < LE).
    Conflict between rule 103 and token LSH resolved as shift (COMM < LSH).
    Conflict between rule 103 and token NE resolved as shift (COMM < NE).
    Conflict between rule 103 and token OROR resolved as shift (COMM < OROR).
    Conflict between rule 103 and token RSH resolved as shift (COMM < RSH).
    Conflict between rule 103 and token '%' resolved as shift (COMM < '%').
    Conflict between rule 103 and token '&' resolved as shift (COMM < '&').
    Conflict between rule 103 and token '*' resolved as shift (COMM < '*').
    Conflict between rule 103 and token '+' resolved as shift (COMM < '+').
    Conflict between rule 103 and token '-' resolved as shift (COMM < '-').
    Conflict between rule 103 and token '/' resolved as shift (COMM < '/').
    Conflict between rule 103 and token '<' resolved as shift (COMM < '<').
    Conflict between rule 103 and token '>' resolved as shift (COMM < '>').
    Conflict between rule 103 and token '^' resolved as shift (COMM < '^').
    Conflict between rule 103 and token '|' resolved as shift (COMM < '|').


State 269

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   86     | Expr EQ Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', ':', ';', '<', '=', '>', '}', ']', ',']
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 86 (Expr)

    Conflict between rule 86 and token ANDAND resolved as reduce (ANDAND < EQ).
    Conflict between rule 86 and token ANDNOT resolved as shift (EQ < ANDNOT).
    Conflict between rule 86 and token COMM resolved as reduce (COMM < EQ).
    Conflict between rule 86 and token EQ resolved as reduce (%left EQ).
    Conflict between rule 86 and token GE resolved as reduce (%left GE).
    Conflict between rule 86 and token LE resolved as reduce (%left LE).
    Conflict between rule 86 and token LSH resolved as shift (EQ < LSH).
    Conflict between rule 86 and token NE resolved as reduce (%left NE).
    Conflict between rule 86 and token OROR resolved as reduce (OROR < EQ).
    Conflict between rule 86 and token RSH resolved as shift (EQ < RSH).
    Conflict between rule 86 and token '%' resolved as shift (EQ < '%').
    Conflict between rule 86 and token '&' resolved as shift (EQ < '&').
    Conflict between rule 86 and token '*' resolved as shift (EQ < '*').
    Conflict between rule 86 and token '+' resolved as shift (EQ < '+').
    Conflict between rule 86 and token '-' resolved as shift (EQ < '-').
    Conflict between rule 86 and token '/' resolved as shift (EQ < '/').
    Conflict between rule 86 and token '<' resolved as reduce (%left '<').
    Conflict between rule 86 and token '>' resolved as reduce (%left '>').
    Conflict between rule 86 and token '^' resolved as shift (EQ < '^').
    Conflict between rule 86 and token '|' resolved as shift (EQ < '|').


State 270

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   90     | Expr GE Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', ':', ';', '<', '=', '>', '}', ']', ',']
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 90 (Expr)

    Conflict between rule 90 and token ANDAND resolved as reduce (ANDAND < GE).
    Conflict between rule 90 and token ANDNOT resolved as shift (GE < ANDNOT).
    Conflict between rule 90 and token COMM resolved as reduce (COMM < GE).
    Conflict between rule 90 and token EQ resolved as reduce (%left EQ).
    Conflict between rule 90 and token GE resolved as reduce (%left GE).
    Conflict between rule 90 and token LE resolved as reduce (%left LE).
    Conflict between rule 90 and token LSH resolved as shift (GE < LSH).
    Conflict between rule 90 and token NE resolved as reduce (%left NE).
    Conflict between rule 90 and token OROR resolved as reduce (OROR < GE).
    Conflict between rule 90 and token RSH resolved as shift (GE < RSH).
    Conflict between rule 90 and token '%' resolved as shift (GE < '%').
    Conflict between rule 90 and token '&' resolved as shift (GE < '&').
    Conflict between rule 90 and token '*' resolved as shift (GE < '*').
    Conflict between rule 90 and token '+' resolved as shift (GE < '+').
    Conflict between rule 90 and token '-' resolved as shift (GE < '-').
    Conflict between rule 90 and token '/' resolved as shift (GE < '/').
    Conflict between rule 90 and token '<' resolved as reduce (%left '<').
    Conflict between rule 90 and token '>' resolved as reduce (%left '>').
    Conflict between rule 90 and token '^' resolved as shift (GE < '^').
    Conflict between rule 90 and token '|' resolved as shift (GE < '|').


State 271

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   89     | Expr LE Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', ':', ';', '<', '=', '>', '}', ']', ',']
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 89 (Expr)

    Conflict between rule 89 and token ANDAND resolved as reduce (ANDAND < LE).
    Conflict between rule 89 and token ANDNOT resolved as shift (LE < ANDNOT).
    Conflict between rule 89 and token COMM resolved as reduce (COMM < LE).
    Conflict between rule 89 and token EQ resolved as reduce (%left EQ).
    Conflict between rule 89 and token GE resolved as reduce (%left GE).
    Conflict between rule 89 and token LE resolved as reduce (%left LE).
    Conflict between rule 89 and token LSH resolved as shift (LE < LSH).
    Conflict between rule 89 and token NE resolved as reduce (%left NE).
    Conflict between rule 89 and token OROR resolved as reduce (OROR < LE).
    Conflict between rule 89 and token RSH resolved as shift (LE < RSH).
    Conflict between rule 89 and token '%' resolved as shift (LE < '%').
    Conflict between rule 89 and token '&' resolved as shift (LE < '&').
    Conflict between rule 89 and token '*' resolved as shift (LE < '*').
    Conflict between rule 89 and token '+' resolved as shift (LE < '+').
    Conflict between rule 89 and token '-' resolved as shift (LE < '-').
    Conflict between rule 89 and token '/' resolved as shift (LE < '/').
    Conflict between rule 89 and token '<' resolved as reduce (%left '<').
    Conflict between rule 89 and token '>' resolved as reduce (%left '>').
    Conflict between rule 89 and token '^' resolved as shift (LE < '^').
    Conflict between rule 89 and token '|' resolved as shift (LE < '|').


State 272

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  101     | Expr LSH Expr .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', ')', '*', '+', '-', '/', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    $výchozí  reduce using rule 101 (Expr)

    Conflict between rule 101 and token ANDAND resolved as reduce (ANDAND < LSH).
    Conflict between rule 101 and token ANDNOT resolved as reduce (%left ANDNOT).
    Conflict between rule 101 and token COMM resolved as reduce (COMM < LSH).
    Conflict between rule 101 and token EQ resolved as reduce (EQ < LSH).
    Conflict between rule 101 and token GE resolved as reduce (GE < LSH).
    Conflict between rule 101 and token LE resolved as reduce (LE < LSH).
    Conflict between rule 101 and token LSH resolved as reduce (%left LSH).
    Conflict between rule 101 and token NE resolved as reduce (NE < LSH).
    Conflict between rule 101 and token OROR resolved as reduce (OROR < LSH).
    Conflict between rule 101 and token RSH resolved as reduce (%left RSH).
    Conflict between rule 101 and token '%' resolved as reduce (%left '%').
    Conflict between rule 101 and token '&' resolved as reduce (%left '&').
    Conflict between rule 101 and token '*' resolved as reduce (%left '*').
    Conflict between rule 101 and token '+' resolved as reduce ('+' < LSH).
    Conflict between rule 101 and token '-' resolved as reduce ('-' < LSH).
    Conflict between rule 101 and token '/' resolved as reduce (%left '/').
    Conflict between rule 101 and token '<' resolved as reduce ('<' < LSH).
    Conflict between rule 101 and token '>' resolved as reduce ('>' < LSH).
    Conflict between rule 101 and token '^' resolved as reduce ('^' < LSH).
    Conflict between rule 101 and token '|' resolved as reduce ('|' < LSH).


State 273

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   87     | Expr NE Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', ':', ';', '<', '=', '>', '}', ']', ',']
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 87 (Expr)

    Conflict between rule 87 and token ANDAND resolved as reduce (ANDAND < NE).
    Conflict between rule 87 and token ANDNOT resolved as shift (NE < ANDNOT).
    Conflict between rule 87 and token COMM resolved as reduce (COMM < NE).
    Conflict between rule 87 and token EQ resolved as reduce (%left EQ).
    Conflict between rule 87 and token GE resolved as reduce (%left GE).
    Conflict between rule 87 and token LE resolved as reduce (%left LE).
    Conflict between rule 87 and token LSH resolved as shift (NE < LSH).
    Conflict between rule 87 and token NE resolved as reduce (%left NE).
    Conflict between rule 87 and token OROR resolved as reduce (OROR < NE).
    Conflict between rule 87 and token RSH resolved as shift (NE < RSH).
    Conflict between rule 87 and token '%' resolved as shift (NE < '%').
    Conflict between rule 87 and token '&' resolved as shift (NE < '&').
    Conflict between rule 87 and token '*' resolved as shift (NE < '*').
    Conflict between rule 87 and token '+' resolved as shift (NE < '+').
    Conflict between rule 87 and token '-' resolved as shift (NE < '-').
    Conflict between rule 87 and token '/' resolved as shift (NE < '/').
    Conflict between rule 87 and token '<' resolved as reduce (%left '<').
    Conflict between rule 87 and token '>' resolved as reduce (%left '>').
    Conflict between rule 87 and token '^' resolved as shift (NE < '^').
    Conflict between rule 87 and token '|' resolved as shift (NE < '|').


State 274

   84 Expr: Expr . OROR Expr
   84     | Expr OROR Expr .  [ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, INC, OROR, ')', ':', ';', '=', '}', ']', ',']
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 84 (Expr)

    Conflict between rule 84 and token ANDAND resolved as shift (OROR < ANDAND).
    Conflict between rule 84 and token ANDNOT resolved as shift (OROR < ANDNOT).
    Conflict between rule 84 and token COMM resolved as reduce (COMM < OROR).
    Conflict between rule 84 and token EQ resolved as shift (OROR < EQ).
    Conflict between rule 84 and token GE resolved as shift (OROR < GE).
    Conflict between rule 84 and token LE resolved as shift (OROR < LE).
    Conflict between rule 84 and token LSH resolved as shift (OROR < LSH).
    Conflict between rule 84 and token NE resolved as shift (OROR < NE).
    Conflict between rule 84 and token OROR resolved as reduce (%left OROR).
    Conflict between rule 84 and token RSH resolved as shift (OROR < RSH).
    Conflict between rule 84 and token '%' resolved as shift (OROR < '%').
    Conflict between rule 84 and token '&' resolved as shift (OROR < '&').
    Conflict between rule 84 and token '*' resolved as shift (OROR < '*').
    Conflict between rule 84 and token '+' resolved as shift (OROR < '+').
    Conflict between rule 84 and token '-' resolved as shift (OROR < '-').
    Conflict between rule 84 and token '/' resolved as shift (OROR < '/').
    Conflict between rule 84 and token '<' resolved as shift (OROR < '<').
    Conflict between rule 84 and token '>' resolved as shift (OROR < '>').
    Conflict between rule 84 and token '^' resolved as shift (OROR < '^').
    Conflict between rule 84 and token '|' resolved as shift (OROR < '|').


State 275

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  102     | Expr RSH Expr .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', ')', '*', '+', '-', '/', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
  103     | Expr . COMM Expr

    $výchozí  reduce using rule 102 (Expr)

    Conflict between rule 102 and token ANDAND resolved as reduce (ANDAND < RSH).
    Conflict between rule 102 and token ANDNOT resolved as reduce (%left ANDNOT).
    Conflict between rule 102 and token COMM resolved as reduce (COMM < RSH).
    Conflict between rule 102 and token EQ resolved as reduce (EQ < RSH).
    Conflict between rule 102 and token GE resolved as reduce (GE < RSH).
    Conflict between rule 102 and token LE resolved as reduce (LE < RSH).
    Conflict between rule 102 and token LSH resolved as reduce (%left LSH).
    Conflict between rule 102 and token NE resolved as reduce (NE < RSH).
    Conflict between rule 102 and token OROR resolved as reduce (OROR < RSH).
    Conflict between rule 102 and token RSH resolved as reduce (%left RSH).
    Conflict between rule 102 and token '%' resolved as reduce (%left '%').
    Conflict between rule 102 and token '&' resolved as reduce (%left '&').
    Conflict between rule 102 and token '*' resolved as reduce (%left '*').
    Conflict between rule 102 and token '+' resolved as reduce ('+' < RSH).
    Conflict between rule 102 and token '-' resolved as reduce ('-' < RSH).
    Conflict between rule 102 and token '/' resolved as reduce (%left '/').
    Conflict between rule 102 and token '<' resolved as reduce ('<' < RSH).
    Conflict between rule 102 and token '>' resolved as reduce ('>' < RSH).
    Conflict between rule 102 and token '^' resolved as reduce ('^' < RSH).
    Conflict between rule 102 and token '|' resolved as reduce ('|' < RSH).


State 276

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   98     | Expr '%' Expr .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', ')', '*', '+', '-', '/', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    $výchozí  reduce using rule 98 (Expr)

    Conflict between rule 98 and token ANDAND resolved as reduce (ANDAND < '%').
    Conflict between rule 98 and token ANDNOT resolved as reduce (%left ANDNOT).
    Conflict between rule 98 and token COMM resolved as reduce (COMM < '%').
    Conflict between rule 98 and token EQ resolved as reduce (EQ < '%').
    Conflict between rule 98 and token GE resolved as reduce (GE < '%').
    Conflict between rule 98 and token LE resolved as reduce (LE < '%').
    Conflict between rule 98 and token LSH resolved as reduce (%left LSH).
    Conflict between rule 98 and token NE resolved as reduce (NE < '%').
    Conflict between rule 98 and token OROR resolved as reduce (OROR < '%').
    Conflict between rule 98 and token RSH resolved as reduce (%left RSH).
    Conflict between rule 98 and token '%' resolved as reduce (%left '%').
    Conflict between rule 98 and token '&' resolved as reduce (%left '&').
    Conflict between rule 98 and token '*' resolved as reduce (%left '*').
    Conflict between rule 98 and token '+' resolved as reduce ('+' < '%').
    Conflict between rule 98 and token '-' resolved as reduce ('-' < '%').
    Conflict between rule 98 and token '/' resolved as reduce (%left '/').
    Conflict between rule 98 and token '<' resolved as reduce ('<' < '%').
    Conflict between rule 98 and token '>' resolved as reduce ('>' < '%').
    Conflict between rule 98 and token '^' resolved as reduce ('^' < '%').
    Conflict between rule 98 and token '|' resolved as reduce ('|' < '%').


State 277

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
   99     | Expr '&' Expr .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', ')', '*', '+', '-', '/', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    $výchozí  reduce using rule 99 (Expr)

    Conflict between rule 99 and token ANDAND resolved as reduce (ANDAND < '&').
    Conflict between rule 99 and token ANDNOT resolved as reduce (%left ANDNOT).
    Conflict between rule 99 and token COMM resolved as reduce (COMM < '&').
    Conflict between rule 99 and token EQ resolved as reduce (EQ < '&').
    Conflict between rule 99 and token GE resolved as reduce (GE < '&').
    Conflict between rule 99 and token LE resolved as reduce (LE < '&').
    Conflict between rule 99 and token LSH resolved as reduce (%left LSH).
    Conflict between rule 99 and token NE resolved as reduce (NE < '&').
    Conflict between rule 99 and token OROR resolved as reduce (OROR < '&').
    Conflict between rule 99 and token RSH resolved as reduce (%left RSH).
    Conflict between rule 99 and token '%' resolved as reduce (%left '%').
    Conflict between rule 99 and token '&' resolved as reduce (%left '&').
    Conflict between rule 99 and token '*' resolved as reduce (%left '*').
    Conflict between rule 99 and token '+' resolved as reduce ('+' < '&').
    Conflict between rule 99 and token '-' resolved as reduce ('-' < '&').
    Conflict between rule 99 and token '/' resolved as reduce (%left '/').
    Conflict between rule 99 and token '<' resolved as reduce ('<' < '&').
    Conflict between rule 99 and token '>' resolved as reduce ('>' < '&').
    Conflict between rule 99 and token '^' resolved as reduce ('^' < '&').
    Conflict between rule 99 and token '|' resolved as reduce ('|' < '&').


State 278

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   96     | Expr '*' Expr .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', ')', '*', '+', '-', '/', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    $výchozí  reduce using rule 96 (Expr)

    Conflict between rule 96 and token ANDAND resolved as reduce (ANDAND < '*').
    Conflict between rule 96 and token ANDNOT resolved as reduce (%left ANDNOT).
    Conflict between rule 96 and token COMM resolved as reduce (COMM < '*').
    Conflict between rule 96 and token EQ resolved as reduce (EQ < '*').
    Conflict between rule 96 and token GE resolved as reduce (GE < '*').
    Conflict between rule 96 and token LE resolved as reduce (LE < '*').
    Conflict between rule 96 and token LSH resolved as reduce (%left LSH).
    Conflict between rule 96 and token NE resolved as reduce (NE < '*').
    Conflict between rule 96 and token OROR resolved as reduce (OROR < '*').
    Conflict between rule 96 and token RSH resolved as reduce (%left RSH).
    Conflict between rule 96 and token '%' resolved as reduce (%left '%').
    Conflict between rule 96 and token '&' resolved as reduce (%left '&').
    Conflict between rule 96 and token '*' resolved as reduce (%left '*').
    Conflict between rule 96 and token '+' resolved as reduce ('+' < '*').
    Conflict between rule 96 and token '-' resolved as reduce ('-' < '*').
    Conflict between rule 96 and token '/' resolved as reduce (%left '/').
    Conflict between rule 96 and token '<' resolved as reduce ('<' < '*').
    Conflict between rule 96 and token '>' resolved as reduce ('>' < '*').
    Conflict between rule 96 and token '^' resolved as reduce ('^' < '*').
    Conflict between rule 96 and token '|' resolved as reduce ('|' < '*').


State 279

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   92     | Expr '+' Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', '+', '-', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '/'     posunout a přejít do stavu 173

    $výchozí  reduce using rule 92 (Expr)

    Conflict between rule 92 and token ANDAND resolved as reduce (ANDAND < '+').
    Conflict between rule 92 and token ANDNOT resolved as shift ('+' < ANDNOT).
    Conflict between rule 92 and token COMM resolved as reduce (COMM < '+').
    Conflict between rule 92 and token EQ resolved as reduce (EQ < '+').
    Conflict between rule 92 and token GE resolved as reduce (GE < '+').
    Conflict between rule 92 and token LE resolved as reduce (LE < '+').
    Conflict between rule 92 and token LSH resolved as shift ('+' < LSH).
    Conflict between rule 92 and token NE resolved as reduce (NE < '+').
    Conflict between rule 92 and token OROR resolved as reduce (OROR < '+').
    Conflict between rule 92 and token RSH resolved as shift ('+' < RSH).
    Conflict between rule 92 and token '%' resolved as shift ('+' < '%').
    Conflict between rule 92 and token '&' resolved as shift ('+' < '&').
    Conflict between rule 92 and token '*' resolved as shift ('+' < '*').
    Conflict between rule 92 and token '+' resolved as reduce (%left '+').
    Conflict between rule 92 and token '-' resolved as reduce (%left '-').
    Conflict between rule 92 and token '/' resolved as shift ('+' < '/').
    Conflict between rule 92 and token '<' resolved as reduce ('<' < '+').
    Conflict between rule 92 and token '>' resolved as reduce ('>' < '+').
    Conflict between rule 92 and token '^' resolved as reduce (%left '^').
    Conflict between rule 92 and token '|' resolved as reduce (%left '|').


State 280

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   93     | Expr '-' Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', '+', '-', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '/'     posunout a přejít do stavu 173

    $výchozí  reduce using rule 93 (Expr)

    Conflict between rule 93 and token ANDAND resolved as reduce (ANDAND < '-').
    Conflict between rule 93 and token ANDNOT resolved as shift ('-' < ANDNOT).
    Conflict between rule 93 and token COMM resolved as reduce (COMM < '-').
    Conflict between rule 93 and token EQ resolved as reduce (EQ < '-').
    Conflict between rule 93 and token GE resolved as reduce (GE < '-').
    Conflict between rule 93 and token LE resolved as reduce (LE < '-').
    Conflict between rule 93 and token LSH resolved as shift ('-' < LSH).
    Conflict between rule 93 and token NE resolved as reduce (NE < '-').
    Conflict between rule 93 and token OROR resolved as reduce (OROR < '-').
    Conflict between rule 93 and token RSH resolved as shift ('-' < RSH).
    Conflict between rule 93 and token '%' resolved as shift ('-' < '%').
    Conflict between rule 93 and token '&' resolved as shift ('-' < '&').
    Conflict between rule 93 and token '*' resolved as shift ('-' < '*').
    Conflict between rule 93 and token '+' resolved as reduce (%left '+').
    Conflict between rule 93 and token '-' resolved as reduce (%left '-').
    Conflict between rule 93 and token '/' resolved as shift ('-' < '/').
    Conflict between rule 93 and token '<' resolved as reduce ('<' < '-').
    Conflict between rule 93 and token '>' resolved as reduce ('>' < '-').
    Conflict between rule 93 and token '^' resolved as reduce (%left '^').
    Conflict between rule 93 and token '|' resolved as reduce (%left '|').


State 281

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   97     | Expr '/' Expr .  [ANDAND, ANDNOT, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, LSH, NE, OROR, RSH, '%', '&', ')', '*', '+', '-', '/', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    $výchozí  reduce using rule 97 (Expr)

    Conflict between rule 97 and token ANDAND resolved as reduce (ANDAND < '/').
    Conflict between rule 97 and token ANDNOT resolved as reduce (%left ANDNOT).
    Conflict between rule 97 and token COMM resolved as reduce (COMM < '/').
    Conflict between rule 97 and token EQ resolved as reduce (EQ < '/').
    Conflict between rule 97 and token GE resolved as reduce (GE < '/').
    Conflict between rule 97 and token LE resolved as reduce (LE < '/').
    Conflict between rule 97 and token LSH resolved as reduce (%left LSH).
    Conflict between rule 97 and token NE resolved as reduce (NE < '/').
    Conflict between rule 97 and token OROR resolved as reduce (OROR < '/').
    Conflict between rule 97 and token RSH resolved as reduce (%left RSH).
    Conflict between rule 97 and token '%' resolved as reduce (%left '%').
    Conflict between rule 97 and token '&' resolved as reduce (%left '&').
    Conflict between rule 97 and token '*' resolved as reduce (%left '*').
    Conflict between rule 97 and token '+' resolved as reduce ('+' < '/').
    Conflict between rule 97 and token '-' resolved as reduce ('-' < '/').
    Conflict between rule 97 and token '/' resolved as reduce (%left '/').
    Conflict between rule 97 and token '<' resolved as reduce ('<' < '/').
    Conflict between rule 97 and token '>' resolved as reduce ('>' < '/').
    Conflict between rule 97 and token '^' resolved as reduce ('^' < '/').
    Conflict between rule 97 and token '|' resolved as reduce ('|' < '/').


State 282

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   88     | Expr '<' Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', ':', ';', '<', '=', '>', '}', ']', ',']
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 88 (Expr)

    Conflict between rule 88 and token ANDAND resolved as reduce (ANDAND < '<').
    Conflict between rule 88 and token ANDNOT resolved as shift ('<' < ANDNOT).
    Conflict between rule 88 and token COMM resolved as reduce (COMM < '<').
    Conflict between rule 88 and token EQ resolved as reduce (%left EQ).
    Conflict between rule 88 and token GE resolved as reduce (%left GE).
    Conflict between rule 88 and token LE resolved as reduce (%left LE).
    Conflict between rule 88 and token LSH resolved as shift ('<' < LSH).
    Conflict between rule 88 and token NE resolved as reduce (%left NE).
    Conflict between rule 88 and token OROR resolved as reduce (OROR < '<').
    Conflict between rule 88 and token RSH resolved as shift ('<' < RSH).
    Conflict between rule 88 and token '%' resolved as shift ('<' < '%').
    Conflict between rule 88 and token '&' resolved as shift ('<' < '&').
    Conflict between rule 88 and token '*' resolved as shift ('<' < '*').
    Conflict between rule 88 and token '+' resolved as shift ('<' < '+').
    Conflict between rule 88 and token '-' resolved as shift ('<' < '-').
    Conflict between rule 88 and token '/' resolved as shift ('<' < '/').
    Conflict between rule 88 and token '<' resolved as reduce (%left '<').
    Conflict between rule 88 and token '>' resolved as reduce (%left '>').
    Conflict between rule 88 and token '^' resolved as shift ('<' < '^').
    Conflict between rule 88 and token '|' resolved as shift ('<' < '|').


State 283

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   91     | Expr '>' Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', ':', ';', '<', '=', '>', '}', ']', ',']
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 91 (Expr)

    Conflict between rule 91 and token ANDAND resolved as reduce (ANDAND < '>').
    Conflict between rule 91 and token ANDNOT resolved as shift ('>' < ANDNOT).
    Conflict between rule 91 and token COMM resolved as reduce (COMM < '>').
    Conflict between rule 91 and token EQ resolved as reduce (%left EQ).
    Conflict between rule 91 and token GE resolved as reduce (%left GE).
    Conflict between rule 91 and token LE resolved as reduce (%left LE).
    Conflict between rule 91 and token LSH resolved as shift ('>' < LSH).
    Conflict between rule 91 and token NE resolved as reduce (%left NE).
    Conflict between rule 91 and token OROR resolved as reduce (OROR < '>').
    Conflict between rule 91 and token RSH resolved as shift ('>' < RSH).
    Conflict between rule 91 and token '%' resolved as shift ('>' < '%').
    Conflict between rule 91 and token '&' resolved as shift ('>' < '&').
    Conflict between rule 91 and token '*' resolved as shift ('>' < '*').
    Conflict between rule 91 and token '+' resolved as shift ('>' < '+').
    Conflict between rule 91 and token '-' resolved as shift ('>' < '-').
    Conflict between rule 91 and token '/' resolved as shift ('>' < '/').
    Conflict between rule 91 and token '<' resolved as reduce (%left '<').
    Conflict between rule 91 and token '>' resolved as reduce (%left '>').
    Conflict between rule 91 and token '^' resolved as shift ('>' < '^').
    Conflict between rule 91 and token '|' resolved as shift ('>' < '|').


State 284

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   95     | Expr '^' Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', '+', '-', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '/'     posunout a přejít do stavu 173

    $výchozí  reduce using rule 95 (Expr)

    Conflict between rule 95 and token ANDAND resolved as reduce (ANDAND < '^').
    Conflict between rule 95 and token ANDNOT resolved as shift ('^' < ANDNOT).
    Conflict between rule 95 and token COMM resolved as reduce (COMM < '^').
    Conflict between rule 95 and token EQ resolved as reduce (EQ < '^').
    Conflict between rule 95 and token GE resolved as reduce (GE < '^').
    Conflict between rule 95 and token LE resolved as reduce (LE < '^').
    Conflict between rule 95 and token LSH resolved as shift ('^' < LSH).
    Conflict between rule 95 and token NE resolved as reduce (NE < '^').
    Conflict between rule 95 and token OROR resolved as reduce (OROR < '^').
    Conflict between rule 95 and token RSH resolved as shift ('^' < RSH).
    Conflict between rule 95 and token '%' resolved as shift ('^' < '%').
    Conflict between rule 95 and token '&' resolved as shift ('^' < '&').
    Conflict between rule 95 and token '*' resolved as shift ('^' < '*').
    Conflict between rule 95 and token '+' resolved as reduce (%left '+').
    Conflict between rule 95 and token '-' resolved as reduce (%left '-').
    Conflict between rule 95 and token '/' resolved as shift ('^' < '/').
    Conflict between rule 95 and token '<' resolved as reduce ('<' < '^').
    Conflict between rule 95 and token '>' resolved as reduce ('>' < '^').
    Conflict between rule 95 and token '^' resolved as reduce (%left '^').
    Conflict between rule 95 and token '|' resolved as reduce (%left '|').


State 285

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   94     | Expr '|' Expr .  [ANDAND, ASOP, BODY, CASE, COLAS, COMM, DDD, DEC, DEFAULT, EQ, GE, INC, LE, NE, OROR, ')', '+', '-', ':', ';', '<', '=', '>', '^', '|', '}', ']', ',']
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDNOT  posunout a přejít do stavu 156
    LSH     posunout a přejít do stavu 164
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '/'     posunout a přejít do stavu 173

    $výchozí  reduce using rule 94 (Expr)

    Conflict between rule 94 and token ANDAND resolved as reduce (ANDAND < '|').
    Conflict between rule 94 and token ANDNOT resolved as shift ('|' < ANDNOT).
    Conflict between rule 94 and token COMM resolved as reduce (COMM < '|').
    Conflict between rule 94 and token EQ resolved as reduce (EQ < '|').
    Conflict between rule 94 and token GE resolved as reduce (GE < '|').
    Conflict between rule 94 and token LE resolved as reduce (LE < '|').
    Conflict between rule 94 and token LSH resolved as shift ('|' < LSH).
    Conflict between rule 94 and token NE resolved as reduce (NE < '|').
    Conflict between rule 94 and token OROR resolved as reduce (OROR < '|').
    Conflict between rule 94 and token RSH resolved as shift ('|' < RSH).
    Conflict between rule 94 and token '%' resolved as shift ('|' < '%').
    Conflict between rule 94 and token '&' resolved as shift ('|' < '&').
    Conflict between rule 94 and token '*' resolved as shift ('|' < '*').
    Conflict between rule 94 and token '+' resolved as reduce (%left '+').
    Conflict between rule 94 and token '-' resolved as reduce (%left '-').
    Conflict between rule 94 and token '/' resolved as shift ('|' < '/').
    Conflict between rule 94 and token '<' resolved as reduce ('<' < '|').
    Conflict between rule 94 and token '>' resolved as reduce ('>' < '|').
    Conflict between rule 94 and token '^' resolved as reduce (%left '^').
    Conflict between rule 94 and token '|' resolved as reduce (%left '|').


State 286

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  128                   | PrimaryExprNoParen '{' StartCompLit . BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  132 Keyval: . Expr ':' CompLitExpr
  133 BareCompLitExpr: . Expr
  134                | . '{' StartCompLit BracedKeyvalList '}'
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  271 KeyvalList: . Keyval
  272           | . BareCompLitExpr
  273           | . KeyvalList ',' Keyval
  274           | . KeyvalList ',' BareCompLitExpr
  275 BracedKeyvalList: . %empty  ['}']
  276                 | . KeyvalList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 373
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 275 (BracedKeyvalList)

    Expr                přejít do stavu 374
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    Keyval              přejít do stavu 375
    BareCompLitExpr     přejít do stavu 376
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    KeyvalList          přejít do stavu 377
    BracedKeyvalList    přejít do stavu 378


State 287

  113 PseudoCall: PrimaryExpr '(' ')' .

    $výchozí  reduce using rule 113 (PseudoCall)


State 288

  269 ExprOrTypeList: ExprOrType .

    $výchozí  reduce using rule 269 (ExprOrTypeList)


State 289

  114 PseudoCall: PrimaryExpr '(' ExprOrTypeList . oComma ')'
  115           | PrimaryExpr '(' ExprOrTypeList . DDD oComma ')'
  270 ExprOrTypeList: ExprOrTypeList . ',' ExprOrType
  279 oComma: . %empty  [')']
  280       | . ','

    DDD  posunout a přejít do stavu 379
    ','  posunout a přejít do stavu 380

    $výchozí  reduce using rule 279 (oComma)

    oComma  přejít do stavu 381


State 290

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  119                   | PrimaryExpr '.' '(' . ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  120                   | PrimaryExpr '.' '(' . TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  139 ExprOrType: . Expr
  140           | . NonExprType
  148 Symbol: . IDENT
  149 Name: . Symbol
  159 NonExprType: . RecvChanType
  160            | . FuncType
  161            | . OtherType
  162            | . '*' NonExprType
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 135
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    TYPE       posunout a přejít do stavu 382
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 136
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 137
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    ExprOrType          přejít do stavu 383
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    NonExprType         přejít do stavu 139
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 140
    RecvChanType        přejít do stavu 141
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 142
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 291

  118 PrimaryExprNoParen: PrimaryExpr '.' Symbol .

    $výchozí  reduce using rule 118 (PrimaryExprNoParen)


State 292

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr
  121 PrimaryExprNoParen: PrimaryExpr '[' Expr . ']'
  282 oExpr: Expr .  [':']

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177
    ']'     posunout a přejít do stavu 384

    $výchozí  reduce using rule 282 (oExpr)


State 293

  122 PrimaryExprNoParen: PrimaryExpr '[' oExpr . ':' oExpr ']'
  123                   | PrimaryExpr '[' oExpr . ':' oExpr ':' oExpr ']'

    ':'  posunout a přejít do stavu 385


State 294

  246 Statement: error .

    $výchozí  reduce using rule 246 (Statement)


State 295

   51 $@2: . %empty
   52 CompoundStmt: '{' . $@2 StmtList '}'

    $výchozí  reduce using rule 51 ($@2)

    $@2  přejít do stavu 386


State 296

  244 Statement: CommonDecl .

    $výchozí  reduce using rule 244 (Statement)


State 297

  243 Statement: CompoundStmt .

    $výchozí  reduce using rule 243 (Statement)


State 298

  252 NonDclStmt: LabelName ':' Statement .

    $výchozí  reduce using rule 252 (NonDclStmt)


State 299

  245 Statement: NonDclStmt .

    $výchozí  reduce using rule 245 (Statement)


State 300

  126 PrimaryExprNoParen: ConvType '(' ')' .

    $výchozí  reduce using rule 126 (PrimaryExprNoParen)


State 301

  125 PrimaryExprNoParen: ConvType '(' ExprList . oComma ')'
  268 ExprList: ExprList . ',' Expr
  279 oComma: . %empty  [')']
  280       | . ','

    ','  posunout a přejít do stavu 387

    $výchozí  reduce using rule 279 (oComma)

    oComma  přejít do stavu 388


State 302

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  127                   | CompLitType LBrace StartCompLit . BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  132 Keyval: . Expr ':' CompLitExpr
  133 BareCompLitExpr: . Expr
  134                | . '{' StartCompLit BracedKeyvalList '}'
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  271 KeyvalList: . Keyval
  272           | . BareCompLitExpr
  273           | . KeyvalList ',' Keyval
  274           | . KeyvalList ',' BareCompLitExpr
  275 BracedKeyvalList: . %empty  ['}']
  276                 | . KeyvalList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 373
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 275 (BracedKeyvalList)

    Expr                přejít do stavu 374
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    Keyval              přejít do stavu 375
    BareCompLitExpr     přejít do stavu 376
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    KeyvalList          přejít do stavu 377
    BracedKeyvalList    přejít do stavu 389


State 303

  261 StmtList: Statement .

    $výchozí  reduce using rule 261 (StmtList)


State 304

  207 FuncLit: FuncLitDecl LBrace StmtList . '}'
  262 StmtList: StmtList . ';' Statement

    ';'  posunout a přejít do stavu 390
    '}'  posunout a přejít do stavu 391


State 305

   44 SimpleStmt: ExprList COLAS ExprList .  [BODY, CASE, DEFAULT, ';', '}']
  268 ExprList: ExprList . ',' Expr

    ','  posunout a přejít do stavu 189

    $výchozí  reduce using rule 44 (SimpleStmt)


State 306

   43 SimpleStmt: ExprList '=' ExprList .  [BODY, CASE, DEFAULT, ';', '}']
  268 ExprList: ExprList . ',' Expr

    ','  posunout a přejít do stavu 189

    $výchozí  reduce using rule 43 (SimpleStmt)


State 307

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr
  268 ExprList: ExprList ',' Expr .  [BODY, CASE, COLAS, DEFAULT, ')', ';', '=', '}', ',']

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 268 (ExprList)


State 308

   14 ImportDeclList: ImportDeclList ';' ImportDecl .

    $výchozí  reduce using rule 14 (ImportDeclList)


State 309

    8 Import: IMPORT '(' ImportDeclList oSemi ')' .

    $výchozí  reduce using rule 8 (Import)


State 310

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  188             | COMM CHAN . Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 361
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 311

  158 Type: '(' Type . ')'

    ')'  posunout a přejít do stavu 392


State 312

  200 FuncType: FUNC '(' oArgTypeListOComma . ')' FuncResult

    ')'  posunout a přejít do stavu 393


State 313

  167 NonRecvChanType: '(' Type ')' .

    $výchozí  reduce using rule 167 (NonRecvChanType)


State 314

  177 TypeName: Name '.' Symbol .

    $výchozí  reduce using rule 177 (TypeName)


State 315

   57 $@4: . %empty
   58 LoopBody: BODY . $@4 StmtList '}'

    $výchozí  reduce using rule 57 ($@4)

    $@4  přejít do stavu 394


State 316

   64 ForBody: ForHeader LoopBody .

    $výchozí  reduce using rule 64 (ForBody)


State 317

   44 SimpleStmt: ExprList COLAS . ExprList
   60 RangeStmt: ExprList COLAS . RANGE Expr
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RANGE      posunout a přejít do stavu 395
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 305


State 318

   43 SimpleStmt: ExprList '=' . ExprList
   59 RangeStmt: ExprList '=' . RANGE Expr
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RANGE      posunout a přejít do stavu 396
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 306


State 319

   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   61 ForHeader: oSimpleStmt ';' . oSimpleStmt ';' oSimpleStmt
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr
  285 oSimpleStmt: . %empty  [';']
  286            | . SimpleStmt

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 285 (oSimpleStmt)

    SimpleStmt          přejít do stavu 204
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 81
    oSimpleStmt         přejít do stavu 397


State 320

  152 Ddd: DDD Type .

    $výchozí  reduce using rule 152 (Ddd)


State 321

  235 ArgType: Symbol NameOrType .

    $výchozí  reduce using rule 235 (ArgType)


State 322

  236 ArgType: Symbol Ddd .

    $výchozí  reduce using rule 236 (ArgType)


State 323

  141 NameOrType: . Type
  148 Symbol: . IDENT
  149 Name: . Symbol
  151 Ddd: . DDD
  152    | . DDD Type
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  234 ArgType: . NameOrType
  235        | . Symbol NameOrType
  236        | . Symbol Ddd
  237        | . Ddd
  239 ArgTypeList: ArgTypeList ',' . ArgType
  280 oComma: ',' .  [')']

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    DDD        posunout a přejít do stavu 210
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 280 (oComma)

    NameOrType     přejít do stavu 211
    Symbol         přejít do stavu 212
    Name           přejít do stavu 95
    Ddd            přejít do stavu 213
    Type           přejít do stavu 214
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199
    ArgType        přejít do stavu 398


State 324

  241 oArgTypeListOComma: ArgTypeList oComma .

    $výchozí  reduce using rule 241 (oArgTypeListOComma)


State 325

  148 Symbol: . IDENT
  149 Name: . Symbol
  171 FuncRetType: . RecvChanType
  172            | . FuncType
  173            | . OtherType
  174            | . PtrType
  175            | . TypeName
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  199 FuncDecl1: '(' oArgTypeListOComma ')' . Symbol '(' oArgTypeListOComma ')' FuncResult
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  200         | FUNC '(' oArgTypeListOComma ')' . FuncResult
  203 FuncResult: . %empty  [error, BODY, '{']
  204           | . FuncRetType
  205           | . '(' oArgTypeListOComma ')'

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 399
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 203 (FuncResult)

    Symbol         přejít do stavu 400
    Name           přejít do stavu 95
    FuncRetType    přejít do stavu 401
    TypeName       přejít do stavu 402
    OtherType      přejít do stavu 403
    PtrType        přejít do stavu 404
    RecvChanType   přejít do stavu 405
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 406
    FuncResult     přejít do stavu 407

    Conflict between rule 203 and token '(' resolved as shift (notParen < '(').


State 326

  198 FuncDecl1: Symbol '(' oArgTypeListOComma . ')' FuncResult

    ')'  posunout a přejít do stavu 408


State 327

  202 FuncBody: '{' StmtList . '}'
  262 StmtList: StmtList . ';' Statement

    ';'  posunout a přejít do stavu 390
    '}'  posunout a přejít do stavu 409


State 328

   70 $@7: . %empty
   71 IfStmt: IF $@6 IfHeader LoopBody . $@7 ElseIfList Else

    $výchozí  reduce using rule 70 ($@7)

    $@7  přejít do stavu 410


State 329

   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   68 IfHeader: oSimpleStmt ';' . oSimpleStmt
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr
  285 oSimpleStmt: . %empty  [BODY]
  286            | . SimpleStmt

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 285 (oSimpleStmt)

    SimpleStmt          přejít do stavu 204
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 81
    oSimpleStmt         přejít do stavu 411


State 330

  148 Symbol: . IDENT
  228 Qualident: IDENT '.' . Symbol

    IDENT  posunout a přejít do stavu 4

    Symbol  přejít do stavu 412


State 331

  227 Qualident: IDENT .  [LITERAL, ')', ';', '}']
  228          | IDENT . '.' Symbol

    '.'  posunout a přejít do stavu 330

    $výchozí  reduce using rule 227 (Qualident)


State 332

  232 InterfaceDecl: '(' Qualident . ')'

    ')'  posunout a přejít do stavu 413


State 333

  141 NameOrType: . Type
  148 Symbol: . IDENT
  149 Name: . Symbol
  151 Ddd: . DDD
  152    | . DDD Type
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  233 InterfaceMethodDecl: '(' . oArgTypeListOComma ')' FuncResult
  234 ArgType: . NameOrType
  235        | . Symbol NameOrType
  236        | . Symbol Ddd
  237        | . Ddd
  238 ArgTypeList: . ArgType
  239            | . ArgTypeList ',' ArgType
  240 oArgTypeListOComma: . %empty  [')']
  241                   | . ArgTypeList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    DDD        posunout a přejít do stavu 210
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 240 (oArgTypeListOComma)

    NameOrType          přejít do stavu 211
    Symbol              přejít do stavu 212
    Name                přejít do stavu 95
    Ddd                 přejít do stavu 213
    Type                přejít do stavu 214
    TypeName            přejít do stavu 195
    OtherType           přejít do stavu 196
    PtrType             přejít do stavu 197
    RecvChanType        přejít do stavu 198
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 199
    ArgType             přejít do stavu 215
    ArgTypeList         přejít do stavu 216
    oArgTypeListOComma  přejít do stavu 414


State 334

  230 InterfaceDecl: NewName InterfaceMethodDecl .

    $výchozí  reduce using rule 230 (InterfaceDecl)


State 335

  144 NewName: . Symbol
  148 Symbol: . IDENT
  220 InterfaceDeclList: InterfaceDeclList ';' . InterfaceDecl
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  230 InterfaceDecl: . NewName InterfaceMethodDecl
  231              | . Qualident
  232              | . '(' Qualident ')'
  278 oSemi: ';' .  ['}']

    IDENT  posunout a přejít do stavu 223
    '('    posunout a přejít do stavu 224

    $výchozí  reduce using rule 278 (oSemi)

    NewName        přejít do stavu 226
    Symbol         přejít do stavu 89
    Qualident      přejít do stavu 228
    InterfaceDecl  přejít do stavu 415


State 336

  195 InterfaceType: INTERFACE LBrace InterfaceDeclList oSemi . '}'

    '}'  posunout a přejít do stavu 416


State 337

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  182          | MAP '[' Type ']' . Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 417
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 338

   47 Case: . CASE ExprOrTypeList ':'
   48     | . CASE ExprOrTypeList '=' Expr ':'
   49     | . CASE ExprOrTypeList COLAS Expr ':'
   50     | . DEFAULT ':'
   54 CaseBlock: . Case $@3 StmtList
   56 CaseBlockList: CaseBlockList . CaseBlock
   82 SelectStmt: SELECT $@11 BODY CaseBlockList . '}'

    CASE     posunout a přejít do stavu 418
    DEFAULT  posunout a přejít do stavu 419
    '}'      posunout a přejít do stavu 420

    Case       přejít do stavu 421
    CaseBlock  přejít do stavu 422


State 339

  225 StructDecl: '(' '*' . Embedded ')' oLiteral
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  229 Embedded: . Qualident

    IDENT  posunout a přejít do stavu 331

    Qualident  přejít do stavu 238
    Embedded   přejít do stavu 423


State 340

  223 StructDecl: '(' Embedded . ')' oLiteral

    ')'  posunout a přejít do stavu 424


State 341

  226 StructDecl: '*' '(' . Embedded ')' oLiteral
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  229 Embedded: . Qualident

    IDENT  posunout a přejít do stavu 331

    Qualident  přejít do stavu 238
    Embedded   přejít do stavu 425


State 342

  224 StructDecl: '*' Embedded . oLiteral
  287 oLiteral: . %empty  [';', '}']
  288         | . LITERAL

    LITERAL  posunout a přejít do stavu 345

    $výchozí  reduce using rule 287 (oLiteral)

    oLiteral  přejít do stavu 426


State 343

  144 NewName: . Symbol
  148 Symbol: . IDENT
  218 StructDeclList: StructDeclList ';' . StructDecl
  221 StructDecl: . NewNameList Type oLiteral
  222           | . Embedded oLiteral
  223           | . '(' Embedded ')' oLiteral
  224           | . '*' Embedded oLiteral
  225           | . '(' '*' Embedded ')' oLiteral
  226           | . '*' '(' Embedded ')' oLiteral
  227 Qualident: . IDENT
  228          | . IDENT '.' Symbol
  229 Embedded: . Qualident
  263 NewNameList: . NewName
  264            | . NewNameList ',' NewName
  278 oSemi: ';' .  ['}']

    IDENT  posunout a přejít do stavu 223
    '('    posunout a přejít do stavu 232
    '*'    posunout a přejít do stavu 233

    $výchozí  reduce using rule 278 (oSemi)

    NewName      přejít do stavu 235
    Symbol       přejít do stavu 89
    StructDecl   přejít do stavu 427
    Qualident    přejít do stavu 238
    Embedded     přejít do stavu 239
    NewNameList  přejít do stavu 240


State 344

  189 StructType: STRUCT LBrace StructDeclList oSemi . '}'

    '}'  posunout a přejít do stavu 428


State 345

  288 oLiteral: LITERAL .

    $výchozí  reduce using rule 288 (oLiteral)


State 346

  222 StructDecl: Embedded oLiteral .

    $výchozí  reduce using rule 222 (StructDecl)


State 347

  144 NewName: . Symbol
  148 Symbol: . IDENT
  264 NewNameList: NewNameList ',' . NewName

    IDENT  posunout a přejít do stavu 4

    NewName  přejít do stavu 429
    Symbol   přejít do stavu 89


State 348

  221 StructDecl: NewNameList Type . oLiteral
  287 oLiteral: . %empty  [';', '}']
  288         | . LITERAL

    LITERAL  posunout a přejít do stavu 345

    $výchozí  reduce using rule 287 (oLiteral)

    oLiteral  přejít do stavu 430


State 349

   55 CaseBlockList: . %empty
   56              | . CaseBlockList CaseBlock
   80 SwitchStmt: SWITCH $@10 IfHeader BODY . CaseBlockList '}'

    $výchozí  reduce using rule 55 (CaseBlockList)

    CaseBlockList  přejít do stavu 431


State 350

   39 TypeDeclName: . Symbol
   40 TypeDecl: . TypeDeclName Type
  148 Symbol: . IDENT
  216 TypeDeclList: TypeDeclList ';' . TypeDecl
  278 oSemi: ';' .  [')']

    IDENT  posunout a přejít do stavu 4

    $výchozí  reduce using rule 278 (oSemi)

    TypeDeclName  přejít do stavu 123
    TypeDecl      přejít do stavu 432
    Symbol        přejít do stavu 125


State 351

   28 CommonDecl: TYPE '(' TypeDeclList oSemi . ')'

    ')'  posunout a přejít do stavu 433


State 352

  191 UnionType: UNION LBrace StructDeclList oSemi . '}'

    '}'  posunout a přejít do stavu 434


State 353

   31 VarDecl: . DeclNameList Type
   32        | . DeclNameList Type '=' ExprList
   33        | . DeclNameList '=' ExprList
  145 DeclName: . Symbol
  148 Symbol: . IDENT
  212 VarDeclList: VarDeclList ';' . VarDecl
  265 DeclNameList: . DeclName
  266             | . DeclNameList ',' DeclName
  278 oSemi: ';' .  [')']

    IDENT  posunout a přejít do stavu 4

    $výchozí  reduce using rule 278 (oSemi)

    VarDecl       přejít do stavu 435
    DeclName      přejít do stavu 129
    Symbol        přejít do stavu 130
    DeclNameList  přejít do stavu 131


State 354

   21 CommonDecl: VAR '(' VarDeclList oSemi . ')'

    ')'  posunout a přejít do stavu 436


State 355

   33 VarDecl: DeclNameList '=' ExprList .  [CASE, DEFAULT, ')', ';', '}']
  268 ExprList: ExprList . ',' Expr

    ','  posunout a přejít do stavu 189

    $výchozí  reduce using rule 33 (VarDecl)


State 356

  266 DeclNameList: DeclNameList ',' DeclName .

    $výchozí  reduce using rule 266 (DeclNameList)


State 357

   32 VarDecl: DeclNameList Type '=' . ExprList
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 437


State 358

  193 VariantType: VARIANT LBrace StructDeclList oSemi . '}'

    '}'  posunout a přejít do stavu 438


State 359

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  181          | CHAN COMM . Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  188             | COMM . CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 256
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 194
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 360

  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  158     | '(' . Type ')'
  167 NonRecvChanType: '(' . Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 439
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 361

  188 RecvChanType: COMM CHAN Type .

    $výchozí  reduce using rule 188 (RecvChanType)


State 362

  157 Type: TypeName .  [COLAS, DDD, ')', ':', '=', ',']
  166 NonRecvChanType: TypeName .  [BODY, '(', '{']

    BODY        reduce using rule 166 (NonRecvChanType)
    '('         reduce using rule 166 (NonRecvChanType)
    '{'         reduce using rule 166 (NonRecvChanType)
    $výchozí  reduce using rule 157 (Type)


State 363

  155 Type: OtherType .  [COLAS, DDD, ')', ':', '=', ',']
  164 NonRecvChanType: OtherType .  [BODY, '(', '{']

    BODY        reduce using rule 164 (NonRecvChanType)
    '('         reduce using rule 164 (NonRecvChanType)
    '{'         reduce using rule 164 (NonRecvChanType)
    $výchozí  reduce using rule 155 (Type)


State 364

  156 Type: PtrType .  [COLAS, DDD, ')', ':', '=', ',']
  165 NonRecvChanType: PtrType .  [BODY, '(', '{']

    BODY        reduce using rule 165 (NonRecvChanType)
    '('         reduce using rule 165 (NonRecvChanType)
    '{'         reduce using rule 165 (NonRecvChanType)
    $výchozí  reduce using rule 156 (Type)


State 365

  154 Type: FuncType .  [COLAS, DDD, ')', ':', '=', ',']
  163 NonRecvChanType: FuncType .  [BODY, '(', '{']

    BODY        reduce using rule 163 (NonRecvChanType)
    '('         reduce using rule 163 (NonRecvChanType)
    '{'         reduce using rule 163 (NonRecvChanType)
    $výchozí  reduce using rule 154 (Type)


State 366

  129 PrimaryExprNoParen: '(' ExprOrType ')' '{' . StartCompLit BracedKeyvalList '}'
  131 StartCompLit: . %empty

    $výchozí  reduce using rule 131 (StartCompLit)

    StartCompLit  přejít do stavu 440


State 367

  179 OtherType: '[' DDD ']' Type .

    $výchozí  reduce using rule 179 (OtherType)


State 368

  178 OtherType: '[' oExpr ']' Type .

    $výchozí  reduce using rule 178 (OtherType)


State 369

   25 CommonDecl: Const '(' ConstDecl ';' . ConstDeclList oSemi ')'
   34 ConstDecl: . DeclNameList Type '=' ExprList
   35          | . DeclNameList '=' ExprList
   36 ConstDecl1: . ConstDecl
   37           | . DeclNameList Type
   38           | . DeclNameList
  145 DeclName: . Symbol
  148 Symbol: . IDENT
  213 ConstDeclList: . ConstDecl1
  214              | . ConstDeclList ';' ConstDecl1
  265 DeclNameList: . DeclName
  266             | . DeclNameList ',' DeclName
  278 oSemi: ';' .  [')']

    IDENT  posunout a přejít do stavu 4

    $výchozí  reduce using rule 278 (oSemi)

    ConstDecl      přejít do stavu 441
    ConstDecl1     přejít do stavu 442
    DeclName       přejít do stavu 129
    Symbol         přejít do stavu 130
    ConstDeclList  přejít do stavu 443
    DeclNameList   přejít do stavu 444


State 370

   24 CommonDecl: Const '(' ConstDecl oSemi . ')'

    ')'  posunout a přejít do stavu 445


State 371

   35 ConstDecl: DeclNameList '=' ExprList .  [CASE, DEFAULT, ')', ';', '}']
  268 ExprList: ExprList . ',' Expr

    ','  posunout a přejít do stavu 189

    $výchozí  reduce using rule 35 (ConstDecl)


State 372

   34 ConstDecl: DeclNameList Type '=' . ExprList
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 116
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 446


State 373

  131 StartCompLit: . %empty
  134 BareCompLitExpr: '{' . StartCompLit BracedKeyvalList '}'

    $výchozí  reduce using rule 131 (StartCompLit)

    StartCompLit  přejít do stavu 447


State 374

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr
  132 Keyval: Expr . ':' CompLitExpr
  133 BareCompLitExpr: Expr .  ['}', ',']

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    ':'     posunout a přejít do stavu 448
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 133 (BareCompLitExpr)


State 375

  271 KeyvalList: Keyval .

    $výchozí  reduce using rule 271 (KeyvalList)


State 376

  272 KeyvalList: BareCompLitExpr .

    $výchozí  reduce using rule 272 (KeyvalList)


State 377

  273 KeyvalList: KeyvalList . ',' Keyval
  274           | KeyvalList . ',' BareCompLitExpr
  276 BracedKeyvalList: KeyvalList . oComma
  279 oComma: . %empty  ['}']
  280       | . ','

    ','  posunout a přejít do stavu 449

    $výchozí  reduce using rule 279 (oComma)

    oComma  přejít do stavu 450


State 378

  128 PrimaryExprNoParen: PrimaryExprNoParen '{' StartCompLit BracedKeyvalList . '}'

    '}'  posunout a přejít do stavu 451


State 379

  115 PseudoCall: PrimaryExpr '(' ExprOrTypeList DDD . oComma ')'
  279 oComma: . %empty  [')']
  280       | . ','

    ','  posunout a přejít do stavu 452

    $výchozí  reduce using rule 279 (oComma)

    oComma  přejít do stavu 453


State 380

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  139 ExprOrType: . Expr
  140           | . NonExprType
  148 Symbol: . IDENT
  149 Name: . Symbol
  159 NonExprType: . RecvChanType
  160            | . FuncType
  161            | . OtherType
  162            | . '*' NonExprType
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  270 ExprOrTypeList: ExprOrTypeList ',' . ExprOrType
  280 oComma: ',' .  [')']

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 135
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 136
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 280 (oComma)

    Expr                přejít do stavu 137
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    ExprOrType          přejít do stavu 454
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    NonExprType         přejít do stavu 139
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 140
    RecvChanType        přejít do stavu 141
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 142
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 381

  114 PseudoCall: PrimaryExpr '(' ExprOrTypeList oComma . ')'

    ')'  posunout a přejít do stavu 455


State 382

  120 PrimaryExprNoParen: PrimaryExpr '.' '(' TYPE . ')'

    ')'  posunout a přejít do stavu 456


State 383

  119 PrimaryExprNoParen: PrimaryExpr '.' '(' ExprOrType . ')'

    ')'  posunout a přejít do stavu 457


State 384

  121 PrimaryExprNoParen: PrimaryExpr '[' Expr ']' .

    $výchozí  reduce using rule 121 (PrimaryExprNoParen)


State 385

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  122                   | PrimaryExpr '[' oExpr ':' . oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  123                   | PrimaryExpr '[' oExpr ':' . oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  281 oExpr: . %empty  [':', ']']
  282      | . Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 281 (oExpr)

    Expr                přejít do stavu 147
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    oExpr               přejít do stavu 458


State 386

   20 CommonDecl: . VAR VarDecl
   21           | . VAR '(' VarDeclList oSemi ')'
   22           | . VAR '(' ')'
   23           | . Const ConstDecl
   24           | . Const '(' ConstDecl oSemi ')'
   25           | . Const '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | . Const '(' ')'
   27           | . TYPE TypeDecl
   28           | . TYPE '(' TypeDeclList oSemi ')'
   29           | . TYPE '(' ')'
   30 Const: . CONST
   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   52 CompoundStmt: . '{' $@2 StmtList '}'
   52             | '{' $@2 . StmtList '}'
   66 ForStmt: . FOR $@5 ForBody
   71 IfStmt: . IF $@6 IfHeader LoopBody $@7 ElseIfList Else
   80 SwitchStmt: . SWITCH $@10 IfHeader BODY CaseBlockList '}'
   82 SelectStmt: . SELECT $@11 BODY CaseBlockList '}'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  144 NewName: . Symbol
  148 Symbol: . IDENT
  149 Name: . Symbol
  150 LabelName: . NewName
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  242 Statement: . %empty  [';', '}']
  243          | . CompoundStmt
  244          | . CommonDecl
  245          | . NonDclStmt
  246          | . error
  247 NonDclStmt: . SimpleStmt
  248           | . ForStmt
  249           | . SwitchStmt
  250           | . SelectStmt
  251           | . IfStmt
  252           | . LabelName ':' Statement
  253           | . FALL
  254           | . BREAK oNewName
  255           | . CONTINUE oNewName
  256           | . GO PseudoCall
  257           | . DEFER PseudoCall
  258           | . GOTO NewName
  259           | . GOTO
  260           | . RETURN oExprList
  261 StmtList: . Statement
  262         | . StmtList ';' Statement
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    error      posunout a přejít do stavu 294
    BREAK      posunout a přejít do stavu 20
    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    CONST      posunout a přejít do stavu 23
    CONTINUE   posunout a přejít do stavu 24
    DEFER      posunout a přejít do stavu 25
    FALL       posunout a přejít do stavu 26
    FOR        posunout a přejít do stavu 27
    FUNC       posunout a přejít do stavu 91
    GO         posunout a přejít do stavu 29
    GOTO       posunout a přejít do stavu 30
    IDENT      posunout a přejít do stavu 4
    IF         posunout a přejít do stavu 31
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RETURN     posunout a přejít do stavu 35
    SELECT     posunout a přejít do stavu 36
    STRUCT     posunout a přejít do stavu 37
    SWITCH     posunout a přejít do stavu 38
    TYPE       posunout a přejít do stavu 39
    UNION      posunout a přejít do stavu 40
    VAR        posunout a přejít do stavu 41
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 295
    '~'        posunout a přejít do stavu 51

    ';'  reduce using rule 242 (Statement)
    '}'  reduce using rule 242 (Statement)

    CommonDecl          přejít do stavu 296
    Const               přejít do stavu 54
    SimpleStmt          přejít do stavu 55
    CompoundStmt        přejít do stavu 297
    ForStmt             přejít do stavu 56
    IfStmt              přejít do stavu 57
    SwitchStmt          přejít do stavu 58
    SelectStmt          přejít do stavu 59
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    NewName             přejít do stavu 65
    Symbol              přejít do stavu 66
    Name                přejít do stavu 67
    LabelName           přejít do stavu 68
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    Statement           přejít do stavu 303
    NonDclStmt          přejít do stavu 299
    StmtList            přejít do stavu 459
    ExprList            přejít do stavu 81


State 387

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  268 ExprList: ExprList ',' . Expr
  280 oComma: ',' .  [')']

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 280 (oComma)

    Expr                přejít do stavu 307
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 388

  125 PrimaryExprNoParen: ConvType '(' ExprList oComma . ')'

    ')'  posunout a přejít do stavu 460


State 389

  127 PrimaryExprNoParen: CompLitType LBrace StartCompLit BracedKeyvalList . '}'

    '}'  posunout a přejít do stavu 461


State 390

   20 CommonDecl: . VAR VarDecl
   21           | . VAR '(' VarDeclList oSemi ')'
   22           | . VAR '(' ')'
   23           | . Const ConstDecl
   24           | . Const '(' ConstDecl oSemi ')'
   25           | . Const '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | . Const '(' ')'
   27           | . TYPE TypeDecl
   28           | . TYPE '(' TypeDeclList oSemi ')'
   29           | . TYPE '(' ')'
   30 Const: . CONST
   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   52 CompoundStmt: . '{' $@2 StmtList '}'
   66 ForStmt: . FOR $@5 ForBody
   71 IfStmt: . IF $@6 IfHeader LoopBody $@7 ElseIfList Else
   80 SwitchStmt: . SWITCH $@10 IfHeader BODY CaseBlockList '}'
   82 SelectStmt: . SELECT $@11 BODY CaseBlockList '}'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  144 NewName: . Symbol
  148 Symbol: . IDENT
  149 Name: . Symbol
  150 LabelName: . NewName
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  242 Statement: . %empty  [CASE, DEFAULT, ';', '}']
  243          | . CompoundStmt
  244          | . CommonDecl
  245          | . NonDclStmt
  246          | . error
  247 NonDclStmt: . SimpleStmt
  248           | . ForStmt
  249           | . SwitchStmt
  250           | . SelectStmt
  251           | . IfStmt
  252           | . LabelName ':' Statement
  253           | . FALL
  254           | . BREAK oNewName
  255           | . CONTINUE oNewName
  256           | . GO PseudoCall
  257           | . DEFER PseudoCall
  258           | . GOTO NewName
  259           | . GOTO
  260           | . RETURN oExprList
  262 StmtList: StmtList ';' . Statement
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    error      posunout a přejít do stavu 294
    BREAK      posunout a přejít do stavu 20
    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    CONST      posunout a přejít do stavu 23
    CONTINUE   posunout a přejít do stavu 24
    DEFER      posunout a přejít do stavu 25
    FALL       posunout a přejít do stavu 26
    FOR        posunout a přejít do stavu 27
    FUNC       posunout a přejít do stavu 91
    GO         posunout a přejít do stavu 29
    GOTO       posunout a přejít do stavu 30
    IDENT      posunout a přejít do stavu 4
    IF         posunout a přejít do stavu 31
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RETURN     posunout a přejít do stavu 35
    SELECT     posunout a přejít do stavu 36
    STRUCT     posunout a přejít do stavu 37
    SWITCH     posunout a přejít do stavu 38
    TYPE       posunout a přejít do stavu 39
    UNION      posunout a přejít do stavu 40
    VAR        posunout a přejít do stavu 41
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 295
    '~'        posunout a přejít do stavu 51

    CASE     reduce using rule 242 (Statement)
    DEFAULT  reduce using rule 242 (Statement)
    ';'      reduce using rule 242 (Statement)
    '}'      reduce using rule 242 (Statement)

    CommonDecl          přejít do stavu 296
    Const               přejít do stavu 54
    SimpleStmt          přejít do stavu 55
    CompoundStmt        přejít do stavu 297
    ForStmt             přejít do stavu 56
    IfStmt              přejít do stavu 57
    SwitchStmt          přejít do stavu 58
    SelectStmt          přejít do stavu 59
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    NewName             přejít do stavu 65
    Symbol              přejít do stavu 66
    Name                přejít do stavu 67
    LabelName           přejít do stavu 68
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    Statement           přejít do stavu 462
    NonDclStmt          přejít do stavu 299
    ExprList            přejít do stavu 81


State 391

  207 FuncLit: FuncLitDecl LBrace StmtList '}' .

    $výchozí  reduce using rule 207 (FuncLit)


State 392

  158 Type: '(' Type ')' .

    $výchozí  reduce using rule 158 (Type)


State 393

  148 Symbol: . IDENT
  149 Name: . Symbol
  171 FuncRetType: . RecvChanType
  172            | . FuncType
  173            | . OtherType
  174            | . PtrType
  175            | . TypeName
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  200         | FUNC '(' oArgTypeListOComma ')' . FuncResult
  203 FuncResult: . %empty  [error, BODY, CASE, COLAS, DDD, DEFAULT, LITERAL, ')', ':', ';', '=', '{', '}', ']', ',']
  204           | . FuncRetType
  205           | . '(' oArgTypeListOComma ')'

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 399
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 203 (FuncResult)

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    FuncRetType    přejít do stavu 401
    TypeName       přejít do stavu 402
    OtherType      přejít do stavu 403
    PtrType        přejít do stavu 404
    RecvChanType   přejít do stavu 405
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 406
    FuncResult     přejít do stavu 407

    Conflict between rule 203 and token '(' resolved as shift (notParen < '(').


State 394

   20 CommonDecl: . VAR VarDecl
   21           | . VAR '(' VarDeclList oSemi ')'
   22           | . VAR '(' ')'
   23           | . Const ConstDecl
   24           | . Const '(' ConstDecl oSemi ')'
   25           | . Const '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | . Const '(' ')'
   27           | . TYPE TypeDecl
   28           | . TYPE '(' TypeDeclList oSemi ')'
   29           | . TYPE '(' ')'
   30 Const: . CONST
   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   52 CompoundStmt: . '{' $@2 StmtList '}'
   58 LoopBody: BODY $@4 . StmtList '}'
   66 ForStmt: . FOR $@5 ForBody
   71 IfStmt: . IF $@6 IfHeader LoopBody $@7 ElseIfList Else
   80 SwitchStmt: . SWITCH $@10 IfHeader BODY CaseBlockList '}'
   82 SelectStmt: . SELECT $@11 BODY CaseBlockList '}'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  144 NewName: . Symbol
  148 Symbol: . IDENT
  149 Name: . Symbol
  150 LabelName: . NewName
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  242 Statement: . %empty  [';', '}']
  243          | . CompoundStmt
  244          | . CommonDecl
  245          | . NonDclStmt
  246          | . error
  247 NonDclStmt: . SimpleStmt
  248           | . ForStmt
  249           | . SwitchStmt
  250           | . SelectStmt
  251           | . IfStmt
  252           | . LabelName ':' Statement
  253           | . FALL
  254           | . BREAK oNewName
  255           | . CONTINUE oNewName
  256           | . GO PseudoCall
  257           | . DEFER PseudoCall
  258           | . GOTO NewName
  259           | . GOTO
  260           | . RETURN oExprList
  261 StmtList: . Statement
  262         | . StmtList ';' Statement
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    error      posunout a přejít do stavu 294
    BREAK      posunout a přejít do stavu 20
    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    CONST      posunout a přejít do stavu 23
    CONTINUE   posunout a přejít do stavu 24
    DEFER      posunout a přejít do stavu 25
    FALL       posunout a přejít do stavu 26
    FOR        posunout a přejít do stavu 27
    FUNC       posunout a přejít do stavu 91
    GO         posunout a přejít do stavu 29
    GOTO       posunout a přejít do stavu 30
    IDENT      posunout a přejít do stavu 4
    IF         posunout a přejít do stavu 31
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RETURN     posunout a přejít do stavu 35
    SELECT     posunout a přejít do stavu 36
    STRUCT     posunout a přejít do stavu 37
    SWITCH     posunout a přejít do stavu 38
    TYPE       posunout a přejít do stavu 39
    UNION      posunout a přejít do stavu 40
    VAR        posunout a přejít do stavu 41
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 295
    '~'        posunout a přejít do stavu 51

    ';'  reduce using rule 242 (Statement)
    '}'  reduce using rule 242 (Statement)

    CommonDecl          přejít do stavu 296
    Const               přejít do stavu 54
    SimpleStmt          přejít do stavu 55
    CompoundStmt        přejít do stavu 297
    ForStmt             přejít do stavu 56
    IfStmt              přejít do stavu 57
    SwitchStmt          přejít do stavu 58
    SelectStmt          přejít do stavu 59
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    NewName             přejít do stavu 65
    Symbol              přejít do stavu 66
    Name                přejít do stavu 67
    LabelName           přejít do stavu 68
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    Statement           přejít do stavu 303
    NonDclStmt          přejít do stavu 299
    StmtList            přejít do stavu 463
    ExprList            přejít do stavu 81


State 395

   60 RangeStmt: ExprList COLAS RANGE . Expr
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 464
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 396

   59 RangeStmt: ExprList '=' RANGE . Expr
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 465
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 397

   61 ForHeader: oSimpleStmt ';' oSimpleStmt . ';' oSimpleStmt

    ';'  posunout a přejít do stavu 466


State 398

  239 ArgTypeList: ArgTypeList ',' ArgType .

    $výchozí  reduce using rule 239 (ArgTypeList)


State 399

  141 NameOrType: . Type
  148 Symbol: . IDENT
  149 Name: . Symbol
  151 Ddd: . DDD
  152    | . DDD Type
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  205 FuncResult: '(' . oArgTypeListOComma ')'
  234 ArgType: . NameOrType
  235        | . Symbol NameOrType
  236        | . Symbol Ddd
  237        | . Ddd
  238 ArgTypeList: . ArgType
  239            | . ArgTypeList ',' ArgType
  240 oArgTypeListOComma: . %empty  [')']
  241                   | . ArgTypeList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    DDD        posunout a přejít do stavu 210
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 240 (oArgTypeListOComma)

    NameOrType          přejít do stavu 211
    Symbol              přejít do stavu 212
    Name                přejít do stavu 95
    Ddd                 přejít do stavu 213
    Type                přejít do stavu 214
    TypeName            přejít do stavu 195
    OtherType           přejít do stavu 196
    PtrType             přejít do stavu 197
    RecvChanType        přejít do stavu 198
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 199
    ArgType             přejít do stavu 215
    ArgTypeList         přejít do stavu 216
    oArgTypeListOComma  přejít do stavu 467


State 400

  149 Name: Symbol .  [error, BODY, '.', '{']
  199 FuncDecl1: '(' oArgTypeListOComma ')' Symbol . '(' oArgTypeListOComma ')' FuncResult

    '('  posunout a přejít do stavu 468

    $výchozí  reduce using rule 149 (Name)

    Conflict between rule 149 and token '(' resolved as shift (notParen < '(').


State 401

  204 FuncResult: FuncRetType .

    $výchozí  reduce using rule 204 (FuncResult)


State 402

  175 FuncRetType: TypeName .

    $výchozí  reduce using rule 175 (FuncRetType)


State 403

  173 FuncRetType: OtherType .

    $výchozí  reduce using rule 173 (FuncRetType)


State 404

  174 FuncRetType: PtrType .

    $výchozí  reduce using rule 174 (FuncRetType)


State 405

  171 FuncRetType: RecvChanType .

    $výchozí  reduce using rule 171 (FuncRetType)


State 406

  172 FuncRetType: FuncType .

    $výchozí  reduce using rule 172 (FuncRetType)


State 407

  200 FuncType: FUNC '(' oArgTypeListOComma ')' FuncResult .

    $výchozí  reduce using rule 200 (FuncType)


State 408

  148 Symbol: . IDENT
  149 Name: . Symbol
  171 FuncRetType: . RecvChanType
  172            | . FuncType
  173            | . OtherType
  174            | . PtrType
  175            | . TypeName
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  198 FuncDecl1: Symbol '(' oArgTypeListOComma ')' . FuncResult
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  203 FuncResult: . %empty  [';', '{']
  204           | . FuncRetType
  205           | . '(' oArgTypeListOComma ')'

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 399
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 203 (FuncResult)

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    FuncRetType    přejít do stavu 401
    TypeName       přejít do stavu 402
    OtherType      přejít do stavu 403
    PtrType        přejít do stavu 404
    RecvChanType   přejít do stavu 405
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 406
    FuncResult     přejít do stavu 469


State 409

  202 FuncBody: '{' StmtList '}' .

    $výchozí  reduce using rule 202 (FuncBody)


State 410

   71 IfStmt: IF $@6 IfHeader LoopBody $@7 . ElseIfList Else
   74 ElseIfList: . %empty
   75           | . ElseIfList ElseIf

    $výchozí  reduce using rule 74 (ElseIfList)

    ElseIfList  přejít do stavu 470


State 411

   68 IfHeader: oSimpleStmt ';' oSimpleStmt .

    $výchozí  reduce using rule 68 (IfHeader)


State 412

  228 Qualident: IDENT '.' Symbol .

    $výchozí  reduce using rule 228 (Qualident)


State 413

  232 InterfaceDecl: '(' Qualident ')' .

    $výchozí  reduce using rule 232 (InterfaceDecl)


State 414

  233 InterfaceMethodDecl: '(' oArgTypeListOComma . ')' FuncResult

    ')'  posunout a přejít do stavu 471


State 415

  220 InterfaceDeclList: InterfaceDeclList ';' InterfaceDecl .

    $výchozí  reduce using rule 220 (InterfaceDeclList)


State 416

  195 InterfaceType: INTERFACE LBrace InterfaceDeclList oSemi '}' .

    $výchozí  reduce using rule 195 (InterfaceType)


State 417

  182 OtherType: MAP '[' Type ']' Type .

    $výchozí  reduce using rule 182 (OtherType)


State 418

   47 Case: CASE . ExprOrTypeList ':'
   48     | CASE . ExprOrTypeList '=' Expr ':'
   49     | CASE . ExprOrTypeList COLAS Expr ':'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  139 ExprOrType: . Expr
  140           | . NonExprType
  148 Symbol: . IDENT
  149 Name: . Symbol
  159 NonExprType: . RecvChanType
  160            | . FuncType
  161            | . OtherType
  162            | . '*' NonExprType
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  269 ExprOrTypeList: . ExprOrType
  270               | . ExprOrTypeList ',' ExprOrType

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 135
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 136
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 137
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    ExprOrType          přejít do stavu 288
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    NonExprType         přejít do stavu 139
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 140
    RecvChanType        přejít do stavu 141
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 142
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprOrTypeList      přejít do stavu 472


State 419

   50 Case: DEFAULT . ':'

    ':'  posunout a přejít do stavu 473


State 420

   82 SelectStmt: SELECT $@11 BODY CaseBlockList '}' .

    $výchozí  reduce using rule 82 (SelectStmt)


State 421

   53 $@3: . %empty
   54 CaseBlock: Case . $@3 StmtList

    $výchozí  reduce using rule 53 ($@3)

    $@3  přejít do stavu 474


State 422

   56 CaseBlockList: CaseBlockList CaseBlock .

    $výchozí  reduce using rule 56 (CaseBlockList)


State 423

  225 StructDecl: '(' '*' Embedded . ')' oLiteral

    ')'  posunout a přejít do stavu 475


State 424

  223 StructDecl: '(' Embedded ')' . oLiteral
  287 oLiteral: . %empty  [';', '}']
  288         | . LITERAL

    LITERAL  posunout a přejít do stavu 345

    $výchozí  reduce using rule 287 (oLiteral)

    oLiteral  přejít do stavu 476


State 425

  226 StructDecl: '*' '(' Embedded . ')' oLiteral

    ')'  posunout a přejít do stavu 477


State 426

  224 StructDecl: '*' Embedded oLiteral .

    $výchozí  reduce using rule 224 (StructDecl)


State 427

  218 StructDeclList: StructDeclList ';' StructDecl .

    $výchozí  reduce using rule 218 (StructDeclList)


State 428

  189 StructType: STRUCT LBrace StructDeclList oSemi '}' .

    $výchozí  reduce using rule 189 (StructType)


State 429

  264 NewNameList: NewNameList ',' NewName .

    $výchozí  reduce using rule 264 (NewNameList)


State 430

  221 StructDecl: NewNameList Type oLiteral .

    $výchozí  reduce using rule 221 (StructDecl)


State 431

   47 Case: . CASE ExprOrTypeList ':'
   48     | . CASE ExprOrTypeList '=' Expr ':'
   49     | . CASE ExprOrTypeList COLAS Expr ':'
   50     | . DEFAULT ':'
   54 CaseBlock: . Case $@3 StmtList
   56 CaseBlockList: CaseBlockList . CaseBlock
   80 SwitchStmt: SWITCH $@10 IfHeader BODY CaseBlockList . '}'

    CASE     posunout a přejít do stavu 418
    DEFAULT  posunout a přejít do stavu 419
    '}'      posunout a přejít do stavu 478

    Case       přejít do stavu 421
    CaseBlock  přejít do stavu 422


State 432

  216 TypeDeclList: TypeDeclList ';' TypeDecl .

    $výchozí  reduce using rule 216 (TypeDeclList)


State 433

   28 CommonDecl: TYPE '(' TypeDeclList oSemi ')' .

    $výchozí  reduce using rule 28 (CommonDecl)


State 434

  191 UnionType: UNION LBrace StructDeclList oSemi '}' .

    $výchozí  reduce using rule 191 (UnionType)


State 435

  212 VarDeclList: VarDeclList ';' VarDecl .

    $výchozí  reduce using rule 212 (VarDeclList)


State 436

   21 CommonDecl: VAR '(' VarDeclList oSemi ')' .

    $výchozí  reduce using rule 21 (CommonDecl)


State 437

   32 VarDecl: DeclNameList Type '=' ExprList .  [CASE, DEFAULT, ')', ';', '}']
  268 ExprList: ExprList . ',' Expr

    ','  posunout a přejít do stavu 189

    $výchozí  reduce using rule 32 (VarDecl)


State 438

  193 VariantType: VARIANT LBrace StructDeclList oSemi '}' .

    $výchozí  reduce using rule 193 (VariantType)


State 439

  158 Type: '(' Type . ')'
  167 NonRecvChanType: '(' Type . ')'

    ')'  posunout a přejít do stavu 479


State 440

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  129                   | '(' ExprOrType ')' '{' StartCompLit . BracedKeyvalList '}'
  130                   | . FuncLit
  132 Keyval: . Expr ':' CompLitExpr
  133 BareCompLitExpr: . Expr
  134                | . '{' StartCompLit BracedKeyvalList '}'
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  271 KeyvalList: . Keyval
  272           | . BareCompLitExpr
  273           | . KeyvalList ',' Keyval
  274           | . KeyvalList ',' BareCompLitExpr
  275 BracedKeyvalList: . %empty  ['}']
  276                 | . KeyvalList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 373
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 275 (BracedKeyvalList)

    Expr                přejít do stavu 374
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    Keyval              přejít do stavu 375
    BareCompLitExpr     přejít do stavu 376
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    KeyvalList          přejít do stavu 377
    BracedKeyvalList    přejít do stavu 480


State 441

   36 ConstDecl1: ConstDecl .

    $výchozí  reduce using rule 36 (ConstDecl1)


State 442

  213 ConstDeclList: ConstDecl1 .

    $výchozí  reduce using rule 213 (ConstDeclList)


State 443

   25 CommonDecl: Const '(' ConstDecl ';' ConstDeclList . oSemi ')'
  214 ConstDeclList: ConstDeclList . ';' ConstDecl1
  277 oSemi: . %empty  [')']
  278      | . ';'

    ';'  posunout a přejít do stavu 481

    $výchozí  reduce using rule 277 (oSemi)

    oSemi  přejít do stavu 482


State 444

   34 ConstDecl: DeclNameList . Type '=' ExprList
   35          | DeclNameList . '=' ExprList
   37 ConstDecl1: DeclNameList . Type
   38           | DeclNameList .  [')', ';']
  148 Symbol: . IDENT
  149 Name: . Symbol
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  266 DeclNameList: DeclNameList . ',' DeclName

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '='        posunout a přejít do stavu 263
    '['        posunout a přejít do stavu 49
    ','        posunout a přejít do stavu 252

    $výchozí  reduce using rule 38 (ConstDecl1)

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    Type           přejít do stavu 483
    TypeName       přejít do stavu 195
    OtherType      přejít do stavu 196
    PtrType        přejít do stavu 197
    RecvChanType   přejít do stavu 198
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 199


State 445

   24 CommonDecl: Const '(' ConstDecl oSemi ')' .

    $výchozí  reduce using rule 24 (CommonDecl)


State 446

   34 ConstDecl: DeclNameList Type '=' ExprList .  [CASE, DEFAULT, ')', ';', '}']
  268 ExprList: ExprList . ',' Expr

    ','  posunout a přejít do stavu 189

    $výchozí  reduce using rule 34 (ConstDecl)


State 447

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  132 Keyval: . Expr ':' CompLitExpr
  133 BareCompLitExpr: . Expr
  134                | . '{' StartCompLit BracedKeyvalList '}'
  134                | '{' StartCompLit . BracedKeyvalList '}'
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  271 KeyvalList: . Keyval
  272           | . BareCompLitExpr
  273           | . KeyvalList ',' Keyval
  274           | . KeyvalList ',' BareCompLitExpr
  275 BracedKeyvalList: . %empty  ['}']
  276                 | . KeyvalList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 373
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 275 (BracedKeyvalList)

    Expr                přejít do stavu 374
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    Keyval              přejít do stavu 375
    BareCompLitExpr     přejít do stavu 376
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    KeyvalList          přejít do stavu 377
    BracedKeyvalList    přejít do stavu 484


State 448

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  132 Keyval: Expr ':' . CompLitExpr
  135 CompLitExpr: . Expr
  136            | . '{' StartCompLit BracedKeyvalList '}'
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 485
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 486
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    CompLitExpr         přejít do stavu 487
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 449

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  132 Keyval: . Expr ':' CompLitExpr
  133 BareCompLitExpr: . Expr
  134                | . '{' StartCompLit BracedKeyvalList '}'
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  273 KeyvalList: KeyvalList ',' . Keyval
  274           | KeyvalList ',' . BareCompLitExpr
  280 oComma: ',' .  ['}']

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 373
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 280 (oComma)

    Expr                přejít do stavu 374
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    Keyval              přejít do stavu 488
    BareCompLitExpr     přejít do stavu 489
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 450

  276 BracedKeyvalList: KeyvalList oComma .

    $výchozí  reduce using rule 276 (BracedKeyvalList)


State 451

  128 PrimaryExprNoParen: PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}' .

    $výchozí  reduce using rule 128 (PrimaryExprNoParen)


State 452

  280 oComma: ',' .

    $výchozí  reduce using rule 280 (oComma)


State 453

  115 PseudoCall: PrimaryExpr '(' ExprOrTypeList DDD oComma . ')'

    ')'  posunout a přejít do stavu 490


State 454

  270 ExprOrTypeList: ExprOrTypeList ',' ExprOrType .

    $výchozí  reduce using rule 270 (ExprOrTypeList)


State 455

  114 PseudoCall: PrimaryExpr '(' ExprOrTypeList oComma ')' .

    $výchozí  reduce using rule 114 (PseudoCall)


State 456

  120 PrimaryExprNoParen: PrimaryExpr '.' '(' TYPE ')' .

    $výchozí  reduce using rule 120 (PrimaryExprNoParen)


State 457

  119 PrimaryExprNoParen: PrimaryExpr '.' '(' ExprOrType ')' .

    $výchozí  reduce using rule 119 (PrimaryExprNoParen)


State 458

  122 PrimaryExprNoParen: PrimaryExpr '[' oExpr ':' oExpr . ']'
  123                   | PrimaryExpr '[' oExpr ':' oExpr . ':' oExpr ']'

    ':'  posunout a přejít do stavu 491
    ']'  posunout a přejít do stavu 492


State 459

   52 CompoundStmt: '{' $@2 StmtList . '}'
  262 StmtList: StmtList . ';' Statement

    ';'  posunout a přejít do stavu 390
    '}'  posunout a přejít do stavu 493


State 460

  125 PrimaryExprNoParen: ConvType '(' ExprList oComma ')' .

    $výchozí  reduce using rule 125 (PrimaryExprNoParen)


State 461

  127 PrimaryExprNoParen: CompLitType LBrace StartCompLit BracedKeyvalList '}' .

    $výchozí  reduce using rule 127 (PrimaryExprNoParen)


State 462

  262 StmtList: StmtList ';' Statement .

    $výchozí  reduce using rule 262 (StmtList)


State 463

   58 LoopBody: BODY $@4 StmtList . '}'
  262 StmtList: StmtList . ';' Statement

    ';'  posunout a přejít do stavu 390
    '}'  posunout a přejít do stavu 494


State 464

   60 RangeStmt: ExprList COLAS RANGE Expr .  [BODY]
   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 60 (RangeStmt)


State 465

   59 RangeStmt: ExprList '=' RANGE Expr .  [BODY]
   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 59 (RangeStmt)


State 466

   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   61 ForHeader: oSimpleStmt ';' oSimpleStmt ';' . oSimpleStmt
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr
  285 oSimpleStmt: . %empty  [BODY]
  286            | . SimpleStmt

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 285 (oSimpleStmt)

    SimpleStmt          přejít do stavu 204
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 81
    oSimpleStmt         přejít do stavu 495


State 467

  205 FuncResult: '(' oArgTypeListOComma . ')'

    ')'  posunout a přejít do stavu 496


State 468

  141 NameOrType: . Type
  148 Symbol: . IDENT
  149 Name: . Symbol
  151 Ddd: . DDD
  152    | . DDD Type
  153 Type: . RecvChanType
  154     | . FuncType
  155     | . OtherType
  156     | . PtrType
  157     | . TypeName
  158     | . '(' Type ')'
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  199 FuncDecl1: '(' oArgTypeListOComma ')' Symbol '(' . oArgTypeListOComma ')' FuncResult
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  234 ArgType: . NameOrType
  235        | . Symbol NameOrType
  236        | . Symbol Ddd
  237        | . Ddd
  238 ArgTypeList: . ArgType
  239            | . ArgTypeList ',' ArgType
  240 oArgTypeListOComma: . %empty  [')']
  241                   | . ArgTypeList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    DDD        posunout a přejít do stavu 210
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 193
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 240 (oArgTypeListOComma)

    NameOrType          přejít do stavu 211
    Symbol              přejít do stavu 212
    Name                přejít do stavu 95
    Ddd                 přejít do stavu 213
    Type                přejít do stavu 214
    TypeName            přejít do stavu 195
    OtherType           přejít do stavu 196
    PtrType             přejít do stavu 197
    RecvChanType        přejít do stavu 198
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 199
    ArgType             přejít do stavu 215
    ArgTypeList         přejít do stavu 216
    oArgTypeListOComma  přejít do stavu 497


State 469

  198 FuncDecl1: Symbol '(' oArgTypeListOComma ')' FuncResult .

    $výchozí  reduce using rule 198 (FuncDecl1)


State 470

   71 IfStmt: IF $@6 IfHeader LoopBody $@7 ElseIfList . Else
   73 ElseIf: . ELSE IF IfHeader $@8 LoopBody
   75 ElseIfList: ElseIfList . ElseIf
   76 Else: . %empty  [CASE, DEFAULT, ';', '}']
   78     | . ELSE $@9 CompoundStmt

    ELSE  posunout a přejít do stavu 498

    $výchozí  reduce using rule 76 (Else)

    ElseIf  přejít do stavu 499
    Else    přejít do stavu 500


State 471

  148 Symbol: . IDENT
  149 Name: . Symbol
  171 FuncRetType: . RecvChanType
  172            | . FuncType
  173            | . OtherType
  174            | . PtrType
  175            | . TypeName
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  203 FuncResult: . %empty  [';', '}']
  204           | . FuncRetType
  205           | . '(' oArgTypeListOComma ')'
  233 InterfaceMethodDecl: '(' oArgTypeListOComma ')' . FuncResult

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 399
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 203 (FuncResult)

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    FuncRetType    přejít do stavu 401
    TypeName       přejít do stavu 402
    OtherType      přejít do stavu 403
    PtrType        přejít do stavu 404
    RecvChanType   přejít do stavu 405
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 406
    FuncResult     přejít do stavu 501


State 472

   47 Case: CASE ExprOrTypeList . ':'
   48     | CASE ExprOrTypeList . '=' Expr ':'
   49     | CASE ExprOrTypeList . COLAS Expr ':'
  270 ExprOrTypeList: ExprOrTypeList . ',' ExprOrType

    COLAS  posunout a přejít do stavu 502
    ':'    posunout a přejít do stavu 503
    '='    posunout a přejít do stavu 504
    ','    posunout a přejít do stavu 505


State 473

   50 Case: DEFAULT ':' .

    $výchozí  reduce using rule 50 (Case)


State 474

   20 CommonDecl: . VAR VarDecl
   21           | . VAR '(' VarDeclList oSemi ')'
   22           | . VAR '(' ')'
   23           | . Const ConstDecl
   24           | . Const '(' ConstDecl oSemi ')'
   25           | . Const '(' ConstDecl ';' ConstDeclList oSemi ')'
   26           | . Const '(' ')'
   27           | . TYPE TypeDecl
   28           | . TYPE '(' TypeDeclList oSemi ')'
   29           | . TYPE '(' ')'
   30 Const: . CONST
   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   52 CompoundStmt: . '{' $@2 StmtList '}'
   54 CaseBlock: Case $@3 . StmtList
   66 ForStmt: . FOR $@5 ForBody
   71 IfStmt: . IF $@6 IfHeader LoopBody $@7 ElseIfList Else
   80 SwitchStmt: . SWITCH $@10 IfHeader BODY CaseBlockList '}'
   82 SelectStmt: . SELECT $@11 BODY CaseBlockList '}'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  144 NewName: . Symbol
  148 Symbol: . IDENT
  149 Name: . Symbol
  150 LabelName: . NewName
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  242 Statement: . %empty  [CASE, DEFAULT, ';', '}']
  243          | . CompoundStmt
  244          | . CommonDecl
  245          | . NonDclStmt
  246          | . error
  247 NonDclStmt: . SimpleStmt
  248           | . ForStmt
  249           | . SwitchStmt
  250           | . SelectStmt
  251           | . IfStmt
  252           | . LabelName ':' Statement
  253           | . FALL
  254           | . BREAK oNewName
  255           | . CONTINUE oNewName
  256           | . GO PseudoCall
  257           | . DEFER PseudoCall
  258           | . GOTO NewName
  259           | . GOTO
  260           | . RETURN oExprList
  261 StmtList: . Statement
  262         | . StmtList ';' Statement
  267 ExprList: . Expr
  268         | . ExprList ',' Expr

    error      posunout a přejít do stavu 294
    BREAK      posunout a přejít do stavu 20
    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    CONST      posunout a přejít do stavu 23
    CONTINUE   posunout a přejít do stavu 24
    DEFER      posunout a přejít do stavu 25
    FALL       posunout a přejít do stavu 26
    FOR        posunout a přejít do stavu 27
    FUNC       posunout a přejít do stavu 91
    GO         posunout a přejít do stavu 29
    GOTO       posunout a přejít do stavu 30
    IDENT      posunout a přejít do stavu 4
    IF         posunout a přejít do stavu 31
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    RETURN     posunout a přejít do stavu 35
    SELECT     posunout a přejít do stavu 36
    STRUCT     posunout a přejít do stavu 37
    SWITCH     posunout a přejít do stavu 38
    TYPE       posunout a přejít do stavu 39
    UNION      posunout a přejít do stavu 40
    VAR        posunout a přejít do stavu 41
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 295
    '~'        posunout a přejít do stavu 51

    CASE     reduce using rule 242 (Statement)
    DEFAULT  reduce using rule 242 (Statement)
    ';'      reduce using rule 242 (Statement)
    '}'      reduce using rule 242 (Statement)

    CommonDecl          přejít do stavu 296
    Const               přejít do stavu 54
    SimpleStmt          přejít do stavu 55
    CompoundStmt        přejít do stavu 297
    ForStmt             přejít do stavu 56
    IfStmt              přejít do stavu 57
    SwitchStmt          přejít do stavu 58
    SelectStmt          přejít do stavu 59
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    NewName             přejít do stavu 65
    Symbol              přejít do stavu 66
    Name                přejít do stavu 67
    LabelName           přejít do stavu 68
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    Statement           přejít do stavu 303
    NonDclStmt          přejít do stavu 299
    StmtList            přejít do stavu 506
    ExprList            přejít do stavu 81


State 475

  225 StructDecl: '(' '*' Embedded ')' . oLiteral
  287 oLiteral: . %empty  [';', '}']
  288         | . LITERAL

    LITERAL  posunout a přejít do stavu 345

    $výchozí  reduce using rule 287 (oLiteral)

    oLiteral  přejít do stavu 507


State 476

  223 StructDecl: '(' Embedded ')' oLiteral .

    $výchozí  reduce using rule 223 (StructDecl)


State 477

  226 StructDecl: '*' '(' Embedded ')' . oLiteral
  287 oLiteral: . %empty  [';', '}']
  288         | . LITERAL

    LITERAL  posunout a přejít do stavu 345

    $výchozí  reduce using rule 287 (oLiteral)

    oLiteral  přejít do stavu 508


State 478

   80 SwitchStmt: SWITCH $@10 IfHeader BODY CaseBlockList '}' .

    $výchozí  reduce using rule 80 (SwitchStmt)


State 479

  158 Type: '(' Type ')' .  [COLAS, DDD, ')', ':', '=', ',']
  167 NonRecvChanType: '(' Type ')' .  [BODY, '(', '{']

    BODY        reduce using rule 167 (NonRecvChanType)
    '('         reduce using rule 167 (NonRecvChanType)
    '{'         reduce using rule 167 (NonRecvChanType)
    $výchozí  reduce using rule 158 (Type)


State 480

  129 PrimaryExprNoParen: '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList . '}'

    '}'  posunout a přejít do stavu 509


State 481

   34 ConstDecl: . DeclNameList Type '=' ExprList
   35          | . DeclNameList '=' ExprList
   36 ConstDecl1: . ConstDecl
   37           | . DeclNameList Type
   38           | . DeclNameList
  145 DeclName: . Symbol
  148 Symbol: . IDENT
  214 ConstDeclList: ConstDeclList ';' . ConstDecl1
  265 DeclNameList: . DeclName
  266             | . DeclNameList ',' DeclName
  278 oSemi: ';' .  [')']

    IDENT  posunout a přejít do stavu 4

    $výchozí  reduce using rule 278 (oSemi)

    ConstDecl     přejít do stavu 441
    ConstDecl1    přejít do stavu 510
    DeclName      přejít do stavu 129
    Symbol        přejít do stavu 130
    DeclNameList  přejít do stavu 444


State 482

   25 CommonDecl: Const '(' ConstDecl ';' ConstDeclList oSemi . ')'

    ')'  posunout a přejít do stavu 511


State 483

   34 ConstDecl: DeclNameList Type . '=' ExprList
   37 ConstDecl1: DeclNameList Type .  [')', ';']

    '='  posunout a přejít do stavu 372

    $výchozí  reduce using rule 37 (ConstDecl1)


State 484

  134 BareCompLitExpr: '{' StartCompLit BracedKeyvalList . '}'

    '}'  posunout a přejít do stavu 512


State 485

  131 StartCompLit: . %empty
  136 CompLitExpr: '{' . StartCompLit BracedKeyvalList '}'

    $výchozí  reduce using rule 131 (StartCompLit)

    StartCompLit  přejít do stavu 513


State 486

   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr
  135 CompLitExpr: Expr .  ['}', ',']

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177

    $výchozí  reduce using rule 135 (CompLitExpr)


State 487

  132 Keyval: Expr ':' CompLitExpr .

    $výchozí  reduce using rule 132 (Keyval)


State 488

  273 KeyvalList: KeyvalList ',' Keyval .

    $výchozí  reduce using rule 273 (KeyvalList)


State 489

  274 KeyvalList: KeyvalList ',' BareCompLitExpr .

    $výchozí  reduce using rule 274 (KeyvalList)


State 490

  115 PseudoCall: PrimaryExpr '(' ExprOrTypeList DDD oComma ')' .

    $výchozí  reduce using rule 115 (PseudoCall)


State 491

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  123                   | PrimaryExpr '[' oExpr ':' oExpr ':' . oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  281 oExpr: . %empty  [']']
  282      | . Expr

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 281 (oExpr)

    Expr                přejít do stavu 147
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    oExpr               přejít do stavu 514


State 492

  122 PrimaryExprNoParen: PrimaryExpr '[' oExpr ':' oExpr ']' .

    $výchozí  reduce using rule 122 (PrimaryExprNoParen)


State 493

   52 CompoundStmt: '{' $@2 StmtList '}' .

    $výchozí  reduce using rule 52 (CompoundStmt)


State 494

   58 LoopBody: BODY $@4 StmtList '}' .

    $výchozí  reduce using rule 58 (LoopBody)


State 495

   61 ForHeader: oSimpleStmt ';' oSimpleStmt ';' oSimpleStmt .

    $výchozí  reduce using rule 61 (ForHeader)


State 496

  205 FuncResult: '(' oArgTypeListOComma ')' .

    $výchozí  reduce using rule 205 (FuncResult)


State 497

  199 FuncDecl1: '(' oArgTypeListOComma ')' Symbol '(' oArgTypeListOComma . ')' FuncResult

    ')'  posunout a přejít do stavu 515


State 498

   73 ElseIf: ELSE . IF IfHeader $@8 LoopBody
   77 $@9: . %empty  ['{']
   78 Else: ELSE . $@9 CompoundStmt

    IF  posunout a přejít do stavu 516

    $výchozí  reduce using rule 77 ($@9)

    $@9  přejít do stavu 517


State 499

   75 ElseIfList: ElseIfList ElseIf .

    $výchozí  reduce using rule 75 (ElseIfList)


State 500

   71 IfStmt: IF $@6 IfHeader LoopBody $@7 ElseIfList Else .

    $výchozí  reduce using rule 71 (IfStmt)


State 501

  233 InterfaceMethodDecl: '(' oArgTypeListOComma ')' FuncResult .

    $výchozí  reduce using rule 233 (InterfaceMethodDecl)


State 502

   49 Case: CASE ExprOrTypeList COLAS . Expr ':'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 518
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 503

   47 Case: CASE ExprOrTypeList ':' .

    $výchozí  reduce using rule 47 (Case)


State 504

   48 Case: CASE ExprOrTypeList '=' . Expr ':'
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 519
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 505

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  139 ExprOrType: . Expr
  140           | . NonExprType
  148 Symbol: . IDENT
  149 Name: . Symbol
  159 NonExprType: . RecvChanType
  160            | . FuncType
  161            | . OtherType
  162            | . '*' NonExprType
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  270 ExprOrTypeList: ExprOrTypeList ',' . ExprOrType

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 135
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 136
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    Expr                přejít do stavu 137
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    ExprOrType          přejít do stavu 454
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    NonExprType         přejít do stavu 139
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 140
    RecvChanType        přejít do stavu 141
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 142
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79


State 506

   54 CaseBlock: Case $@3 StmtList .  [CASE, DEFAULT, '}']
  262 StmtList: StmtList . ';' Statement

    ';'  posunout a přejít do stavu 390

    $výchozí  reduce using rule 54 (CaseBlock)


State 507

  225 StructDecl: '(' '*' Embedded ')' oLiteral .

    $výchozí  reduce using rule 225 (StructDecl)


State 508

  226 StructDecl: '*' '(' Embedded ')' oLiteral .

    $výchozí  reduce using rule 226 (StructDecl)


State 509

  129 PrimaryExprNoParen: '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}' .

    $výchozí  reduce using rule 129 (PrimaryExprNoParen)


State 510

  214 ConstDeclList: ConstDeclList ';' ConstDecl1 .

    $výchozí  reduce using rule 214 (ConstDeclList)


State 511

   25 CommonDecl: Const '(' ConstDecl ';' ConstDeclList oSemi ')' .

    $výchozí  reduce using rule 25 (CommonDecl)


State 512

  134 BareCompLitExpr: '{' StartCompLit BracedKeyvalList '}' .

    $výchozí  reduce using rule 134 (BareCompLitExpr)


State 513

   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  132 Keyval: . Expr ':' CompLitExpr
  133 BareCompLitExpr: . Expr
  134                | . '{' StartCompLit BracedKeyvalList '}'
  136 CompLitExpr: '{' StartCompLit . BracedKeyvalList '}'
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  271 KeyvalList: . Keyval
  272           | . BareCompLitExpr
  273           | . KeyvalList ',' Keyval
  274           | . KeyvalList ',' BareCompLitExpr
  275 BracedKeyvalList: . %empty  ['}']
  276                 | . KeyvalList oComma

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '{'        posunout a přejít do stavu 373
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 275 (BracedKeyvalList)

    Expr                přejít do stavu 374
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    Keyval              přejít do stavu 375
    BareCompLitExpr     přejít do stavu 376
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    KeyvalList          přejít do stavu 377
    BracedKeyvalList    přejít do stavu 520


State 514

  123 PrimaryExprNoParen: PrimaryExpr '[' oExpr ':' oExpr ':' oExpr . ']'

    ']'  posunout a přejít do stavu 521


State 515

  148 Symbol: . IDENT
  149 Name: . Symbol
  171 FuncRetType: . RecvChanType
  172            | . FuncType
  173            | . OtherType
  174            | . PtrType
  175            | . TypeName
  176 TypeName: . Name
  177         | . Name '.' Symbol
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  187 PtrType: . '*' Type
  188 RecvChanType: . COMM CHAN Type
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  199 FuncDecl1: '(' oArgTypeListOComma ')' Symbol '(' oArgTypeListOComma ')' . FuncResult
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  203 FuncResult: . %empty  [';', '{']
  204           | . FuncRetType
  205           | . '(' oArgTypeListOComma ')'

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 192
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '('        posunout a přejít do stavu 399
    '*'        posunout a přejít do stavu 93
    '['        posunout a přejít do stavu 49

    $výchozí  reduce using rule 203 (FuncResult)

    Symbol         přejít do stavu 94
    Name           přejít do stavu 95
    FuncRetType    přejít do stavu 401
    TypeName       přejít do stavu 402
    OtherType      přejít do stavu 403
    PtrType        přejít do stavu 404
    RecvChanType   přejít do stavu 405
    StructType     přejít do stavu 72
    UnionType      přejít do stavu 73
    VariantType    přejít do stavu 74
    InterfaceType  přejít do stavu 75
    FuncType       přejít do stavu 406
    FuncResult     přejít do stavu 522


State 516

   41 SimpleStmt: . Expr
   42           | . Expr ASOP Expr
   43           | . ExprList '=' ExprList
   44           | . ExprList COLAS ExprList
   45           | . Expr INC
   46           | . Expr DEC
   67 IfHeader: . oSimpleStmt
   68         | . oSimpleStmt ';' oSimpleStmt
   73 ElseIf: ELSE IF . IfHeader $@8 LoopBody
   83 Expr: . UnaryExpr
   84     | . Expr OROR Expr
   85     | . Expr ANDAND Expr
   86     | . Expr EQ Expr
   87     | . Expr NE Expr
   88     | . Expr '<' Expr
   89     | . Expr LE Expr
   90     | . Expr GE Expr
   91     | . Expr '>' Expr
   92     | . Expr '+' Expr
   93     | . Expr '-' Expr
   94     | . Expr '|' Expr
   95     | . Expr '^' Expr
   96     | . Expr '*' Expr
   97     | . Expr '/' Expr
   98     | . Expr '%' Expr
   99     | . Expr '&' Expr
  100     | . Expr ANDNOT Expr
  101     | . Expr LSH Expr
  102     | . Expr RSH Expr
  103     | . Expr COMM Expr
  104 UnaryExpr: . PrimaryExpr
  105          | . '*' UnaryExpr
  106          | . '&' UnaryExpr
  107          | . '+' UnaryExpr
  108          | . '-' UnaryExpr
  109          | . '!' UnaryExpr
  110          | . '~' UnaryExpr
  111          | . '^' UnaryExpr
  112          | . COMM UnaryExpr
  113 PseudoCall: . PrimaryExpr '(' ')'
  114           | . PrimaryExpr '(' ExprOrTypeList oComma ')'
  115           | . PrimaryExpr '(' ExprOrTypeList DDD oComma ')'
  116 PrimaryExprNoParen: . LITERAL
  117                   | . Name
  118                   | . PrimaryExpr '.' Symbol
  119                   | . PrimaryExpr '.' '(' ExprOrType ')'
  120                   | . PrimaryExpr '.' '(' TYPE ')'
  121                   | . PrimaryExpr '[' Expr ']'
  122                   | . PrimaryExpr '[' oExpr ':' oExpr ']'
  123                   | . PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']'
  124                   | . PseudoCall
  125                   | . ConvType '(' ExprList oComma ')'
  126                   | . ConvType '(' ')'
  127                   | . CompLitType LBrace StartCompLit BracedKeyvalList '}'
  128                   | . PrimaryExprNoParen '{' StartCompLit BracedKeyvalList '}'
  129                   | . '(' ExprOrType ')' '{' StartCompLit BracedKeyvalList '}'
  130                   | . FuncLit
  137 PrimaryExpr: . PrimaryExprNoParen
  138            | . '(' ExprOrType ')'
  148 Symbol: . IDENT
  149 Name: . Symbol
  168 ConvType: . FuncType
  169         | . OtherType
  170 CompLitType: . OtherType
  178 OtherType: . '[' oExpr ']' Type
  179          | . '[' DDD ']' Type
  180          | . CHAN NonRecvChanType
  181          | . CHAN COMM Type
  182          | . MAP '[' Type ']' Type
  183          | . StructType
  184          | . UnionType
  185          | . VariantType
  186          | . InterfaceType
  189 StructType: . STRUCT LBrace StructDeclList oSemi '}'
  190           | . STRUCT LBrace '}'
  191 UnionType: . UNION LBrace StructDeclList oSemi '}'
  192          | . UNION LBrace '}'
  193 VariantType: . VARIANT LBrace StructDeclList oSemi '}'
  194            | . VARIANT LBrace '}'
  195 InterfaceType: . INTERFACE LBrace InterfaceDeclList oSemi '}'
  196              | . INTERFACE LBrace '}'
  200 FuncType: . FUNC '(' oArgTypeListOComma ')' FuncResult
  206 FuncLitDecl: . FuncType
  207 FuncLit: . FuncLitDecl LBrace StmtList '}'
  208        | . FuncLitDecl error
  267 ExprList: . Expr
  268         | . ExprList ',' Expr
  285 oSimpleStmt: . %empty  [BODY, ';']
  286            | . SimpleStmt

    CHAN       posunout a přejít do stavu 21
    COMM       posunout a přejít do stavu 22
    FUNC       posunout a přejít do stavu 91
    IDENT      posunout a přejít do stavu 4
    INTERFACE  posunout a přejít do stavu 32
    LITERAL    posunout a přejít do stavu 33
    MAP        posunout a přejít do stavu 34
    STRUCT     posunout a přejít do stavu 37
    UNION      posunout a přejít do stavu 40
    VARIANT    posunout a přejít do stavu 42
    '!'        posunout a přejít do stavu 43
    '&'        posunout a přejít do stavu 44
    '('        posunout a přejít do stavu 45
    '*'        posunout a přejít do stavu 46
    '+'        posunout a přejít do stavu 47
    '-'        posunout a přejít do stavu 48
    '['        posunout a přejít do stavu 49
    '^'        posunout a přejít do stavu 50
    '~'        posunout a přejít do stavu 51

    $výchozí  reduce using rule 285 (oSimpleStmt)

    SimpleStmt          přejít do stavu 204
    IfHeader            přejít do stavu 523
    Expr                přejít do stavu 60
    UnaryExpr           přejít do stavu 61
    PseudoCall          přejít do stavu 62
    PrimaryExprNoParen  přejít do stavu 63
    PrimaryExpr         přejít do stavu 64
    Symbol              přejít do stavu 94
    Name                přejít do stavu 67
    ConvType            přejít do stavu 69
    CompLitType         přejít do stavu 70
    OtherType           přejít do stavu 71
    StructType          přejít do stavu 72
    UnionType           přejít do stavu 73
    VariantType         přejít do stavu 74
    InterfaceType       přejít do stavu 75
    FuncType            přejít do stavu 77
    FuncLitDecl         přejít do stavu 78
    FuncLit             přejít do stavu 79
    ExprList            přejít do stavu 81
    oSimpleStmt         přejít do stavu 222


State 517

   52 CompoundStmt: . '{' $@2 StmtList '}'
   78 Else: ELSE $@9 . CompoundStmt

    '{'  posunout a přejít do stavu 295

    CompoundStmt  přejít do stavu 524


State 518

   49 Case: CASE ExprOrTypeList COLAS Expr . ':'
   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    ':'     posunout a přejít do stavu 525
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177


State 519

   48 Case: CASE ExprOrTypeList '=' Expr . ':'
   84 Expr: Expr . OROR Expr
   85     | Expr . ANDAND Expr
   86     | Expr . EQ Expr
   87     | Expr . NE Expr
   88     | Expr . '<' Expr
   89     | Expr . LE Expr
   90     | Expr . GE Expr
   91     | Expr . '>' Expr
   92     | Expr . '+' Expr
   93     | Expr . '-' Expr
   94     | Expr . '|' Expr
   95     | Expr . '^' Expr
   96     | Expr . '*' Expr
   97     | Expr . '/' Expr
   98     | Expr . '%' Expr
   99     | Expr . '&' Expr
  100     | Expr . ANDNOT Expr
  101     | Expr . LSH Expr
  102     | Expr . RSH Expr
  103     | Expr . COMM Expr

    ANDAND  posunout a přejít do stavu 155
    ANDNOT  posunout a přejít do stavu 156
    COMM    posunout a přejít do stavu 158
    EQ      posunout a přejít do stavu 160
    GE      posunout a přejít do stavu 161
    LE      posunout a přejít do stavu 163
    LSH     posunout a přejít do stavu 164
    NE      posunout a přejít do stavu 165
    OROR    posunout a přejít do stavu 166
    RSH     posunout a přejít do stavu 167
    '%'     posunout a přejít do stavu 168
    '&'     posunout a přejít do stavu 169
    '*'     posunout a přejít do stavu 170
    '+'     posunout a přejít do stavu 171
    '-'     posunout a přejít do stavu 172
    '/'     posunout a přejít do stavu 173
    ':'     posunout a přejít do stavu 526
    '<'     posunout a přejít do stavu 174
    '>'     posunout a přejít do stavu 175
    '^'     posunout a přejít do stavu 176
    '|'     posunout a přejít do stavu 177


State 520

  136 CompLitExpr: '{' StartCompLit BracedKeyvalList . '}'

    '}'  posunout a přejít do stavu 527


State 521

  123 PrimaryExprNoParen: PrimaryExpr '[' oExpr ':' oExpr ':' oExpr ']' .

    $výchozí  reduce using rule 123 (PrimaryExprNoParen)


State 522

  199 FuncDecl1: '(' oArgTypeListOComma ')' Symbol '(' oArgTypeListOComma ')' FuncResult .

    $výchozí  reduce using rule 199 (FuncDecl1)


State 523

   72 $@8: . %empty
   73 ElseIf: ELSE IF IfHeader . $@8 LoopBody

    $výchozí  reduce using rule 72 ($@8)

    $@8  přejít do stavu 528


State 524

   78 Else: ELSE $@9 CompoundStmt .

    $výchozí  reduce using rule 78 (Else)


State 525

   49 Case: CASE ExprOrTypeList COLAS Expr ':' .

    $výchozí  reduce using rule 49 (Case)


State 526

   48 Case: CASE ExprOrTypeList '=' Expr ':' .

    $výchozí  reduce using rule 48 (Case)


State 527

  136 CompLitExpr: '{' StartCompLit BracedKeyvalList '}' .

    $výchozí  reduce using rule 136 (CompLitExpr)


State 528

   58 LoopBody: . BODY $@4 StmtList '}'
   73 ElseIf: ELSE IF IfHeader $@8 . LoopBody

    BODY  posunout a přejít do stavu 315

    LoopBody  přejít do stavu 529


State 529

   73 ElseIf: ELSE IF IfHeader $@8 LoopBody .

    $výchozí  reduce using rule 73 (ElseIf)
