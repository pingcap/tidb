Rules useless in parser due to conflicts

    2 AnonymousField1: %empty

  100 ForClause1: %empty

  104 ForClause3: %empty

  126 IfStmt1: %empty

  224 RecvStmt1: %empty

  322 TypeSwitchStmt1: %empty
  323                | SimpleStmt ';'


Stav 41 conflicts: 1 shift/reduce
Stav 59 conflicts: 1 shift/reduce
Stav 85 conflicts: 1 shift/reduce
Stav 88 conflicts: 1 shift/reduce
Stav 111 conflicts: 1 shift/reduce, 5 reduce/reduce
Stav 135 conflicts: 17 shift/reduce, 1 reduce/reduce
Stav 136 conflicts: 5 reduce/reduce
Stav 141 conflicts: 1 reduce/reduce
Stav 155 conflicts: 10 shift/reduce
Stav 179 conflicts: 1 shift/reduce
Stav 192 conflicts: 16 shift/reduce
Stav 212 conflicts: 1 shift/reduce
Stav 223 conflicts: 1 shift/reduce
Stav 243 conflicts: 1 shift/reduce
Stav 245 conflicts: 1 shift/reduce
Stav 250 conflicts: 1 shift/reduce, 3 reduce/reduce
Stav 259 conflicts: 1 shift/reduce
Stav 264 conflicts: 1 shift/reduce
Stav 272 conflicts: 1 shift/reduce
Stav 275 conflicts: 1 shift/reduce, 4 reduce/reduce
Stav 280 conflicts: 1 reduce/reduce
Stav 283 conflicts: 16 shift/reduce
Stav 286 conflicts: 16 shift/reduce, 15 reduce/reduce
Stav 335 conflicts: 1 shift/reduce
Stav 344 conflicts: 16 shift/reduce
Stav 359 conflicts: 1 shift/reduce, 4 reduce/reduce
Stav 381 conflicts: 1 shift/reduce
Stav 392 conflicts: 16 shift/reduce
Stav 393 conflicts: 1 shift/reduce
Stav 408 conflicts: 15 reduce/reduce
Stav 423 conflicts: 16 shift/reduce
Stav 432 conflicts: 1 reduce/reduce


Gramatika

    0 $accept: Start $end

    1 AnonymousField: AnonymousField1 TypeName

    2 AnonymousField1: %empty
    3                | '*'

    4 ArgumentList: ExpressionList ArgumentList1

    5 ArgumentList1: %empty
    6              | TOK4

    7 ArrayLength: Expression

    8 ArrayType: '[' ArrayLength ']' ElementType

    9 Assignment: ExpressionList ASSIGN_OP ExpressionList

   10 BaseType: Type

   11 BaseTypeName: IDENTIFIER

   12 BasicLit: INT_LIT
   13         | FLOAT_LIT
   14         | IMAGINARY_LIT
   15         | RUNE_LIT
   16         | STRING_LIT

   17 Block: '{' StatementList '}'

   18 BreakStmt: BREAK BreakStmt1

   19 BreakStmt1: %empty
   20           | Label

   21 BuiltinArgs: Type BuiltinArgs1
   22            | ArgumentList

   23 BuiltinArgs1: %empty
   24             | ',' ArgumentList

   25 BuiltinCall: IDENTIFIER '(' BuiltinCall1 ')'

   26 BuiltinCall1: %empty
   27             | BuiltinArgs BuiltinCall11

   28 BuiltinCall11: %empty
   29              | ','

   30 Call: '(' Call1 ')'

   31 Call1: %empty
   32      | ArgumentList Call11

   33 Call11: %empty
   34       | ','

   35 Channel: Expression

   36 ChannelType: ChannelType1 ElementType

   37 ChannelType1: CHAN
   38             | CHAN TOK5
   39             | TOK5 CHAN

   40 CommCase: CASE CommCase1
   41         | DEFAULT

   42 CommCase1: SendStmt
   43          | RecvStmt

   44 CommClause: CommCase ':' StatementList

   45 CompositeLit: LiteralType LiteralValue

   46 Condition: Expression

   47 ConstDecl: CONST ConstDecl1

   48 ConstDecl1: ConstSpec
   49           | '(' ConstDecl11 ')'

   50 ConstDecl11: %empty
   51            | ConstDecl11 ConstSpec ';'

   52 ConstSpec: IdentifierList ConstSpec1

   53 ConstSpec1: %empty
   54           | ConstSpec11 '=' ExpressionList

   55 ConstSpec11: %empty
   56            | Type

   57 ContinueStmt: CONTINUE ContinueStmt1

   58 ContinueStmt1: %empty
   59              | Label

   60 Conversion: Type '(' Expression Conversion1 ')'

   61 Conversion1: %empty
   62            | ','

   63 Declaration: ConstDecl
   64            | TypeDecl
   65            | VarDecl

   66 DeferStmt: DEFER Expression

   67 Element: Element1 Value

   68 Element1: %empty
   69         | Key ':'

   70 ElementIndex: Expression

   71 ElementList: Element ElementList1

   72 ElementList1: %empty
   73             | ElementList1 ',' Element

   74 ElementType: Type

   75 EmptyStmt: %empty

   76 ExprCaseClause: ExprSwitchCase ':' StatementList

   77 ExprSwitchCase: CASE ExpressionList
   78               | DEFAULT

   79 ExprSwitchStmt: SWITCH ExprSwitchStmt1 ExprSwitchStmt2 '{' ExprSwitchStmt3 '}'

   80 ExprSwitchStmt1: %empty
   81                | SimpleStmt ';'

   82 ExprSwitchStmt2: %empty
   83                | Expression

   84 ExprSwitchStmt3: %empty
   85                | ExprSwitchStmt3 ExprCaseClause

   86 Expression: UnaryExpr
   87           | Expression BINARY_OP UnaryExpr

   88 ExpressionList: Expression ExpressionList1

   89 ExpressionList1: %empty
   90                | ExpressionList1 ',' Expression

   91 ExpressionStmt: Expression

   92 FallthroughStmt: FALLTHROUGH

   93 FieldDecl: FieldDecl1 FieldDecl2

   94 FieldDecl1: IdentifierList Type
   95           | AnonymousField

   96 FieldDecl2: %empty
   97           | Tag

   98 FieldName: IDENTIFIER

   99 ForClause: ForClause1 ';' ForClause2 ';' ForClause3

  100 ForClause1: %empty
  101           | InitStmt

  102 ForClause2: %empty
  103           | Condition

  104 ForClause3: %empty
  105           | PostStmt

  106 ForStmt: FOR ForStmt1 Block

  107 ForStmt1: %empty
  108         | ForStmt11

  109 ForStmt11: Condition
  110          | ForClause
  111          | RangeClause

  112 Function: Signature FunctionBody

  113 FunctionBody: Block

  114 FunctionDecl: FUNC FunctionName FunctionDecl1

  115 FunctionDecl1: Function
  116              | Signature

  117 FunctionLit: FUNC Function

  118 FunctionName: IDENTIFIER

  119 FunctionType: FUNC Signature

  120 GoStmt: GO Expression

  121 GotoStmt: GOTO Label

  122 IdentifierList: IDENTIFIER IdentifierList1

  123 IdentifierList1: %empty
  124                | IdentifierList1 ',' IDENTIFIER

  125 IfStmt: IF IfStmt1 Expression Block IfStmt2

  126 IfStmt1: %empty
  127        | SimpleStmt ';'

  128 IfStmt2: %empty
  129        | ELSE IfStmt21

  130 IfStmt21: IfStmt
  131         | Block

  132 ImportDecl: IMPORT ImportDecl1

  133 ImportDecl1: ImportSpec
  134            | '(' ImportDecl11 ')'

  135 ImportDecl11: %empty
  136             | ImportDecl11 ImportSpec ';'

  137 ImportPath: STRING_LIT

  138 ImportSpec: ImportSpec1 ImportPath

  139 ImportSpec1: %empty
  140            | ImportSpec11

  141 ImportSpec11: '.'
  142             | PackageName

  143 IncDecStmt: Expression IncDecStmt1

  144 IncDecStmt1: TOK2
  145            | TOK1

  146 Index: '[' Expression ']'

  147 InitStmt: SimpleStmt

  148 InterfaceType: INTERFACE '{' InterfaceType1 '}'

  149 InterfaceType1: %empty
  150               | InterfaceType1 MethodSpec ';'

  151 InterfaceTypeName: TypeName

  152 Key: FieldName
  153    | ElementIndex

  154 KeyType: Type

  155 Label: IDENTIFIER

  156 LabeledStmt: Label ':' Statement

  157 Literal: BasicLit
  158        | CompositeLit
  159        | FunctionLit

  160 LiteralType: StructType
  161            | ArrayType
  162            | '[' TOK4 ']' ElementType
  163            | SliceType
  164            | MapType
  165            | TypeName

  166 LiteralValue: '{' LiteralValue1 '}'

  167 LiteralValue1: %empty
  168              | ElementList LiteralValue11

  169 LiteralValue11: %empty
  170               | ','

  171 MapType: MAP '[' KeyType ']' ElementType

  172 MethodDecl: FUNC Receiver MethodName MethodDecl1

  173 MethodDecl1: Function
  174            | Signature

  175 MethodExpr: ReceiverType '.' MethodName

  176 MethodName: IDENTIFIER

  177 MethodSpec: MethodName Signature
  178           | InterfaceTypeName

  179 Operand: Literal
  180        | OperandName
  181        | MethodExpr
  182        | '(' Expression ')'

  183 OperandName: IDENTIFIER
  184            | QualifiedIdent

  185 PackageClause: PACKAGE PackageName

  186 PackageName: IDENTIFIER

  187 ParameterDecl: ParameterDecl1 ParameterDecl2 Type

  188 ParameterDecl1: %empty
  189               | IdentifierList

  190 ParameterDecl2: %empty
  191               | TOK4

  192 ParameterList: ParameterDecl ParameterList1

  193 ParameterList1: %empty
  194               | ParameterList1 ',' ParameterDecl

  195 Parameters: '(' Parameters1 ')'

  196 Parameters1: %empty
  197            | ParameterList Parameters11

  198 Parameters11: %empty
  199             | ','

  200 PointerType: '*' BaseType

  201 PostStmt: SimpleStmt

  202 PrimaryExpr: Operand
  203            | Conversion
  204            | BuiltinCall
  205            | PrimaryExpr Selector
  206            | PrimaryExpr Index
  207            | PrimaryExpr Slice
  208            | PrimaryExpr TypeAssertion
  209            | PrimaryExpr Call

  210 QualifiedIdent: PackageName '.' IDENTIFIER

  211 RangeClause: RangeClause1 RANGE Expression

  212 RangeClause1: ExpressionList '='
  213             | IdentifierList TOK3

  214 Receiver: '(' Receiver1 Receiver2 BaseTypeName ')'

  215 Receiver1: %empty
  216          | IDENTIFIER

  217 Receiver2: %empty
  218          | '*'

  219 ReceiverType: TypeName
  220             | '(' '*' TypeName ')'
  221             | '(' ReceiverType ')'

  222 RecvExpr: Expression

  223 RecvStmt: RecvStmt1 RecvExpr

  224 RecvStmt1: %empty
  225          | RecvStmt11

  226 RecvStmt11: ExpressionList '='
  227           | IdentifierList TOK3

  228 Result: Parameters
  229       | Type

  230 ReturnStmt: RETURN ReturnStmt1

  231 ReturnStmt1: %empty
  232            | ExpressionList

  233 SelectStmt: SELECT '{' SelectStmt1 '}'

  234 SelectStmt1: %empty
  235            | SelectStmt1 CommClause

  236 Selector: '.' IDENTIFIER

  237 SendStmt: Channel TOK5 Expression

  238 ShortVarDecl: IdentifierList TOK3 ExpressionList

  239 Signature: Parameters Signature1

  240 Signature1: %empty
  241           | Result

  242 SimpleStmt: EmptyStmt
  243           | ExpressionStmt
  244           | SendStmt
  245           | IncDecStmt
  246           | Assignment
  247           | ShortVarDecl

  248 Slice: '[' Slice1
  249      | Slice2 ']'

  250 Slice1: Slice11 ':' Slice12

  251 Slice11: %empty
  252        | Expression

  253 Slice12: %empty
  254        | Expression

  255 Slice2: Slice21 ':' Expression ':' Expression

  256 Slice21: %empty
  257        | Expression

  258 SliceType: '[' ']' ElementType

  259 SourceFile: PackageClause ';' SourceFile1 SourceFile2

  260 SourceFile1: %empty
  261            | SourceFile1 ImportDecl ';'

  262 SourceFile2: %empty
  263            | SourceFile2 TopLevelDecl ';'

  264 Start: SourceFile

  265 Statement: Declaration
  266          | LabeledStmt
  267          | SimpleStmt
  268          | GoStmt
  269          | ReturnStmt
  270          | BreakStmt
  271          | ContinueStmt
  272          | GotoStmt
  273          | FallthroughStmt
  274          | Block
  275          | IfStmt
  276          | SwitchStmt
  277          | SelectStmt
  278          | ForStmt
  279          | DeferStmt

  280 StatementList: StatementList1

  281 StatementList1: %empty
  282               | StatementList1 Statement ';'

  283 StructType: STRUCT '{' StructType1 '}'

  284 StructType1: %empty
  285            | StructType1 FieldDecl ';'

  286 SwitchStmt: ExprSwitchStmt
  287           | TypeSwitchStmt

  288 Tag: STRING_LIT

  289 TopLevelDecl: Declaration
  290             | FunctionDecl
  291             | MethodDecl

  292 Type: TypeName
  293     | TypeLit
  294     | '(' Type ')'

  295 TypeAssertion: '.' '(' Type ')'

  296 TypeCaseClause: TypeSwitchCase ':' StatementList

  297 TypeDecl: TYPE TypeDecl1

  298 TypeDecl1: TypeSpec
  299          | '(' TypeDecl11 ')'

  300 TypeDecl11: %empty
  301           | TypeDecl11 TypeSpec ';'

  302 TypeList: Type TypeList1

  303 TypeList1: %empty
  304          | TypeList1 ',' Type

  305 TypeLit: ArrayType
  306        | StructType
  307        | PointerType
  308        | FunctionType
  309        | InterfaceType
  310        | SliceType
  311        | MapType
  312        | ChannelType

  313 TypeName: IDENTIFIER
  314         | QualifiedIdent

  315 TypeSpec: IDENTIFIER Type

  316 TypeSwitchCase: CASE TypeList
  317               | DEFAULT

  318 TypeSwitchGuard: TypeSwitchGuard1 PrimaryExpr '.' '(' TYPE ')'

  319 TypeSwitchGuard1: %empty
  320                 | IDENTIFIER TOK3

  321 TypeSwitchStmt: SWITCH TypeSwitchStmt1 TypeSwitchGuard '{' TypeSwitchStmt2 '}'

  322 TypeSwitchStmt1: %empty
  323                | SimpleStmt ';'

  324 TypeSwitchStmt2: %empty
  325                | TypeSwitchStmt2 TypeCaseClause

  326 UnaryExpr: PrimaryExpr
  327          | UNARY_OP UnaryExpr

  328 Value: Expression
  329      | LiteralValue

  330 VarDecl: VAR VarDecl1

  331 VarDecl1: VarSpec
  332         | '(' VarDecl11 ')'

  333 VarDecl11: %empty
  334          | VarDecl11 VarSpec ';'

  335 VarSpec: IdentifierList VarSpec1

  336 VarSpec1: Type VarSpec11
  337         | '=' ExpressionList

  338 VarSpec11: %empty
  339          | '=' ExpressionList


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'(' (40) 25 30 49 60 134 182 195 214 220 221 294 295 299 318 332
')' (41) 25 30 49 60 134 182 195 214 220 221 294 295 299 318 332
'*' (42) 3 200 218 220
',' (44) 24 29 34 62 73 90 124 170 194 199 304
'.' (46) 141 175 210 236 295 318
':' (58) 44 69 76 156 250 255 296
';' (59) 51 81 99 127 136 150 259 261 263 282 285 301 323 334
'=' (61) 54 212 226 337 339
'[' (91) 8 146 162 171 248 258
']' (93) 8 146 162 171 249 258
'{' (123) 17 79 148 166 233 283 321
'}' (125) 17 79 148 166 233 283 321
error (256)
ASSIGN_OP (258) 9
BINARY_OP (259) 87
FLOAT_LIT (260) 13
IDENTIFIER (261) 11 25 98 118 122 124 155 176 183 186 210 216 236 313
    315 320
IMAGINARY_LIT (262) 14
INT_LIT (263) 12
RUNE_LIT (264) 15
STRING_LIT (265) 16 137 288
UNARY_OP (266) 327
TOK1 (267) 145
TOK2 (268) 144
TOK3 (269) 213 227 238 320
TOK4 (270) 6 162 191
TOK5 (271) 38 39 237
BREAK (272) 18
CASE (273) 40 77 316
CHAN (274) 37 38 39
CONST (275) 47
CONTINUE (276) 57
DEFAULT (277) 41 78 317
DEFER (278) 66
ELSE (279) 129
FALLTHROUGH (280) 92
FOR (281) 106
FUNC (282) 114 117 119 172
GO (283) 120
GOTO (284) 121
IF (285) 125
IMPORT (286) 132
INTERFACE (287) 148
MAP (288) 171
PACKAGE (289) 185
RANGE (290) 211
RETURN (291) 230
SELECT (292) 233
STRUCT (293) 283
SWITCH (294) 79 321
TYPE (295) 297 318
VAR (296) 330


Neterminály s pravidly, ve kterých se objevují

$accept (54)
    vlevo: 0
AnonymousField (55)
    vlevo: 1, vpravo: 95
AnonymousField1 (56)
    vlevo: 2 3, vpravo: 1
ArgumentList (57)
    vlevo: 4, vpravo: 22 24 32
ArgumentList1 (58)
    vlevo: 5 6, vpravo: 4
ArrayLength (59)
    vlevo: 7, vpravo: 8
ArrayType (60)
    vlevo: 8, vpravo: 161 305
Assignment (61)
    vlevo: 9, vpravo: 246
BaseType (62)
    vlevo: 10, vpravo: 200
BaseTypeName (63)
    vlevo: 11, vpravo: 214
BasicLit (64)
    vlevo: 12 13 14 15 16, vpravo: 157
Block (65)
    vlevo: 17, vpravo: 106 113 125 131 274
BreakStmt (66)
    vlevo: 18, vpravo: 270
BreakStmt1 (67)
    vlevo: 19 20, vpravo: 18
BuiltinArgs (68)
    vlevo: 21 22, vpravo: 27
BuiltinArgs1 (69)
    vlevo: 23 24, vpravo: 21
BuiltinCall (70)
    vlevo: 25, vpravo: 204
BuiltinCall1 (71)
    vlevo: 26 27, vpravo: 25
BuiltinCall11 (72)
    vlevo: 28 29, vpravo: 27
Call (73)
    vlevo: 30, vpravo: 209
Call1 (74)
    vlevo: 31 32, vpravo: 30
Call11 (75)
    vlevo: 33 34, vpravo: 32
Channel (76)
    vlevo: 35, vpravo: 237
ChannelType (77)
    vlevo: 36, vpravo: 312
ChannelType1 (78)
    vlevo: 37 38 39, vpravo: 36
CommCase (79)
    vlevo: 40 41, vpravo: 44
CommCase1 (80)
    vlevo: 42 43, vpravo: 40
CommClause (81)
    vlevo: 44, vpravo: 235
CompositeLit (82)
    vlevo: 45, vpravo: 158
Condition (83)
    vlevo: 46, vpravo: 103 109
ConstDecl (84)
    vlevo: 47, vpravo: 63
ConstDecl1 (85)
    vlevo: 48 49, vpravo: 47
ConstDecl11 (86)
    vlevo: 50 51, vpravo: 49 51
ConstSpec (87)
    vlevo: 52, vpravo: 48 51
ConstSpec1 (88)
    vlevo: 53 54, vpravo: 52
ConstSpec11 (89)
    vlevo: 55 56, vpravo: 54
ContinueStmt (90)
    vlevo: 57, vpravo: 271
ContinueStmt1 (91)
    vlevo: 58 59, vpravo: 57
Conversion (92)
    vlevo: 60, vpravo: 203
Conversion1 (93)
    vlevo: 61 62, vpravo: 60
Declaration (94)
    vlevo: 63 64 65, vpravo: 265 289
DeferStmt (95)
    vlevo: 66, vpravo: 279
Element (96)
    vlevo: 67, vpravo: 71 73
Element1 (97)
    vlevo: 68 69, vpravo: 67
ElementIndex (98)
    vlevo: 70, vpravo: 153
ElementList (99)
    vlevo: 71, vpravo: 168
ElementList1 (100)
    vlevo: 72 73, vpravo: 71 73
ElementType (101)
    vlevo: 74, vpravo: 8 36 162 171 258
EmptyStmt (102)
    vlevo: 75, vpravo: 242
ExprCaseClause (103)
    vlevo: 76, vpravo: 85
ExprSwitchCase (104)
    vlevo: 77 78, vpravo: 76
ExprSwitchStmt (105)
    vlevo: 79, vpravo: 286
ExprSwitchStmt1 (106)
    vlevo: 80 81, vpravo: 79
ExprSwitchStmt2 (107)
    vlevo: 82 83, vpravo: 79
ExprSwitchStmt3 (108)
    vlevo: 84 85, vpravo: 79 85
Expression (109)
    vlevo: 86 87, vpravo: 7 35 46 60 66 70 83 87 88 90 91 120 125 143
    146 182 211 222 237 252 254 255 257 328
ExpressionList (110)
    vlevo: 88, vpravo: 4 9 54 77 212 226 232 238 337 339
ExpressionList1 (111)
    vlevo: 89 90, vpravo: 88 90
ExpressionStmt (112)
    vlevo: 91, vpravo: 243
FallthroughStmt (113)
    vlevo: 92, vpravo: 273
FieldDecl (114)
    vlevo: 93, vpravo: 285
FieldDecl1 (115)
    vlevo: 94 95, vpravo: 93
FieldDecl2 (116)
    vlevo: 96 97, vpravo: 93
FieldName (117)
    vlevo: 98, vpravo: 152
ForClause (118)
    vlevo: 99, vpravo: 110
ForClause1 (119)
    vlevo: 100 101, vpravo: 99
ForClause2 (120)
    vlevo: 102 103, vpravo: 99
ForClause3 (121)
    vlevo: 104 105, vpravo: 99
ForStmt (122)
    vlevo: 106, vpravo: 278
ForStmt1 (123)
    vlevo: 107 108, vpravo: 106
ForStmt11 (124)
    vlevo: 109 110 111, vpravo: 108
Function (125)
    vlevo: 112, vpravo: 115 117 173
FunctionBody (126)
    vlevo: 113, vpravo: 112
FunctionDecl (127)
    vlevo: 114, vpravo: 290
FunctionDecl1 (128)
    vlevo: 115 116, vpravo: 114
FunctionLit (129)
    vlevo: 117, vpravo: 159
FunctionName (130)
    vlevo: 118, vpravo: 114
FunctionType (131)
    vlevo: 119, vpravo: 308
GoStmt (132)
    vlevo: 120, vpravo: 268
GotoStmt (133)
    vlevo: 121, vpravo: 272
IdentifierList (134)
    vlevo: 122, vpravo: 52 94 189 213 227 238 335
IdentifierList1 (135)
    vlevo: 123 124, vpravo: 122 124
IfStmt (136)
    vlevo: 125, vpravo: 130 275
IfStmt1 (137)
    vlevo: 126 127, vpravo: 125
IfStmt2 (138)
    vlevo: 128 129, vpravo: 125
IfStmt21 (139)
    vlevo: 130 131, vpravo: 129
ImportDecl (140)
    vlevo: 132, vpravo: 261
ImportDecl1 (141)
    vlevo: 133 134, vpravo: 132
ImportDecl11 (142)
    vlevo: 135 136, vpravo: 134 136
ImportPath (143)
    vlevo: 137, vpravo: 138
ImportSpec (144)
    vlevo: 138, vpravo: 133 136
ImportSpec1 (145)
    vlevo: 139 140, vpravo: 138
ImportSpec11 (146)
    vlevo: 141 142, vpravo: 140
IncDecStmt (147)
    vlevo: 143, vpravo: 245
IncDecStmt1 (148)
    vlevo: 144 145, vpravo: 143
Index (149)
    vlevo: 146, vpravo: 206
InitStmt (150)
    vlevo: 147, vpravo: 101
InterfaceType (151)
    vlevo: 148, vpravo: 309
InterfaceType1 (152)
    vlevo: 149 150, vpravo: 148 150
InterfaceTypeName (153)
    vlevo: 151, vpravo: 178
Key (154)
    vlevo: 152 153, vpravo: 69
KeyType (155)
    vlevo: 154, vpravo: 171
Label (156)
    vlevo: 155, vpravo: 20 59 121 156
LabeledStmt (157)
    vlevo: 156, vpravo: 266
Literal (158)
    vlevo: 157 158 159, vpravo: 179
LiteralType (159)
    vlevo: 160 161 162 163 164 165, vpravo: 45
LiteralValue (160)
    vlevo: 166, vpravo: 45 329
LiteralValue1 (161)
    vlevo: 167 168, vpravo: 166
LiteralValue11 (162)
    vlevo: 169 170, vpravo: 168
MapType (163)
    vlevo: 171, vpravo: 164 311
MethodDecl (164)
    vlevo: 172, vpravo: 291
MethodDecl1 (165)
    vlevo: 173 174, vpravo: 172
MethodExpr (166)
    vlevo: 175, vpravo: 181
MethodName (167)
    vlevo: 176, vpravo: 172 175 177
MethodSpec (168)
    vlevo: 177 178, vpravo: 150
Operand (169)
    vlevo: 179 180 181 182, vpravo: 202
OperandName (170)
    vlevo: 183 184, vpravo: 180
PackageClause (171)
    vlevo: 185, vpravo: 259
PackageName (172)
    vlevo: 186, vpravo: 142 185 210
ParameterDecl (173)
    vlevo: 187, vpravo: 192 194
ParameterDecl1 (174)
    vlevo: 188 189, vpravo: 187
ParameterDecl2 (175)
    vlevo: 190 191, vpravo: 187
ParameterList (176)
    vlevo: 192, vpravo: 197
ParameterList1 (177)
    vlevo: 193 194, vpravo: 192 194
Parameters (178)
    vlevo: 195, vpravo: 228 239
Parameters1 (179)
    vlevo: 196 197, vpravo: 195
Parameters11 (180)
    vlevo: 198 199, vpravo: 197
PointerType (181)
    vlevo: 200, vpravo: 307
PostStmt (182)
    vlevo: 201, vpravo: 105
PrimaryExpr (183)
    vlevo: 202 203 204 205 206 207 208 209, vpravo: 205 206 207 208
    209 318 326
QualifiedIdent (184)
    vlevo: 210, vpravo: 184 314
RangeClause (185)
    vlevo: 211, vpravo: 111
RangeClause1 (186)
    vlevo: 212 213, vpravo: 211
Receiver (187)
    vlevo: 214, vpravo: 172
Receiver1 (188)
    vlevo: 215 216, vpravo: 214
Receiver2 (189)
    vlevo: 217 218, vpravo: 214
ReceiverType (190)
    vlevo: 219 220 221, vpravo: 175 221
RecvExpr (191)
    vlevo: 222, vpravo: 223
RecvStmt (192)
    vlevo: 223, vpravo: 43
RecvStmt1 (193)
    vlevo: 224 225, vpravo: 223
RecvStmt11 (194)
    vlevo: 226 227, vpravo: 225
Result (195)
    vlevo: 228 229, vpravo: 241
ReturnStmt (196)
    vlevo: 230, vpravo: 269
ReturnStmt1 (197)
    vlevo: 231 232, vpravo: 230
SelectStmt (198)
    vlevo: 233, vpravo: 277
SelectStmt1 (199)
    vlevo: 234 235, vpravo: 233 235
Selector (200)
    vlevo: 236, vpravo: 205
SendStmt (201)
    vlevo: 237, vpravo: 42 244
ShortVarDecl (202)
    vlevo: 238, vpravo: 247
Signature (203)
    vlevo: 239, vpravo: 112 116 119 174 177
Signature1 (204)
    vlevo: 240 241, vpravo: 239
SimpleStmt (205)
    vlevo: 242 243 244 245 246 247, vpravo: 81 127 147 201 267 323
Slice (206)
    vlevo: 248 249, vpravo: 207
Slice1 (207)
    vlevo: 250, vpravo: 248
Slice11 (208)
    vlevo: 251 252, vpravo: 250
Slice12 (209)
    vlevo: 253 254, vpravo: 250
Slice2 (210)
    vlevo: 255, vpravo: 249
Slice21 (211)
    vlevo: 256 257, vpravo: 255
SliceType (212)
    vlevo: 258, vpravo: 163 310
SourceFile (213)
    vlevo: 259, vpravo: 264
SourceFile1 (214)
    vlevo: 260 261, vpravo: 259 261
SourceFile2 (215)
    vlevo: 262 263, vpravo: 259 263
Start (216)
    vlevo: 264, vpravo: 0
Statement (217)
    vlevo: 265 266 267 268 269 270 271 272 273 274 275 276 277 278
    279, vpravo: 156 282
StatementList (218)
    vlevo: 280, vpravo: 17 44 76 296
StatementList1 (219)
    vlevo: 281 282, vpravo: 280 282
StructType (220)
    vlevo: 283, vpravo: 160 306
StructType1 (221)
    vlevo: 284 285, vpravo: 283 285
SwitchStmt (222)
    vlevo: 286 287, vpravo: 276
Tag (223)
    vlevo: 288, vpravo: 97
TopLevelDecl (224)
    vlevo: 289 290 291, vpravo: 263
Type (225)
    vlevo: 292 293 294, vpravo: 10 21 56 60 74 94 154 187 229 294 295
    302 304 315 336
TypeAssertion (226)
    vlevo: 295, vpravo: 208
TypeCaseClause (227)
    vlevo: 296, vpravo: 325
TypeDecl (228)
    vlevo: 297, vpravo: 64
TypeDecl1 (229)
    vlevo: 298 299, vpravo: 297
TypeDecl11 (230)
    vlevo: 300 301, vpravo: 299 301
TypeList (231)
    vlevo: 302, vpravo: 316
TypeList1 (232)
    vlevo: 303 304, vpravo: 302 304
TypeLit (233)
    vlevo: 305 306 307 308 309 310 311 312, vpravo: 293
TypeName (234)
    vlevo: 313 314, vpravo: 1 151 165 219 220 292
TypeSpec (235)
    vlevo: 315, vpravo: 298 301
TypeSwitchCase (236)
    vlevo: 316 317, vpravo: 296
TypeSwitchGuard (237)
    vlevo: 318, vpravo: 321
TypeSwitchGuard1 (238)
    vlevo: 319 320, vpravo: 318
TypeSwitchStmt (239)
    vlevo: 321, vpravo: 287
TypeSwitchStmt1 (240)
    vlevo: 322 323, vpravo: 321
TypeSwitchStmt2 (241)
    vlevo: 324 325, vpravo: 321 325
UnaryExpr (242)
    vlevo: 326 327, vpravo: 86 87 327
Value (243)
    vlevo: 328 329, vpravo: 67
VarDecl (244)
    vlevo: 330, vpravo: 65
VarDecl1 (245)
    vlevo: 331 332, vpravo: 330
VarDecl11 (246)
    vlevo: 333 334, vpravo: 332 334
VarSpec (247)
    vlevo: 335, vpravo: 331 334
VarSpec1 (248)
    vlevo: 336 337, vpravo: 335
VarSpec11 (249)
    vlevo: 338 339, vpravo: 336


State 0

    0 $accept: . Start $end
  185 PackageClause: . PACKAGE PackageName
  259 SourceFile: . PackageClause ';' SourceFile1 SourceFile2
  264 Start: . SourceFile

    PACKAGE  posunout a přejít do stavu 1

    PackageClause  přejít do stavu 2
    SourceFile     přejít do stavu 3
    Start          přejít do stavu 4


State 1

  185 PackageClause: PACKAGE . PackageName
  186 PackageName: . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 5

    PackageName  přejít do stavu 6


State 2

  259 SourceFile: PackageClause . ';' SourceFile1 SourceFile2

    ';'  posunout a přejít do stavu 7


State 3

  264 Start: SourceFile .

    $výchozí  reduce using rule 264 (Start)


State 4

    0 $accept: Start . $end

    $end  posunout a přejít do stavu 8


State 5

  186 PackageName: IDENTIFIER .

    $výchozí  reduce using rule 186 (PackageName)


State 6

  185 PackageClause: PACKAGE PackageName .

    $výchozí  reduce using rule 185 (PackageClause)


State 7

  259 SourceFile: PackageClause ';' . SourceFile1 SourceFile2
  260 SourceFile1: . %empty
  261            | . SourceFile1 ImportDecl ';'

    $výchozí  reduce using rule 260 (SourceFile1)

    SourceFile1  přejít do stavu 9


State 8

    0 $accept: Start $end .

    $výchozí  přijmout


State 9

  132 ImportDecl: . IMPORT ImportDecl1
  259 SourceFile: PackageClause ';' SourceFile1 . SourceFile2
  261 SourceFile1: SourceFile1 . ImportDecl ';'
  262 SourceFile2: . %empty  [$end, CONST, FUNC, TYPE, VAR]
  263            | . SourceFile2 TopLevelDecl ';'

    IMPORT  posunout a přejít do stavu 10

    $výchozí  reduce using rule 262 (SourceFile2)

    ImportDecl   přejít do stavu 11
    SourceFile2  přejít do stavu 12


State 10

  132 ImportDecl: IMPORT . ImportDecl1
  133 ImportDecl1: . ImportSpec
  134            | . '(' ImportDecl11 ')'
  138 ImportSpec: . ImportSpec1 ImportPath
  139 ImportSpec1: . %empty  [STRING_LIT]
  140            | . ImportSpec11
  141 ImportSpec11: . '.'
  142             | . PackageName
  186 PackageName: . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 5
    '('         posunout a přejít do stavu 13
    '.'         posunout a přejít do stavu 14

    $výchozí  reduce using rule 139 (ImportSpec1)

    ImportDecl1   přejít do stavu 15
    ImportSpec    přejít do stavu 16
    ImportSpec1   přejít do stavu 17
    ImportSpec11  přejít do stavu 18
    PackageName   přejít do stavu 19


State 11

  261 SourceFile1: SourceFile1 ImportDecl . ';'

    ';'  posunout a přejít do stavu 20


State 12

   47 ConstDecl: . CONST ConstDecl1
   63 Declaration: . ConstDecl
   64            | . TypeDecl
   65            | . VarDecl
  114 FunctionDecl: . FUNC FunctionName FunctionDecl1
  172 MethodDecl: . FUNC Receiver MethodName MethodDecl1
  259 SourceFile: PackageClause ';' SourceFile1 SourceFile2 .  [$end]
  263 SourceFile2: SourceFile2 . TopLevelDecl ';'
  289 TopLevelDecl: . Declaration
  290             | . FunctionDecl
  291             | . MethodDecl
  297 TypeDecl: . TYPE TypeDecl1
  330 VarDecl: . VAR VarDecl1

    CONST  posunout a přejít do stavu 21
    FUNC   posunout a přejít do stavu 22
    TYPE   posunout a přejít do stavu 23
    VAR    posunout a přejít do stavu 24

    $výchozí  reduce using rule 259 (SourceFile)

    ConstDecl     přejít do stavu 25
    Declaration   přejít do stavu 26
    FunctionDecl  přejít do stavu 27
    MethodDecl    přejít do stavu 28
    TopLevelDecl  přejít do stavu 29
    TypeDecl      přejít do stavu 30
    VarDecl       přejít do stavu 31


State 13

  134 ImportDecl1: '(' . ImportDecl11 ')'
  135 ImportDecl11: . %empty
  136             | . ImportDecl11 ImportSpec ';'

    $výchozí  reduce using rule 135 (ImportDecl11)

    ImportDecl11  přejít do stavu 32


State 14

  141 ImportSpec11: '.' .

    $výchozí  reduce using rule 141 (ImportSpec11)


State 15

  132 ImportDecl: IMPORT ImportDecl1 .

    $výchozí  reduce using rule 132 (ImportDecl)


State 16

  133 ImportDecl1: ImportSpec .

    $výchozí  reduce using rule 133 (ImportDecl1)


State 17

  137 ImportPath: . STRING_LIT
  138 ImportSpec: ImportSpec1 . ImportPath

    STRING_LIT  posunout a přejít do stavu 33

    ImportPath  přejít do stavu 34


State 18

  140 ImportSpec1: ImportSpec11 .

    $výchozí  reduce using rule 140 (ImportSpec1)


State 19

  142 ImportSpec11: PackageName .

    $výchozí  reduce using rule 142 (ImportSpec11)


State 20

  261 SourceFile1: SourceFile1 ImportDecl ';' .

    $výchozí  reduce using rule 261 (SourceFile1)


State 21

   47 ConstDecl: CONST . ConstDecl1
   48 ConstDecl1: . ConstSpec
   49           | . '(' ConstDecl11 ')'
   52 ConstSpec: . IdentifierList ConstSpec1
  122 IdentifierList: . IDENTIFIER IdentifierList1

    IDENTIFIER  posunout a přejít do stavu 35
    '('         posunout a přejít do stavu 36

    ConstDecl1      přejít do stavu 37
    ConstSpec       přejít do stavu 38
    IdentifierList  přejít do stavu 39


State 22

  114 FunctionDecl: FUNC . FunctionName FunctionDecl1
  118 FunctionName: . IDENTIFIER
  172 MethodDecl: FUNC . Receiver MethodName MethodDecl1
  214 Receiver: . '(' Receiver1 Receiver2 BaseTypeName ')'

    IDENTIFIER  posunout a přejít do stavu 40
    '('         posunout a přejít do stavu 41

    FunctionName  přejít do stavu 42
    Receiver      přejít do stavu 43


State 23

  297 TypeDecl: TYPE . TypeDecl1
  298 TypeDecl1: . TypeSpec
  299          | . '(' TypeDecl11 ')'
  315 TypeSpec: . IDENTIFIER Type

    IDENTIFIER  posunout a přejít do stavu 44
    '('         posunout a přejít do stavu 45

    TypeDecl1  přejít do stavu 46
    TypeSpec   přejít do stavu 47


State 24

  122 IdentifierList: . IDENTIFIER IdentifierList1
  330 VarDecl: VAR . VarDecl1
  331 VarDecl1: . VarSpec
  332         | . '(' VarDecl11 ')'
  335 VarSpec: . IdentifierList VarSpec1

    IDENTIFIER  posunout a přejít do stavu 35
    '('         posunout a přejít do stavu 48

    IdentifierList  přejít do stavu 49
    VarDecl1        přejít do stavu 50
    VarSpec         přejít do stavu 51


State 25

   63 Declaration: ConstDecl .

    $výchozí  reduce using rule 63 (Declaration)


State 26

  289 TopLevelDecl: Declaration .

    $výchozí  reduce using rule 289 (TopLevelDecl)


State 27

  290 TopLevelDecl: FunctionDecl .

    $výchozí  reduce using rule 290 (TopLevelDecl)


State 28

  291 TopLevelDecl: MethodDecl .

    $výchozí  reduce using rule 291 (TopLevelDecl)


State 29

  263 SourceFile2: SourceFile2 TopLevelDecl . ';'

    ';'  posunout a přejít do stavu 52


State 30

   64 Declaration: TypeDecl .

    $výchozí  reduce using rule 64 (Declaration)


State 31

   65 Declaration: VarDecl .

    $výchozí  reduce using rule 65 (Declaration)


State 32

  134 ImportDecl1: '(' ImportDecl11 . ')'
  136 ImportDecl11: ImportDecl11 . ImportSpec ';'
  138 ImportSpec: . ImportSpec1 ImportPath
  139 ImportSpec1: . %empty  [STRING_LIT]
  140            | . ImportSpec11
  141 ImportSpec11: . '.'
  142             | . PackageName
  186 PackageName: . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 5
    ')'         posunout a přejít do stavu 53
    '.'         posunout a přejít do stavu 14

    $výchozí  reduce using rule 139 (ImportSpec1)

    ImportSpec    přejít do stavu 54
    ImportSpec1   přejít do stavu 17
    ImportSpec11  přejít do stavu 18
    PackageName   přejít do stavu 19


State 33

  137 ImportPath: STRING_LIT .

    $výchozí  reduce using rule 137 (ImportPath)


State 34

  138 ImportSpec: ImportSpec1 ImportPath .

    $výchozí  reduce using rule 138 (ImportSpec)


State 35

  122 IdentifierList: IDENTIFIER . IdentifierList1
  123 IdentifierList1: . %empty
  124                | . IdentifierList1 ',' IDENTIFIER

    $výchozí  reduce using rule 123 (IdentifierList1)

    IdentifierList1  přejít do stavu 55


State 36

   49 ConstDecl1: '(' . ConstDecl11 ')'
   50 ConstDecl11: . %empty
   51            | . ConstDecl11 ConstSpec ';'

    $výchozí  reduce using rule 50 (ConstDecl11)

    ConstDecl11  přejít do stavu 56


State 37

   47 ConstDecl: CONST ConstDecl1 .

    $výchozí  reduce using rule 47 (ConstDecl)


State 38

   48 ConstDecl1: ConstSpec .

    $výchozí  reduce using rule 48 (ConstDecl1)


State 39

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   52 ConstSpec: IdentifierList . ConstSpec1
   53 ConstSpec1: . %empty  [';']
   54           | . ConstSpec11 '=' ExpressionList
   55 ConstSpec11: . %empty  ['=']
   56            | . Type
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    '='         reduce using rule 55 (ConstSpec11)
    $výchozí  reduce using rule 53 (ConstSpec1)

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    ConstSpec1      přejít do stavu 70
    ConstSpec11     přejít do stavu 71
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 80
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 40

  118 FunctionName: IDENTIFIER .

    $výchozí  reduce using rule 118 (FunctionName)


State 41

  214 Receiver: '(' . Receiver1 Receiver2 BaseTypeName ')'
  215 Receiver1: . %empty  [IDENTIFIER, '*']
  216          | . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 83

    IDENTIFIER  [reduce using rule 215 (Receiver1)]
    $výchozí  reduce using rule 215 (Receiver1)

    Receiver1  přejít do stavu 84


State 42

  112 Function: . Signature FunctionBody
  114 FunctionDecl: FUNC FunctionName . FunctionDecl1
  115 FunctionDecl1: . Function
  116              | . Signature
  195 Parameters: . '(' Parameters1 ')'
  239 Signature: . Parameters Signature1

    '('  posunout a přejít do stavu 85

    Function       přejít do stavu 86
    FunctionDecl1  přejít do stavu 87
    Parameters     přejít do stavu 88
    Signature      přejít do stavu 89


State 43

  172 MethodDecl: FUNC Receiver . MethodName MethodDecl1
  176 MethodName: . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 90

    MethodName  přejít do stavu 91


State 44

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  315 TypeSpec: IDENTIFIER . Type

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 92
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 45

  299 TypeDecl1: '(' . TypeDecl11 ')'
  300 TypeDecl11: . %empty
  301           | . TypeDecl11 TypeSpec ';'

    $výchozí  reduce using rule 300 (TypeDecl11)

    TypeDecl11  přejít do stavu 93


State 46

  297 TypeDecl: TYPE TypeDecl1 .

    $výchozí  reduce using rule 297 (TypeDecl)


State 47

  298 TypeDecl1: TypeSpec .

    $výchozí  reduce using rule 298 (TypeDecl1)


State 48

  332 VarDecl1: '(' . VarDecl11 ')'
  333 VarDecl11: . %empty
  334          | . VarDecl11 VarSpec ';'

    $výchozí  reduce using rule 333 (VarDecl11)

    VarDecl11  přejít do stavu 94


State 49

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  335 VarSpec: IdentifierList . VarSpec1
  336 VarSpec1: . Type VarSpec11
  337         | . '=' ExpressionList

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66
    '='         posunout a přejít do stavu 95

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 96
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82
    VarSpec1        přejít do stavu 97


State 50

  330 VarDecl: VAR VarDecl1 .

    $výchozí  reduce using rule 330 (VarDecl)


State 51

  331 VarDecl1: VarSpec .

    $výchozí  reduce using rule 331 (VarDecl1)


State 52

  263 SourceFile2: SourceFile2 TopLevelDecl ';' .

    $výchozí  reduce using rule 263 (SourceFile2)


State 53

  134 ImportDecl1: '(' ImportDecl11 ')' .

    $výchozí  reduce using rule 134 (ImportDecl1)


State 54

  136 ImportDecl11: ImportDecl11 ImportSpec . ';'

    ';'  posunout a přejít do stavu 98


State 55

  122 IdentifierList: IDENTIFIER IdentifierList1 .  [IDENTIFIER, TOK3, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(', ';', '=']
  124 IdentifierList1: IdentifierList1 . ',' IDENTIFIER

    ','  posunout a přejít do stavu 99

    $výchozí  reduce using rule 122 (IdentifierList)


State 56

   49 ConstDecl1: '(' ConstDecl11 . ')'
   51 ConstDecl11: ConstDecl11 . ConstSpec ';'
   52 ConstSpec: . IdentifierList ConstSpec1
  122 IdentifierList: . IDENTIFIER IdentifierList1

    IDENTIFIER  posunout a přejít do stavu 35
    ')'         posunout a přejít do stavu 100

    ConstSpec       přejít do stavu 101
    IdentifierList  přejít do stavu 39


State 57

  186 PackageName: IDENTIFIER .  ['.']
  313 TypeName: IDENTIFIER .  [STRING_LIT, ']', '{', ',', '(', ')', ':', ';', '=']

    '.'         reduce using rule 186 (PackageName)
    $výchozí  reduce using rule 313 (TypeName)


State 58

   39 ChannelType1: TOK5 . CHAN

    CHAN  posunout a přejít do stavu 102


State 59

   37 ChannelType1: CHAN .  [IDENTIFIER, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']
   38             | CHAN . TOK5

    TOK5  posunout a přejít do stavu 103

    TOK5        [reduce using rule 37 (ChannelType1)]
    $výchozí  reduce using rule 37 (ChannelType1)


State 60

  119 FunctionType: FUNC . Signature
  195 Parameters: . '(' Parameters1 ')'
  239 Signature: . Parameters Signature1

    '('  posunout a přejít do stavu 85

    Parameters  přejít do stavu 88
    Signature   přejít do stavu 104


State 61

  148 InterfaceType: INTERFACE . '{' InterfaceType1 '}'

    '{'  posunout a přejít do stavu 105


State 62

  171 MapType: MAP . '[' KeyType ']' ElementType

    '['  posunout a přejít do stavu 106


State 63

  283 StructType: STRUCT . '{' StructType1 '}'

    '{'  posunout a přejít do stavu 107


State 64

    8 ArrayType: . '[' ArrayLength ']' ElementType
   10 BaseType: . Type
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  200            | '*' . BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    BaseType        přejít do stavu 108
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 109
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 65

    7 ArrayLength: . Expression
    8 ArrayType: . '[' ArrayLength ']' ElementType
    8          | '[' . ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  258          | '[' . ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    ']'            posunout a přejít do stavu 119
    '('            posunout a přejít do stavu 120

    ArrayLength     přejít do stavu 121
    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 127
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 66

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  294     | '(' . Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 143
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 67

  305 TypeLit: ArrayType .

    $výchozí  reduce using rule 305 (TypeLit)


State 68

  312 TypeLit: ChannelType .

    $výchozí  reduce using rule 312 (TypeLit)


State 69

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   36            | ChannelType1 . ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   74 ElementType: . Type
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    ElementType     přejít do stavu 144
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 145
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 70

   52 ConstSpec: IdentifierList ConstSpec1 .

    $výchozí  reduce using rule 52 (ConstSpec)


State 71

   54 ConstSpec1: ConstSpec11 . '=' ExpressionList

    '='  posunout a přejít do stavu 146


State 72

  308 TypeLit: FunctionType .

    $výchozí  reduce using rule 308 (TypeLit)


State 73

  309 TypeLit: InterfaceType .

    $výchozí  reduce using rule 309 (TypeLit)


State 74

  311 TypeLit: MapType .

    $výchozí  reduce using rule 311 (TypeLit)


State 75

  210 QualifiedIdent: PackageName . '.' IDENTIFIER

    '.'  posunout a přejít do stavu 147


State 76

  307 TypeLit: PointerType .

    $výchozí  reduce using rule 307 (TypeLit)


State 77

  314 TypeName: QualifiedIdent .

    $výchozí  reduce using rule 314 (TypeName)


State 78

  310 TypeLit: SliceType .

    $výchozí  reduce using rule 310 (TypeLit)


State 79

  306 TypeLit: StructType .

    $výchozí  reduce using rule 306 (TypeLit)


State 80

   56 ConstSpec11: Type .

    $výchozí  reduce using rule 56 (ConstSpec11)


State 81

  293 Type: TypeLit .

    $výchozí  reduce using rule 293 (Type)


State 82

  292 Type: TypeName .

    $výchozí  reduce using rule 292 (Type)


State 83

  216 Receiver1: IDENTIFIER .

    $výchozí  reduce using rule 216 (Receiver1)


State 84

  214 Receiver: '(' Receiver1 . Receiver2 BaseTypeName ')'
  217 Receiver2: . %empty  [IDENTIFIER]
  218          | . '*'

    '*'  posunout a přejít do stavu 148

    $výchozí  reduce using rule 217 (Receiver2)

    Receiver2  přejít do stavu 149


State 85

  122 IdentifierList: . IDENTIFIER IdentifierList1
  187 ParameterDecl: . ParameterDecl1 ParameterDecl2 Type
  188 ParameterDecl1: . %empty  [IDENTIFIER, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']
  189               | . IdentifierList
  192 ParameterList: . ParameterDecl ParameterList1
  195 Parameters: '(' . Parameters1 ')'
  196 Parameters1: . %empty  [')']
  197            | . ParameterList Parameters11

    IDENTIFIER  posunout a přejít do stavu 35

    IDENTIFIER  [reduce using rule 188 (ParameterDecl1)]
    ')'         reduce using rule 196 (Parameters1)
    $výchozí  reduce using rule 188 (ParameterDecl1)

    IdentifierList  přejít do stavu 150
    ParameterDecl   přejít do stavu 151
    ParameterDecl1  přejít do stavu 152
    ParameterList   přejít do stavu 153
    Parameters1     přejít do stavu 154


State 86

  115 FunctionDecl1: Function .

    $výchozí  reduce using rule 115 (FunctionDecl1)


State 87

  114 FunctionDecl: FUNC FunctionName FunctionDecl1 .

    $výchozí  reduce using rule 114 (FunctionDecl)


State 88

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  195 Parameters: . '(' Parameters1 ')'
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  228 Result: . Parameters
  229       | . Type
  239 Signature: Parameters . Signature1
  240 Signature1: . %empty  [STRING_LIT, ']', '{', ',', '(', ')', ':', ';', '=']
  241           | . Result
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 155

    '('         [reduce using rule 240 (Signature1)]
    $výchozí  reduce using rule 240 (Signature1)

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    Parameters      přejít do stavu 156
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    Result          přejít do stavu 157
    Signature1      přejít do stavu 158
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 159
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 89

   17 Block: . '{' StatementList '}'
  112 Function: Signature . FunctionBody
  113 FunctionBody: . Block
  116 FunctionDecl1: Signature .  [';']

    '{'  posunout a přejít do stavu 160

    $výchozí  reduce using rule 116 (FunctionDecl1)

    Block         přejít do stavu 161
    FunctionBody  přejít do stavu 162


State 90

  176 MethodName: IDENTIFIER .

    $výchozí  reduce using rule 176 (MethodName)


State 91

  112 Function: . Signature FunctionBody
  172 MethodDecl: FUNC Receiver MethodName . MethodDecl1
  173 MethodDecl1: . Function
  174            | . Signature
  195 Parameters: . '(' Parameters1 ')'
  239 Signature: . Parameters Signature1

    '('  posunout a přejít do stavu 85

    Function     přejít do stavu 163
    MethodDecl1  přejít do stavu 164
    Parameters   přejít do stavu 88
    Signature    přejít do stavu 165


State 92

  315 TypeSpec: IDENTIFIER Type .

    $výchozí  reduce using rule 315 (TypeSpec)


State 93

  299 TypeDecl1: '(' TypeDecl11 . ')'
  301 TypeDecl11: TypeDecl11 . TypeSpec ';'
  315 TypeSpec: . IDENTIFIER Type

    IDENTIFIER  posunout a přejít do stavu 44
    ')'         posunout a přejít do stavu 166

    TypeSpec  přejít do stavu 167


State 94

  122 IdentifierList: . IDENTIFIER IdentifierList1
  332 VarDecl1: '(' VarDecl11 . ')'
  334 VarDecl11: VarDecl11 . VarSpec ';'
  335 VarSpec: . IdentifierList VarSpec1

    IDENTIFIER  posunout a přejít do stavu 35
    ')'         posunout a přejít do stavu 168

    IdentifierList  přejít do stavu 49
    VarSpec         přejít do stavu 169


State 95

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr
  337 VarSpec1: '=' . ExpressionList

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 171
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 96

  336 VarSpec1: Type . VarSpec11
  338 VarSpec11: . %empty  [';']
  339          | . '=' ExpressionList

    '='  posunout a přejít do stavu 172

    $výchozí  reduce using rule 338 (VarSpec11)

    VarSpec11  přejít do stavu 173


State 97

  335 VarSpec: IdentifierList VarSpec1 .

    $výchozí  reduce using rule 335 (VarSpec)


State 98

  136 ImportDecl11: ImportDecl11 ImportSpec ';' .

    $výchozí  reduce using rule 136 (ImportDecl11)


State 99

  124 IdentifierList1: IdentifierList1 ',' . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 174


State 100

   49 ConstDecl1: '(' ConstDecl11 ')' .

    $výchozí  reduce using rule 49 (ConstDecl1)


State 101

   51 ConstDecl11: ConstDecl11 ConstSpec . ';'

    ';'  posunout a přejít do stavu 175


State 102

   39 ChannelType1: TOK5 CHAN .

    $výchozí  reduce using rule 39 (ChannelType1)


State 103

   38 ChannelType1: CHAN TOK5 .

    $výchozí  reduce using rule 38 (ChannelType1)


State 104

  119 FunctionType: FUNC Signature .

    $výchozí  reduce using rule 119 (FunctionType)


State 105

  148 InterfaceType: INTERFACE '{' . InterfaceType1 '}'
  149 InterfaceType1: . %empty
  150               | . InterfaceType1 MethodSpec ';'

    $výchozí  reduce using rule 149 (InterfaceType1)

    InterfaceType1  přejít do stavu 176


State 106

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  154 KeyType: . Type
  171 MapType: . MAP '[' KeyType ']' ElementType
  171        | MAP '[' . KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    KeyType         přejít do stavu 177
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 178
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 107

  283 StructType: STRUCT '{' . StructType1 '}'
  284 StructType1: . %empty
  285            | . StructType1 FieldDecl ';'

    $výchozí  reduce using rule 284 (StructType1)

    StructType1  přejít do stavu 179


State 108

  200 PointerType: '*' BaseType .

    $výchozí  reduce using rule 200 (PointerType)


State 109

   10 BaseType: Type .

    $výchozí  reduce using rule 10 (BaseType)


State 110

   13 BasicLit: FLOAT_LIT .

    $výchozí  reduce using rule 13 (BasicLit)


State 111

   25 BuiltinCall: IDENTIFIER . '(' BuiltinCall1 ')'
  183 OperandName: IDENTIFIER .  [ASSIGN_OP, BINARY_OP, FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK1, TOK2, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', ']', '{', '}', ',', '(', ')', ':', ';', '=', '.']
  186 PackageName: IDENTIFIER .  ['.']
  313 TypeName: IDENTIFIER .  ['{', ',', '(', ')', '.']

    '('  posunout a přejít do stavu 180

    '{'         reduce using rule 183 (OperandName)
    '{'         [reduce using rule 313 (TypeName)]
    ','         reduce using rule 183 (OperandName)
    ','         [reduce using rule 313 (TypeName)]
    '('         [reduce using rule 183 (OperandName)]
    '('         [reduce using rule 313 (TypeName)]
    ')'         reduce using rule 183 (OperandName)
    ')'         [reduce using rule 313 (TypeName)]
    '.'         reduce using rule 183 (OperandName)
    '.'         [reduce using rule 186 (PackageName)]
    '.'         [reduce using rule 313 (TypeName)]
    $výchozí  reduce using rule 183 (OperandName)


State 112

   14 BasicLit: IMAGINARY_LIT .

    $výchozí  reduce using rule 14 (BasicLit)


State 113

   12 BasicLit: INT_LIT .

    $výchozí  reduce using rule 12 (BasicLit)


State 114

   15 BasicLit: RUNE_LIT .

    $výchozí  reduce using rule 15 (BasicLit)


State 115

   16 BasicLit: STRING_LIT .

    $výchozí  reduce using rule 16 (BasicLit)


State 116

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr
  327          | UNARY_OP . UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 181


State 117

  112 Function: . Signature FunctionBody
  117 FunctionLit: FUNC . Function
  119 FunctionType: FUNC . Signature
  195 Parameters: . '(' Parameters1 ')'
  239 Signature: . Parameters Signature1

    '('  posunout a přejít do stavu 85

    Function    přejít do stavu 182
    Parameters  přejít do stavu 88
    Signature   přejít do stavu 183


State 118

    7 ArrayLength: . Expression
    8 ArrayType: . '[' ArrayLength ']' ElementType
    8          | '[' . ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  162            | '[' . TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  258          | '[' . ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK4           posunout a přejít do stavu 184
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    ']'            posunout a přejít do stavu 119
    '('            posunout a přejít do stavu 120

    ArrayLength     přejít do stavu 121
    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 127
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 119

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   74 ElementType: . Type
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  258          | '[' ']' . ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    ElementType     přejít do stavu 185
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 145
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 120

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  182        | '(' . Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  220             | '(' . '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  221             | '(' . ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  294     | '(' . Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 186
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 187
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 188
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 189
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 121

    8 ArrayType: '[' ArrayLength . ']' ElementType

    ']'  posunout a přejít do stavu 190


State 122

  161 LiteralType: ArrayType .  ['{']
  305 TypeLit: ArrayType .  [',', '(', ')']

    '{'         reduce using rule 161 (LiteralType)
    $výchozí  reduce using rule 305 (TypeLit)


State 123

  157 Literal: BasicLit .

    $výchozí  reduce using rule 157 (Literal)


State 124

  204 PrimaryExpr: BuiltinCall .

    $výchozí  reduce using rule 204 (PrimaryExpr)


State 125

  158 Literal: CompositeLit .

    $výchozí  reduce using rule 158 (Literal)


State 126

  203 PrimaryExpr: Conversion .

    $výchozí  reduce using rule 203 (PrimaryExpr)


State 127

    7 ArrayLength: Expression .  [']']
   87 Expression: Expression . BINARY_OP UnaryExpr

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 7 (ArrayLength)


State 128

  159 Literal: FunctionLit .

    $výchozí  reduce using rule 159 (Literal)


State 129

  179 Operand: Literal .

    $výchozí  reduce using rule 179 (Operand)


State 130

   45 CompositeLit: LiteralType . LiteralValue
  166 LiteralValue: . '{' LiteralValue1 '}'

    '{'  posunout a přejít do stavu 192

    LiteralValue  přejít do stavu 193


State 131

  164 LiteralType: MapType .  ['{']
  311 TypeLit: MapType .  [',', '(', ')']

    '{'         reduce using rule 164 (LiteralType)
    $výchozí  reduce using rule 311 (TypeLit)


State 132

  181 Operand: MethodExpr .

    $výchozí  reduce using rule 181 (Operand)


State 133

  202 PrimaryExpr: Operand .

    $výchozí  reduce using rule 202 (PrimaryExpr)


State 134

  180 Operand: OperandName .

    $výchozí  reduce using rule 180 (Operand)


State 135

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   30 Call: . '(' Call1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  146 Index: . '[' Expression ']'
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  205            | PrimaryExpr . Selector
  206            | . PrimaryExpr Index
  206            | PrimaryExpr . Index
  207            | . PrimaryExpr Slice
  207            | PrimaryExpr . Slice
  208            | . PrimaryExpr TypeAssertion
  208            | PrimaryExpr . TypeAssertion
  209            | . PrimaryExpr Call
  209            | PrimaryExpr . Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  236 Selector: . '.' IDENTIFIER
  248 Slice: . '[' Slice1
  249      | . Slice2 ']'
  255 Slice2: . Slice21 ':' Expression ':' Expression
  256 Slice21: . %empty  [':']
  257        | . Expression
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  295 TypeAssertion: . '.' '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  326          | PrimaryExpr .  [ASSIGN_OP, BINARY_OP, FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK1, TOK2, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', ']', '{', '}', ',', '(', ')', ':', ';', '=', '.']
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 194
    '('            posunout a přejít do stavu 195
    '.'            posunout a přejít do stavu 196

    FLOAT_LIT      [reduce using rule 326 (UnaryExpr)]
    IDENTIFIER     [reduce using rule 326 (UnaryExpr)]
    IMAGINARY_LIT  [reduce using rule 326 (UnaryExpr)]
    INT_LIT        [reduce using rule 326 (UnaryExpr)]
    RUNE_LIT       [reduce using rule 326 (UnaryExpr)]
    STRING_LIT     [reduce using rule 326 (UnaryExpr)]
    UNARY_OP       [reduce using rule 326 (UnaryExpr)]
    TOK5           [reduce using rule 326 (UnaryExpr)]
    CHAN           [reduce using rule 326 (UnaryExpr)]
    FUNC           [reduce using rule 326 (UnaryExpr)]
    INTERFACE      [reduce using rule 326 (UnaryExpr)]
    MAP            [reduce using rule 326 (UnaryExpr)]
    STRUCT         [reduce using rule 326 (UnaryExpr)]
    '*'            [reduce using rule 326 (UnaryExpr)]
    '['            [reduce using rule 326 (UnaryExpr)]
    '('            [reduce using rule 326 (UnaryExpr)]
    ':'            reduce using rule 256 (Slice21)
    ':'            [reduce using rule 326 (UnaryExpr)]
    '.'            [reduce using rule 326 (UnaryExpr)]
    $výchozí     reduce using rule 326 (UnaryExpr)

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    Call            přejít do stavu 197
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 198
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    Index           přejít do stavu 199
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    Selector        přejít do stavu 200
    Slice           přejít do stavu 201
    Slice2          přejít do stavu 202
    Slice21         přejít do stavu 203
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeAssertion   přejít do stavu 204
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 136

  184 OperandName: QualifiedIdent .  [ASSIGN_OP, BINARY_OP, FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK1, TOK2, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', ']', '{', '}', ',', '(', ')', ':', ';', '=', '.']
  314 TypeName: QualifiedIdent .  ['{', ',', '(', ')', '.']

    '{'         reduce using rule 184 (OperandName)
    '{'         [reduce using rule 314 (TypeName)]
    ','         reduce using rule 184 (OperandName)
    ','         [reduce using rule 314 (TypeName)]
    '('         reduce using rule 184 (OperandName)
    '('         [reduce using rule 314 (TypeName)]
    ')'         reduce using rule 184 (OperandName)
    ')'         [reduce using rule 314 (TypeName)]
    '.'         reduce using rule 184 (OperandName)
    '.'         [reduce using rule 314 (TypeName)]
    $výchozí  reduce using rule 184 (OperandName)


State 137

  175 MethodExpr: ReceiverType . '.' MethodName

    '.'  posunout a přejít do stavu 205


State 138

  163 LiteralType: SliceType .  ['{']
  310 TypeLit: SliceType .  [',', '(', ')']

    '{'         reduce using rule 163 (LiteralType)
    $výchozí  reduce using rule 310 (TypeLit)


State 139

  160 LiteralType: StructType .  ['{']
  306 TypeLit: StructType .  [',', '(', ')']

    '{'         reduce using rule 160 (LiteralType)
    $výchozí  reduce using rule 306 (TypeLit)


State 140

   60 Conversion: Type . '(' Expression Conversion1 ')'

    '('  posunout a přejít do stavu 206


State 141

  165 LiteralType: TypeName .  ['{']
  219 ReceiverType: TypeName .  [')', '.']
  292 Type: TypeName .  [',', '(', ')']

    '{'         reduce using rule 165 (LiteralType)
    ','         reduce using rule 292 (Type)
    '('         reduce using rule 292 (Type)
    ')'         reduce using rule 219 (ReceiverType)
    ')'         [reduce using rule 292 (Type)]
    $výchozí  reduce using rule 219 (ReceiverType)


State 142

   86 Expression: UnaryExpr .

    $výchozí  reduce using rule 86 (Expression)


State 143

  294 Type: '(' Type . ')'

    ')'  posunout a přejít do stavu 207


State 144

   36 ChannelType: ChannelType1 ElementType .

    $výchozí  reduce using rule 36 (ChannelType)


State 145

   74 ElementType: Type .

    $výchozí  reduce using rule 74 (ElementType)


State 146

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   54 ConstSpec1: ConstSpec11 '=' . ExpressionList
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 208
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 147

  210 QualifiedIdent: PackageName '.' . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 209


State 148

  218 Receiver2: '*' .

    $výchozí  reduce using rule 218 (Receiver2)


State 149

   11 BaseTypeName: . IDENTIFIER
  214 Receiver: '(' Receiver1 Receiver2 . BaseTypeName ')'

    IDENTIFIER  posunout a přejít do stavu 210

    BaseTypeName  přejít do stavu 211


State 150

  189 ParameterDecl1: IdentifierList .

    $výchozí  reduce using rule 189 (ParameterDecl1)


State 151

  192 ParameterList: ParameterDecl . ParameterList1
  193 ParameterList1: . %empty
  194               | . ParameterList1 ',' ParameterDecl

    $výchozí  reduce using rule 193 (ParameterList1)

    ParameterList1  přejít do stavu 212


State 152

  187 ParameterDecl: ParameterDecl1 . ParameterDecl2 Type
  190 ParameterDecl2: . %empty  [IDENTIFIER, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']
  191               | . TOK4

    TOK4  posunout a přejít do stavu 213

    $výchozí  reduce using rule 190 (ParameterDecl2)

    ParameterDecl2  přejít do stavu 214


State 153

  197 Parameters1: ParameterList . Parameters11
  198 Parameters11: . %empty  [')']
  199             | . ','

    ','  posunout a přejít do stavu 215

    $výchozí  reduce using rule 198 (Parameters11)

    Parameters11  přejít do stavu 216


State 154

  195 Parameters: '(' Parameters1 . ')'

    ')'  posunout a přejít do stavu 217


State 155

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  122 IdentifierList: . IDENTIFIER IdentifierList1
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  187 ParameterDecl: . ParameterDecl1 ParameterDecl2 Type
  188 ParameterDecl1: . %empty  [IDENTIFIER, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']
  189               | . IdentifierList
  192 ParameterList: . ParameterDecl ParameterList1
  195 Parameters: '(' . Parameters1 ')'
  196 Parameters1: . %empty  [')']
  197            | . ParameterList Parameters11
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  294     | '(' . Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 218
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    IDENTIFIER  [reduce using rule 188 (ParameterDecl1)]
    TOK5        [reduce using rule 188 (ParameterDecl1)]
    CHAN        [reduce using rule 188 (ParameterDecl1)]
    FUNC        [reduce using rule 188 (ParameterDecl1)]
    INTERFACE   [reduce using rule 188 (ParameterDecl1)]
    MAP         [reduce using rule 188 (ParameterDecl1)]
    STRUCT      [reduce using rule 188 (ParameterDecl1)]
    '*'         [reduce using rule 188 (ParameterDecl1)]
    '['         [reduce using rule 188 (ParameterDecl1)]
    '('         [reduce using rule 188 (ParameterDecl1)]
    ')'         reduce using rule 196 (Parameters1)
    $výchozí  reduce using rule 188 (ParameterDecl1)

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    IdentifierList  přejít do stavu 150
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    ParameterDecl   přejít do stavu 151
    ParameterDecl1  přejít do stavu 152
    ParameterList   přejít do stavu 153
    Parameters1     přejít do stavu 154
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 143
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 156

  228 Result: Parameters .

    $výchozí  reduce using rule 228 (Result)


State 157

  241 Signature1: Result .

    $výchozí  reduce using rule 241 (Signature1)


State 158

  239 Signature: Parameters Signature1 .

    $výchozí  reduce using rule 239 (Signature)


State 159

  229 Result: Type .

    $výchozí  reduce using rule 229 (Result)


State 160

   17 Block: '{' . StatementList '}'
  280 StatementList: . StatementList1
  281 StatementList1: . %empty
  282               | . StatementList1 Statement ';'

    $výchozí  reduce using rule 281 (StatementList1)

    StatementList   přejít do stavu 219
    StatementList1  přejít do stavu 220


State 161

  113 FunctionBody: Block .

    $výchozí  reduce using rule 113 (FunctionBody)


State 162

  112 Function: Signature FunctionBody .

    $výchozí  reduce using rule 112 (Function)


State 163

  173 MethodDecl1: Function .

    $výchozí  reduce using rule 173 (MethodDecl1)


State 164

  172 MethodDecl: FUNC Receiver MethodName MethodDecl1 .

    $výchozí  reduce using rule 172 (MethodDecl)


State 165

   17 Block: . '{' StatementList '}'
  112 Function: Signature . FunctionBody
  113 FunctionBody: . Block
  174 MethodDecl1: Signature .  [';']

    '{'  posunout a přejít do stavu 160

    $výchozí  reduce using rule 174 (MethodDecl1)

    Block         přejít do stavu 161
    FunctionBody  přejít do stavu 162


State 166

  299 TypeDecl1: '(' TypeDecl11 ')' .

    $výchozí  reduce using rule 299 (TypeDecl1)


State 167

  301 TypeDecl11: TypeDecl11 TypeSpec . ';'

    ';'  posunout a přejít do stavu 221


State 168

  332 VarDecl1: '(' VarDecl11 ')' .

    $výchozí  reduce using rule 332 (VarDecl1)


State 169

  334 VarDecl11: VarDecl11 VarSpec . ';'

    ';'  posunout a přejít do stavu 222


State 170

   87 Expression: Expression . BINARY_OP UnaryExpr
   88 ExpressionList: Expression . ExpressionList1
   89 ExpressionList1: . %empty  [TOK4, '{', ',', ')', ':', ';']
   90                | . ExpressionList1 ',' Expression

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 89 (ExpressionList1)

    ExpressionList1  přejít do stavu 223


State 171

  337 VarSpec1: '=' ExpressionList .

    $výchozí  reduce using rule 337 (VarSpec1)


State 172

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr
  339 VarSpec11: '=' . ExpressionList

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 224
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 173

  336 VarSpec1: Type VarSpec11 .

    $výchozí  reduce using rule 336 (VarSpec1)


State 174

  124 IdentifierList1: IdentifierList1 ',' IDENTIFIER .

    $výchozí  reduce using rule 124 (IdentifierList1)


State 175

   51 ConstDecl11: ConstDecl11 ConstSpec ';' .

    $výchozí  reduce using rule 51 (ConstDecl11)


State 176

  148 InterfaceType: INTERFACE '{' InterfaceType1 . '}'
  150 InterfaceType1: InterfaceType1 . MethodSpec ';'
  151 InterfaceTypeName: . TypeName
  176 MethodName: . IDENTIFIER
  177 MethodSpec: . MethodName Signature
  178           | . InterfaceTypeName
  186 PackageName: . IDENTIFIER
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 225
    '}'         posunout a přejít do stavu 226

    InterfaceTypeName  přejít do stavu 227
    MethodName         přejít do stavu 228
    MethodSpec         přejít do stavu 229
    PackageName        přejít do stavu 75
    QualifiedIdent     přejít do stavu 77
    TypeName           přejít do stavu 230


State 177

  171 MapType: MAP '[' KeyType . ']' ElementType

    ']'  posunout a přejít do stavu 231


State 178

  154 KeyType: Type .

    $výchozí  reduce using rule 154 (KeyType)


State 179

    1 AnonymousField: . AnonymousField1 TypeName
    2 AnonymousField1: . %empty  [IDENTIFIER]
    3                | . '*'
   93 FieldDecl: . FieldDecl1 FieldDecl2
   94 FieldDecl1: . IdentifierList Type
   95           | . AnonymousField
  122 IdentifierList: . IDENTIFIER IdentifierList1
  283 StructType: STRUCT '{' StructType1 . '}'
  285 StructType1: StructType1 . FieldDecl ';'

    IDENTIFIER  posunout a přejít do stavu 35
    '*'         posunout a přejít do stavu 232
    '}'         posunout a přejít do stavu 233

    IDENTIFIER  [reduce using rule 2 (AnonymousField1)]

    AnonymousField   přejít do stavu 234
    AnonymousField1  přejít do stavu 235
    FieldDecl        přejít do stavu 236
    FieldDecl1       přejít do stavu 237
    IdentifierList   přejít do stavu 238


State 180

    4 ArgumentList: . ExpressionList ArgumentList1
    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   21 BuiltinArgs: . Type BuiltinArgs1
   22            | . ArgumentList
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   25            | IDENTIFIER '(' . BuiltinCall1 ')'
   26 BuiltinCall1: . %empty  [')']
   27             | . BuiltinArgs BuiltinCall11
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    $výchozí  reduce using rule 26 (BuiltinCall1)

    ArgumentList    přejít do stavu 239
    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinArgs     přejít do stavu 240
    BuiltinCall     přejít do stavu 124
    BuiltinCall1    přejít do stavu 241
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 242
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 243
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 181

  327 UnaryExpr: UNARY_OP UnaryExpr .

    $výchozí  reduce using rule 327 (UnaryExpr)


State 182

  117 FunctionLit: FUNC Function .

    $výchozí  reduce using rule 117 (FunctionLit)


State 183

   17 Block: . '{' StatementList '}'
  112 Function: Signature . FunctionBody
  113 FunctionBody: . Block
  119 FunctionType: FUNC Signature .  [',', '(', ')']

    '{'  posunout a přejít do stavu 160

    $výchozí  reduce using rule 119 (FunctionType)

    Block         přejít do stavu 161
    FunctionBody  přejít do stavu 162


State 184

  162 LiteralType: '[' TOK4 . ']' ElementType

    ']'  posunout a přejít do stavu 244


State 185

  258 SliceType: '[' ']' ElementType .

    $výchozí  reduce using rule 258 (SliceType)


State 186

    8 ArrayType: . '[' ArrayLength ']' ElementType
   10 BaseType: . Type
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  200            | '*' . BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  220 ReceiverType: '(' '*' . TypeName ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    BaseType        přejít do stavu 108
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 109
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 245


State 187

   87 Expression: Expression . BINARY_OP UnaryExpr
  182 Operand: '(' Expression . ')'

    BINARY_OP  posunout a přejít do stavu 191
    ')'        posunout a přejít do stavu 246


State 188

  175 MethodExpr: ReceiverType . '.' MethodName
  221 ReceiverType: '(' ReceiverType . ')'

    ')'  posunout a přejít do stavu 247
    '.'  posunout a přejít do stavu 205


State 189

   60 Conversion: Type . '(' Expression Conversion1 ')'
  294 Type: '(' Type . ')'

    '('  posunout a přejít do stavu 206
    ')'  posunout a přejít do stavu 207


State 190

    8 ArrayType: . '[' ArrayLength ']' ElementType
    8          | '[' ArrayLength ']' . ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   74 ElementType: . Type
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    ElementType     přejít do stavu 248
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 145
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 191

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   87 Expression: Expression BINARY_OP . UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 249


State 192

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   67 Element: . Element1 Value
   68 Element1: . %empty  [FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '{', '(']
   69         | . Key ':'
   70 ElementIndex: . Expression
   71 ElementList: . Element ElementList1
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   98 FieldName: . IDENTIFIER
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  152 Key: . FieldName
  153    | . ElementIndex
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  166 LiteralValue: '{' . LiteralValue1 '}'
  167 LiteralValue1: . %empty  ['}']
  168              | . ElementList LiteralValue11
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 250
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    FLOAT_LIT      [reduce using rule 68 (Element1)]
    IDENTIFIER     [reduce using rule 68 (Element1)]
    IMAGINARY_LIT  [reduce using rule 68 (Element1)]
    INT_LIT        [reduce using rule 68 (Element1)]
    RUNE_LIT       [reduce using rule 68 (Element1)]
    STRING_LIT     [reduce using rule 68 (Element1)]
    UNARY_OP       [reduce using rule 68 (Element1)]
    TOK5           [reduce using rule 68 (Element1)]
    CHAN           [reduce using rule 68 (Element1)]
    FUNC           [reduce using rule 68 (Element1)]
    INTERFACE      [reduce using rule 68 (Element1)]
    MAP            [reduce using rule 68 (Element1)]
    STRUCT         [reduce using rule 68 (Element1)]
    '*'            [reduce using rule 68 (Element1)]
    '['            [reduce using rule 68 (Element1)]
    '}'            reduce using rule 167 (LiteralValue1)
    '('            [reduce using rule 68 (Element1)]
    $výchozí     reduce using rule 68 (Element1)

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Element         přejít do stavu 251
    Element1        přejít do stavu 252
    ElementIndex    přejít do stavu 253
    ElementList     přejít do stavu 254
    Expression      přejít do stavu 255
    FieldName       přejít do stavu 256
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Key             přejít do stavu 257
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    LiteralValue1   přejít do stavu 258
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 193

   45 CompositeLit: LiteralType LiteralValue .

    $výchozí  reduce using rule 45 (CompositeLit)


State 194

    7 ArrayLength: . Expression
    8 ArrayType: . '[' ArrayLength ']' ElementType
    8          | '[' . ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  146 Index: '[' . Expression ']'
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  162            | '[' . TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  248 Slice: '[' . Slice1
  250 Slice1: . Slice11 ':' Slice12
  251 Slice11: . %empty  [':']
  252        | . Expression
  258 SliceType: . '[' ']' ElementType
  258          | '[' . ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK4           posunout a přejít do stavu 184
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    ']'            posunout a přejít do stavu 119
    '('            posunout a přejít do stavu 120

    $výchozí  reduce using rule 251 (Slice11)

    ArrayLength     přejít do stavu 121
    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 259
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    Slice1          přejít do stavu 260
    Slice11         přejít do stavu 261
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 195

    4 ArgumentList: . ExpressionList ArgumentList1
    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   30 Call: '(' . Call1 ')'
   31 Call1: . %empty  [')']
   32      | . ArgumentList Call11
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  182        | '(' . Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  220             | '(' . '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  221             | '(' . ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  294     | '(' . Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 186
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    $výchozí  reduce using rule 31 (Call1)

    ArgumentList    přejít do stavu 262
    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    Call1           přejít do stavu 263
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 264
    ExpressionList  přejít do stavu 242
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 188
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 189
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 196

  236 Selector: '.' . IDENTIFIER
  295 TypeAssertion: '.' . '(' Type ')'

    IDENTIFIER  posunout a přejít do stavu 265
    '('         posunout a přejít do stavu 266


State 197

  209 PrimaryExpr: PrimaryExpr Call .

    $výchozí  reduce using rule 209 (PrimaryExpr)


State 198

   87 Expression: Expression . BINARY_OP UnaryExpr
  257 Slice21: Expression .  [':']

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 257 (Slice21)


State 199

  206 PrimaryExpr: PrimaryExpr Index .

    $výchozí  reduce using rule 206 (PrimaryExpr)


State 200

  205 PrimaryExpr: PrimaryExpr Selector .

    $výchozí  reduce using rule 205 (PrimaryExpr)


State 201

  207 PrimaryExpr: PrimaryExpr Slice .

    $výchozí  reduce using rule 207 (PrimaryExpr)


State 202

  249 Slice: Slice2 . ']'

    ']'  posunout a přejít do stavu 267


State 203

  255 Slice2: Slice21 . ':' Expression ':' Expression

    ':'  posunout a přejít do stavu 268


State 204

  208 PrimaryExpr: PrimaryExpr TypeAssertion .

    $výchozí  reduce using rule 208 (PrimaryExpr)


State 205

  175 MethodExpr: ReceiverType '.' . MethodName
  176 MethodName: . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 90

    MethodName  přejít do stavu 269


State 206

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   60           | Type '(' . Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 270
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 207

  294 Type: '(' Type ')' .

    $výchozí  reduce using rule 294 (Type)


State 208

   54 ConstSpec1: ConstSpec11 '=' ExpressionList .

    $výchozí  reduce using rule 54 (ConstSpec1)


State 209

  210 QualifiedIdent: PackageName '.' IDENTIFIER .

    $výchozí  reduce using rule 210 (QualifiedIdent)


State 210

   11 BaseTypeName: IDENTIFIER .

    $výchozí  reduce using rule 11 (BaseTypeName)


State 211

  214 Receiver: '(' Receiver1 Receiver2 BaseTypeName . ')'

    ')'  posunout a přejít do stavu 271


State 212

  192 ParameterList: ParameterDecl ParameterList1 .  [',', ')']
  194 ParameterList1: ParameterList1 . ',' ParameterDecl

    ','  posunout a přejít do stavu 272

    ','         [reduce using rule 192 (ParameterList)]
    $výchozí  reduce using rule 192 (ParameterList)


State 213

  191 ParameterDecl2: TOK4 .

    $výchozí  reduce using rule 191 (ParameterDecl2)


State 214

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  187 ParameterDecl: ParameterDecl1 ParameterDecl2 . Type
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 273
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 215

  199 Parameters11: ',' .

    $výchozí  reduce using rule 199 (Parameters11)


State 216

  197 Parameters1: ParameterList Parameters11 .

    $výchozí  reduce using rule 197 (Parameters1)


State 217

  195 Parameters: '(' Parameters1 ')' .

    $výchozí  reduce using rule 195 (Parameters)


State 218

  122 IdentifierList: IDENTIFIER . IdentifierList1
  123 IdentifierList1: . %empty  [IDENTIFIER, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', ',', '(']
  124                | . IdentifierList1 ',' IDENTIFIER
  186 PackageName: IDENTIFIER .  ['.']
  313 TypeName: IDENTIFIER .  [')']

    ')'         reduce using rule 313 (TypeName)
    '.'         reduce using rule 186 (PackageName)
    $výchozí  reduce using rule 123 (IdentifierList1)

    IdentifierList1  přejít do stavu 55


State 219

   17 Block: '{' StatementList . '}'

    '}'  posunout a přejít do stavu 274


State 220

    8 ArrayType: . '[' ArrayLength ']' ElementType
    9 Assignment: . ExpressionList ASSIGN_OP ExpressionList
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   17 Block: . '{' StatementList '}'
   18 BreakStmt: . BREAK BreakStmt1
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   35 Channel: . Expression
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   47 ConstDecl: . CONST ConstDecl1
   57 ContinueStmt: . CONTINUE ContinueStmt1
   60 Conversion: . Type '(' Expression Conversion1 ')'
   63 Declaration: . ConstDecl
   64            | . TypeDecl
   65            | . VarDecl
   66 DeferStmt: . DEFER Expression
   75 EmptyStmt: . %empty  [';']
   79 ExprSwitchStmt: . SWITCH ExprSwitchStmt1 ExprSwitchStmt2 '{' ExprSwitchStmt3 '}'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
   91 ExpressionStmt: . Expression
   92 FallthroughStmt: . FALLTHROUGH
  106 ForStmt: . FOR ForStmt1 Block
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  120 GoStmt: . GO Expression
  121 GotoStmt: . GOTO Label
  122 IdentifierList: . IDENTIFIER IdentifierList1
  125 IfStmt: . IF IfStmt1 Expression Block IfStmt2
  143 IncDecStmt: . Expression IncDecStmt1
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  155 Label: . IDENTIFIER
  156 LabeledStmt: . Label ':' Statement
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  230 ReturnStmt: . RETURN ReturnStmt1
  233 SelectStmt: . SELECT '{' SelectStmt1 '}'
  237 SendStmt: . Channel TOK5 Expression
  238 ShortVarDecl: . IdentifierList TOK3 ExpressionList
  242 SimpleStmt: . EmptyStmt
  243           | . ExpressionStmt
  244           | . SendStmt
  245           | . IncDecStmt
  246           | . Assignment
  247           | . ShortVarDecl
  258 SliceType: . '[' ']' ElementType
  265 Statement: . Declaration
  266          | . LabeledStmt
  267          | . SimpleStmt
  268          | . GoStmt
  269          | . ReturnStmt
  270          | . BreakStmt
  271          | . ContinueStmt
  272          | . GotoStmt
  273          | . FallthroughStmt
  274          | . Block
  275          | . IfStmt
  276          | . SwitchStmt
  277          | . SelectStmt
  278          | . ForStmt
  279          | . DeferStmt
  280 StatementList: StatementList1 .  [CASE, DEFAULT, '}']
  282 StatementList1: StatementList1 . Statement ';'
  283 StructType: . STRUCT '{' StructType1 '}'
  286 SwitchStmt: . ExprSwitchStmt
  287           | . TypeSwitchStmt
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  297 TypeDecl: . TYPE TypeDecl1
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  321 TypeSwitchStmt: . SWITCH TypeSwitchStmt1 TypeSwitchGuard '{' TypeSwitchStmt2 '}'
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr
  330 VarDecl: . VAR VarDecl1

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 275
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    BREAK          posunout a přejít do stavu 276
    CHAN           posunout a přejít do stavu 59
    CONST          posunout a přejít do stavu 21
    CONTINUE       posunout a přejít do stavu 277
    DEFER          posunout a přejít do stavu 278
    FALLTHROUGH    posunout a přejít do stavu 279
    FOR            posunout a přejít do stavu 280
    FUNC           posunout a přejít do stavu 117
    GO             posunout a přejít do stavu 281
    GOTO           posunout a přejít do stavu 282
    IF             posunout a přejít do stavu 283
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    RETURN         posunout a přejít do stavu 284
    SELECT         posunout a přejít do stavu 285
    STRUCT         posunout a přejít do stavu 63
    SWITCH         posunout a přejít do stavu 286
    TYPE           posunout a přejít do stavu 23
    VAR            posunout a přejít do stavu 24
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '{'            posunout a přejít do stavu 160
    '('            posunout a přejít do stavu 120

    ';'         reduce using rule 75 (EmptyStmt)
    $výchozí  reduce using rule 280 (StatementList)

    ArrayType        přejít do stavu 122
    Assignment       přejít do stavu 287
    BasicLit         přejít do stavu 123
    Block            přejít do stavu 288
    BreakStmt        přejít do stavu 289
    BuiltinCall      přejít do stavu 124
    Channel          přejít do stavu 290
    ChannelType      přejít do stavu 68
    ChannelType1     přejít do stavu 69
    CompositeLit     přejít do stavu 125
    ConstDecl        přejít do stavu 25
    ContinueStmt     přejít do stavu 291
    Conversion       přejít do stavu 126
    Declaration      přejít do stavu 292
    DeferStmt        přejít do stavu 293
    EmptyStmt        přejít do stavu 294
    ExprSwitchStmt   přejít do stavu 295
    Expression       přejít do stavu 296
    ExpressionList   přejít do stavu 297
    ExpressionStmt   přejít do stavu 298
    FallthroughStmt  přejít do stavu 299
    ForStmt          přejít do stavu 300
    FunctionLit      přejít do stavu 128
    FunctionType     přejít do stavu 72
    GoStmt           přejít do stavu 301
    GotoStmt         přejít do stavu 302
    IdentifierList   přejít do stavu 303
    IfStmt           přejít do stavu 304
    IncDecStmt       přejít do stavu 305
    InterfaceType    přejít do stavu 73
    Label            přejít do stavu 306
    LabeledStmt      přejít do stavu 307
    Literal          přejít do stavu 129
    LiteralType      přejít do stavu 130
    MapType          přejít do stavu 131
    MethodExpr       přejít do stavu 132
    Operand          přejít do stavu 133
    OperandName      přejít do stavu 134
    PackageName      přejít do stavu 75
    PointerType      přejít do stavu 76
    PrimaryExpr      přejít do stavu 135
    QualifiedIdent   přejít do stavu 136
    ReceiverType     přejít do stavu 137
    ReturnStmt       přejít do stavu 308
    SelectStmt       přejít do stavu 309
    SendStmt         přejít do stavu 310
    ShortVarDecl     přejít do stavu 311
    SimpleStmt       přejít do stavu 312
    SliceType        přejít do stavu 138
    Statement        přejít do stavu 313
    StructType       přejít do stavu 139
    SwitchStmt       přejít do stavu 314
    Type             přejít do stavu 140
    TypeDecl         přejít do stavu 30
    TypeLit          přejít do stavu 81
    TypeName         přejít do stavu 141
    TypeSwitchStmt   přejít do stavu 315
    UnaryExpr        přejít do stavu 142
    VarDecl          přejít do stavu 31


State 221

  301 TypeDecl11: TypeDecl11 TypeSpec ';' .

    $výchozí  reduce using rule 301 (TypeDecl11)


State 222

  334 VarDecl11: VarDecl11 VarSpec ';' .

    $výchozí  reduce using rule 334 (VarDecl11)


State 223

   88 ExpressionList: Expression ExpressionList1 .  [ASSIGN_OP, TOK4, '{', ',', ')', ':', ';', '=']
   90 ExpressionList1: ExpressionList1 . ',' Expression

    ','  posunout a přejít do stavu 316

    ','         [reduce using rule 88 (ExpressionList)]
    $výchozí  reduce using rule 88 (ExpressionList)


State 224

  339 VarSpec11: '=' ExpressionList .

    $výchozí  reduce using rule 339 (VarSpec11)


State 225

  176 MethodName: IDENTIFIER .  ['(']
  186 PackageName: IDENTIFIER .  ['.']
  313 TypeName: IDENTIFIER .  [';']

    ';'         reduce using rule 313 (TypeName)
    '.'         reduce using rule 186 (PackageName)
    $výchozí  reduce using rule 176 (MethodName)


State 226

  148 InterfaceType: INTERFACE '{' InterfaceType1 '}' .

    $výchozí  reduce using rule 148 (InterfaceType)


State 227

  178 MethodSpec: InterfaceTypeName .

    $výchozí  reduce using rule 178 (MethodSpec)


State 228

  177 MethodSpec: MethodName . Signature
  195 Parameters: . '(' Parameters1 ')'
  239 Signature: . Parameters Signature1

    '('  posunout a přejít do stavu 85

    Parameters  přejít do stavu 88
    Signature   přejít do stavu 317


State 229

  150 InterfaceType1: InterfaceType1 MethodSpec . ';'

    ';'  posunout a přejít do stavu 318


State 230

  151 InterfaceTypeName: TypeName .

    $výchozí  reduce using rule 151 (InterfaceTypeName)


State 231

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   74 ElementType: . Type
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  171        | MAP '[' KeyType ']' . ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    ElementType     přejít do stavu 319
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 145
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 232

    3 AnonymousField1: '*' .

    $výchozí  reduce using rule 3 (AnonymousField1)


State 233

  283 StructType: STRUCT '{' StructType1 '}' .

    $výchozí  reduce using rule 283 (StructType)


State 234

   95 FieldDecl1: AnonymousField .

    $výchozí  reduce using rule 95 (FieldDecl1)


State 235

    1 AnonymousField: AnonymousField1 . TypeName
  186 PackageName: . IDENTIFIER
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57

    PackageName     přejít do stavu 75
    QualifiedIdent  přejít do stavu 77
    TypeName        přejít do stavu 320


State 236

  285 StructType1: StructType1 FieldDecl . ';'

    ';'  posunout a přejít do stavu 321


State 237

   93 FieldDecl: FieldDecl1 . FieldDecl2
   96 FieldDecl2: . %empty  [';']
   97           | . Tag
  288 Tag: . STRING_LIT

    STRING_LIT  posunout a přejít do stavu 322

    $výchozí  reduce using rule 96 (FieldDecl2)

    FieldDecl2  přejít do stavu 323
    Tag         přejít do stavu 324


State 238

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   94 FieldDecl1: IdentifierList . Type
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 325
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 239

   22 BuiltinArgs: ArgumentList .

    $výchozí  reduce using rule 22 (BuiltinArgs)


State 240

   27 BuiltinCall1: BuiltinArgs . BuiltinCall11
   28 BuiltinCall11: . %empty  [')']
   29              | . ','

    ','  posunout a přejít do stavu 326

    $výchozí  reduce using rule 28 (BuiltinCall11)

    BuiltinCall11  přejít do stavu 327


State 241

   25 BuiltinCall: IDENTIFIER '(' BuiltinCall1 . ')'

    ')'  posunout a přejít do stavu 328


State 242

    4 ArgumentList: ExpressionList . ArgumentList1
    5 ArgumentList1: . %empty  [',', ')']
    6              | . TOK4

    TOK4  posunout a přejít do stavu 329

    $výchozí  reduce using rule 5 (ArgumentList1)

    ArgumentList1  přejít do stavu 330


State 243

   21 BuiltinArgs: Type . BuiltinArgs1
   23 BuiltinArgs1: . %empty  [',', ')']
   24             | . ',' ArgumentList
   60 Conversion: Type . '(' Expression Conversion1 ')'

    ','  posunout a přejít do stavu 331
    '('  posunout a přejít do stavu 206

    ','         [reduce using rule 23 (BuiltinArgs1)]
    $výchozí  reduce using rule 23 (BuiltinArgs1)

    BuiltinArgs1  přejít do stavu 332


State 244

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   74 ElementType: . Type
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  162 LiteralType: '[' TOK4 ']' . ElementType
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    ElementType     přejít do stavu 333
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 145
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 245

  220 ReceiverType: '(' '*' TypeName . ')'
  292 Type: TypeName .  ['(', ')']

    ')'  posunout a přejít do stavu 334

    ')'         [reduce using rule 292 (Type)]
    $výchozí  reduce using rule 292 (Type)


State 246

  182 Operand: '(' Expression ')' .

    $výchozí  reduce using rule 182 (Operand)


State 247

  221 ReceiverType: '(' ReceiverType ')' .

    $výchozí  reduce using rule 221 (ReceiverType)


State 248

    8 ArrayType: '[' ArrayLength ']' ElementType .

    $výchozí  reduce using rule 8 (ArrayType)


State 249

   87 Expression: Expression BINARY_OP UnaryExpr .

    $výchozí  reduce using rule 87 (Expression)


State 250

   25 BuiltinCall: IDENTIFIER . '(' BuiltinCall1 ')'
   98 FieldName: IDENTIFIER .  [':']
  183 OperandName: IDENTIFIER .  [BINARY_OP, FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(', ':', '.']
  186 PackageName: IDENTIFIER .  ['.']
  313 TypeName: IDENTIFIER .  ['{', '(', '.']

    '('  posunout a přejít do stavu 180

    '{'         reduce using rule 313 (TypeName)
    '('         [reduce using rule 183 (OperandName)]
    '('         [reduce using rule 313 (TypeName)]
    ':'         reduce using rule 98 (FieldName)
    ':'         [reduce using rule 183 (OperandName)]
    '.'         reduce using rule 183 (OperandName)
    '.'         [reduce using rule 186 (PackageName)]
    '.'         [reduce using rule 313 (TypeName)]
    $výchozí  reduce using rule 183 (OperandName)


State 251

   71 ElementList: Element . ElementList1
   72 ElementList1: . %empty
   73             | . ElementList1 ',' Element

    $výchozí  reduce using rule 72 (ElementList1)

    ElementList1  přejít do stavu 335


State 252

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   67 Element: Element1 . Value
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  166 LiteralValue: . '{' LiteralValue1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr
  328 Value: . Expression
  329      | . LiteralValue

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '{'            posunout a přejít do stavu 192
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 336
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    LiteralValue    přejít do stavu 337
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142
    Value           přejít do stavu 338


State 253

  153 Key: ElementIndex .

    $výchozí  reduce using rule 153 (Key)


State 254

  168 LiteralValue1: ElementList . LiteralValue11
  169 LiteralValue11: . %empty  ['}']
  170               | . ','

    ','  posunout a přejít do stavu 339

    $výchozí  reduce using rule 169 (LiteralValue11)

    LiteralValue11  přejít do stavu 340


State 255

   70 ElementIndex: Expression .  [':']
   87 Expression: Expression . BINARY_OP UnaryExpr

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 70 (ElementIndex)


State 256

  152 Key: FieldName .

    $výchozí  reduce using rule 152 (Key)


State 257

   69 Element1: Key . ':'

    ':'  posunout a přejít do stavu 341


State 258

  166 LiteralValue: '{' LiteralValue1 . '}'

    '}'  posunout a přejít do stavu 342


State 259

    7 ArrayLength: Expression .  [']']
   87 Expression: Expression . BINARY_OP UnaryExpr
  146 Index: '[' Expression . ']'
  252 Slice11: Expression .  [':']

    BINARY_OP  posunout a přejít do stavu 191
    ']'        posunout a přejít do stavu 343

    ']'         [reduce using rule 7 (ArrayLength)]
    $výchozí  reduce using rule 252 (Slice11)


State 260

  248 Slice: '[' Slice1 .

    $výchozí  reduce using rule 248 (Slice)


State 261

  250 Slice1: Slice11 . ':' Slice12

    ':'  posunout a přejít do stavu 344


State 262

   32 Call1: ArgumentList . Call11
   33 Call11: . %empty  [')']
   34       | . ','

    ','  posunout a přejít do stavu 345

    $výchozí  reduce using rule 33 (Call11)

    Call11  přejít do stavu 346


State 263

   30 Call: '(' Call1 . ')'

    ')'  posunout a přejít do stavu 347


State 264

   87 Expression: Expression . BINARY_OP UnaryExpr
   88 ExpressionList: Expression . ExpressionList1
   89 ExpressionList1: . %empty  [TOK4, ',', ')']
   90                | . ExpressionList1 ',' Expression
  182 Operand: '(' Expression . ')'

    BINARY_OP  posunout a přejít do stavu 191
    ')'        posunout a přejít do stavu 246

    ')'         [reduce using rule 89 (ExpressionList1)]
    $výchozí  reduce using rule 89 (ExpressionList1)

    ExpressionList1  přejít do stavu 223


State 265

  236 Selector: '.' IDENTIFIER .

    $výchozí  reduce using rule 236 (Selector)


State 266

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  295 TypeAssertion: '.' '(' . Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 348
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 267

  249 Slice: Slice2 ']' .

    $výchozí  reduce using rule 249 (Slice)


State 268

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  255 Slice2: Slice21 ':' . Expression ':' Expression
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 349
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 269

  175 MethodExpr: ReceiverType '.' MethodName .

    $výchozí  reduce using rule 175 (MethodExpr)


State 270

   60 Conversion: Type '(' Expression . Conversion1 ')'
   61 Conversion1: . %empty  [')']
   62            | . ','
   87 Expression: Expression . BINARY_OP UnaryExpr

    BINARY_OP  posunout a přejít do stavu 191
    ','        posunout a přejít do stavu 350

    $výchozí  reduce using rule 61 (Conversion1)

    Conversion1  přejít do stavu 351


State 271

  214 Receiver: '(' Receiver1 Receiver2 BaseTypeName ')' .

    $výchozí  reduce using rule 214 (Receiver)


State 272

  122 IdentifierList: . IDENTIFIER IdentifierList1
  187 ParameterDecl: . ParameterDecl1 ParameterDecl2 Type
  188 ParameterDecl1: . %empty  [IDENTIFIER, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']
  189               | . IdentifierList
  194 ParameterList1: ParameterList1 ',' . ParameterDecl

    IDENTIFIER  posunout a přejít do stavu 35

    IDENTIFIER  [reduce using rule 188 (ParameterDecl1)]
    $výchozí  reduce using rule 188 (ParameterDecl1)

    IdentifierList  přejít do stavu 150
    ParameterDecl   přejít do stavu 352
    ParameterDecl1  přejít do stavu 152


State 273

  187 ParameterDecl: ParameterDecl1 ParameterDecl2 Type .

    $výchozí  reduce using rule 187 (ParameterDecl)


State 274

   17 Block: '{' StatementList '}' .

    $výchozí  reduce using rule 17 (Block)


State 275

   25 BuiltinCall: IDENTIFIER . '(' BuiltinCall1 ')'
  122 IdentifierList: IDENTIFIER . IdentifierList1
  123 IdentifierList1: . %empty  [TOK3, ',']
  124                | . IdentifierList1 ',' IDENTIFIER
  155 Label: IDENTIFIER .  [':']
  183 OperandName: IDENTIFIER .  [ASSIGN_OP, BINARY_OP, FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK1, TOK2, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', ',', '(', ':', ';', '.']
  186 PackageName: IDENTIFIER .  ['.']
  313 TypeName: IDENTIFIER .  ['{', '(', '.']

    '('  posunout a přejít do stavu 180

    TOK3        reduce using rule 123 (IdentifierList1)
    '{'         reduce using rule 313 (TypeName)
    ','         reduce using rule 123 (IdentifierList1)
    ','         [reduce using rule 183 (OperandName)]
    '('         [reduce using rule 183 (OperandName)]
    '('         [reduce using rule 313 (TypeName)]
    ':'         reduce using rule 155 (Label)
    ':'         [reduce using rule 183 (OperandName)]
    '.'         reduce using rule 183 (OperandName)
    '.'         [reduce using rule 186 (PackageName)]
    '.'         [reduce using rule 313 (TypeName)]
    $výchozí  reduce using rule 183 (OperandName)

    IdentifierList1  přejít do stavu 55


State 276

   18 BreakStmt: BREAK . BreakStmt1
   19 BreakStmt1: . %empty  [';']
   20           | . Label
  155 Label: . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 353

    $výchozí  reduce using rule 19 (BreakStmt1)

    BreakStmt1  přejít do stavu 354
    Label       přejít do stavu 355


State 277

   57 ContinueStmt: CONTINUE . ContinueStmt1
   58 ContinueStmt1: . %empty  [';']
   59              | . Label
  155 Label: . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 353

    $výchozí  reduce using rule 58 (ContinueStmt1)

    ContinueStmt1  přejít do stavu 356
    Label          přejít do stavu 357


State 278

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   66 DeferStmt: DEFER . Expression
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 358
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 279

   92 FallthroughStmt: FALLTHROUGH .

    $výchozí  reduce using rule 92 (FallthroughStmt)


State 280

    8 ArrayType: . '[' ArrayLength ']' ElementType
    9 Assignment: . ExpressionList ASSIGN_OP ExpressionList
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   35 Channel: . Expression
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   46 Condition: . Expression
   60 Conversion: . Type '(' Expression Conversion1 ')'
   75 EmptyStmt: . %empty  [';']
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
   91 ExpressionStmt: . Expression
   99 ForClause: . ForClause1 ';' ForClause2 ';' ForClause3
  100 ForClause1: . %empty  [';']
  101           | . InitStmt
  106 ForStmt: FOR . ForStmt1 Block
  107 ForStmt1: . %empty  ['{']
  108         | . ForStmt11
  109 ForStmt11: . Condition
  110          | . ForClause
  111          | . RangeClause
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  122 IdentifierList: . IDENTIFIER IdentifierList1
  143 IncDecStmt: . Expression IncDecStmt1
  147 InitStmt: . SimpleStmt
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  211 RangeClause: . RangeClause1 RANGE Expression
  212 RangeClause1: . ExpressionList '='
  213             | . IdentifierList TOK3
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  237 SendStmt: . Channel TOK5 Expression
  238 ShortVarDecl: . IdentifierList TOK3 ExpressionList
  242 SimpleStmt: . EmptyStmt
  243           | . ExpressionStmt
  244           | . SendStmt
  245           | . IncDecStmt
  246           | . Assignment
  247           | . ShortVarDecl
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 359
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    '{'         reduce using rule 107 (ForStmt1)
    ';'         reduce using rule 75 (EmptyStmt)
    ';'         [reduce using rule 100 (ForClause1)]
    $výchozí  reduce using rule 75 (EmptyStmt)

    ArrayType       přejít do stavu 122
    Assignment      přejít do stavu 287
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    Channel         přejít do stavu 290
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Condition       přejít do stavu 360
    Conversion      přejít do stavu 126
    EmptyStmt       přejít do stavu 294
    Expression      přejít do stavu 361
    ExpressionList  přejít do stavu 362
    ExpressionStmt  přejít do stavu 298
    ForClause       přejít do stavu 363
    ForClause1      přejít do stavu 364
    ForStmt1        přejít do stavu 365
    ForStmt11       přejít do stavu 366
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    IdentifierList  přejít do stavu 367
    IncDecStmt      přejít do stavu 305
    InitStmt        přejít do stavu 368
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    RangeClause     přejít do stavu 369
    RangeClause1    přejít do stavu 370
    ReceiverType    přejít do stavu 137
    SendStmt        přejít do stavu 310
    ShortVarDecl    přejít do stavu 311
    SimpleStmt      přejít do stavu 371
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 281

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  120 GoStmt: GO . Expression
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 372
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 282

  121 GotoStmt: GOTO . Label
  155 Label: . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 353

    Label  přejít do stavu 373


State 283

    8 ArrayType: . '[' ArrayLength ']' ElementType
    9 Assignment: . ExpressionList ASSIGN_OP ExpressionList
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   35 Channel: . Expression
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   75 EmptyStmt: . %empty  [';']
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
   91 ExpressionStmt: . Expression
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  122 IdentifierList: . IDENTIFIER IdentifierList1
  125 IfStmt: IF . IfStmt1 Expression Block IfStmt2
  126 IfStmt1: . %empty  [FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']
  127        | . SimpleStmt ';'
  143 IncDecStmt: . Expression IncDecStmt1
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  237 SendStmt: . Channel TOK5 Expression
  238 ShortVarDecl: . IdentifierList TOK3 ExpressionList
  242 SimpleStmt: . EmptyStmt
  243           | . ExpressionStmt
  244           | . SendStmt
  245           | . IncDecStmt
  246           | . Assignment
  247           | . ShortVarDecl
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 359
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    FLOAT_LIT      [reduce using rule 126 (IfStmt1)]
    IDENTIFIER     [reduce using rule 126 (IfStmt1)]
    IMAGINARY_LIT  [reduce using rule 126 (IfStmt1)]
    INT_LIT        [reduce using rule 126 (IfStmt1)]
    RUNE_LIT       [reduce using rule 126 (IfStmt1)]
    STRING_LIT     [reduce using rule 126 (IfStmt1)]
    UNARY_OP       [reduce using rule 126 (IfStmt1)]
    TOK5           [reduce using rule 126 (IfStmt1)]
    CHAN           [reduce using rule 126 (IfStmt1)]
    FUNC           [reduce using rule 126 (IfStmt1)]
    INTERFACE      [reduce using rule 126 (IfStmt1)]
    MAP            [reduce using rule 126 (IfStmt1)]
    STRUCT         [reduce using rule 126 (IfStmt1)]
    '*'            [reduce using rule 126 (IfStmt1)]
    '['            [reduce using rule 126 (IfStmt1)]
    '('            [reduce using rule 126 (IfStmt1)]
    $výchozí     reduce using rule 75 (EmptyStmt)

    ArrayType       přejít do stavu 122
    Assignment      přejít do stavu 287
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    Channel         přejít do stavu 290
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    EmptyStmt       přejít do stavu 294
    Expression      přejít do stavu 296
    ExpressionList  přejít do stavu 297
    ExpressionStmt  přejít do stavu 298
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    IdentifierList  přejít do stavu 303
    IfStmt1         přejít do stavu 374
    IncDecStmt      přejít do stavu 305
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SendStmt        přejít do stavu 310
    ShortVarDecl    přejít do stavu 311
    SimpleStmt      přejít do stavu 375
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 284

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  230 ReturnStmt: RETURN . ReturnStmt1
  231 ReturnStmt1: . %empty  [';']
  232            | . ExpressionList
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    $výchozí  reduce using rule 231 (ReturnStmt1)

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 376
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    ReturnStmt1     přejít do stavu 377
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 285

  233 SelectStmt: SELECT . '{' SelectStmt1 '}'

    '{'  posunout a přejít do stavu 378


State 286

    8 ArrayType: . '[' ArrayLength ']' ElementType
    9 Assignment: . ExpressionList ASSIGN_OP ExpressionList
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   35 Channel: . Expression
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   75 EmptyStmt: . %empty  [';']
   79 ExprSwitchStmt: SWITCH . ExprSwitchStmt1 ExprSwitchStmt2 '{' ExprSwitchStmt3 '}'
   80 ExprSwitchStmt1: . %empty  [FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '{', '(']
   81                | . SimpleStmt ';'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
   91 ExpressionStmt: . Expression
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  122 IdentifierList: . IDENTIFIER IdentifierList1
  143 IncDecStmt: . Expression IncDecStmt1
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  237 SendStmt: . Channel TOK5 Expression
  238 ShortVarDecl: . IdentifierList TOK3 ExpressionList
  242 SimpleStmt: . EmptyStmt
  243           | . ExpressionStmt
  244           | . SendStmt
  245           | . IncDecStmt
  246           | . Assignment
  247           | . ShortVarDecl
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  321 TypeSwitchStmt: SWITCH . TypeSwitchStmt1 TypeSwitchGuard '{' TypeSwitchStmt2 '}'
  322 TypeSwitchStmt1: . %empty  [FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']
  323                | . SimpleStmt ';'
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 359
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    FLOAT_LIT      [reduce using rule 80 (ExprSwitchStmt1)]
    FLOAT_LIT      [reduce using rule 322 (TypeSwitchStmt1)]
    IDENTIFIER     [reduce using rule 80 (ExprSwitchStmt1)]
    IDENTIFIER     [reduce using rule 322 (TypeSwitchStmt1)]
    IMAGINARY_LIT  [reduce using rule 80 (ExprSwitchStmt1)]
    IMAGINARY_LIT  [reduce using rule 322 (TypeSwitchStmt1)]
    INT_LIT        [reduce using rule 80 (ExprSwitchStmt1)]
    INT_LIT        [reduce using rule 322 (TypeSwitchStmt1)]
    RUNE_LIT       [reduce using rule 80 (ExprSwitchStmt1)]
    RUNE_LIT       [reduce using rule 322 (TypeSwitchStmt1)]
    STRING_LIT     [reduce using rule 80 (ExprSwitchStmt1)]
    STRING_LIT     [reduce using rule 322 (TypeSwitchStmt1)]
    UNARY_OP       [reduce using rule 80 (ExprSwitchStmt1)]
    TOK5           [reduce using rule 80 (ExprSwitchStmt1)]
    TOK5           [reduce using rule 322 (TypeSwitchStmt1)]
    CHAN           [reduce using rule 80 (ExprSwitchStmt1)]
    CHAN           [reduce using rule 322 (TypeSwitchStmt1)]
    FUNC           [reduce using rule 80 (ExprSwitchStmt1)]
    FUNC           [reduce using rule 322 (TypeSwitchStmt1)]
    INTERFACE      [reduce using rule 80 (ExprSwitchStmt1)]
    INTERFACE      [reduce using rule 322 (TypeSwitchStmt1)]
    MAP            [reduce using rule 80 (ExprSwitchStmt1)]
    MAP            [reduce using rule 322 (TypeSwitchStmt1)]
    STRUCT         [reduce using rule 80 (ExprSwitchStmt1)]
    STRUCT         [reduce using rule 322 (TypeSwitchStmt1)]
    '*'            [reduce using rule 80 (ExprSwitchStmt1)]
    '*'            [reduce using rule 322 (TypeSwitchStmt1)]
    '['            [reduce using rule 80 (ExprSwitchStmt1)]
    '['            [reduce using rule 322 (TypeSwitchStmt1)]
    '{'            reduce using rule 80 (ExprSwitchStmt1)
    '('            [reduce using rule 80 (ExprSwitchStmt1)]
    '('            [reduce using rule 322 (TypeSwitchStmt1)]
    $výchozí     reduce using rule 75 (EmptyStmt)

    ArrayType        přejít do stavu 122
    Assignment       přejít do stavu 287
    BasicLit         přejít do stavu 123
    BuiltinCall      přejít do stavu 124
    Channel          přejít do stavu 290
    ChannelType      přejít do stavu 68
    ChannelType1     přejít do stavu 69
    CompositeLit     přejít do stavu 125
    Conversion       přejít do stavu 126
    EmptyStmt        přejít do stavu 294
    ExprSwitchStmt1  přejít do stavu 379
    Expression       přejít do stavu 296
    ExpressionList   přejít do stavu 297
    ExpressionStmt   přejít do stavu 298
    FunctionLit      přejít do stavu 128
    FunctionType     přejít do stavu 72
    IdentifierList   přejít do stavu 303
    IncDecStmt       přejít do stavu 305
    InterfaceType    přejít do stavu 73
    Literal          přejít do stavu 129
    LiteralType      přejít do stavu 130
    MapType          přejít do stavu 131
    MethodExpr       přejít do stavu 132
    Operand          přejít do stavu 133
    OperandName      přejít do stavu 134
    PackageName      přejít do stavu 75
    PointerType      přejít do stavu 76
    PrimaryExpr      přejít do stavu 135
    QualifiedIdent   přejít do stavu 136
    ReceiverType     přejít do stavu 137
    SendStmt         přejít do stavu 310
    ShortVarDecl     přejít do stavu 311
    SimpleStmt       přejít do stavu 380
    SliceType        přejít do stavu 138
    StructType       přejít do stavu 139
    Type             přejít do stavu 140
    TypeLit          přejít do stavu 81
    TypeName         přejít do stavu 141
    TypeSwitchStmt1  přejít do stavu 381
    UnaryExpr        přejít do stavu 142


State 287

  246 SimpleStmt: Assignment .

    $výchozí  reduce using rule 246 (SimpleStmt)


State 288

  274 Statement: Block .

    $výchozí  reduce using rule 274 (Statement)


State 289

  270 Statement: BreakStmt .

    $výchozí  reduce using rule 270 (Statement)


State 290

  237 SendStmt: Channel . TOK5 Expression

    TOK5  posunout a přejít do stavu 382


State 291

  271 Statement: ContinueStmt .

    $výchozí  reduce using rule 271 (Statement)


State 292

  265 Statement: Declaration .

    $výchozí  reduce using rule 265 (Statement)


State 293

  279 Statement: DeferStmt .

    $výchozí  reduce using rule 279 (Statement)


State 294

  242 SimpleStmt: EmptyStmt .

    $výchozí  reduce using rule 242 (SimpleStmt)


State 295

  286 SwitchStmt: ExprSwitchStmt .

    $výchozí  reduce using rule 286 (SwitchStmt)


State 296

   35 Channel: Expression .  [TOK5]
   87 Expression: Expression . BINARY_OP UnaryExpr
   88 ExpressionList: Expression . ExpressionList1
   89 ExpressionList1: . %empty  [ASSIGN_OP, ',']
   90                | . ExpressionList1 ',' Expression
   91 ExpressionStmt: Expression .  ['{', ';']
  143 IncDecStmt: Expression . IncDecStmt1
  144 IncDecStmt1: . TOK2
  145            | . TOK1

    BINARY_OP  posunout a přejít do stavu 191
    TOK1       posunout a přejít do stavu 383
    TOK2       posunout a přejít do stavu 384

    TOK5        reduce using rule 35 (Channel)
    '{'         reduce using rule 91 (ExpressionStmt)
    ';'         reduce using rule 91 (ExpressionStmt)
    $výchozí  reduce using rule 89 (ExpressionList1)

    ExpressionList1  přejít do stavu 223
    IncDecStmt1      přejít do stavu 385


State 297

    9 Assignment: ExpressionList . ASSIGN_OP ExpressionList

    ASSIGN_OP  posunout a přejít do stavu 386


State 298

  243 SimpleStmt: ExpressionStmt .

    $výchozí  reduce using rule 243 (SimpleStmt)


State 299

  273 Statement: FallthroughStmt .

    $výchozí  reduce using rule 273 (Statement)


State 300

  278 Statement: ForStmt .

    $výchozí  reduce using rule 278 (Statement)


State 301

  268 Statement: GoStmt .

    $výchozí  reduce using rule 268 (Statement)


State 302

  272 Statement: GotoStmt .

    $výchozí  reduce using rule 272 (Statement)


State 303

  238 ShortVarDecl: IdentifierList . TOK3 ExpressionList

    TOK3  posunout a přejít do stavu 387


State 304

  275 Statement: IfStmt .

    $výchozí  reduce using rule 275 (Statement)


State 305

  245 SimpleStmt: IncDecStmt .

    $výchozí  reduce using rule 245 (SimpleStmt)


State 306

  156 LabeledStmt: Label . ':' Statement

    ':'  posunout a přejít do stavu 388


State 307

  266 Statement: LabeledStmt .

    $výchozí  reduce using rule 266 (Statement)


State 308

  269 Statement: ReturnStmt .

    $výchozí  reduce using rule 269 (Statement)


State 309

  277 Statement: SelectStmt .

    $výchozí  reduce using rule 277 (Statement)


State 310

  244 SimpleStmt: SendStmt .

    $výchozí  reduce using rule 244 (SimpleStmt)


State 311

  247 SimpleStmt: ShortVarDecl .

    $výchozí  reduce using rule 247 (SimpleStmt)


State 312

  267 Statement: SimpleStmt .

    $výchozí  reduce using rule 267 (Statement)


State 313

  282 StatementList1: StatementList1 Statement . ';'

    ';'  posunout a přejít do stavu 389


State 314

  276 Statement: SwitchStmt .

    $výchozí  reduce using rule 276 (Statement)


State 315

  287 SwitchStmt: TypeSwitchStmt .

    $výchozí  reduce using rule 287 (SwitchStmt)


State 316

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   90 ExpressionList1: ExpressionList1 ',' . Expression
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 390
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 317

  177 MethodSpec: MethodName Signature .

    $výchozí  reduce using rule 177 (MethodSpec)


State 318

  150 InterfaceType1: InterfaceType1 MethodSpec ';' .

    $výchozí  reduce using rule 150 (InterfaceType1)


State 319

  171 MapType: MAP '[' KeyType ']' ElementType .

    $výchozí  reduce using rule 171 (MapType)


State 320

    1 AnonymousField: AnonymousField1 TypeName .

    $výchozí  reduce using rule 1 (AnonymousField)


State 321

  285 StructType1: StructType1 FieldDecl ';' .

    $výchozí  reduce using rule 285 (StructType1)


State 322

  288 Tag: STRING_LIT .

    $výchozí  reduce using rule 288 (Tag)


State 323

   93 FieldDecl: FieldDecl1 FieldDecl2 .

    $výchozí  reduce using rule 93 (FieldDecl)


State 324

   97 FieldDecl2: Tag .

    $výchozí  reduce using rule 97 (FieldDecl2)


State 325

   94 FieldDecl1: IdentifierList Type .

    $výchozí  reduce using rule 94 (FieldDecl1)


State 326

   29 BuiltinCall11: ',' .

    $výchozí  reduce using rule 29 (BuiltinCall11)


State 327

   27 BuiltinCall1: BuiltinArgs BuiltinCall11 .

    $výchozí  reduce using rule 27 (BuiltinCall1)


State 328

   25 BuiltinCall: IDENTIFIER '(' BuiltinCall1 ')' .

    $výchozí  reduce using rule 25 (BuiltinCall)


State 329

    6 ArgumentList1: TOK4 .

    $výchozí  reduce using rule 6 (ArgumentList1)


State 330

    4 ArgumentList: ExpressionList ArgumentList1 .

    $výchozí  reduce using rule 4 (ArgumentList)


State 331

    4 ArgumentList: . ExpressionList ArgumentList1
    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   24 BuiltinArgs1: ',' . ArgumentList
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArgumentList    přejít do stavu 391
    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 242
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 332

   21 BuiltinArgs: Type BuiltinArgs1 .

    $výchozí  reduce using rule 21 (BuiltinArgs)


State 333

  162 LiteralType: '[' TOK4 ']' ElementType .

    $výchozí  reduce using rule 162 (LiteralType)


State 334

  220 ReceiverType: '(' '*' TypeName ')' .

    $výchozí  reduce using rule 220 (ReceiverType)


State 335

   71 ElementList: Element ElementList1 .  ['}', ',']
   73 ElementList1: ElementList1 . ',' Element

    ','  posunout a přejít do stavu 392

    ','         [reduce using rule 71 (ElementList)]
    $výchozí  reduce using rule 71 (ElementList)


State 336

   87 Expression: Expression . BINARY_OP UnaryExpr
  328 Value: Expression .  ['}', ',']

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 328 (Value)


State 337

  329 Value: LiteralValue .

    $výchozí  reduce using rule 329 (Value)


State 338

   67 Element: Element1 Value .

    $výchozí  reduce using rule 67 (Element)


State 339

  170 LiteralValue11: ',' .

    $výchozí  reduce using rule 170 (LiteralValue11)


State 340

  168 LiteralValue1: ElementList LiteralValue11 .

    $výchozí  reduce using rule 168 (LiteralValue1)


State 341

   69 Element1: Key ':' .

    $výchozí  reduce using rule 69 (Element1)


State 342

  166 LiteralValue: '{' LiteralValue1 '}' .

    $výchozí  reduce using rule 166 (LiteralValue)


State 343

  146 Index: '[' Expression ']' .

    $výchozí  reduce using rule 146 (Index)


State 344

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  250 Slice1: Slice11 ':' . Slice12
  253 Slice12: . %empty  [ASSIGN_OP, BINARY_OP, FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK1, TOK2, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', ']', '{', '}', ',', '(', ')', ':', ';', '=', '.']
  254        | . Expression
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    FLOAT_LIT      [reduce using rule 253 (Slice12)]
    IDENTIFIER     [reduce using rule 253 (Slice12)]
    IMAGINARY_LIT  [reduce using rule 253 (Slice12)]
    INT_LIT        [reduce using rule 253 (Slice12)]
    RUNE_LIT       [reduce using rule 253 (Slice12)]
    STRING_LIT     [reduce using rule 253 (Slice12)]
    UNARY_OP       [reduce using rule 253 (Slice12)]
    TOK5           [reduce using rule 253 (Slice12)]
    CHAN           [reduce using rule 253 (Slice12)]
    FUNC           [reduce using rule 253 (Slice12)]
    INTERFACE      [reduce using rule 253 (Slice12)]
    MAP            [reduce using rule 253 (Slice12)]
    STRUCT         [reduce using rule 253 (Slice12)]
    '*'            [reduce using rule 253 (Slice12)]
    '['            [reduce using rule 253 (Slice12)]
    '('            [reduce using rule 253 (Slice12)]
    $výchozí     reduce using rule 253 (Slice12)

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 393
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    Slice12         přejít do stavu 394
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 345

   34 Call11: ',' .

    $výchozí  reduce using rule 34 (Call11)


State 346

   32 Call1: ArgumentList Call11 .

    $výchozí  reduce using rule 32 (Call1)


State 347

   30 Call: '(' Call1 ')' .

    $výchozí  reduce using rule 30 (Call)


State 348

  295 TypeAssertion: '.' '(' Type . ')'

    ')'  posunout a přejít do stavu 395


State 349

   87 Expression: Expression . BINARY_OP UnaryExpr
  255 Slice2: Slice21 ':' Expression . ':' Expression

    BINARY_OP  posunout a přejít do stavu 191
    ':'        posunout a přejít do stavu 396


State 350

   62 Conversion1: ',' .

    $výchozí  reduce using rule 62 (Conversion1)


State 351

   60 Conversion: Type '(' Expression Conversion1 . ')'

    ')'  posunout a přejít do stavu 397


State 352

  194 ParameterList1: ParameterList1 ',' ParameterDecl .

    $výchozí  reduce using rule 194 (ParameterList1)


State 353

  155 Label: IDENTIFIER .

    $výchozí  reduce using rule 155 (Label)


State 354

   18 BreakStmt: BREAK BreakStmt1 .

    $výchozí  reduce using rule 18 (BreakStmt)


State 355

   20 BreakStmt1: Label .

    $výchozí  reduce using rule 20 (BreakStmt1)


State 356

   57 ContinueStmt: CONTINUE ContinueStmt1 .

    $výchozí  reduce using rule 57 (ContinueStmt)


State 357

   59 ContinueStmt1: Label .

    $výchozí  reduce using rule 59 (ContinueStmt1)


State 358

   66 DeferStmt: DEFER Expression .  [';']
   87 Expression: Expression . BINARY_OP UnaryExpr

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 66 (DeferStmt)


State 359

   25 BuiltinCall: IDENTIFIER . '(' BuiltinCall1 ')'
  122 IdentifierList: IDENTIFIER . IdentifierList1
  123 IdentifierList1: . %empty  [TOK3, ',']
  124                | . IdentifierList1 ',' IDENTIFIER
  183 OperandName: IDENTIFIER .  [ASSIGN_OP, BINARY_OP, FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK1, TOK2, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '{', ',', '(', ':', ';', '=', '.']
  186 PackageName: IDENTIFIER .  ['.']
  313 TypeName: IDENTIFIER .  ['{', '(', '.']

    '('  posunout a přejít do stavu 180

    TOK3        reduce using rule 123 (IdentifierList1)
    '{'         reduce using rule 183 (OperandName)
    '{'         [reduce using rule 313 (TypeName)]
    ','         reduce using rule 123 (IdentifierList1)
    ','         [reduce using rule 183 (OperandName)]
    '('         [reduce using rule 183 (OperandName)]
    '('         [reduce using rule 313 (TypeName)]
    '.'         reduce using rule 183 (OperandName)
    '.'         [reduce using rule 186 (PackageName)]
    '.'         [reduce using rule 313 (TypeName)]
    $výchozí  reduce using rule 183 (OperandName)

    IdentifierList1  přejít do stavu 55


State 360

  109 ForStmt11: Condition .

    $výchozí  reduce using rule 109 (ForStmt11)


State 361

   35 Channel: Expression .  [TOK5]
   46 Condition: Expression .  ['{']
   87 Expression: Expression . BINARY_OP UnaryExpr
   88 ExpressionList: Expression . ExpressionList1
   89 ExpressionList1: . %empty  [ASSIGN_OP, ',', '=']
   90                | . ExpressionList1 ',' Expression
   91 ExpressionStmt: Expression .  [';']
  143 IncDecStmt: Expression . IncDecStmt1
  144 IncDecStmt1: . TOK2
  145            | . TOK1

    BINARY_OP  posunout a přejít do stavu 191
    TOK1       posunout a přejít do stavu 383
    TOK2       posunout a přejít do stavu 384

    TOK5        reduce using rule 35 (Channel)
    '{'         reduce using rule 46 (Condition)
    ';'         reduce using rule 91 (ExpressionStmt)
    $výchozí  reduce using rule 89 (ExpressionList1)

    ExpressionList1  přejít do stavu 223
    IncDecStmt1      přejít do stavu 385


State 362

    9 Assignment: ExpressionList . ASSIGN_OP ExpressionList
  212 RangeClause1: ExpressionList . '='

    ASSIGN_OP  posunout a přejít do stavu 386
    '='        posunout a přejít do stavu 398


State 363

  110 ForStmt11: ForClause .

    $výchozí  reduce using rule 110 (ForStmt11)


State 364

   99 ForClause: ForClause1 . ';' ForClause2 ';' ForClause3

    ';'  posunout a přejít do stavu 399


State 365

   17 Block: . '{' StatementList '}'
  106 ForStmt: FOR ForStmt1 . Block

    '{'  posunout a přejít do stavu 160

    Block  přejít do stavu 400


State 366

  108 ForStmt1: ForStmt11 .

    $výchozí  reduce using rule 108 (ForStmt1)


State 367

  213 RangeClause1: IdentifierList . TOK3
  238 ShortVarDecl: IdentifierList . TOK3 ExpressionList

    TOK3  posunout a přejít do stavu 401


State 368

  101 ForClause1: InitStmt .

    $výchozí  reduce using rule 101 (ForClause1)


State 369

  111 ForStmt11: RangeClause .

    $výchozí  reduce using rule 111 (ForStmt11)


State 370

  211 RangeClause: RangeClause1 . RANGE Expression

    RANGE  posunout a přejít do stavu 402


State 371

  147 InitStmt: SimpleStmt .

    $výchozí  reduce using rule 147 (InitStmt)


State 372

   87 Expression: Expression . BINARY_OP UnaryExpr
  120 GoStmt: GO Expression .  [';']

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 120 (GoStmt)


State 373

  121 GotoStmt: GOTO Label .

    $výchozí  reduce using rule 121 (GotoStmt)


State 374

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  125 IfStmt: IF IfStmt1 . Expression Block IfStmt2
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 403
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 375

  127 IfStmt1: SimpleStmt . ';'

    ';'  posunout a přejít do stavu 404


State 376

  232 ReturnStmt1: ExpressionList .

    $výchozí  reduce using rule 232 (ReturnStmt1)


State 377

  230 ReturnStmt: RETURN ReturnStmt1 .

    $výchozí  reduce using rule 230 (ReturnStmt)


State 378

  233 SelectStmt: SELECT '{' . SelectStmt1 '}'
  234 SelectStmt1: . %empty
  235            | . SelectStmt1 CommClause

    $výchozí  reduce using rule 234 (SelectStmt1)

    SelectStmt1  přejít do stavu 405


State 379

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   79 ExprSwitchStmt: SWITCH ExprSwitchStmt1 . ExprSwitchStmt2 '{' ExprSwitchStmt3 '}'
   82 ExprSwitchStmt2: . %empty  ['{']
   83                | . Expression
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    $výchozí  reduce using rule 82 (ExprSwitchStmt2)

    ArrayType        přejít do stavu 122
    BasicLit         přejít do stavu 123
    BuiltinCall      přejít do stavu 124
    ChannelType      přejít do stavu 68
    ChannelType1     přejít do stavu 69
    CompositeLit     přejít do stavu 125
    Conversion       přejít do stavu 126
    ExprSwitchStmt2  přejít do stavu 406
    Expression       přejít do stavu 407
    FunctionLit      přejít do stavu 128
    FunctionType     přejít do stavu 72
    InterfaceType    přejít do stavu 73
    Literal          přejít do stavu 129
    LiteralType      přejít do stavu 130
    MapType          přejít do stavu 131
    MethodExpr       přejít do stavu 132
    Operand          přejít do stavu 133
    OperandName      přejít do stavu 134
    PackageName      přejít do stavu 75
    PointerType      přejít do stavu 76
    PrimaryExpr      přejít do stavu 135
    QualifiedIdent   přejít do stavu 136
    ReceiverType     přejít do stavu 137
    SliceType        přejít do stavu 138
    StructType       přejít do stavu 139
    Type             přejít do stavu 140
    TypeLit          přejít do stavu 81
    TypeName         přejít do stavu 141
    UnaryExpr        přejít do stavu 142


State 380

   81 ExprSwitchStmt1: SimpleStmt . ';'
  323 TypeSwitchStmt1: SimpleStmt . ';'

    ';'  posunout a přejít do stavu 408


State 381

  318 TypeSwitchGuard: . TypeSwitchGuard1 PrimaryExpr '.' '(' TYPE ')'
  319 TypeSwitchGuard1: . %empty  [FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']
  320                 | . IDENTIFIER TOK3
  321 TypeSwitchStmt: SWITCH TypeSwitchStmt1 . TypeSwitchGuard '{' TypeSwitchStmt2 '}'

    IDENTIFIER  posunout a přejít do stavu 409

    IDENTIFIER  [reduce using rule 319 (TypeSwitchGuard1)]
    $výchozí  reduce using rule 319 (TypeSwitchGuard1)

    TypeSwitchGuard   přejít do stavu 410
    TypeSwitchGuard1  přejít do stavu 411


State 382

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  237 SendStmt: Channel TOK5 . Expression
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 412
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 383

  145 IncDecStmt1: TOK1 .

    $výchozí  reduce using rule 145 (IncDecStmt1)


State 384

  144 IncDecStmt1: TOK2 .

    $výchozí  reduce using rule 144 (IncDecStmt1)


State 385

  143 IncDecStmt: Expression IncDecStmt1 .

    $výchozí  reduce using rule 143 (IncDecStmt)


State 386

    8 ArrayType: . '[' ArrayLength ']' ElementType
    9 Assignment: ExpressionList ASSIGN_OP . ExpressionList
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 413
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 387

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  238 ShortVarDecl: IdentifierList TOK3 . ExpressionList
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 414
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 388

    8 ArrayType: . '[' ArrayLength ']' ElementType
    9 Assignment: . ExpressionList ASSIGN_OP ExpressionList
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   17 Block: . '{' StatementList '}'
   18 BreakStmt: . BREAK BreakStmt1
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   35 Channel: . Expression
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   47 ConstDecl: . CONST ConstDecl1
   57 ContinueStmt: . CONTINUE ContinueStmt1
   60 Conversion: . Type '(' Expression Conversion1 ')'
   63 Declaration: . ConstDecl
   64            | . TypeDecl
   65            | . VarDecl
   66 DeferStmt: . DEFER Expression
   75 EmptyStmt: . %empty  [';']
   79 ExprSwitchStmt: . SWITCH ExprSwitchStmt1 ExprSwitchStmt2 '{' ExprSwitchStmt3 '}'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
   91 ExpressionStmt: . Expression
   92 FallthroughStmt: . FALLTHROUGH
  106 ForStmt: . FOR ForStmt1 Block
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  120 GoStmt: . GO Expression
  121 GotoStmt: . GOTO Label
  122 IdentifierList: . IDENTIFIER IdentifierList1
  125 IfStmt: . IF IfStmt1 Expression Block IfStmt2
  143 IncDecStmt: . Expression IncDecStmt1
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  155 Label: . IDENTIFIER
  156 LabeledStmt: . Label ':' Statement
  156            | Label ':' . Statement
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  230 ReturnStmt: . RETURN ReturnStmt1
  233 SelectStmt: . SELECT '{' SelectStmt1 '}'
  237 SendStmt: . Channel TOK5 Expression
  238 ShortVarDecl: . IdentifierList TOK3 ExpressionList
  242 SimpleStmt: . EmptyStmt
  243           | . ExpressionStmt
  244           | . SendStmt
  245           | . IncDecStmt
  246           | . Assignment
  247           | . ShortVarDecl
  258 SliceType: . '[' ']' ElementType
  265 Statement: . Declaration
  266          | . LabeledStmt
  267          | . SimpleStmt
  268          | . GoStmt
  269          | . ReturnStmt
  270          | . BreakStmt
  271          | . ContinueStmt
  272          | . GotoStmt
  273          | . FallthroughStmt
  274          | . Block
  275          | . IfStmt
  276          | . SwitchStmt
  277          | . SelectStmt
  278          | . ForStmt
  279          | . DeferStmt
  283 StructType: . STRUCT '{' StructType1 '}'
  286 SwitchStmt: . ExprSwitchStmt
  287           | . TypeSwitchStmt
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  297 TypeDecl: . TYPE TypeDecl1
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  321 TypeSwitchStmt: . SWITCH TypeSwitchStmt1 TypeSwitchGuard '{' TypeSwitchStmt2 '}'
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr
  330 VarDecl: . VAR VarDecl1

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 275
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    BREAK          posunout a přejít do stavu 276
    CHAN           posunout a přejít do stavu 59
    CONST          posunout a přejít do stavu 21
    CONTINUE       posunout a přejít do stavu 277
    DEFER          posunout a přejít do stavu 278
    FALLTHROUGH    posunout a přejít do stavu 279
    FOR            posunout a přejít do stavu 280
    FUNC           posunout a přejít do stavu 117
    GO             posunout a přejít do stavu 281
    GOTO           posunout a přejít do stavu 282
    IF             posunout a přejít do stavu 283
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    RETURN         posunout a přejít do stavu 284
    SELECT         posunout a přejít do stavu 285
    STRUCT         posunout a přejít do stavu 63
    SWITCH         posunout a přejít do stavu 286
    TYPE           posunout a přejít do stavu 23
    VAR            posunout a přejít do stavu 24
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '{'            posunout a přejít do stavu 160
    '('            posunout a přejít do stavu 120

    $výchozí  reduce using rule 75 (EmptyStmt)

    ArrayType        přejít do stavu 122
    Assignment       přejít do stavu 287
    BasicLit         přejít do stavu 123
    Block            přejít do stavu 288
    BreakStmt        přejít do stavu 289
    BuiltinCall      přejít do stavu 124
    Channel          přejít do stavu 290
    ChannelType      přejít do stavu 68
    ChannelType1     přejít do stavu 69
    CompositeLit     přejít do stavu 125
    ConstDecl        přejít do stavu 25
    ContinueStmt     přejít do stavu 291
    Conversion       přejít do stavu 126
    Declaration      přejít do stavu 292
    DeferStmt        přejít do stavu 293
    EmptyStmt        přejít do stavu 294
    ExprSwitchStmt   přejít do stavu 295
    Expression       přejít do stavu 296
    ExpressionList   přejít do stavu 297
    ExpressionStmt   přejít do stavu 298
    FallthroughStmt  přejít do stavu 299
    ForStmt          přejít do stavu 300
    FunctionLit      přejít do stavu 128
    FunctionType     přejít do stavu 72
    GoStmt           přejít do stavu 301
    GotoStmt         přejít do stavu 302
    IdentifierList   přejít do stavu 303
    IfStmt           přejít do stavu 304
    IncDecStmt       přejít do stavu 305
    InterfaceType    přejít do stavu 73
    Label            přejít do stavu 306
    LabeledStmt      přejít do stavu 307
    Literal          přejít do stavu 129
    LiteralType      přejít do stavu 130
    MapType          přejít do stavu 131
    MethodExpr       přejít do stavu 132
    Operand          přejít do stavu 133
    OperandName      přejít do stavu 134
    PackageName      přejít do stavu 75
    PointerType      přejít do stavu 76
    PrimaryExpr      přejít do stavu 135
    QualifiedIdent   přejít do stavu 136
    ReceiverType     přejít do stavu 137
    ReturnStmt       přejít do stavu 308
    SelectStmt       přejít do stavu 309
    SendStmt         přejít do stavu 310
    ShortVarDecl     přejít do stavu 311
    SimpleStmt       přejít do stavu 312
    SliceType        přejít do stavu 138
    Statement        přejít do stavu 415
    StructType       přejít do stavu 139
    SwitchStmt       přejít do stavu 314
    Type             přejít do stavu 140
    TypeDecl         přejít do stavu 30
    TypeLit          přejít do stavu 81
    TypeName         přejít do stavu 141
    TypeSwitchStmt   přejít do stavu 315
    UnaryExpr        přejít do stavu 142
    VarDecl          přejít do stavu 31


State 389

  282 StatementList1: StatementList1 Statement ';' .

    $výchozí  reduce using rule 282 (StatementList1)


State 390

   87 Expression: Expression . BINARY_OP UnaryExpr
   90 ExpressionList1: ExpressionList1 ',' Expression .  [ASSIGN_OP, TOK4, '{', ',', ')', ':', ';', '=']

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 90 (ExpressionList1)


State 391

   24 BuiltinArgs1: ',' ArgumentList .

    $výchozí  reduce using rule 24 (BuiltinArgs1)


State 392

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   67 Element: . Element1 Value
   68 Element1: . %empty  [FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '{', '(']
   69         | . Key ':'
   70 ElementIndex: . Expression
   73 ElementList1: ElementList1 ',' . Element
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   98 FieldName: . IDENTIFIER
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  152 Key: . FieldName
  153    | . ElementIndex
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 250
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    FLOAT_LIT      [reduce using rule 68 (Element1)]
    IDENTIFIER     [reduce using rule 68 (Element1)]
    IMAGINARY_LIT  [reduce using rule 68 (Element1)]
    INT_LIT        [reduce using rule 68 (Element1)]
    RUNE_LIT       [reduce using rule 68 (Element1)]
    STRING_LIT     [reduce using rule 68 (Element1)]
    UNARY_OP       [reduce using rule 68 (Element1)]
    TOK5           [reduce using rule 68 (Element1)]
    CHAN           [reduce using rule 68 (Element1)]
    FUNC           [reduce using rule 68 (Element1)]
    INTERFACE      [reduce using rule 68 (Element1)]
    MAP            [reduce using rule 68 (Element1)]
    STRUCT         [reduce using rule 68 (Element1)]
    '*'            [reduce using rule 68 (Element1)]
    '['            [reduce using rule 68 (Element1)]
    '('            [reduce using rule 68 (Element1)]
    $výchozí     reduce using rule 68 (Element1)

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Element         přejít do stavu 416
    Element1        přejít do stavu 252
    ElementIndex    přejít do stavu 253
    Expression      přejít do stavu 255
    FieldName       přejít do stavu 256
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Key             přejít do stavu 257
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 393

   87 Expression: Expression . BINARY_OP UnaryExpr
  254 Slice12: Expression .  [ASSIGN_OP, BINARY_OP, FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK1, TOK2, TOK4, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', ']', '{', '}', ',', '(', ')', ':', ';', '=', '.']

    BINARY_OP  posunout a přejít do stavu 191

    BINARY_OP   [reduce using rule 254 (Slice12)]
    $výchozí  reduce using rule 254 (Slice12)


State 394

  250 Slice1: Slice11 ':' Slice12 .

    $výchozí  reduce using rule 250 (Slice1)


State 395

  295 TypeAssertion: '.' '(' Type ')' .

    $výchozí  reduce using rule 295 (TypeAssertion)


State 396

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  255 Slice2: Slice21 ':' Expression ':' . Expression
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 417
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 397

   60 Conversion: Type '(' Expression Conversion1 ')' .

    $výchozí  reduce using rule 60 (Conversion)


State 398

  212 RangeClause1: ExpressionList '=' .

    $výchozí  reduce using rule 212 (RangeClause1)


State 399

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   46 Condition: . Expression
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   99 ForClause: ForClause1 ';' . ForClause2 ';' ForClause3
  102 ForClause2: . %empty  [';']
  103           | . Condition
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    $výchozí  reduce using rule 102 (ForClause2)

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Condition       přejít do stavu 418
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 419
    ForClause2      přejít do stavu 420
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 400

  106 ForStmt: FOR ForStmt1 Block .

    $výchozí  reduce using rule 106 (ForStmt)


State 401

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  213 RangeClause1: IdentifierList TOK3 .  [RANGE]
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  238 ShortVarDecl: IdentifierList TOK3 . ExpressionList
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    $výchozí  reduce using rule 213 (RangeClause1)

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 414
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 402

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  211 RangeClause: RangeClause1 RANGE . Expression
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 421
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 403

   17 Block: . '{' StatementList '}'
   87 Expression: Expression . BINARY_OP UnaryExpr
  125 IfStmt: IF IfStmt1 Expression . Block IfStmt2

    BINARY_OP  posunout a přejít do stavu 191
    '{'        posunout a přejít do stavu 160

    Block  přejít do stavu 422


State 404

  127 IfStmt1: SimpleStmt ';' .

    $výchozí  reduce using rule 127 (IfStmt1)


State 405

   40 CommCase: . CASE CommCase1
   41         | . DEFAULT
   44 CommClause: . CommCase ':' StatementList
  233 SelectStmt: SELECT '{' SelectStmt1 . '}'
  235 SelectStmt1: SelectStmt1 . CommClause

    CASE     posunout a přejít do stavu 423
    DEFAULT  posunout a přejít do stavu 424
    '}'      posunout a přejít do stavu 425

    CommCase    přejít do stavu 426
    CommClause  přejít do stavu 427


State 406

   79 ExprSwitchStmt: SWITCH ExprSwitchStmt1 ExprSwitchStmt2 . '{' ExprSwitchStmt3 '}'

    '{'  posunout a přejít do stavu 428


State 407

   83 ExprSwitchStmt2: Expression .  ['{']
   87 Expression: Expression . BINARY_OP UnaryExpr

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 83 (ExprSwitchStmt2)


State 408

   81 ExprSwitchStmt1: SimpleStmt ';' .  [FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '{', '(']
  323 TypeSwitchStmt1: SimpleStmt ';' .  [FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']

    FLOAT_LIT      reduce using rule 81 (ExprSwitchStmt1)
    FLOAT_LIT      [reduce using rule 323 (TypeSwitchStmt1)]
    IDENTIFIER     reduce using rule 81 (ExprSwitchStmt1)
    IDENTIFIER     [reduce using rule 323 (TypeSwitchStmt1)]
    IMAGINARY_LIT  reduce using rule 81 (ExprSwitchStmt1)
    IMAGINARY_LIT  [reduce using rule 323 (TypeSwitchStmt1)]
    INT_LIT        reduce using rule 81 (ExprSwitchStmt1)
    INT_LIT        [reduce using rule 323 (TypeSwitchStmt1)]
    RUNE_LIT       reduce using rule 81 (ExprSwitchStmt1)
    RUNE_LIT       [reduce using rule 323 (TypeSwitchStmt1)]
    STRING_LIT     reduce using rule 81 (ExprSwitchStmt1)
    STRING_LIT     [reduce using rule 323 (TypeSwitchStmt1)]
    TOK5           reduce using rule 81 (ExprSwitchStmt1)
    TOK5           [reduce using rule 323 (TypeSwitchStmt1)]
    CHAN           reduce using rule 81 (ExprSwitchStmt1)
    CHAN           [reduce using rule 323 (TypeSwitchStmt1)]
    FUNC           reduce using rule 81 (ExprSwitchStmt1)
    FUNC           [reduce using rule 323 (TypeSwitchStmt1)]
    INTERFACE      reduce using rule 81 (ExprSwitchStmt1)
    INTERFACE      [reduce using rule 323 (TypeSwitchStmt1)]
    MAP            reduce using rule 81 (ExprSwitchStmt1)
    MAP            [reduce using rule 323 (TypeSwitchStmt1)]
    STRUCT         reduce using rule 81 (ExprSwitchStmt1)
    STRUCT         [reduce using rule 323 (TypeSwitchStmt1)]
    '*'            reduce using rule 81 (ExprSwitchStmt1)
    '*'            [reduce using rule 323 (TypeSwitchStmt1)]
    '['            reduce using rule 81 (ExprSwitchStmt1)
    '['            [reduce using rule 323 (TypeSwitchStmt1)]
    '('            reduce using rule 81 (ExprSwitchStmt1)
    '('            [reduce using rule 323 (TypeSwitchStmt1)]
    $výchozí     reduce using rule 81 (ExprSwitchStmt1)


State 409

  320 TypeSwitchGuard1: IDENTIFIER . TOK3

    TOK3  posunout a přejít do stavu 429


State 410

  321 TypeSwitchStmt: SWITCH TypeSwitchStmt1 TypeSwitchGuard . '{' TypeSwitchStmt2 '}'

    '{'  posunout a přejít do stavu 430


State 411

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  318 TypeSwitchGuard: TypeSwitchGuard1 . PrimaryExpr '.' '(' TYPE ')'

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 431
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141


State 412

   87 Expression: Expression . BINARY_OP UnaryExpr
  237 SendStmt: Channel TOK5 Expression .  ['{', ':', ';']

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 237 (SendStmt)


State 413

    9 Assignment: ExpressionList ASSIGN_OP ExpressionList .

    $výchozí  reduce using rule 9 (Assignment)


State 414

  238 ShortVarDecl: IdentifierList TOK3 ExpressionList .

    $výchozí  reduce using rule 238 (ShortVarDecl)


State 415

  156 LabeledStmt: Label ':' Statement .

    $výchozí  reduce using rule 156 (LabeledStmt)


State 416

   73 ElementList1: ElementList1 ',' Element .

    $výchozí  reduce using rule 73 (ElementList1)


State 417

   87 Expression: Expression . BINARY_OP UnaryExpr
  255 Slice2: Slice21 ':' Expression ':' Expression .  [']']

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 255 (Slice2)


State 418

  103 ForClause2: Condition .

    $výchozí  reduce using rule 103 (ForClause2)


State 419

   46 Condition: Expression .  [';']
   87 Expression: Expression . BINARY_OP UnaryExpr

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 46 (Condition)


State 420

   99 ForClause: ForClause1 ';' ForClause2 . ';' ForClause3

    ';'  posunout a přejít do stavu 432


State 421

   87 Expression: Expression . BINARY_OP UnaryExpr
  211 RangeClause: RangeClause1 RANGE Expression .  ['{']

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 211 (RangeClause)


State 422

  125 IfStmt: IF IfStmt1 Expression Block . IfStmt2
  128 IfStmt2: . %empty  [';']
  129        | . ELSE IfStmt21

    ELSE  posunout a přejít do stavu 433

    $výchozí  reduce using rule 128 (IfStmt2)

    IfStmt2  přejít do stavu 434


State 423

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   35 Channel: . Expression
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   40 CommCase: CASE . CommCase1
   42 CommCase1: . SendStmt
   43          | . RecvStmt
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  122 IdentifierList: . IDENTIFIER IdentifierList1
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  223 RecvStmt: . RecvStmt1 RecvExpr
  224 RecvStmt1: . %empty  [FLOAT_LIT, IDENTIFIER, IMAGINARY_LIT, INT_LIT, RUNE_LIT, STRING_LIT, UNARY_OP, TOK5, CHAN, FUNC, INTERFACE, MAP, STRUCT, '*', '[', '(']
  225          | . RecvStmt11
  226 RecvStmt11: . ExpressionList '='
  227           | . IdentifierList TOK3
  237 SendStmt: . Channel TOK5 Expression
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 359
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    FLOAT_LIT      [reduce using rule 224 (RecvStmt1)]
    IDENTIFIER     [reduce using rule 224 (RecvStmt1)]
    IMAGINARY_LIT  [reduce using rule 224 (RecvStmt1)]
    INT_LIT        [reduce using rule 224 (RecvStmt1)]
    RUNE_LIT       [reduce using rule 224 (RecvStmt1)]
    STRING_LIT     [reduce using rule 224 (RecvStmt1)]
    UNARY_OP       [reduce using rule 224 (RecvStmt1)]
    TOK5           [reduce using rule 224 (RecvStmt1)]
    CHAN           [reduce using rule 224 (RecvStmt1)]
    FUNC           [reduce using rule 224 (RecvStmt1)]
    INTERFACE      [reduce using rule 224 (RecvStmt1)]
    MAP            [reduce using rule 224 (RecvStmt1)]
    STRUCT         [reduce using rule 224 (RecvStmt1)]
    '*'            [reduce using rule 224 (RecvStmt1)]
    '['            [reduce using rule 224 (RecvStmt1)]
    '('            [reduce using rule 224 (RecvStmt1)]

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    Channel         přejít do stavu 290
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CommCase1       přejít do stavu 435
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 436
    ExpressionList  přejít do stavu 437
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    IdentifierList  přejít do stavu 438
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    RecvStmt        přejít do stavu 439
    RecvStmt1       přejít do stavu 440
    RecvStmt11      přejít do stavu 441
    SendStmt        přejít do stavu 442
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 424

   41 CommCase: DEFAULT .

    $výchozí  reduce using rule 41 (CommCase)


State 425

  233 SelectStmt: SELECT '{' SelectStmt1 '}' .

    $výchozí  reduce using rule 233 (SelectStmt)


State 426

   44 CommClause: CommCase . ':' StatementList

    ':'  posunout a přejít do stavu 443


State 427

  235 SelectStmt1: SelectStmt1 CommClause .

    $výchozí  reduce using rule 235 (SelectStmt1)


State 428

   79 ExprSwitchStmt: SWITCH ExprSwitchStmt1 ExprSwitchStmt2 '{' . ExprSwitchStmt3 '}'
   84 ExprSwitchStmt3: . %empty
   85                | . ExprSwitchStmt3 ExprCaseClause

    $výchozí  reduce using rule 84 (ExprSwitchStmt3)

    ExprSwitchStmt3  přejít do stavu 444


State 429

  320 TypeSwitchGuard1: IDENTIFIER TOK3 .

    $výchozí  reduce using rule 320 (TypeSwitchGuard1)


State 430

  321 TypeSwitchStmt: SWITCH TypeSwitchStmt1 TypeSwitchGuard '{' . TypeSwitchStmt2 '}'
  324 TypeSwitchStmt2: . %empty
  325                | . TypeSwitchStmt2 TypeCaseClause

    $výchozí  reduce using rule 324 (TypeSwitchStmt2)

    TypeSwitchStmt2  přejít do stavu 445


State 431

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   30 Call: . '(' Call1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  146 Index: . '[' Expression ']'
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  205            | PrimaryExpr . Selector
  206            | . PrimaryExpr Index
  206            | PrimaryExpr . Index
  207            | . PrimaryExpr Slice
  207            | PrimaryExpr . Slice
  208            | . PrimaryExpr TypeAssertion
  208            | PrimaryExpr . TypeAssertion
  209            | . PrimaryExpr Call
  209            | PrimaryExpr . Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  236 Selector: . '.' IDENTIFIER
  248 Slice: . '[' Slice1
  249      | . Slice2 ']'
  255 Slice2: . Slice21 ':' Expression ':' Expression
  256 Slice21: . %empty  [':']
  257        | . Expression
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  295 TypeAssertion: . '.' '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  318 TypeSwitchGuard: TypeSwitchGuard1 PrimaryExpr . '.' '(' TYPE ')'
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 194
    '('            posunout a přejít do stavu 195
    '.'            posunout a přejít do stavu 446

    $výchozí  reduce using rule 256 (Slice21)

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    Call            přejít do stavu 197
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 198
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    Index           přejít do stavu 199
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    Selector        přejít do stavu 200
    Slice           přejít do stavu 201
    Slice2          přejít do stavu 202
    Slice21         přejít do stavu 203
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeAssertion   přejít do stavu 204
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 432

    8 ArrayType: . '[' ArrayLength ']' ElementType
    9 Assignment: . ExpressionList ASSIGN_OP ExpressionList
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   35 Channel: . Expression
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   75 EmptyStmt: . %empty  ['{']
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
   91 ExpressionStmt: . Expression
   99 ForClause: ForClause1 ';' ForClause2 ';' . ForClause3
  104 ForClause3: . %empty  ['{']
  105           | . PostStmt
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  122 IdentifierList: . IDENTIFIER IdentifierList1
  143 IncDecStmt: . Expression IncDecStmt1
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  201 PostStmt: . SimpleStmt
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  237 SendStmt: . Channel TOK5 Expression
  238 ShortVarDecl: . IdentifierList TOK3 ExpressionList
  242 SimpleStmt: . EmptyStmt
  243           | . ExpressionStmt
  244           | . SendStmt
  245           | . IncDecStmt
  246           | . Assignment
  247           | . ShortVarDecl
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 359
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    '{'         reduce using rule 75 (EmptyStmt)
    '{'         [reduce using rule 104 (ForClause3)]
    $výchozí  reduce using rule 75 (EmptyStmt)

    ArrayType       přejít do stavu 122
    Assignment      přejít do stavu 287
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    Channel         přejít do stavu 290
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    EmptyStmt       přejít do stavu 294
    Expression      přejít do stavu 296
    ExpressionList  přejít do stavu 297
    ExpressionStmt  přejít do stavu 298
    ForClause3      přejít do stavu 447
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    IdentifierList  přejít do stavu 303
    IncDecStmt      přejít do stavu 305
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PostStmt        přejít do stavu 448
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SendStmt        přejít do stavu 310
    ShortVarDecl    přejít do stavu 311
    SimpleStmt      přejít do stavu 449
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 433

   17 Block: . '{' StatementList '}'
  125 IfStmt: . IF IfStmt1 Expression Block IfStmt2
  129 IfStmt2: ELSE . IfStmt21
  130 IfStmt21: . IfStmt
  131         | . Block

    IF   posunout a přejít do stavu 283
    '{'  posunout a přejít do stavu 160

    Block     přejít do stavu 450
    IfStmt    přejít do stavu 451
    IfStmt21  přejít do stavu 452


State 434

  125 IfStmt: IF IfStmt1 Expression Block IfStmt2 .

    $výchozí  reduce using rule 125 (IfStmt)


State 435

   40 CommCase: CASE CommCase1 .

    $výchozí  reduce using rule 40 (CommCase)


State 436

   35 Channel: Expression .  [TOK5]
   87 Expression: Expression . BINARY_OP UnaryExpr
   88 ExpressionList: Expression . ExpressionList1
   89 ExpressionList1: . %empty  [',', '=']
   90                | . ExpressionList1 ',' Expression

    BINARY_OP  posunout a přejít do stavu 191

    TOK5        reduce using rule 35 (Channel)
    $výchozí  reduce using rule 89 (ExpressionList1)

    ExpressionList1  přejít do stavu 223


State 437

  226 RecvStmt11: ExpressionList . '='

    '='  posunout a přejít do stavu 453


State 438

  227 RecvStmt11: IdentifierList . TOK3

    TOK3  posunout a přejít do stavu 454


State 439

   43 CommCase1: RecvStmt .

    $výchozí  reduce using rule 43 (CommCase1)


State 440

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  222 RecvExpr: . Expression
  223 RecvStmt: RecvStmt1 . RecvExpr
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 455
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    RecvExpr        přejít do stavu 456
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 441

  225 RecvStmt1: RecvStmt11 .

    $výchozí  reduce using rule 225 (RecvStmt1)


State 442

   42 CommCase1: SendStmt .

    $výchozí  reduce using rule 42 (CommCase1)


State 443

   44 CommClause: CommCase ':' . StatementList
  280 StatementList: . StatementList1
  281 StatementList1: . %empty
  282               | . StatementList1 Statement ';'

    $výchozí  reduce using rule 281 (StatementList1)

    StatementList   přejít do stavu 457
    StatementList1  přejít do stavu 220


State 444

   76 ExprCaseClause: . ExprSwitchCase ':' StatementList
   77 ExprSwitchCase: . CASE ExpressionList
   78               | . DEFAULT
   79 ExprSwitchStmt: SWITCH ExprSwitchStmt1 ExprSwitchStmt2 '{' ExprSwitchStmt3 . '}'
   85 ExprSwitchStmt3: ExprSwitchStmt3 . ExprCaseClause

    CASE     posunout a přejít do stavu 458
    DEFAULT  posunout a přejít do stavu 459
    '}'      posunout a přejít do stavu 460

    ExprCaseClause  přejít do stavu 461
    ExprSwitchCase  přejít do stavu 462


State 445

  296 TypeCaseClause: . TypeSwitchCase ':' StatementList
  316 TypeSwitchCase: . CASE TypeList
  317               | . DEFAULT
  321 TypeSwitchStmt: SWITCH TypeSwitchStmt1 TypeSwitchGuard '{' TypeSwitchStmt2 . '}'
  325 TypeSwitchStmt2: TypeSwitchStmt2 . TypeCaseClause

    CASE     posunout a přejít do stavu 463
    DEFAULT  posunout a přejít do stavu 464
    '}'      posunout a přejít do stavu 465

    TypeCaseClause  přejít do stavu 466
    TypeSwitchCase  přejít do stavu 467


State 446

  236 Selector: '.' . IDENTIFIER
  295 TypeAssertion: '.' . '(' Type ')'
  318 TypeSwitchGuard: TypeSwitchGuard1 PrimaryExpr '.' . '(' TYPE ')'

    IDENTIFIER  posunout a přejít do stavu 265
    '('         posunout a přejít do stavu 468


State 447

   99 ForClause: ForClause1 ';' ForClause2 ';' ForClause3 .

    $výchozí  reduce using rule 99 (ForClause)


State 448

  105 ForClause3: PostStmt .

    $výchozí  reduce using rule 105 (ForClause3)


State 449

  201 PostStmt: SimpleStmt .

    $výchozí  reduce using rule 201 (PostStmt)


State 450

  131 IfStmt21: Block .

    $výchozí  reduce using rule 131 (IfStmt21)


State 451

  130 IfStmt21: IfStmt .

    $výchozí  reduce using rule 130 (IfStmt21)


State 452

  129 IfStmt2: ELSE IfStmt21 .

    $výchozí  reduce using rule 129 (IfStmt2)


State 453

  226 RecvStmt11: ExpressionList '=' .

    $výchozí  reduce using rule 226 (RecvStmt11)


State 454

  227 RecvStmt11: IdentifierList TOK3 .

    $výchozí  reduce using rule 227 (RecvStmt11)


State 455

   87 Expression: Expression . BINARY_OP UnaryExpr
  222 RecvExpr: Expression .  [':']

    BINARY_OP  posunout a přejít do stavu 191

    $výchozí  reduce using rule 222 (RecvExpr)


State 456

  223 RecvStmt: RecvStmt1 RecvExpr .

    $výchozí  reduce using rule 223 (RecvStmt)


State 457

   44 CommClause: CommCase ':' StatementList .

    $výchozí  reduce using rule 44 (CommClause)


State 458

    8 ArrayType: . '[' ArrayLength ']' ElementType
   12 BasicLit: . INT_LIT
   13         | . FLOAT_LIT
   14         | . IMAGINARY_LIT
   15         | . RUNE_LIT
   16         | . STRING_LIT
   25 BuiltinCall: . IDENTIFIER '(' BuiltinCall1 ')'
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
   45 CompositeLit: . LiteralType LiteralValue
   60 Conversion: . Type '(' Expression Conversion1 ')'
   77 ExprSwitchCase: CASE . ExpressionList
   86 Expression: . UnaryExpr
   87           | . Expression BINARY_OP UnaryExpr
   88 ExpressionList: . Expression ExpressionList1
  117 FunctionLit: . FUNC Function
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  157 Literal: . BasicLit
  158        | . CompositeLit
  159        | . FunctionLit
  160 LiteralType: . StructType
  161            | . ArrayType
  162            | . '[' TOK4 ']' ElementType
  163            | . SliceType
  164            | . MapType
  165            | . TypeName
  171 MapType: . MAP '[' KeyType ']' ElementType
  175 MethodExpr: . ReceiverType '.' MethodName
  179 Operand: . Literal
  180        | . OperandName
  181        | . MethodExpr
  182        | . '(' Expression ')'
  183 OperandName: . IDENTIFIER
  184            | . QualifiedIdent
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  202 PrimaryExpr: . Operand
  203            | . Conversion
  204            | . BuiltinCall
  205            | . PrimaryExpr Selector
  206            | . PrimaryExpr Index
  207            | . PrimaryExpr Slice
  208            | . PrimaryExpr TypeAssertion
  209            | . PrimaryExpr Call
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  219 ReceiverType: . TypeName
  220             | . '(' '*' TypeName ')'
  221             | . '(' ReceiverType ')'
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  326 UnaryExpr: . PrimaryExpr
  327          | . UNARY_OP UnaryExpr

    FLOAT_LIT      posunout a přejít do stavu 110
    IDENTIFIER     posunout a přejít do stavu 111
    IMAGINARY_LIT  posunout a přejít do stavu 112
    INT_LIT        posunout a přejít do stavu 113
    RUNE_LIT       posunout a přejít do stavu 114
    STRING_LIT     posunout a přejít do stavu 115
    UNARY_OP       posunout a přejít do stavu 116
    TOK5           posunout a přejít do stavu 58
    CHAN           posunout a přejít do stavu 59
    FUNC           posunout a přejít do stavu 117
    INTERFACE      posunout a přejít do stavu 61
    MAP            posunout a přejít do stavu 62
    STRUCT         posunout a přejít do stavu 63
    '*'            posunout a přejít do stavu 64
    '['            posunout a přejít do stavu 118
    '('            posunout a přejít do stavu 120

    ArrayType       přejít do stavu 122
    BasicLit        přejít do stavu 123
    BuiltinCall     přejít do stavu 124
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    CompositeLit    přejít do stavu 125
    Conversion      přejít do stavu 126
    Expression      přejít do stavu 170
    ExpressionList  přejít do stavu 469
    FunctionLit     přejít do stavu 128
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    Literal         přejít do stavu 129
    LiteralType     přejít do stavu 130
    MapType         přejít do stavu 131
    MethodExpr      přejít do stavu 132
    Operand         přejít do stavu 133
    OperandName     přejít do stavu 134
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    PrimaryExpr     přejít do stavu 135
    QualifiedIdent  přejít do stavu 136
    ReceiverType    přejít do stavu 137
    SliceType       přejít do stavu 138
    StructType      přejít do stavu 139
    Type            přejít do stavu 140
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 141
    UnaryExpr       přejít do stavu 142


State 459

   78 ExprSwitchCase: DEFAULT .

    $výchozí  reduce using rule 78 (ExprSwitchCase)


State 460

   79 ExprSwitchStmt: SWITCH ExprSwitchStmt1 ExprSwitchStmt2 '{' ExprSwitchStmt3 '}' .

    $výchozí  reduce using rule 79 (ExprSwitchStmt)


State 461

   85 ExprSwitchStmt3: ExprSwitchStmt3 ExprCaseClause .

    $výchozí  reduce using rule 85 (ExprSwitchStmt3)


State 462

   76 ExprCaseClause: ExprSwitchCase . ':' StatementList

    ':'  posunout a přejít do stavu 470


State 463

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  302 TypeList: . Type TypeList1
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  316 TypeSwitchCase: CASE . TypeList

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 471
    TypeList        přejít do stavu 472
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 464

  317 TypeSwitchCase: DEFAULT .

    $výchozí  reduce using rule 317 (TypeSwitchCase)


State 465

  321 TypeSwitchStmt: SWITCH TypeSwitchStmt1 TypeSwitchGuard '{' TypeSwitchStmt2 '}' .

    $výchozí  reduce using rule 321 (TypeSwitchStmt)


State 466

  325 TypeSwitchStmt2: TypeSwitchStmt2 TypeCaseClause .

    $výchozí  reduce using rule 325 (TypeSwitchStmt2)


State 467

  296 TypeCaseClause: TypeSwitchCase . ':' StatementList

    ':'  posunout a přejít do stavu 473


State 468

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  295 TypeAssertion: '.' '(' . Type ')'
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent
  318 TypeSwitchGuard: TypeSwitchGuard1 PrimaryExpr '.' '(' . TYPE ')'

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    TYPE        posunout a přejít do stavu 474
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 348
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 469

   77 ExprSwitchCase: CASE ExpressionList .

    $výchozí  reduce using rule 77 (ExprSwitchCase)


State 470

   76 ExprCaseClause: ExprSwitchCase ':' . StatementList
  280 StatementList: . StatementList1
  281 StatementList1: . %empty
  282               | . StatementList1 Statement ';'

    $výchozí  reduce using rule 281 (StatementList1)

    StatementList   přejít do stavu 475
    StatementList1  přejít do stavu 220


State 471

  302 TypeList: Type . TypeList1
  303 TypeList1: . %empty
  304          | . TypeList1 ',' Type

    $výchozí  reduce using rule 303 (TypeList1)

    TypeList1  přejít do stavu 476


State 472

  316 TypeSwitchCase: CASE TypeList .

    $výchozí  reduce using rule 316 (TypeSwitchCase)


State 473

  280 StatementList: . StatementList1
  281 StatementList1: . %empty
  282               | . StatementList1 Statement ';'
  296 TypeCaseClause: TypeSwitchCase ':' . StatementList

    $výchozí  reduce using rule 281 (StatementList1)

    StatementList   přejít do stavu 477
    StatementList1  přejít do stavu 220


State 474

  318 TypeSwitchGuard: TypeSwitchGuard1 PrimaryExpr '.' '(' TYPE . ')'

    ')'  posunout a přejít do stavu 478


State 475

   76 ExprCaseClause: ExprSwitchCase ':' StatementList .

    $výchozí  reduce using rule 76 (ExprCaseClause)


State 476

  302 TypeList: Type TypeList1 .  [':']
  304 TypeList1: TypeList1 . ',' Type

    ','  posunout a přejít do stavu 479

    $výchozí  reduce using rule 302 (TypeList)


State 477

  296 TypeCaseClause: TypeSwitchCase ':' StatementList .

    $výchozí  reduce using rule 296 (TypeCaseClause)


State 478

  318 TypeSwitchGuard: TypeSwitchGuard1 PrimaryExpr '.' '(' TYPE ')' .

    $výchozí  reduce using rule 318 (TypeSwitchGuard)


State 479

    8 ArrayType: . '[' ArrayLength ']' ElementType
   36 ChannelType: . ChannelType1 ElementType
   37 ChannelType1: . CHAN
   38             | . CHAN TOK5
   39             | . TOK5 CHAN
  119 FunctionType: . FUNC Signature
  148 InterfaceType: . INTERFACE '{' InterfaceType1 '}'
  171 MapType: . MAP '[' KeyType ']' ElementType
  186 PackageName: . IDENTIFIER
  200 PointerType: . '*' BaseType
  210 QualifiedIdent: . PackageName '.' IDENTIFIER
  258 SliceType: . '[' ']' ElementType
  283 StructType: . STRUCT '{' StructType1 '}'
  292 Type: . TypeName
  293     | . TypeLit
  294     | . '(' Type ')'
  304 TypeList1: TypeList1 ',' . Type
  305 TypeLit: . ArrayType
  306        | . StructType
  307        | . PointerType
  308        | . FunctionType
  309        | . InterfaceType
  310        | . SliceType
  311        | . MapType
  312        | . ChannelType
  313 TypeName: . IDENTIFIER
  314         | . QualifiedIdent

    IDENTIFIER  posunout a přejít do stavu 57
    TOK5        posunout a přejít do stavu 58
    CHAN        posunout a přejít do stavu 59
    FUNC        posunout a přejít do stavu 60
    INTERFACE   posunout a přejít do stavu 61
    MAP         posunout a přejít do stavu 62
    STRUCT      posunout a přejít do stavu 63
    '*'         posunout a přejít do stavu 64
    '['         posunout a přejít do stavu 65
    '('         posunout a přejít do stavu 66

    ArrayType       přejít do stavu 67
    ChannelType     přejít do stavu 68
    ChannelType1    přejít do stavu 69
    FunctionType    přejít do stavu 72
    InterfaceType   přejít do stavu 73
    MapType         přejít do stavu 74
    PackageName     přejít do stavu 75
    PointerType     přejít do stavu 76
    QualifiedIdent  přejít do stavu 77
    SliceType       přejít do stavu 78
    StructType      přejít do stavu 79
    Type            přejít do stavu 480
    TypeLit         přejít do stavu 81
    TypeName        přejít do stavu 82


State 480

  304 TypeList1: TypeList1 ',' Type .

    $výchozí  reduce using rule 304 (TypeList1)
