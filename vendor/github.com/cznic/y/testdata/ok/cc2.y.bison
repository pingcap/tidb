Terminals unused in grammar

   PPHEADER_NAME
   PPNUMBER
   PPPASTE


Gramatika

    0 $accept: Start $end

    1 AbstractDeclarator: Pointer
    2                   | PointerOpt DirectAbstractDeclarator

    3 AbstractDeclaratorOpt: %empty
    4                      | AbstractDeclarator

    5 AdditiveExpression: MultiplicativeExpression
    6                   | AdditiveExpression '+' MultiplicativeExpression
    7                   | AdditiveExpression '-' MultiplicativeExpression

    8 AndExpression: EqualityExpression
    9              | AndExpression '&' EqualityExpression

   10 ArgumentExpressionList: AssignmentExpression
   11                       | ArgumentExpressionList ',' AssignmentExpression

   12 ArgumentExpressionListOpt: %empty
   13                          | ArgumentExpressionList

   14 AssignmentExpression: ConditionalExpression
   15                     | UnaryExpression AssignmentOperator AssignmentExpression

   16 AssignmentExpressionOpt: %empty
   17                        | AssignmentExpression

   18 AssignmentOperator: '='
   19                   | MULASSIGN
   20                   | DIVASSIGN
   21                   | MODASSIGN
   22                   | ADDASSIGN
   23                   | SUBASSIGN
   24                   | LSHASSIGN
   25                   | RSHASSIGN
   26                   | ANDASSIGN
   27                   | XORASSIGN
   28                   | ORASSIGN

   29 BlockItem: Declaration
   30          | Statement

   31 BlockItemList: BlockItem
   32              | BlockItemList BlockItem

   33 BlockItemListOpt: %empty
   34                 | BlockItemList

   35 CastExpression: UnaryExpression
   36               | '(' TypeName ')' CastExpression

   37 CompoundStatement: '{' BlockItemListOpt '}'

   38 ConditionalExpression: LogicalOrExpression
   39                      | LogicalOrExpression '?' Expression ':' ConditionalExpression

   40 Constant: CHARCONST
   41         | FLOATCONST
   42         | INTCONST
   43         | LONGCHARCONST
   44         | LONGSTRINGLITERAL
   45         | STRINGLITERAL

   46 ConstantExpression: ConditionalExpression

   47 ControlLine: PPDEFINE IDENTIFIER ReplacementList
   48            | PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   51            | PPERROR PpTokenListOpt
   52            | PPHASH_NL
   53            | PPINCLUDE PpTokenList
   54            | PPLINE PpTokenList
   55            | PPPRAGMA PpTokenListOpt
   56            | PPUNDEF IDENTIFIER '\n'
   57            | PPASSERT PpTokenList
   58            | PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
   60            | PPIDENT PpTokenList
   61            | PPIMPORT PpTokenList
   62            | PPINCLUDE_NEXT PpTokenList
   63            | PPUNASSERT PpTokenList
   64            | PPWARNING PpTokenList

   65 Declaration: DeclarationSpecifiers InitDeclaratorListOpt ';'

   66 DeclarationList: Declaration
   67                | DeclarationList Declaration

   68 DeclarationListOpt: %empty
   69                   | DeclarationList

   70 DeclarationSpecifiers: StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | TypeSpecifier DeclarationSpecifiersOpt
   72                      | TypeQualifier DeclarationSpecifiersOpt
   73                      | FunctionSpecifier DeclarationSpecifiersOpt

   74 DeclarationSpecifiersOpt: %empty
   75                         | DeclarationSpecifiers

   76 Declarator: PointerOpt DirectDeclarator

   77 DeclaratorOpt: %empty
   78              | Declarator

   79 Designation: DesignatorList '='

   80 DesignationOpt: %empty
   81               | Designation

   82 Designator: '[' ConstantExpression ']'
   83           | '.' IDENTIFIER

   84 DesignatorList: Designator
   85               | DesignatorList Designator

   86 DirectAbstractDeclarator: '(' AbstractDeclarator ')'
   87                         | DirectAbstractDeclaratorOpt '[' AssignmentExpressionOpt ']'
   88                         | DirectAbstractDeclaratorOpt '[' TypeQualifierList AssignmentExpressionOpt ']'
   89                         | DirectAbstractDeclaratorOpt '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
   90                         | DirectAbstractDeclaratorOpt '[' TypeQualifierList STATIC AssignmentExpression ']'
   91                         | DirectAbstractDeclaratorOpt '[' '*' ']'
   92                         | '(' ParameterTypeListOpt ')'
   93                         | DirectAbstractDeclarator '(' ParameterTypeListOpt ')'

   94 DirectAbstractDeclaratorOpt: %empty
   95                            | DirectAbstractDeclarator

   96 DirectDeclarator: IDENTIFIER
   97                 | '(' Declarator ')'
   98                 | DirectDeclarator '[' TypeQualifierListOpt AssignmentExpressionOpt ']'
   99                 | DirectDeclarator '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
  100                 | DirectDeclarator '[' TypeQualifierList STATIC AssignmentExpression ']'
  101                 | DirectDeclarator '[' TypeQualifierListOpt '*' ']'
  102                 | DirectDeclarator '(' ParameterTypeList ')'
  103                 | DirectDeclarator '(' IdentifierListOpt ')'

  104 ElifGroup: PPELIF PpTokenList GroupListOpt

  105 ElifGroupList: ElifGroup
  106              | ElifGroupList ElifGroup

  107 ElifGroupListOpt: %empty
  108                 | ElifGroupList

  109 ElseGroup: PPELSE '\n' GroupListOpt

  110 ElseGroupOpt: %empty
  111             | ElseGroup

  112 EndifLine: PPENDIF PpTokenListOpt

  113 EnumSpecifier: ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | ENUM IDENTIFIER

  116 EnumerationConstant: IDENTIFIER

  117 Enumerator: EnumerationConstant
  118           | EnumerationConstant '=' ConstantExpression

  119 EnumeratorList: Enumerator
  120               | EnumeratorList ',' Enumerator

  121 EqualityExpression: RelationalExpression
  122                   | EqualityExpression EQ RelationalExpression
  123                   | EqualityExpression NEQ RelationalExpression

  124 ExclusiveOrExpression: AndExpression
  125                      | ExclusiveOrExpression '^' AndExpression

  126 Expression: AssignmentExpression
  127           | Expression ',' AssignmentExpression

  128 ExpressionOpt: %empty
  129              | Expression

  130 ExpressionStatement: ExpressionOpt ';'

  131 ExternalDeclaration: FunctionDefinition
  132                    | Declaration

  133 FunctionDefinition: DeclarationSpecifiers Declarator DeclarationListOpt CompoundStatement

  134 FunctionSpecifier: INLINE

  135 GroupList: GroupPart
  136          | GroupList GroupPart

  137 GroupListOpt: %empty
  138             | GroupList

  139 GroupPart: ControlLine
  140          | IfSection
  141          | PPNONDIRECTIVE PpTokenList
  142          | TextLine

  143 IdentifierList: IDENTIFIER
  144               | IdentifierList ',' IDENTIFIER

  145 IdentifierListOpt: %empty
  146                  | IdentifierList

  147 IdentifierOpt: %empty
  148              | IDENTIFIER

  149 IfGroup: PPIF PpTokenList GroupListOpt
  150        | PPIFDEF IDENTIFIER '\n' GroupListOpt
  151        | PPIFNDEF IDENTIFIER '\n' GroupListOpt

  152 IfSection: IfGroup ElifGroupListOpt ElseGroupOpt EndifLine

  153 InclusiveOrExpression: ExclusiveOrExpression
  154                      | InclusiveOrExpression '|' ExclusiveOrExpression

  155 InitDeclarator: Declarator
  156               | Declarator '=' Initializer

  157 InitDeclaratorList: InitDeclarator
  158                   | InitDeclaratorList ',' InitDeclarator

  159 InitDeclaratorListOpt: %empty
  160                      | InitDeclaratorList

  161 Initializer: AssignmentExpression
  162            | '{' InitializerList '}'
  163            | '{' InitializerList ',' '}'

  164 InitializerList: DesignationOpt Initializer
  165                | InitializerList ',' DesignationOpt Initializer

  166 IterationStatement: WHILE '(' Expression ')' Statement
  167                   | DO Statement WHILE '(' Expression ')' ';'
  168                   | FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement

  170 JumpStatement: GOTO IDENTIFIER ';'
  171              | CONTINUE ';'
  172              | BREAK ';'
  173              | RETURN ExpressionOpt ';'

  174 LabeledStatement: IDENTIFIER ':' Statement
  175                 | CASE ConstantExpression ':' Statement
  176                 | DEFAULT ':' Statement

  177 LogicalAndExpression: InclusiveOrExpression
  178                     | LogicalAndExpression ANDAND InclusiveOrExpression

  179 LogicalOrExpression: LogicalAndExpression
  180                    | LogicalOrExpression OROR LogicalAndExpression

  181 MultiplicativeExpression: CastExpression
  182                         | MultiplicativeExpression '*' CastExpression
  183                         | MultiplicativeExpression '/' CastExpression
  184                         | MultiplicativeExpression '%' CastExpression

  185 ParameterDeclaration: DeclarationSpecifiers Declarator
  186                     | DeclarationSpecifiers AbstractDeclaratorOpt

  187 ParameterList: ParameterDeclaration
  188              | ParameterList ',' ParameterDeclaration

  189 ParameterTypeList: ParameterList
  190                  | ParameterList ',' DDD

  191 ParameterTypeListOpt: %empty
  192                     | ParameterTypeList

  193 Pointer: '*' TypeQualifierListOpt
  194        | '*' TypeQualifierListOpt Pointer

  195 PointerOpt: %empty
  196           | Pointer

  197 PostfixExpression: PrimaryExpression
  198                  | PostfixExpression '[' Expression ']'
  199                  | PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | PostfixExpression '.' IDENTIFIER
  201                  | PostfixExpression ARROW IDENTIFIER
  202                  | PostfixExpression INC
  203                  | PostfixExpression DEC
  204                  | '(' TypeName ')' '{' InitializerList '}'
  205                  | '(' TypeName ')' '{' InitializerList ',' '}'

  206 PpTokenList: PpTokens '\n'

  207 PpTokenListOpt: '\n'
  208               | PpTokenList

  209 PpTokens: PPOTHER
  210         | PpTokens PPOTHER

  211 PreprocessingFile: GroupList

  212 PrimaryExpression: IDENTIFIER
  213                  | Constant
  214                  | '(' Expression ')'

  215 RelationalExpression: ShiftExpression
  216                     | RelationalExpression '<' ShiftExpression
  217                     | RelationalExpression '>' ShiftExpression
  218                     | RelationalExpression LEQ ShiftExpression
  219                     | RelationalExpression GEQ ShiftExpression

  220 ReplacementList: PpTokenListOpt

  221 SelectionStatement: IF '(' Expression ')' Statement
  222                   | IF '(' Expression ')' Statement ELSE Statement
  223                   | SWITCH '(' Expression ')' Statement

  224 ShiftExpression: AdditiveExpression
  225                | ShiftExpression LSH AdditiveExpression
  226                | ShiftExpression RSH AdditiveExpression

  227 SpecifierQualifierList: TypeSpecifier SpecifierQualifierListOpt
  228                       | TypeQualifier SpecifierQualifierListOpt

  229 SpecifierQualifierListOpt: %empty
  230                          | SpecifierQualifierList

  231 Start: PREPROCESSINGFILE PreprocessingFile
  232      | TRANSLATIONUNIT TranslationUnit

  233 Statement: LabeledStatement
  234          | CompoundStatement
  235          | ExpressionStatement
  236          | SelectionStatement
  237          | IterationStatement
  238          | JumpStatement

  239 StorageClassSpecifier: TYPEDEF
  240                      | EXTERN
  241                      | STATIC
  242                      | AUTO
  243                      | REGISTER

  244 StructDeclaration: SpecifierQualifierList StructDeclaratorList ';'

  245 StructDeclarationList: StructDeclaration
  246                      | StructDeclarationList StructDeclaration

  247 StructDeclarator: Declarator
  248                 | DeclaratorOpt ':' ConstantExpression

  249 StructDeclaratorList: StructDeclarator
  250                     | StructDeclaratorList ',' StructDeclarator

  251 StructOrUnion: STRUCT
  252              | UNION

  253 StructOrUnionSpecifier: StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | StructOrUnion IDENTIFIER

  255 TextLine: PpTokenListOpt

  256 TranslationUnit: ExternalDeclaration
  257                | TranslationUnit ExternalDeclaration

  258 TypeName: SpecifierQualifierList AbstractDeclaratorOpt

  259 TypeQualifier: CONST
  260              | RESTRICT
  261              | VOLATILE

  262 TypeQualifierList: TypeQualifier
  263                  | TypeQualifierList TypeQualifier

  264 TypeQualifierListOpt: %empty
  265                     | TypeQualifierList

  266 TypeSpecifier: VOID
  267              | CHAR
  268              | SHORT
  269              | INT
  270              | LONG
  271              | FLOAT
  272              | DOUBLE
  273              | SIGNED
  274              | UNSIGNED
  275              | BOOL
  276              | COMPLEX
  277              | StructOrUnionSpecifier
  278              | EnumSpecifier
  279              | TYPEDEFNAME

  280 UnaryExpression: PostfixExpression
  281                | INC UnaryExpression
  282                | DEC UnaryExpression
  283                | UnaryOperator CastExpression
  284                | SIZEOF UnaryExpression
  285                | SIZEOF '(' TypeName ')'

  286 UnaryOperator: '&'
  287              | '*'
  288              | '+'
  289              | '-'
  290              | '~'
  291              | '!'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'\n' (10) 56 109 150 151 206 207
'!' (33) 291
'%' (37) 184
'&' (38) 9 286
'(' (40) 36 86 92 93 97 102 103 166 167 168 169 199 204 205 214 221
    222 223 285
')' (41) 36 48 49 50 58 59 86 92 93 97 102 103 166 167 168 169 199
    204 205 214 221 222 223 285
'*' (42) 91 101 182 193 194 287
'+' (43) 6 288
',' (44) 11 49 59 114 120 127 144 158 163 165 188 190 205 250
'-' (45) 7 289
'.' (46) 83 200
'/' (47) 183
':' (58) 39 174 175 176 248
';' (59) 65 130 167 168 169 170 171 172 173 244
'<' (60) 216
'=' (61) 18 79 118 156
'>' (62) 217
'?' (63) 39
'[' (91) 82 87 88 89 90 91 98 99 100 101 198
']' (93) 82 87 88 89 90 91 98 99 100 101 198
'^' (94) 125
'{' (123) 37 113 114 162 163 204 205 253
'|' (124) 154
'}' (125) 37 113 114 162 163 204 205 253
'~' (126) 290
error (256)
NOELSE (258)
ADDASSIGN (259) 22
ANDAND (260) 178
ANDASSIGN (261) 26
ARROW (262) 201
AUTO (263) 242
BOOL (264) 275
BREAK (265) 172
CASE (266) 175
CHAR (267) 267
CHARCONST (268) 40
COMPLEX (269) 276
CONST (270) 259
CONTINUE (271) 171
DDD (272) 48 49 58 59 190
DEC (273) 203 282
DEFAULT (274) 176
DIVASSIGN (275) 20
DO (276) 167
DOUBLE (277) 272
ELSE (278) 222
ENUM (279) 113 114 115
EQ (280) 122
EXTERN (281) 240
FLOAT (282) 271
FLOATCONST (283) 41
FOR (284) 168 169
GEQ (285) 219
GOTO (286) 170
IDENTIFIER (287) 47 56 58 59 83 96 115 116 143 144 148 150 151 170
    174 200 201 212 254
IDENTIFIER_LPAREN (288) 48 49 50 58 59
IF (289) 221 222
INC (290) 202 281
INLINE (291) 134
INT (292) 269
INTCONST (293) 42
LEQ (294) 218
LONG (295) 270
LONGCHARCONST (296) 43
LONGSTRINGLITERAL (297) 44
LSH (298) 225
LSHASSIGN (299) 24
MODASSIGN (300) 21
MULASSIGN (301) 19
NEQ (302) 123
ORASSIGN (303) 28
OROR (304) 180
PPASSERT (305) 57
PPDEFINE (306) 47 48 49 50 58 59
PPELIF (307) 104
PPELSE (308) 109
PPENDIF (309) 112
PPERROR (310) 51
PPHASH_NL (311) 52
PPHEADER_NAME (312)
PPIDENT (313) 60
PPIF (314) 149
PPIFDEF (315) 150
PPIFNDEF (316) 151
PPIMPORT (317) 61
PPINCLUDE (318) 53
PPINCLUDE_NEXT (319) 62
PPLINE (320) 54
PPNONDIRECTIVE (321) 141
PPNUMBER (322)
PPOTHER (323) 209 210
PPPASTE (324)
PPPRAGMA (325) 55
PPUNASSERT (326) 63
PPUNDEF (327) 56
PPWARNING (328) 64
PREPROCESSINGFILE (329) 231
REGISTER (330) 243
RESTRICT (331) 260
RETURN (332) 173
RSH (333) 226
RSHASSIGN (334) 25
SHORT (335) 268
SIGNED (336) 273
SIZEOF (337) 284 285
STATIC (338) 89 90 99 100 241
STRINGLITERAL (339) 45
STRUCT (340) 251
SUBASSIGN (341) 23
SWITCH (342) 223
TRANSLATIONUNIT (343) 232
TYPEDEF (344) 239
TYPEDEFNAME (345) 279
UNION (346) 252
UNSIGNED (347) 274
VOID (348) 266
VOLATILE (349) 261
WHILE (350) 166 167
XORASSIGN (351) 27


Neterminály s pravidly, ve kterých se objevují

$accept (122)
    vlevo: 0
AbstractDeclarator (123)
    vlevo: 1 2, vpravo: 4 86
AbstractDeclaratorOpt (124)
    vlevo: 3 4, vpravo: 186 258
AdditiveExpression (125)
    vlevo: 5 6 7, vpravo: 6 7 224 225 226
AndExpression (126)
    vlevo: 8 9, vpravo: 9 124 125
ArgumentExpressionList (127)
    vlevo: 10 11, vpravo: 11 13
ArgumentExpressionListOpt (128)
    vlevo: 12 13, vpravo: 199
AssignmentExpression (129)
    vlevo: 14 15, vpravo: 10 11 15 17 89 90 99 100 126 127 161
AssignmentExpressionOpt (130)
    vlevo: 16 17, vpravo: 87 88 98
AssignmentOperator (131)
    vlevo: 18 19 20 21 22 23 24 25 26 27 28, vpravo: 15
BlockItem (132)
    vlevo: 29 30, vpravo: 31 32
BlockItemList (133)
    vlevo: 31 32, vpravo: 32 34
BlockItemListOpt (134)
    vlevo: 33 34, vpravo: 37
CastExpression (135)
    vlevo: 35 36, vpravo: 36 181 182 183 184 283
CompoundStatement (136)
    vlevo: 37, vpravo: 133 234
ConditionalExpression (137)
    vlevo: 38 39, vpravo: 14 39 46
Constant (138)
    vlevo: 40 41 42 43 44 45, vpravo: 213
ConstantExpression (139)
    vlevo: 46, vpravo: 82 118 175 248
ControlLine (140)
    vlevo: 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64, vpravo:
    139
Declaration (141)
    vlevo: 65, vpravo: 29 66 67 132 169
DeclarationList (142)
    vlevo: 66 67, vpravo: 67 69
DeclarationListOpt (143)
    vlevo: 68 69, vpravo: 133
DeclarationSpecifiers (144)
    vlevo: 70 71 72 73, vpravo: 65 75 133 185 186
DeclarationSpecifiersOpt (145)
    vlevo: 74 75, vpravo: 70 71 72 73
Declarator (146)
    vlevo: 76, vpravo: 78 97 133 155 156 185 247
DeclaratorOpt (147)
    vlevo: 77 78, vpravo: 248
Designation (148)
    vlevo: 79, vpravo: 81
DesignationOpt (149)
    vlevo: 80 81, vpravo: 164 165
Designator (150)
    vlevo: 82 83, vpravo: 84 85
DesignatorList (151)
    vlevo: 84 85, vpravo: 79 85
DirectAbstractDeclarator (152)
    vlevo: 86 87 88 89 90 91 92 93, vpravo: 2 93 95
DirectAbstractDeclaratorOpt (153)
    vlevo: 94 95, vpravo: 87 88 89 90 91
DirectDeclarator (154)
    vlevo: 96 97 98 99 100 101 102 103, vpravo: 76 98 99 100 101 102
    103
ElifGroup (155)
    vlevo: 104, vpravo: 105 106
ElifGroupList (156)
    vlevo: 105 106, vpravo: 106 108
ElifGroupListOpt (157)
    vlevo: 107 108, vpravo: 152
ElseGroup (158)
    vlevo: 109, vpravo: 111
ElseGroupOpt (159)
    vlevo: 110 111, vpravo: 152
EndifLine (160)
    vlevo: 112, vpravo: 152
EnumSpecifier (161)
    vlevo: 113 114 115, vpravo: 278
EnumerationConstant (162)
    vlevo: 116, vpravo: 117 118
Enumerator (163)
    vlevo: 117 118, vpravo: 119 120
EnumeratorList (164)
    vlevo: 119 120, vpravo: 113 114 120
EqualityExpression (165)
    vlevo: 121 122 123, vpravo: 8 9 122 123
ExclusiveOrExpression (166)
    vlevo: 124 125, vpravo: 125 153 154
Expression (167)
    vlevo: 126 127, vpravo: 39 127 129 166 167 198 214 221 222 223
ExpressionOpt (168)
    vlevo: 128 129, vpravo: 130 168 169 173
ExpressionStatement (169)
    vlevo: 130, vpravo: 235
ExternalDeclaration (170)
    vlevo: 131 132, vpravo: 256 257
FunctionDefinition (171)
    vlevo: 133, vpravo: 131
FunctionSpecifier (172)
    vlevo: 134, vpravo: 73
GroupList (173)
    vlevo: 135 136, vpravo: 136 138 211
GroupListOpt (174)
    vlevo: 137 138, vpravo: 104 109 149 150 151
GroupPart (175)
    vlevo: 139 140 141 142, vpravo: 135 136
IdentifierList (176)
    vlevo: 143 144, vpravo: 49 59 144 146
IdentifierListOpt (177)
    vlevo: 145 146, vpravo: 50 103
IdentifierOpt (178)
    vlevo: 147 148, vpravo: 113 114 253
IfGroup (179)
    vlevo: 149 150 151, vpravo: 152
IfSection (180)
    vlevo: 152, vpravo: 140
InclusiveOrExpression (181)
    vlevo: 153 154, vpravo: 154 177 178
InitDeclarator (182)
    vlevo: 155 156, vpravo: 157 158
InitDeclaratorList (183)
    vlevo: 157 158, vpravo: 158 160
InitDeclaratorListOpt (184)
    vlevo: 159 160, vpravo: 65
Initializer (185)
    vlevo: 161 162 163, vpravo: 156 164 165
InitializerList (186)
    vlevo: 164 165, vpravo: 162 163 165 204 205
IterationStatement (187)
    vlevo: 166 167 168 169, vpravo: 237
JumpStatement (188)
    vlevo: 170 171 172 173, vpravo: 238
LabeledStatement (189)
    vlevo: 174 175 176, vpravo: 233
LogicalAndExpression (190)
    vlevo: 177 178, vpravo: 178 179 180
LogicalOrExpression (191)
    vlevo: 179 180, vpravo: 38 39 180
MultiplicativeExpression (192)
    vlevo: 181 182 183 184, vpravo: 5 6 7 182 183 184
ParameterDeclaration (193)
    vlevo: 185 186, vpravo: 187 188
ParameterList (194)
    vlevo: 187 188, vpravo: 188 189 190
ParameterTypeList (195)
    vlevo: 189 190, vpravo: 102 192
ParameterTypeListOpt (196)
    vlevo: 191 192, vpravo: 92 93
Pointer (197)
    vlevo: 193 194, vpravo: 1 194 196
PointerOpt (198)
    vlevo: 195 196, vpravo: 2 76
PostfixExpression (199)
    vlevo: 197 198 199 200 201 202 203 204 205, vpravo: 198 199 200
    201 202 203 280
PpTokenList (200)
    vlevo: 206, vpravo: 53 54 57 60 61 62 63 64 104 141 149 208
PpTokenListOpt (201)
    vlevo: 207 208, vpravo: 51 55 112 220 255
PpTokens (202)
    vlevo: 209 210, vpravo: 206 210
PreprocessingFile (203)
    vlevo: 211, vpravo: 231
PrimaryExpression (204)
    vlevo: 212 213 214, vpravo: 197
RelationalExpression (205)
    vlevo: 215 216 217 218 219, vpravo: 121 122 123 216 217 218 219
ReplacementList (206)
    vlevo: 220, vpravo: 47 48 49 50 58 59
SelectionStatement (207)
    vlevo: 221 222 223, vpravo: 236
ShiftExpression (208)
    vlevo: 224 225 226, vpravo: 215 216 217 218 219 225 226
SpecifierQualifierList (209)
    vlevo: 227 228, vpravo: 230 244 258
SpecifierQualifierListOpt (210)
    vlevo: 229 230, vpravo: 227 228
Start (211)
    vlevo: 231 232, vpravo: 0
Statement (212)
    vlevo: 233 234 235 236 237 238, vpravo: 30 166 167 168 169 174
    175 176 221 222 223
StorageClassSpecifier (213)
    vlevo: 239 240 241 242 243, vpravo: 70
StructDeclaration (214)
    vlevo: 244, vpravo: 245 246
StructDeclarationList (215)
    vlevo: 245 246, vpravo: 246 253
StructDeclarator (216)
    vlevo: 247 248, vpravo: 249 250
StructDeclaratorList (217)
    vlevo: 249 250, vpravo: 244 250
StructOrUnion (218)
    vlevo: 251 252, vpravo: 253 254
StructOrUnionSpecifier (219)
    vlevo: 253 254, vpravo: 277
TextLine (220)
    vlevo: 255, vpravo: 142
TranslationUnit (221)
    vlevo: 256 257, vpravo: 232 257
TypeName (222)
    vlevo: 258, vpravo: 36 204 205 285
TypeQualifier (223)
    vlevo: 259 260 261, vpravo: 72 228 262 263
TypeQualifierList (224)
    vlevo: 262 263, vpravo: 88 90 100 263 265
TypeQualifierListOpt (225)
    vlevo: 264 265, vpravo: 89 98 99 101 193 194
TypeSpecifier (226)
    vlevo: 266 267 268 269 270 271 272 273 274 275 276 277 278 279,
    vpravo: 71 227
UnaryExpression (227)
    vlevo: 280 281 282 283 284 285, vpravo: 15 35 281 282 284
UnaryOperator (228)
    vlevo: 286 287 288 289 290 291, vpravo: 283


State 0

    0 $accept: . Start $end
  231 Start: . PREPROCESSINGFILE PreprocessingFile
  232      | . TRANSLATIONUNIT TranslationUnit

    PREPROCESSINGFILE  posunout a přejít do stavu 1
    TRANSLATIONUNIT    posunout a přejít do stavu 2

    Start  přejít do stavu 3


State 1

   47 ControlLine: . PPDEFINE IDENTIFIER ReplacementList
   48            | . PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | . PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   51            | . PPERROR PpTokenListOpt
   52            | . PPHASH_NL
   53            | . PPINCLUDE PpTokenList
   54            | . PPLINE PpTokenList
   55            | . PPPRAGMA PpTokenListOpt
   56            | . PPUNDEF IDENTIFIER '\n'
   57            | . PPASSERT PpTokenList
   58            | . PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
   60            | . PPIDENT PpTokenList
   61            | . PPIMPORT PpTokenList
   62            | . PPINCLUDE_NEXT PpTokenList
   63            | . PPUNASSERT PpTokenList
   64            | . PPWARNING PpTokenList
  135 GroupList: . GroupPart
  136          | . GroupList GroupPart
  139 GroupPart: . ControlLine
  140          | . IfSection
  141          | . PPNONDIRECTIVE PpTokenList
  142          | . TextLine
  149 IfGroup: . PPIF PpTokenList GroupListOpt
  150        | . PPIFDEF IDENTIFIER '\n' GroupListOpt
  151        | . PPIFNDEF IDENTIFIER '\n' GroupListOpt
  152 IfSection: . IfGroup ElifGroupListOpt ElseGroupOpt EndifLine
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  211 PreprocessingFile: . GroupList
  231 Start: PREPROCESSINGFILE . PreprocessingFile
  255 TextLine: . PpTokenListOpt

    '\n'            posunout a přejít do stavu 4
    PPASSERT        posunout a přejít do stavu 5
    PPDEFINE        posunout a přejít do stavu 6
    PPERROR         posunout a přejít do stavu 7
    PPHASH_NL       posunout a přejít do stavu 8
    PPIDENT         posunout a přejít do stavu 9
    PPIF            posunout a přejít do stavu 10
    PPIFDEF         posunout a přejít do stavu 11
    PPIFNDEF        posunout a přejít do stavu 12
    PPIMPORT        posunout a přejít do stavu 13
    PPINCLUDE       posunout a přejít do stavu 14
    PPINCLUDE_NEXT  posunout a přejít do stavu 15
    PPLINE          posunout a přejít do stavu 16
    PPNONDIRECTIVE  posunout a přejít do stavu 17
    PPOTHER         posunout a přejít do stavu 18
    PPPRAGMA        posunout a přejít do stavu 19
    PPUNASSERT      posunout a přejít do stavu 20
    PPUNDEF         posunout a přejít do stavu 21
    PPWARNING       posunout a přejít do stavu 22

    ControlLine        přejít do stavu 23
    GroupList          přejít do stavu 24
    GroupPart          přejít do stavu 25
    IfGroup            přejít do stavu 26
    IfSection          přejít do stavu 27
    PpTokenList        přejít do stavu 28
    PpTokenListOpt     přejít do stavu 29
    PpTokens           přejít do stavu 30
    PreprocessingFile  přejít do stavu 31
    TextLine           přejít do stavu 32


State 2

   65 Declaration: . DeclarationSpecifiers InitDeclaratorListOpt ';'
   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  131 ExternalDeclaration: . FunctionDefinition
  132                    | . Declaration
  133 FunctionDefinition: . DeclarationSpecifiers Declarator DeclarationListOpt CompoundStatement
  134 FunctionSpecifier: . INLINE
  232 Start: TRANSLATIONUNIT . TranslationUnit
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  256 TranslationUnit: . ExternalDeclaration
  257                | . TranslationUnit ExternalDeclaration
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    Declaration             přejít do stavu 57
    DeclarationSpecifiers   přejít do stavu 58
    EnumSpecifier           přejít do stavu 59
    ExternalDeclaration     přejít do stavu 60
    FunctionDefinition      přejít do stavu 61
    FunctionSpecifier       přejít do stavu 62
    StorageClassSpecifier   přejít do stavu 63
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TranslationUnit         přejít do stavu 66
    TypeQualifier           přejít do stavu 67
    TypeSpecifier           přejít do stavu 68


State 3

    0 $accept: Start . $end

    $end  posunout a přejít do stavu 69


State 4

  207 PpTokenListOpt: '\n' .

    $výchozí  reduce using rule 207 (PpTokenListOpt)


State 5

   57 ControlLine: PPASSERT . PpTokenList
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 70
    PpTokens     přejít do stavu 30


State 6

   47 ControlLine: PPDEFINE . IDENTIFIER ReplacementList
   48            | PPDEFINE . IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | PPDEFINE . IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | PPDEFINE . IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   58            | PPDEFINE . IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | PPDEFINE . IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList

    IDENTIFIER         posunout a přejít do stavu 71
    IDENTIFIER_LPAREN  posunout a přejít do stavu 72


State 7

   51 ControlLine: PPERROR . PpTokenListOpt
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    '\n'     posunout a přejít do stavu 4
    PPOTHER  posunout a přejít do stavu 18

    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 73
    PpTokens        přejít do stavu 30


State 8

   52 ControlLine: PPHASH_NL .

    $výchozí  reduce using rule 52 (ControlLine)


State 9

   60 ControlLine: PPIDENT . PpTokenList
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 74
    PpTokens     přejít do stavu 30


State 10

  149 IfGroup: PPIF . PpTokenList GroupListOpt
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 75
    PpTokens     přejít do stavu 30


State 11

  150 IfGroup: PPIFDEF . IDENTIFIER '\n' GroupListOpt

    IDENTIFIER  posunout a přejít do stavu 76


State 12

  151 IfGroup: PPIFNDEF . IDENTIFIER '\n' GroupListOpt

    IDENTIFIER  posunout a přejít do stavu 77


State 13

   61 ControlLine: PPIMPORT . PpTokenList
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 78
    PpTokens     přejít do stavu 30


State 14

   53 ControlLine: PPINCLUDE . PpTokenList
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 79
    PpTokens     přejít do stavu 30


State 15

   62 ControlLine: PPINCLUDE_NEXT . PpTokenList
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 80
    PpTokens     přejít do stavu 30


State 16

   54 ControlLine: PPLINE . PpTokenList
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 81
    PpTokens     přejít do stavu 30


State 17

  141 GroupPart: PPNONDIRECTIVE . PpTokenList
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 82
    PpTokens     přejít do stavu 30


State 18

  209 PpTokens: PPOTHER .

    $výchozí  reduce using rule 209 (PpTokens)


State 19

   55 ControlLine: PPPRAGMA . PpTokenListOpt
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    '\n'     posunout a přejít do stavu 4
    PPOTHER  posunout a přejít do stavu 18

    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 83
    PpTokens        přejít do stavu 30


State 20

   63 ControlLine: PPUNASSERT . PpTokenList
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 84
    PpTokens     přejít do stavu 30


State 21

   56 ControlLine: PPUNDEF . IDENTIFIER '\n'

    IDENTIFIER  posunout a přejít do stavu 85


State 22

   64 ControlLine: PPWARNING . PpTokenList
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 86
    PpTokens     přejít do stavu 30


State 23

  139 GroupPart: ControlLine .

    $výchozí  reduce using rule 139 (GroupPart)


State 24

   47 ControlLine: . PPDEFINE IDENTIFIER ReplacementList
   48            | . PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | . PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   51            | . PPERROR PpTokenListOpt
   52            | . PPHASH_NL
   53            | . PPINCLUDE PpTokenList
   54            | . PPLINE PpTokenList
   55            | . PPPRAGMA PpTokenListOpt
   56            | . PPUNDEF IDENTIFIER '\n'
   57            | . PPASSERT PpTokenList
   58            | . PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
   60            | . PPIDENT PpTokenList
   61            | . PPIMPORT PpTokenList
   62            | . PPINCLUDE_NEXT PpTokenList
   63            | . PPUNASSERT PpTokenList
   64            | . PPWARNING PpTokenList
  136 GroupList: GroupList . GroupPart
  139 GroupPart: . ControlLine
  140          | . IfSection
  141          | . PPNONDIRECTIVE PpTokenList
  142          | . TextLine
  149 IfGroup: . PPIF PpTokenList GroupListOpt
  150        | . PPIFDEF IDENTIFIER '\n' GroupListOpt
  151        | . PPIFNDEF IDENTIFIER '\n' GroupListOpt
  152 IfSection: . IfGroup ElifGroupListOpt ElseGroupOpt EndifLine
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  211 PreprocessingFile: GroupList .  [$end]
  255 TextLine: . PpTokenListOpt

    '\n'            posunout a přejít do stavu 4
    PPASSERT        posunout a přejít do stavu 5
    PPDEFINE        posunout a přejít do stavu 6
    PPERROR         posunout a přejít do stavu 7
    PPHASH_NL       posunout a přejít do stavu 8
    PPIDENT         posunout a přejít do stavu 9
    PPIF            posunout a přejít do stavu 10
    PPIFDEF         posunout a přejít do stavu 11
    PPIFNDEF        posunout a přejít do stavu 12
    PPIMPORT        posunout a přejít do stavu 13
    PPINCLUDE       posunout a přejít do stavu 14
    PPINCLUDE_NEXT  posunout a přejít do stavu 15
    PPLINE          posunout a přejít do stavu 16
    PPNONDIRECTIVE  posunout a přejít do stavu 17
    PPOTHER         posunout a přejít do stavu 18
    PPPRAGMA        posunout a přejít do stavu 19
    PPUNASSERT      posunout a přejít do stavu 20
    PPUNDEF         posunout a přejít do stavu 21
    PPWARNING       posunout a přejít do stavu 22

    $výchozí  reduce using rule 211 (PreprocessingFile)

    ControlLine     přejít do stavu 23
    GroupPart       přejít do stavu 87
    IfGroup         přejít do stavu 26
    IfSection       přejít do stavu 27
    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 29
    PpTokens        přejít do stavu 30
    TextLine        přejít do stavu 32


State 25

  135 GroupList: GroupPart .

    $výchozí  reduce using rule 135 (GroupList)


State 26

  104 ElifGroup: . PPELIF PpTokenList GroupListOpt
  105 ElifGroupList: . ElifGroup
  106              | . ElifGroupList ElifGroup
  107 ElifGroupListOpt: . %empty  [PPELSE, PPENDIF]
  108                 | . ElifGroupList
  152 IfSection: IfGroup . ElifGroupListOpt ElseGroupOpt EndifLine

    PPELIF  posunout a přejít do stavu 88

    $výchozí  reduce using rule 107 (ElifGroupListOpt)

    ElifGroup         přejít do stavu 89
    ElifGroupList     přejít do stavu 90
    ElifGroupListOpt  přejít do stavu 91


State 27

  140 GroupPart: IfSection .

    $výchozí  reduce using rule 140 (GroupPart)


State 28

  208 PpTokenListOpt: PpTokenList .

    $výchozí  reduce using rule 208 (PpTokenListOpt)


State 29

  255 TextLine: PpTokenListOpt .

    $výchozí  reduce using rule 255 (TextLine)


State 30

  206 PpTokenList: PpTokens . '\n'
  210 PpTokens: PpTokens . PPOTHER

    '\n'     posunout a přejít do stavu 92
    PPOTHER  posunout a přejít do stavu 93


State 31

  231 Start: PREPROCESSINGFILE PreprocessingFile .

    $výchozí  reduce using rule 231 (Start)


State 32

  142 GroupPart: TextLine .

    $výchozí  reduce using rule 142 (GroupPart)


State 33

  242 StorageClassSpecifier: AUTO .

    $výchozí  reduce using rule 242 (StorageClassSpecifier)


State 34

  275 TypeSpecifier: BOOL .

    $výchozí  reduce using rule 275 (TypeSpecifier)


State 35

  267 TypeSpecifier: CHAR .

    $výchozí  reduce using rule 267 (TypeSpecifier)


State 36

  276 TypeSpecifier: COMPLEX .

    $výchozí  reduce using rule 276 (TypeSpecifier)


State 37

  259 TypeQualifier: CONST .

    $výchozí  reduce using rule 259 (TypeQualifier)


State 38

  272 TypeSpecifier: DOUBLE .

    $výchozí  reduce using rule 272 (TypeSpecifier)


State 39

  113 EnumSpecifier: ENUM . IdentifierOpt '{' EnumeratorList '}'
  114              | ENUM . IdentifierOpt '{' EnumeratorList ',' '}'
  115              | ENUM . IDENTIFIER
  147 IdentifierOpt: . %empty  ['{']
  148              | . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 94

    $výchozí  reduce using rule 147 (IdentifierOpt)

    IdentifierOpt  přejít do stavu 95


State 40

  240 StorageClassSpecifier: EXTERN .

    $výchozí  reduce using rule 240 (StorageClassSpecifier)


State 41

  271 TypeSpecifier: FLOAT .

    $výchozí  reduce using rule 271 (TypeSpecifier)


State 42

  134 FunctionSpecifier: INLINE .

    $výchozí  reduce using rule 134 (FunctionSpecifier)


State 43

  269 TypeSpecifier: INT .

    $výchozí  reduce using rule 269 (TypeSpecifier)


State 44

  270 TypeSpecifier: LONG .

    $výchozí  reduce using rule 270 (TypeSpecifier)


State 45

  243 StorageClassSpecifier: REGISTER .

    $výchozí  reduce using rule 243 (StorageClassSpecifier)


State 46

  260 TypeQualifier: RESTRICT .

    $výchozí  reduce using rule 260 (TypeQualifier)


State 47

  268 TypeSpecifier: SHORT .

    $výchozí  reduce using rule 268 (TypeSpecifier)


State 48

  273 TypeSpecifier: SIGNED .

    $výchozí  reduce using rule 273 (TypeSpecifier)


State 49

  241 StorageClassSpecifier: STATIC .

    $výchozí  reduce using rule 241 (StorageClassSpecifier)


State 50

  251 StructOrUnion: STRUCT .

    $výchozí  reduce using rule 251 (StructOrUnion)


State 51

  239 StorageClassSpecifier: TYPEDEF .

    $výchozí  reduce using rule 239 (StorageClassSpecifier)


State 52

  279 TypeSpecifier: TYPEDEFNAME .

    $výchozí  reduce using rule 279 (TypeSpecifier)


State 53

  252 StructOrUnion: UNION .

    $výchozí  reduce using rule 252 (StructOrUnion)


State 54

  274 TypeSpecifier: UNSIGNED .

    $výchozí  reduce using rule 274 (TypeSpecifier)


State 55

  266 TypeSpecifier: VOID .

    $výchozí  reduce using rule 266 (TypeSpecifier)


State 56

  261 TypeQualifier: VOLATILE .

    $výchozí  reduce using rule 261 (TypeQualifier)


State 57

  132 ExternalDeclaration: Declaration .

    $výchozí  reduce using rule 132 (ExternalDeclaration)


State 58

   65 Declaration: DeclarationSpecifiers . InitDeclaratorListOpt ';'
   76 Declarator: . PointerOpt DirectDeclarator
  133 FunctionDefinition: DeclarationSpecifiers . Declarator DeclarationListOpt CompoundStatement
  155 InitDeclarator: . Declarator
  156               | . Declarator '=' Initializer
  157 InitDeclaratorList: . InitDeclarator
  158                   | . InitDeclaratorList ',' InitDeclarator
  159 InitDeclaratorListOpt: . %empty  [';']
  160                      | . InitDeclaratorList
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', IDENTIFIER]
  196           | . Pointer

    '*'  posunout a přejít do stavu 96

    ';'         reduce using rule 159 (InitDeclaratorListOpt)
    $výchozí  reduce using rule 195 (PointerOpt)

    Declarator             přejít do stavu 97
    InitDeclarator         přejít do stavu 98
    InitDeclaratorList     přejít do stavu 99
    InitDeclaratorListOpt  přejít do stavu 100
    Pointer                přejít do stavu 101
    PointerOpt             přejít do stavu 102


State 59

  278 TypeSpecifier: EnumSpecifier .

    $výchozí  reduce using rule 278 (TypeSpecifier)


State 60

  256 TranslationUnit: ExternalDeclaration .

    $výchozí  reduce using rule 256 (TranslationUnit)


State 61

  131 ExternalDeclaration: FunctionDefinition .

    $výchozí  reduce using rule 131 (ExternalDeclaration)


State 62

   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
   73                      | FunctionSpecifier . DeclarationSpecifiersOpt
   74 DeclarationSpecifiersOpt: . %empty  ['(', ')', '*', ',', ';', '[', IDENTIFIER]
   75                         | . DeclarationSpecifiers
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 74 (DeclarationSpecifiersOpt)

    DeclarationSpecifiers     přejít do stavu 103
    DeclarationSpecifiersOpt  přejít do stavu 104
    EnumSpecifier             přejít do stavu 59
    FunctionSpecifier         přejít do stavu 62
    StorageClassSpecifier     přejít do stavu 63
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeQualifier             přejít do stavu 67
    TypeSpecifier             přejít do stavu 68


State 63

   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   70                      | StorageClassSpecifier . DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
   74 DeclarationSpecifiersOpt: . %empty  ['(', ')', '*', ',', ';', '[', IDENTIFIER]
   75                         | . DeclarationSpecifiers
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 74 (DeclarationSpecifiersOpt)

    DeclarationSpecifiers     přejít do stavu 103
    DeclarationSpecifiersOpt  přejít do stavu 105
    EnumSpecifier             přejít do stavu 59
    FunctionSpecifier         přejít do stavu 62
    StorageClassSpecifier     přejít do stavu 63
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeQualifier             přejít do stavu 67
    TypeSpecifier             přejít do stavu 68


State 64

  147 IdentifierOpt: . %empty  ['{']
  148              | . IDENTIFIER
  253 StructOrUnionSpecifier: StructOrUnion . IdentifierOpt '{' StructDeclarationList '}'
  254                       | StructOrUnion . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 106

    $výchozí  reduce using rule 147 (IdentifierOpt)

    IdentifierOpt  přejít do stavu 107


State 65

  277 TypeSpecifier: StructOrUnionSpecifier .

    $výchozí  reduce using rule 277 (TypeSpecifier)


State 66

   65 Declaration: . DeclarationSpecifiers InitDeclaratorListOpt ';'
   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  131 ExternalDeclaration: . FunctionDefinition
  132                    | . Declaration
  133 FunctionDefinition: . DeclarationSpecifiers Declarator DeclarationListOpt CompoundStatement
  134 FunctionSpecifier: . INLINE
  232 Start: TRANSLATIONUNIT TranslationUnit .  [$end]
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  257 TranslationUnit: TranslationUnit . ExternalDeclaration
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 232 (Start)

    Declaration             přejít do stavu 57
    DeclarationSpecifiers   přejít do stavu 58
    EnumSpecifier           přejít do stavu 59
    ExternalDeclaration     přejít do stavu 108
    FunctionDefinition      přejít do stavu 61
    FunctionSpecifier       přejít do stavu 62
    StorageClassSpecifier   přejít do stavu 63
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 67
    TypeSpecifier           přejít do stavu 68


State 67

   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   72                      | TypeQualifier . DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
   74 DeclarationSpecifiersOpt: . %empty  ['(', ')', '*', ',', ';', '[', IDENTIFIER]
   75                         | . DeclarationSpecifiers
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 74 (DeclarationSpecifiersOpt)

    DeclarationSpecifiers     přejít do stavu 103
    DeclarationSpecifiersOpt  přejít do stavu 109
    EnumSpecifier             přejít do stavu 59
    FunctionSpecifier         přejít do stavu 62
    StorageClassSpecifier     přejít do stavu 63
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeQualifier             přejít do stavu 67
    TypeSpecifier             přejít do stavu 68


State 68

   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   71                      | TypeSpecifier . DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
   74 DeclarationSpecifiersOpt: . %empty  ['(', ')', '*', ',', ';', '[', IDENTIFIER]
   75                         | . DeclarationSpecifiers
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 74 (DeclarationSpecifiersOpt)

    DeclarationSpecifiers     přejít do stavu 103
    DeclarationSpecifiersOpt  přejít do stavu 110
    EnumSpecifier             přejít do stavu 59
    FunctionSpecifier         přejít do stavu 62
    StorageClassSpecifier     přejít do stavu 63
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeQualifier             přejít do stavu 67
    TypeSpecifier             přejít do stavu 68


State 69

    0 $accept: Start $end .

    $výchozí  přijmout


State 70

   57 ControlLine: PPASSERT PpTokenList .

    $výchozí  reduce using rule 57 (ControlLine)


State 71

   47 ControlLine: PPDEFINE IDENTIFIER . ReplacementList
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  220 ReplacementList: . PpTokenListOpt

    '\n'     posunout a přejít do stavu 4
    PPOTHER  posunout a přejít do stavu 18

    PpTokenList      přejít do stavu 28
    PpTokenListOpt   přejít do stavu 111
    PpTokens         přejít do stavu 30
    ReplacementList  přejít do stavu 112


State 72

   48 ControlLine: PPDEFINE IDENTIFIER_LPAREN . DDD ')' ReplacementList
   49            | PPDEFINE IDENTIFIER_LPAREN . IdentifierList ',' DDD ')' ReplacementList
   50            | PPDEFINE IDENTIFIER_LPAREN . IdentifierListOpt ')' ReplacementList
   58            | PPDEFINE IDENTIFIER_LPAREN . IDENTIFIER DDD ')' ReplacementList
   59            | PPDEFINE IDENTIFIER_LPAREN . IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
  143 IdentifierList: . IDENTIFIER
  144               | . IdentifierList ',' IDENTIFIER
  145 IdentifierListOpt: . %empty  [')']
  146                  | . IdentifierList

    DDD         posunout a přejít do stavu 113
    IDENTIFIER  posunout a přejít do stavu 114

    $výchozí  reduce using rule 145 (IdentifierListOpt)

    IdentifierList     přejít do stavu 115
    IdentifierListOpt  přejít do stavu 116


State 73

   51 ControlLine: PPERROR PpTokenListOpt .

    $výchozí  reduce using rule 51 (ControlLine)


State 74

   60 ControlLine: PPIDENT PpTokenList .

    $výchozí  reduce using rule 60 (ControlLine)


State 75

   47 ControlLine: . PPDEFINE IDENTIFIER ReplacementList
   48            | . PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | . PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   51            | . PPERROR PpTokenListOpt
   52            | . PPHASH_NL
   53            | . PPINCLUDE PpTokenList
   54            | . PPLINE PpTokenList
   55            | . PPPRAGMA PpTokenListOpt
   56            | . PPUNDEF IDENTIFIER '\n'
   57            | . PPASSERT PpTokenList
   58            | . PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
   60            | . PPIDENT PpTokenList
   61            | . PPIMPORT PpTokenList
   62            | . PPINCLUDE_NEXT PpTokenList
   63            | . PPUNASSERT PpTokenList
   64            | . PPWARNING PpTokenList
  135 GroupList: . GroupPart
  136          | . GroupList GroupPart
  137 GroupListOpt: . %empty  [PPELIF, PPELSE, PPENDIF]
  138             | . GroupList
  139 GroupPart: . ControlLine
  140          | . IfSection
  141          | . PPNONDIRECTIVE PpTokenList
  142          | . TextLine
  149 IfGroup: . PPIF PpTokenList GroupListOpt
  149        | PPIF PpTokenList . GroupListOpt
  150        | . PPIFDEF IDENTIFIER '\n' GroupListOpt
  151        | . PPIFNDEF IDENTIFIER '\n' GroupListOpt
  152 IfSection: . IfGroup ElifGroupListOpt ElseGroupOpt EndifLine
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  255 TextLine: . PpTokenListOpt

    '\n'            posunout a přejít do stavu 4
    PPASSERT        posunout a přejít do stavu 5
    PPDEFINE        posunout a přejít do stavu 6
    PPERROR         posunout a přejít do stavu 7
    PPHASH_NL       posunout a přejít do stavu 8
    PPIDENT         posunout a přejít do stavu 9
    PPIF            posunout a přejít do stavu 10
    PPIFDEF         posunout a přejít do stavu 11
    PPIFNDEF        posunout a přejít do stavu 12
    PPIMPORT        posunout a přejít do stavu 13
    PPINCLUDE       posunout a přejít do stavu 14
    PPINCLUDE_NEXT  posunout a přejít do stavu 15
    PPLINE          posunout a přejít do stavu 16
    PPNONDIRECTIVE  posunout a přejít do stavu 17
    PPOTHER         posunout a přejít do stavu 18
    PPPRAGMA        posunout a přejít do stavu 19
    PPUNASSERT      posunout a přejít do stavu 20
    PPUNDEF         posunout a přejít do stavu 21
    PPWARNING       posunout a přejít do stavu 22

    $výchozí  reduce using rule 137 (GroupListOpt)

    ControlLine     přejít do stavu 23
    GroupList       přejít do stavu 117
    GroupListOpt    přejít do stavu 118
    GroupPart       přejít do stavu 25
    IfGroup         přejít do stavu 26
    IfSection       přejít do stavu 27
    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 29
    PpTokens        přejít do stavu 30
    TextLine        přejít do stavu 32


State 76

  150 IfGroup: PPIFDEF IDENTIFIER . '\n' GroupListOpt

    '\n'  posunout a přejít do stavu 119


State 77

  151 IfGroup: PPIFNDEF IDENTIFIER . '\n' GroupListOpt

    '\n'  posunout a přejít do stavu 120


State 78

   61 ControlLine: PPIMPORT PpTokenList .

    $výchozí  reduce using rule 61 (ControlLine)


State 79

   53 ControlLine: PPINCLUDE PpTokenList .

    $výchozí  reduce using rule 53 (ControlLine)


State 80

   62 ControlLine: PPINCLUDE_NEXT PpTokenList .

    $výchozí  reduce using rule 62 (ControlLine)


State 81

   54 ControlLine: PPLINE PpTokenList .

    $výchozí  reduce using rule 54 (ControlLine)


State 82

  141 GroupPart: PPNONDIRECTIVE PpTokenList .

    $výchozí  reduce using rule 141 (GroupPart)


State 83

   55 ControlLine: PPPRAGMA PpTokenListOpt .

    $výchozí  reduce using rule 55 (ControlLine)


State 84

   63 ControlLine: PPUNASSERT PpTokenList .

    $výchozí  reduce using rule 63 (ControlLine)


State 85

   56 ControlLine: PPUNDEF IDENTIFIER . '\n'

    '\n'  posunout a přejít do stavu 121


State 86

   64 ControlLine: PPWARNING PpTokenList .

    $výchozí  reduce using rule 64 (ControlLine)


State 87

  136 GroupList: GroupList GroupPart .

    $výchozí  reduce using rule 136 (GroupList)


State 88

  104 ElifGroup: PPELIF . PpTokenList GroupListOpt
  206 PpTokenList: . PpTokens '\n'
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    PPOTHER  posunout a přejít do stavu 18

    PpTokenList  přejít do stavu 122
    PpTokens     přejít do stavu 30


State 89

  105 ElifGroupList: ElifGroup .

    $výchozí  reduce using rule 105 (ElifGroupList)


State 90

  104 ElifGroup: . PPELIF PpTokenList GroupListOpt
  106 ElifGroupList: ElifGroupList . ElifGroup
  108 ElifGroupListOpt: ElifGroupList .  [PPELSE, PPENDIF]

    PPELIF  posunout a přejít do stavu 88

    $výchozí  reduce using rule 108 (ElifGroupListOpt)

    ElifGroup  přejít do stavu 123


State 91

  109 ElseGroup: . PPELSE '\n' GroupListOpt
  110 ElseGroupOpt: . %empty  [PPENDIF]
  111             | . ElseGroup
  152 IfSection: IfGroup ElifGroupListOpt . ElseGroupOpt EndifLine

    PPELSE  posunout a přejít do stavu 124

    $výchozí  reduce using rule 110 (ElseGroupOpt)

    ElseGroup     přejít do stavu 125
    ElseGroupOpt  přejít do stavu 126


State 92

  206 PpTokenList: PpTokens '\n' .

    $výchozí  reduce using rule 206 (PpTokenList)


State 93

  210 PpTokens: PpTokens PPOTHER .

    $výchozí  reduce using rule 210 (PpTokens)


State 94

  115 EnumSpecifier: ENUM IDENTIFIER .  ['(', ')', '*', ',', ':', ';', '[', AUTO, BOOL, CHAR, COMPLEX, CONST, DOUBLE, ENUM, EXTERN, FLOAT, IDENTIFIER, INLINE, INT, LONG, REGISTER, RESTRICT, SHORT, SIGNED, STATIC, STRUCT, TYPEDEF, TYPEDEFNAME, UNION, UNSIGNED, VOID, VOLATILE]
  148 IdentifierOpt: IDENTIFIER .  ['{']

    '{'         reduce using rule 148 (IdentifierOpt)
    $výchozí  reduce using rule 115 (EnumSpecifier)


State 95

  113 EnumSpecifier: ENUM IdentifierOpt . '{' EnumeratorList '}'
  114              | ENUM IdentifierOpt . '{' EnumeratorList ',' '}'

    '{'  posunout a přejít do stavu 127


State 96

  193 Pointer: '*' . TypeQualifierListOpt
  194        | '*' . TypeQualifierListOpt Pointer
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  262 TypeQualifierList: . TypeQualifier
  263                  | . TypeQualifierList TypeQualifier
  264 TypeQualifierListOpt: . %empty  ['(', ')', '*', ',', '[', IDENTIFIER]
  265                     | . TypeQualifierList

    CONST     posunout a přejít do stavu 37
    RESTRICT  posunout a přejít do stavu 46
    VOLATILE  posunout a přejít do stavu 56

    $výchozí  reduce using rule 264 (TypeQualifierListOpt)

    TypeQualifier         přejít do stavu 128
    TypeQualifierList     přejít do stavu 129
    TypeQualifierListOpt  přejít do stavu 130


State 97

   65 Declaration: . DeclarationSpecifiers InitDeclaratorListOpt ';'
   66 DeclarationList: . Declaration
   67                | . DeclarationList Declaration
   68 DeclarationListOpt: . %empty  ['{']
   69                   | . DeclarationList
   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  133 FunctionDefinition: DeclarationSpecifiers Declarator . DeclarationListOpt CompoundStatement
  134 FunctionSpecifier: . INLINE
  155 InitDeclarator: Declarator .  [',', ';']
  156               | Declarator . '=' Initializer
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    '='          posunout a přejít do stavu 131
    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    '{'         reduce using rule 68 (DeclarationListOpt)
    $výchozí  reduce using rule 155 (InitDeclarator)

    Declaration             přejít do stavu 132
    DeclarationList         přejít do stavu 133
    DeclarationListOpt      přejít do stavu 134
    DeclarationSpecifiers   přejít do stavu 135
    EnumSpecifier           přejít do stavu 59
    FunctionSpecifier       přejít do stavu 62
    StorageClassSpecifier   přejít do stavu 63
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 67
    TypeSpecifier           přejít do stavu 68


State 98

  157 InitDeclaratorList: InitDeclarator .

    $výchozí  reduce using rule 157 (InitDeclaratorList)


State 99

  158 InitDeclaratorList: InitDeclaratorList . ',' InitDeclarator
  160 InitDeclaratorListOpt: InitDeclaratorList .  [';']

    ','  posunout a přejít do stavu 136

    $výchozí  reduce using rule 160 (InitDeclaratorListOpt)


State 100

   65 Declaration: DeclarationSpecifiers InitDeclaratorListOpt . ';'

    ';'  posunout a přejít do stavu 137


State 101

  196 PointerOpt: Pointer .

    $výchozí  reduce using rule 196 (PointerOpt)


State 102

   76 Declarator: PointerOpt . DirectDeclarator
   96 DirectDeclarator: . IDENTIFIER
   97                 | . '(' Declarator ')'
   98                 | . DirectDeclarator '[' TypeQualifierListOpt AssignmentExpressionOpt ']'
   99                 | . DirectDeclarator '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
  100                 | . DirectDeclarator '[' TypeQualifierList STATIC AssignmentExpression ']'
  101                 | . DirectDeclarator '[' TypeQualifierListOpt '*' ']'
  102                 | . DirectDeclarator '(' ParameterTypeList ')'
  103                 | . DirectDeclarator '(' IdentifierListOpt ')'

    '('         posunout a přejít do stavu 138
    IDENTIFIER  posunout a přejít do stavu 139

    DirectDeclarator  přejít do stavu 140


State 103

   75 DeclarationSpecifiersOpt: DeclarationSpecifiers .

    $výchozí  reduce using rule 75 (DeclarationSpecifiersOpt)


State 104

   73 DeclarationSpecifiers: FunctionSpecifier DeclarationSpecifiersOpt .

    $výchozí  reduce using rule 73 (DeclarationSpecifiers)


State 105

   70 DeclarationSpecifiers: StorageClassSpecifier DeclarationSpecifiersOpt .

    $výchozí  reduce using rule 70 (DeclarationSpecifiers)


State 106

  148 IdentifierOpt: IDENTIFIER .  ['{']
  254 StructOrUnionSpecifier: StructOrUnion IDENTIFIER .  ['(', ')', '*', ',', ':', ';', '[', AUTO, BOOL, CHAR, COMPLEX, CONST, DOUBLE, ENUM, EXTERN, FLOAT, IDENTIFIER, INLINE, INT, LONG, REGISTER, RESTRICT, SHORT, SIGNED, STATIC, STRUCT, TYPEDEF, TYPEDEFNAME, UNION, UNSIGNED, VOID, VOLATILE]

    '{'         reduce using rule 148 (IdentifierOpt)
    $výchozí  reduce using rule 254 (StructOrUnionSpecifier)


State 107

  253 StructOrUnionSpecifier: StructOrUnion IdentifierOpt . '{' StructDeclarationList '}'

    '{'  posunout a přejít do stavu 141


State 108

  257 TranslationUnit: TranslationUnit ExternalDeclaration .

    $výchozí  reduce using rule 257 (TranslationUnit)


State 109

   72 DeclarationSpecifiers: TypeQualifier DeclarationSpecifiersOpt .

    $výchozí  reduce using rule 72 (DeclarationSpecifiers)


State 110

   71 DeclarationSpecifiers: TypeSpecifier DeclarationSpecifiersOpt .

    $výchozí  reduce using rule 71 (DeclarationSpecifiers)


State 111

  220 ReplacementList: PpTokenListOpt .

    $výchozí  reduce using rule 220 (ReplacementList)


State 112

   47 ControlLine: PPDEFINE IDENTIFIER ReplacementList .

    $výchozí  reduce using rule 47 (ControlLine)


State 113

   48 ControlLine: PPDEFINE IDENTIFIER_LPAREN DDD . ')' ReplacementList

    ')'  posunout a přejít do stavu 142


State 114

   58 ControlLine: PPDEFINE IDENTIFIER_LPAREN IDENTIFIER . DDD ')' ReplacementList
  143 IdentifierList: IDENTIFIER .  [')', ',']

    DDD  posunout a přejít do stavu 143

    $výchozí  reduce using rule 143 (IdentifierList)


State 115

   49 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierList . ',' DDD ')' ReplacementList
   59            | PPDEFINE IDENTIFIER_LPAREN IdentifierList . ',' IDENTIFIER DDD ')' ReplacementList
  144 IdentifierList: IdentifierList . ',' IDENTIFIER
  146 IdentifierListOpt: IdentifierList .  [')']

    ','  posunout a přejít do stavu 144

    $výchozí  reduce using rule 146 (IdentifierListOpt)


State 116

   50 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt . ')' ReplacementList

    ')'  posunout a přejít do stavu 145


State 117

   47 ControlLine: . PPDEFINE IDENTIFIER ReplacementList
   48            | . PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | . PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   51            | . PPERROR PpTokenListOpt
   52            | . PPHASH_NL
   53            | . PPINCLUDE PpTokenList
   54            | . PPLINE PpTokenList
   55            | . PPPRAGMA PpTokenListOpt
   56            | . PPUNDEF IDENTIFIER '\n'
   57            | . PPASSERT PpTokenList
   58            | . PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
   60            | . PPIDENT PpTokenList
   61            | . PPIMPORT PpTokenList
   62            | . PPINCLUDE_NEXT PpTokenList
   63            | . PPUNASSERT PpTokenList
   64            | . PPWARNING PpTokenList
  136 GroupList: GroupList . GroupPart
  138 GroupListOpt: GroupList .  [PPELIF, PPELSE, PPENDIF]
  139 GroupPart: . ControlLine
  140          | . IfSection
  141          | . PPNONDIRECTIVE PpTokenList
  142          | . TextLine
  149 IfGroup: . PPIF PpTokenList GroupListOpt
  150        | . PPIFDEF IDENTIFIER '\n' GroupListOpt
  151        | . PPIFNDEF IDENTIFIER '\n' GroupListOpt
  152 IfSection: . IfGroup ElifGroupListOpt ElseGroupOpt EndifLine
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  255 TextLine: . PpTokenListOpt

    '\n'            posunout a přejít do stavu 4
    PPASSERT        posunout a přejít do stavu 5
    PPDEFINE        posunout a přejít do stavu 6
    PPERROR         posunout a přejít do stavu 7
    PPHASH_NL       posunout a přejít do stavu 8
    PPIDENT         posunout a přejít do stavu 9
    PPIF            posunout a přejít do stavu 10
    PPIFDEF         posunout a přejít do stavu 11
    PPIFNDEF        posunout a přejít do stavu 12
    PPIMPORT        posunout a přejít do stavu 13
    PPINCLUDE       posunout a přejít do stavu 14
    PPINCLUDE_NEXT  posunout a přejít do stavu 15
    PPLINE          posunout a přejít do stavu 16
    PPNONDIRECTIVE  posunout a přejít do stavu 17
    PPOTHER         posunout a přejít do stavu 18
    PPPRAGMA        posunout a přejít do stavu 19
    PPUNASSERT      posunout a přejít do stavu 20
    PPUNDEF         posunout a přejít do stavu 21
    PPWARNING       posunout a přejít do stavu 22

    $výchozí  reduce using rule 138 (GroupListOpt)

    ControlLine     přejít do stavu 23
    GroupPart       přejít do stavu 87
    IfGroup         přejít do stavu 26
    IfSection       přejít do stavu 27
    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 29
    PpTokens        přejít do stavu 30
    TextLine        přejít do stavu 32


State 118

  149 IfGroup: PPIF PpTokenList GroupListOpt .

    $výchozí  reduce using rule 149 (IfGroup)


State 119

   47 ControlLine: . PPDEFINE IDENTIFIER ReplacementList
   48            | . PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | . PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   51            | . PPERROR PpTokenListOpt
   52            | . PPHASH_NL
   53            | . PPINCLUDE PpTokenList
   54            | . PPLINE PpTokenList
   55            | . PPPRAGMA PpTokenListOpt
   56            | . PPUNDEF IDENTIFIER '\n'
   57            | . PPASSERT PpTokenList
   58            | . PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
   60            | . PPIDENT PpTokenList
   61            | . PPIMPORT PpTokenList
   62            | . PPINCLUDE_NEXT PpTokenList
   63            | . PPUNASSERT PpTokenList
   64            | . PPWARNING PpTokenList
  135 GroupList: . GroupPart
  136          | . GroupList GroupPart
  137 GroupListOpt: . %empty  [PPELIF, PPELSE, PPENDIF]
  138             | . GroupList
  139 GroupPart: . ControlLine
  140          | . IfSection
  141          | . PPNONDIRECTIVE PpTokenList
  142          | . TextLine
  149 IfGroup: . PPIF PpTokenList GroupListOpt
  150        | . PPIFDEF IDENTIFIER '\n' GroupListOpt
  150        | PPIFDEF IDENTIFIER '\n' . GroupListOpt
  151        | . PPIFNDEF IDENTIFIER '\n' GroupListOpt
  152 IfSection: . IfGroup ElifGroupListOpt ElseGroupOpt EndifLine
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  255 TextLine: . PpTokenListOpt

    '\n'            posunout a přejít do stavu 4
    PPASSERT        posunout a přejít do stavu 5
    PPDEFINE        posunout a přejít do stavu 6
    PPERROR         posunout a přejít do stavu 7
    PPHASH_NL       posunout a přejít do stavu 8
    PPIDENT         posunout a přejít do stavu 9
    PPIF            posunout a přejít do stavu 10
    PPIFDEF         posunout a přejít do stavu 11
    PPIFNDEF        posunout a přejít do stavu 12
    PPIMPORT        posunout a přejít do stavu 13
    PPINCLUDE       posunout a přejít do stavu 14
    PPINCLUDE_NEXT  posunout a přejít do stavu 15
    PPLINE          posunout a přejít do stavu 16
    PPNONDIRECTIVE  posunout a přejít do stavu 17
    PPOTHER         posunout a přejít do stavu 18
    PPPRAGMA        posunout a přejít do stavu 19
    PPUNASSERT      posunout a přejít do stavu 20
    PPUNDEF         posunout a přejít do stavu 21
    PPWARNING       posunout a přejít do stavu 22

    $výchozí  reduce using rule 137 (GroupListOpt)

    ControlLine     přejít do stavu 23
    GroupList       přejít do stavu 117
    GroupListOpt    přejít do stavu 146
    GroupPart       přejít do stavu 25
    IfGroup         přejít do stavu 26
    IfSection       přejít do stavu 27
    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 29
    PpTokens        přejít do stavu 30
    TextLine        přejít do stavu 32


State 120

   47 ControlLine: . PPDEFINE IDENTIFIER ReplacementList
   48            | . PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | . PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   51            | . PPERROR PpTokenListOpt
   52            | . PPHASH_NL
   53            | . PPINCLUDE PpTokenList
   54            | . PPLINE PpTokenList
   55            | . PPPRAGMA PpTokenListOpt
   56            | . PPUNDEF IDENTIFIER '\n'
   57            | . PPASSERT PpTokenList
   58            | . PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
   60            | . PPIDENT PpTokenList
   61            | . PPIMPORT PpTokenList
   62            | . PPINCLUDE_NEXT PpTokenList
   63            | . PPUNASSERT PpTokenList
   64            | . PPWARNING PpTokenList
  135 GroupList: . GroupPart
  136          | . GroupList GroupPart
  137 GroupListOpt: . %empty  [PPELIF, PPELSE, PPENDIF]
  138             | . GroupList
  139 GroupPart: . ControlLine
  140          | . IfSection
  141          | . PPNONDIRECTIVE PpTokenList
  142          | . TextLine
  149 IfGroup: . PPIF PpTokenList GroupListOpt
  150        | . PPIFDEF IDENTIFIER '\n' GroupListOpt
  151        | . PPIFNDEF IDENTIFIER '\n' GroupListOpt
  151        | PPIFNDEF IDENTIFIER '\n' . GroupListOpt
  152 IfSection: . IfGroup ElifGroupListOpt ElseGroupOpt EndifLine
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  255 TextLine: . PpTokenListOpt

    '\n'            posunout a přejít do stavu 4
    PPASSERT        posunout a přejít do stavu 5
    PPDEFINE        posunout a přejít do stavu 6
    PPERROR         posunout a přejít do stavu 7
    PPHASH_NL       posunout a přejít do stavu 8
    PPIDENT         posunout a přejít do stavu 9
    PPIF            posunout a přejít do stavu 10
    PPIFDEF         posunout a přejít do stavu 11
    PPIFNDEF        posunout a přejít do stavu 12
    PPIMPORT        posunout a přejít do stavu 13
    PPINCLUDE       posunout a přejít do stavu 14
    PPINCLUDE_NEXT  posunout a přejít do stavu 15
    PPLINE          posunout a přejít do stavu 16
    PPNONDIRECTIVE  posunout a přejít do stavu 17
    PPOTHER         posunout a přejít do stavu 18
    PPPRAGMA        posunout a přejít do stavu 19
    PPUNASSERT      posunout a přejít do stavu 20
    PPUNDEF         posunout a přejít do stavu 21
    PPWARNING       posunout a přejít do stavu 22

    $výchozí  reduce using rule 137 (GroupListOpt)

    ControlLine     přejít do stavu 23
    GroupList       přejít do stavu 117
    GroupListOpt    přejít do stavu 147
    GroupPart       přejít do stavu 25
    IfGroup         přejít do stavu 26
    IfSection       přejít do stavu 27
    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 29
    PpTokens        přejít do stavu 30
    TextLine        přejít do stavu 32


State 121

   56 ControlLine: PPUNDEF IDENTIFIER '\n' .

    $výchozí  reduce using rule 56 (ControlLine)


State 122

   47 ControlLine: . PPDEFINE IDENTIFIER ReplacementList
   48            | . PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | . PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   51            | . PPERROR PpTokenListOpt
   52            | . PPHASH_NL
   53            | . PPINCLUDE PpTokenList
   54            | . PPLINE PpTokenList
   55            | . PPPRAGMA PpTokenListOpt
   56            | . PPUNDEF IDENTIFIER '\n'
   57            | . PPASSERT PpTokenList
   58            | . PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
   60            | . PPIDENT PpTokenList
   61            | . PPIMPORT PpTokenList
   62            | . PPINCLUDE_NEXT PpTokenList
   63            | . PPUNASSERT PpTokenList
   64            | . PPWARNING PpTokenList
  104 ElifGroup: PPELIF PpTokenList . GroupListOpt
  135 GroupList: . GroupPart
  136          | . GroupList GroupPart
  137 GroupListOpt: . %empty  [PPELIF, PPELSE, PPENDIF]
  138             | . GroupList
  139 GroupPart: . ControlLine
  140          | . IfSection
  141          | . PPNONDIRECTIVE PpTokenList
  142          | . TextLine
  149 IfGroup: . PPIF PpTokenList GroupListOpt
  150        | . PPIFDEF IDENTIFIER '\n' GroupListOpt
  151        | . PPIFNDEF IDENTIFIER '\n' GroupListOpt
  152 IfSection: . IfGroup ElifGroupListOpt ElseGroupOpt EndifLine
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  255 TextLine: . PpTokenListOpt

    '\n'            posunout a přejít do stavu 4
    PPASSERT        posunout a přejít do stavu 5
    PPDEFINE        posunout a přejít do stavu 6
    PPERROR         posunout a přejít do stavu 7
    PPHASH_NL       posunout a přejít do stavu 8
    PPIDENT         posunout a přejít do stavu 9
    PPIF            posunout a přejít do stavu 10
    PPIFDEF         posunout a přejít do stavu 11
    PPIFNDEF        posunout a přejít do stavu 12
    PPIMPORT        posunout a přejít do stavu 13
    PPINCLUDE       posunout a přejít do stavu 14
    PPINCLUDE_NEXT  posunout a přejít do stavu 15
    PPLINE          posunout a přejít do stavu 16
    PPNONDIRECTIVE  posunout a přejít do stavu 17
    PPOTHER         posunout a přejít do stavu 18
    PPPRAGMA        posunout a přejít do stavu 19
    PPUNASSERT      posunout a přejít do stavu 20
    PPUNDEF         posunout a přejít do stavu 21
    PPWARNING       posunout a přejít do stavu 22

    $výchozí  reduce using rule 137 (GroupListOpt)

    ControlLine     přejít do stavu 23
    GroupList       přejít do stavu 117
    GroupListOpt    přejít do stavu 148
    GroupPart       přejít do stavu 25
    IfGroup         přejít do stavu 26
    IfSection       přejít do stavu 27
    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 29
    PpTokens        přejít do stavu 30
    TextLine        přejít do stavu 32


State 123

  106 ElifGroupList: ElifGroupList ElifGroup .

    $výchozí  reduce using rule 106 (ElifGroupList)


State 124

  109 ElseGroup: PPELSE . '\n' GroupListOpt

    '\n'  posunout a přejít do stavu 149


State 125

  111 ElseGroupOpt: ElseGroup .

    $výchozí  reduce using rule 111 (ElseGroupOpt)


State 126

  112 EndifLine: . PPENDIF PpTokenListOpt
  152 IfSection: IfGroup ElifGroupListOpt ElseGroupOpt . EndifLine

    PPENDIF  posunout a přejít do stavu 150

    EndifLine  přejít do stavu 151


State 127

  113 EnumSpecifier: ENUM IdentifierOpt '{' . EnumeratorList '}'
  114              | ENUM IdentifierOpt '{' . EnumeratorList ',' '}'
  116 EnumerationConstant: . IDENTIFIER
  117 Enumerator: . EnumerationConstant
  118           | . EnumerationConstant '=' ConstantExpression
  119 EnumeratorList: . Enumerator
  120               | . EnumeratorList ',' Enumerator

    IDENTIFIER  posunout a přejít do stavu 152

    EnumerationConstant  přejít do stavu 153
    Enumerator           přejít do stavu 154
    EnumeratorList       přejít do stavu 155


State 128

  262 TypeQualifierList: TypeQualifier .

    $výchozí  reduce using rule 262 (TypeQualifierList)


State 129

  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  263 TypeQualifierList: TypeQualifierList . TypeQualifier
  265 TypeQualifierListOpt: TypeQualifierList .  ['!', '&', '(', ')', '*', '+', ',', '-', '[', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]

    CONST     posunout a přejít do stavu 37
    RESTRICT  posunout a přejít do stavu 46
    VOLATILE  posunout a přejít do stavu 56

    $výchozí  reduce using rule 265 (TypeQualifierListOpt)

    TypeQualifier  přejít do stavu 156


State 130

  193 Pointer: . '*' TypeQualifierListOpt
  193        | '*' TypeQualifierListOpt .  ['(', ')', ',', '[', IDENTIFIER]
  194        | . '*' TypeQualifierListOpt Pointer
  194        | '*' TypeQualifierListOpt . Pointer

    '*'  posunout a přejít do stavu 96

    $výchozí  reduce using rule 193 (Pointer)

    Pointer  přejít do stavu 157


State 131

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  156 InitDeclarator: Declarator '=' . Initializer
  161 Initializer: . AssignmentExpression
  162            | . '{' InitializerList '}'
  163            | . '{' InitializerList ',' '}'
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 164
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 178
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    Initializer               přejít do stavu 185
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 132

   66 DeclarationList: Declaration .

    $výchozí  reduce using rule 66 (DeclarationList)


State 133

   65 Declaration: . DeclarationSpecifiers InitDeclaratorListOpt ';'
   67 DeclarationList: DeclarationList . Declaration
   69 DeclarationListOpt: DeclarationList .  ['{']
   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 69 (DeclarationListOpt)

    Declaration             přejít do stavu 195
    DeclarationSpecifiers   přejít do stavu 135
    EnumSpecifier           přejít do stavu 59
    FunctionSpecifier       přejít do stavu 62
    StorageClassSpecifier   přejít do stavu 63
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 67
    TypeSpecifier           přejít do stavu 68


State 134

   37 CompoundStatement: . '{' BlockItemListOpt '}'
  133 FunctionDefinition: DeclarationSpecifiers Declarator DeclarationListOpt . CompoundStatement

    '{'  posunout a přejít do stavu 196

    CompoundStatement  přejít do stavu 197


State 135

   65 Declaration: DeclarationSpecifiers . InitDeclaratorListOpt ';'
   76 Declarator: . PointerOpt DirectDeclarator
  155 InitDeclarator: . Declarator
  156               | . Declarator '=' Initializer
  157 InitDeclaratorList: . InitDeclarator
  158                   | . InitDeclaratorList ',' InitDeclarator
  159 InitDeclaratorListOpt: . %empty  [';']
  160                      | . InitDeclaratorList
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', IDENTIFIER]
  196           | . Pointer

    '*'  posunout a přejít do stavu 96

    ';'         reduce using rule 159 (InitDeclaratorListOpt)
    $výchozí  reduce using rule 195 (PointerOpt)

    Declarator             přejít do stavu 198
    InitDeclarator         přejít do stavu 98
    InitDeclaratorList     přejít do stavu 99
    InitDeclaratorListOpt  přejít do stavu 100
    Pointer                přejít do stavu 101
    PointerOpt             přejít do stavu 102


State 136

   76 Declarator: . PointerOpt DirectDeclarator
  155 InitDeclarator: . Declarator
  156               | . Declarator '=' Initializer
  158 InitDeclaratorList: InitDeclaratorList ',' . InitDeclarator
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', IDENTIFIER]
  196           | . Pointer

    '*'  posunout a přejít do stavu 96

    $výchozí  reduce using rule 195 (PointerOpt)

    Declarator      přejít do stavu 198
    InitDeclarator  přejít do stavu 199
    Pointer         přejít do stavu 101
    PointerOpt      přejít do stavu 102


State 137

   65 Declaration: DeclarationSpecifiers InitDeclaratorListOpt ';' .

    $výchozí  reduce using rule 65 (Declaration)


State 138

   76 Declarator: . PointerOpt DirectDeclarator
   97 DirectDeclarator: '(' . Declarator ')'
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', IDENTIFIER]
  196           | . Pointer

    '*'  posunout a přejít do stavu 96

    $výchozí  reduce using rule 195 (PointerOpt)

    Declarator  přejít do stavu 200
    Pointer     přejít do stavu 101
    PointerOpt  přejít do stavu 102


State 139

   96 DirectDeclarator: IDENTIFIER .

    $výchozí  reduce using rule 96 (DirectDeclarator)


State 140

   76 Declarator: PointerOpt DirectDeclarator .  [')', ',', ':', ';', '=', '{', AUTO, BOOL, CHAR, COMPLEX, CONST, DOUBLE, ENUM, EXTERN, FLOAT, INLINE, INT, LONG, REGISTER, RESTRICT, SHORT, SIGNED, STATIC, STRUCT, TYPEDEF, TYPEDEFNAME, UNION, UNSIGNED, VOID, VOLATILE]
   98 DirectDeclarator: DirectDeclarator . '[' TypeQualifierListOpt AssignmentExpressionOpt ']'
   99                 | DirectDeclarator . '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
  100                 | DirectDeclarator . '[' TypeQualifierList STATIC AssignmentExpression ']'
  101                 | DirectDeclarator . '[' TypeQualifierListOpt '*' ']'
  102                 | DirectDeclarator . '(' ParameterTypeList ')'
  103                 | DirectDeclarator . '(' IdentifierListOpt ')'

    '('  posunout a přejít do stavu 201
    '['  posunout a přejít do stavu 202

    $výchozí  reduce using rule 76 (Declarator)


State 141

  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  227 SpecifierQualifierList: . TypeSpecifier SpecifierQualifierListOpt
  228                       | . TypeQualifier SpecifierQualifierListOpt
  244 StructDeclaration: . SpecifierQualifierList StructDeclaratorList ';'
  245 StructDeclarationList: . StructDeclaration
  246                      | . StructDeclarationList StructDeclaration
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  253                       | StructOrUnion IdentifierOpt '{' . StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    FLOAT        posunout a přejít do stavu 41
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STRUCT       posunout a přejít do stavu 50
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    EnumSpecifier           přejít do stavu 59
    SpecifierQualifierList  přejít do stavu 203
    StructDeclaration       přejít do stavu 204
    StructDeclarationList   přejít do stavu 205
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 206
    TypeSpecifier           přejít do stavu 207


State 142

   48 ControlLine: PPDEFINE IDENTIFIER_LPAREN DDD ')' . ReplacementList
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  220 ReplacementList: . PpTokenListOpt

    '\n'     posunout a přejít do stavu 4
    PPOTHER  posunout a přejít do stavu 18

    PpTokenList      přejít do stavu 28
    PpTokenListOpt   přejít do stavu 111
    PpTokens         přejít do stavu 30
    ReplacementList  přejít do stavu 208


State 143

   58 ControlLine: PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD . ')' ReplacementList

    ')'  posunout a přejít do stavu 209


State 144

   49 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' . DDD ')' ReplacementList
   59            | PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' . IDENTIFIER DDD ')' ReplacementList
  144 IdentifierList: IdentifierList ',' . IDENTIFIER

    DDD         posunout a přejít do stavu 210
    IDENTIFIER  posunout a přejít do stavu 211


State 145

   50 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' . ReplacementList
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  220 ReplacementList: . PpTokenListOpt

    '\n'     posunout a přejít do stavu 4
    PPOTHER  posunout a přejít do stavu 18

    PpTokenList      přejít do stavu 28
    PpTokenListOpt   přejít do stavu 111
    PpTokens         přejít do stavu 30
    ReplacementList  přejít do stavu 212


State 146

  150 IfGroup: PPIFDEF IDENTIFIER '\n' GroupListOpt .

    $výchozí  reduce using rule 150 (IfGroup)


State 147

  151 IfGroup: PPIFNDEF IDENTIFIER '\n' GroupListOpt .

    $výchozí  reduce using rule 151 (IfGroup)


State 148

  104 ElifGroup: PPELIF PpTokenList GroupListOpt .

    $výchozí  reduce using rule 104 (ElifGroup)


State 149

   47 ControlLine: . PPDEFINE IDENTIFIER ReplacementList
   48            | . PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList
   49            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList
   50            | . PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList
   51            | . PPERROR PpTokenListOpt
   52            | . PPHASH_NL
   53            | . PPINCLUDE PpTokenList
   54            | . PPLINE PpTokenList
   55            | . PPPRAGMA PpTokenListOpt
   56            | . PPUNDEF IDENTIFIER '\n'
   57            | . PPASSERT PpTokenList
   58            | . PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList
   59            | . PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList
   60            | . PPIDENT PpTokenList
   61            | . PPIMPORT PpTokenList
   62            | . PPINCLUDE_NEXT PpTokenList
   63            | . PPUNASSERT PpTokenList
   64            | . PPWARNING PpTokenList
  109 ElseGroup: PPELSE '\n' . GroupListOpt
  135 GroupList: . GroupPart
  136          | . GroupList GroupPart
  137 GroupListOpt: . %empty  [PPENDIF]
  138             | . GroupList
  139 GroupPart: . ControlLine
  140          | . IfSection
  141          | . PPNONDIRECTIVE PpTokenList
  142          | . TextLine
  149 IfGroup: . PPIF PpTokenList GroupListOpt
  150        | . PPIFDEF IDENTIFIER '\n' GroupListOpt
  151        | . PPIFNDEF IDENTIFIER '\n' GroupListOpt
  152 IfSection: . IfGroup ElifGroupListOpt ElseGroupOpt EndifLine
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  255 TextLine: . PpTokenListOpt

    '\n'            posunout a přejít do stavu 4
    PPASSERT        posunout a přejít do stavu 5
    PPDEFINE        posunout a přejít do stavu 6
    PPERROR         posunout a přejít do stavu 7
    PPHASH_NL       posunout a přejít do stavu 8
    PPIDENT         posunout a přejít do stavu 9
    PPIF            posunout a přejít do stavu 10
    PPIFDEF         posunout a přejít do stavu 11
    PPIFNDEF        posunout a přejít do stavu 12
    PPIMPORT        posunout a přejít do stavu 13
    PPINCLUDE       posunout a přejít do stavu 14
    PPINCLUDE_NEXT  posunout a přejít do stavu 15
    PPLINE          posunout a přejít do stavu 16
    PPNONDIRECTIVE  posunout a přejít do stavu 17
    PPOTHER         posunout a přejít do stavu 18
    PPPRAGMA        posunout a přejít do stavu 19
    PPUNASSERT      posunout a přejít do stavu 20
    PPUNDEF         posunout a přejít do stavu 21
    PPWARNING       posunout a přejít do stavu 22

    $výchozí  reduce using rule 137 (GroupListOpt)

    ControlLine     přejít do stavu 23
    GroupList       přejít do stavu 117
    GroupListOpt    přejít do stavu 213
    GroupPart       přejít do stavu 25
    IfGroup         přejít do stavu 26
    IfSection       přejít do stavu 27
    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 29
    PpTokens        přejít do stavu 30
    TextLine        přejít do stavu 32


State 150

  112 EndifLine: PPENDIF . PpTokenListOpt
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER

    '\n'     posunout a přejít do stavu 4
    PPOTHER  posunout a přejít do stavu 18

    PpTokenList     přejít do stavu 28
    PpTokenListOpt  přejít do stavu 214
    PpTokens        přejít do stavu 30


State 151

  152 IfSection: IfGroup ElifGroupListOpt ElseGroupOpt EndifLine .

    $výchozí  reduce using rule 152 (IfSection)


State 152

  116 EnumerationConstant: IDENTIFIER .

    $výchozí  reduce using rule 116 (EnumerationConstant)


State 153

  117 Enumerator: EnumerationConstant .  [',', '}']
  118           | EnumerationConstant . '=' ConstantExpression

    '='  posunout a přejít do stavu 215

    $výchozí  reduce using rule 117 (Enumerator)


State 154

  119 EnumeratorList: Enumerator .

    $výchozí  reduce using rule 119 (EnumeratorList)


State 155

  113 EnumSpecifier: ENUM IdentifierOpt '{' EnumeratorList . '}'
  114              | ENUM IdentifierOpt '{' EnumeratorList . ',' '}'
  120 EnumeratorList: EnumeratorList . ',' Enumerator

    ','  posunout a přejít do stavu 216
    '}'  posunout a přejít do stavu 217


State 156

  263 TypeQualifierList: TypeQualifierList TypeQualifier .

    $výchozí  reduce using rule 263 (TypeQualifierList)


State 157

  194 Pointer: '*' TypeQualifierListOpt Pointer .

    $výchozí  reduce using rule 194 (Pointer)


State 158

  291 UnaryOperator: '!' .

    $výchozí  reduce using rule 291 (UnaryOperator)


State 159

  286 UnaryOperator: '&' .

    $výchozí  reduce using rule 286 (UnaryOperator)


State 160

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   36               | '(' . TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  204                  | '(' . TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  205                  | '(' . TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  214                  | '(' . Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  227 SpecifierQualifierList: . TypeSpecifier SpecifierQualifierListOpt
  228                       | . TypeQualifier SpecifierQualifierListOpt
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  258 TypeName: . SpecifierQualifierList AbstractDeclaratorOpt
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    BOOL               posunout a přejít do stavu 34
    CHAR               posunout a přejít do stavu 35
    CHARCONST          posunout a přejít do stavu 166
    COMPLEX            posunout a přejít do stavu 36
    CONST              posunout a přejít do stavu 37
    DEC                posunout a přejít do stavu 167
    DOUBLE             posunout a přejít do stavu 38
    ENUM               posunout a přejít do stavu 39
    FLOAT              posunout a přejít do stavu 41
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INT                posunout a přejít do stavu 43
    INTCONST           posunout a přejít do stavu 171
    LONG               posunout a přejít do stavu 44
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RESTRICT           posunout a přejít do stavu 46
    SHORT              posunout a přejít do stavu 47
    SIGNED             posunout a přejít do stavu 48
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    STRUCT             posunout a přejít do stavu 50
    TYPEDEFNAME        posunout a přejít do stavu 52
    UNION              posunout a přejít do stavu 53
    UNSIGNED           posunout a přejít do stavu 54
    VOID               posunout a přejít do stavu 55
    VOLATILE           posunout a přejít do stavu 56

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EnumSpecifier             přejít do stavu 59
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 219
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    SpecifierQualifierList    přejít do stavu 220
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeName                  přejít do stavu 221
    TypeQualifier             přejít do stavu 206
    TypeSpecifier             přejít do stavu 207
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 161

  287 UnaryOperator: '*' .

    $výchozí  reduce using rule 287 (UnaryOperator)


State 162

  288 UnaryOperator: '+' .

    $výchozí  reduce using rule 288 (UnaryOperator)


State 163

  289 UnaryOperator: '-' .

    $výchozí  reduce using rule 289 (UnaryOperator)


State 164

   79 Designation: . DesignatorList '='
   80 DesignationOpt: . %empty  ['!', '&', '(', '*', '+', '-', '{', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]
   81               | . Designation
   82 Designator: . '[' ConstantExpression ']'
   83           | . '.' IDENTIFIER
   84 DesignatorList: . Designator
   85               | . DesignatorList Designator
  162 Initializer: '{' . InitializerList '}'
  163            | '{' . InitializerList ',' '}'
  164 InitializerList: . DesignationOpt Initializer
  165                | . InitializerList ',' DesignationOpt Initializer

    '.'  posunout a přejít do stavu 222
    '['  posunout a přejít do stavu 223

    $výchozí  reduce using rule 80 (DesignationOpt)

    Designation      přejít do stavu 224
    DesignationOpt   přejít do stavu 225
    Designator       přejít do stavu 226
    DesignatorList   přejít do stavu 227
    InitializerList  přejít do stavu 228


State 165

  290 UnaryOperator: '~' .

    $výchozí  reduce using rule 290 (UnaryOperator)


State 166

   40 Constant: CHARCONST .

    $výchozí  reduce using rule 40 (Constant)


State 167

   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  282                | DEC . UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 229
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    Constant           přejít do stavu 181
    PostfixExpression  přejít do stavu 189
    PrimaryExpression  přejít do stavu 190
    UnaryExpression    přejít do stavu 230
    UnaryOperator      přejít do stavu 194


State 168

   41 Constant: FLOATCONST .

    $výchozí  reduce using rule 41 (Constant)


State 169

  212 PrimaryExpression: IDENTIFIER .

    $výchozí  reduce using rule 212 (PrimaryExpression)


State 170

   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  281                | INC . UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 229
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    Constant           přejít do stavu 181
    PostfixExpression  přejít do stavu 189
    PrimaryExpression  přejít do stavu 190
    UnaryExpression    přejít do stavu 231
    UnaryOperator      přejít do stavu 194


State 171

   42 Constant: INTCONST .

    $výchozí  reduce using rule 42 (Constant)


State 172

   43 Constant: LONGCHARCONST .

    $výchozí  reduce using rule 43 (Constant)


State 173

   44 Constant: LONGSTRINGLITERAL .

    $výchozí  reduce using rule 44 (Constant)


State 174

   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  284                | SIZEOF . UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  285                | SIZEOF . '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 232
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    Constant           přejít do stavu 181
    PostfixExpression  přejít do stavu 189
    PrimaryExpression  přejít do stavu 190
    UnaryExpression    přejít do stavu 233
    UnaryOperator      přejít do stavu 194


State 175

   45 Constant: STRINGLITERAL .

    $výchozí  reduce using rule 45 (Constant)


State 176

    6 AdditiveExpression: AdditiveExpression . '+' MultiplicativeExpression
    7                   | AdditiveExpression . '-' MultiplicativeExpression
  224 ShiftExpression: AdditiveExpression .  ['&', ')', ',', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, LSH, NEQ, OROR, RSH]

    '+'  posunout a přejít do stavu 234
    '-'  posunout a přejít do stavu 235

    $výchozí  reduce using rule 224 (ShiftExpression)


State 177

    9 AndExpression: AndExpression . '&' EqualityExpression
  124 ExclusiveOrExpression: AndExpression .  [')', ',', ':', ';', '?', ']', '^', '|', '}', ANDAND, OROR]

    '&'  posunout a přejít do stavu 236

    $výchozí  reduce using rule 124 (ExclusiveOrExpression)


State 178

  161 Initializer: AssignmentExpression .

    $výchozí  reduce using rule 161 (Initializer)


State 179

  181 MultiplicativeExpression: CastExpression .

    $výchozí  reduce using rule 181 (MultiplicativeExpression)


State 180

   14 AssignmentExpression: ConditionalExpression .

    $výchozí  reduce using rule 14 (AssignmentExpression)


State 181

  213 PrimaryExpression: Constant .

    $výchozí  reduce using rule 213 (PrimaryExpression)


State 182

    8 AndExpression: EqualityExpression .  ['&', ')', ',', ':', ';', '?', ']', '^', '|', '}', ANDAND, OROR]
  122 EqualityExpression: EqualityExpression . EQ RelationalExpression
  123                   | EqualityExpression . NEQ RelationalExpression

    EQ   posunout a přejít do stavu 237
    NEQ  posunout a přejít do stavu 238

    $výchozí  reduce using rule 8 (AndExpression)


State 183

  125 ExclusiveOrExpression: ExclusiveOrExpression . '^' AndExpression
  153 InclusiveOrExpression: ExclusiveOrExpression .  [')', ',', ':', ';', '?', ']', '|', '}', ANDAND, OROR]

    '^'  posunout a přejít do stavu 239

    $výchozí  reduce using rule 153 (InclusiveOrExpression)


State 184

  154 InclusiveOrExpression: InclusiveOrExpression . '|' ExclusiveOrExpression
  177 LogicalAndExpression: InclusiveOrExpression .  [')', ',', ':', ';', '?', ']', '}', ANDAND, OROR]

    '|'  posunout a přejít do stavu 240

    $výchozí  reduce using rule 177 (LogicalAndExpression)


State 185

  156 InitDeclarator: Declarator '=' Initializer .

    $výchozí  reduce using rule 156 (InitDeclarator)


State 186

  178 LogicalAndExpression: LogicalAndExpression . ANDAND InclusiveOrExpression
  179 LogicalOrExpression: LogicalAndExpression .  [')', ',', ':', ';', '?', ']', '}', OROR]

    ANDAND  posunout a přejít do stavu 241

    $výchozí  reduce using rule 179 (LogicalOrExpression)


State 187

   38 ConditionalExpression: LogicalOrExpression .  [')', ',', ':', ';', ']', '}']
   39                      | LogicalOrExpression . '?' Expression ':' ConditionalExpression
  180 LogicalOrExpression: LogicalOrExpression . OROR LogicalAndExpression

    '?'   posunout a přejít do stavu 242
    OROR  posunout a přejít do stavu 243

    $výchozí  reduce using rule 38 (ConditionalExpression)


State 188

    5 AdditiveExpression: MultiplicativeExpression .  ['&', ')', '+', ',', '-', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, LSH, NEQ, OROR, RSH]
  182 MultiplicativeExpression: MultiplicativeExpression . '*' CastExpression
  183                         | MultiplicativeExpression . '/' CastExpression
  184                         | MultiplicativeExpression . '%' CastExpression

    '%'  posunout a přejít do stavu 244
    '*'  posunout a přejít do stavu 245
    '/'  posunout a přejít do stavu 246

    $výchozí  reduce using rule 5 (AdditiveExpression)


State 189

  198 PostfixExpression: PostfixExpression . '[' Expression ']'
  199                  | PostfixExpression . '(' ArgumentExpressionListOpt ')'
  200                  | PostfixExpression . '.' IDENTIFIER
  201                  | PostfixExpression . ARROW IDENTIFIER
  202                  | PostfixExpression . INC
  203                  | PostfixExpression . DEC
  280 UnaryExpression: PostfixExpression .  ['%', '&', ')', '*', '+', ',', '-', '/', ':', ';', '<', '=', '>', '?', ']', '^', '|', '}', ADDASSIGN, ANDAND, ANDASSIGN, DIVASSIGN, EQ, GEQ, LEQ, LSH, LSHASSIGN, MODASSIGN, MULASSIGN, NEQ, ORASSIGN, OROR, RSH, RSHASSIGN, SUBASSIGN, XORASSIGN]

    '('    posunout a přejít do stavu 247
    '.'    posunout a přejít do stavu 248
    '['    posunout a přejít do stavu 249
    ARROW  posunout a přejít do stavu 250
    DEC    posunout a přejít do stavu 251
    INC    posunout a přejít do stavu 252

    $výchozí  reduce using rule 280 (UnaryExpression)


State 190

  197 PostfixExpression: PrimaryExpression .

    $výchozí  reduce using rule 197 (PostfixExpression)


State 191

  121 EqualityExpression: RelationalExpression .  ['&', ')', ',', ':', ';', '?', ']', '^', '|', '}', ANDAND, EQ, NEQ, OROR]
  216 RelationalExpression: RelationalExpression . '<' ShiftExpression
  217                     | RelationalExpression . '>' ShiftExpression
  218                     | RelationalExpression . LEQ ShiftExpression
  219                     | RelationalExpression . GEQ ShiftExpression

    '<'  posunout a přejít do stavu 253
    '>'  posunout a přejít do stavu 254
    GEQ  posunout a přejít do stavu 255
    LEQ  posunout a přejít do stavu 256

    $výchozí  reduce using rule 121 (EqualityExpression)


State 192

  215 RelationalExpression: ShiftExpression .  ['&', ')', ',', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, NEQ, OROR]
  225 ShiftExpression: ShiftExpression . LSH AdditiveExpression
  226                | ShiftExpression . RSH AdditiveExpression

    LSH  posunout a přejít do stavu 257
    RSH  posunout a přejít do stavu 258

    $výchozí  reduce using rule 215 (RelationalExpression)


State 193

   15 AssignmentExpression: UnaryExpression . AssignmentOperator AssignmentExpression
   18 AssignmentOperator: . '='
   19                   | . MULASSIGN
   20                   | . DIVASSIGN
   21                   | . MODASSIGN
   22                   | . ADDASSIGN
   23                   | . SUBASSIGN
   24                   | . LSHASSIGN
   25                   | . RSHASSIGN
   26                   | . ANDASSIGN
   27                   | . XORASSIGN
   28                   | . ORASSIGN
   35 CastExpression: UnaryExpression .  ['%', '&', ')', '*', '+', ',', '-', '/', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, LSH, NEQ, OROR, RSH]

    '='        posunout a přejít do stavu 259
    ADDASSIGN  posunout a přejít do stavu 260
    ANDASSIGN  posunout a přejít do stavu 261
    DIVASSIGN  posunout a přejít do stavu 262
    LSHASSIGN  posunout a přejít do stavu 263
    MODASSIGN  posunout a přejít do stavu 264
    MULASSIGN  posunout a přejít do stavu 265
    ORASSIGN   posunout a přejít do stavu 266
    RSHASSIGN  posunout a přejít do stavu 267
    SUBASSIGN  posunout a přejít do stavu 268
    XORASSIGN  posunout a přejít do stavu 269

    $výchozí  reduce using rule 35 (CastExpression)

    AssignmentOperator  přejít do stavu 270


State 194

   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  283                | UnaryOperator . CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    CastExpression     přejít do stavu 271
    Constant           přejít do stavu 181
    PostfixExpression  přejít do stavu 189
    PrimaryExpression  přejít do stavu 190
    UnaryExpression    přejít do stavu 272
    UnaryOperator      přejít do stavu 194


State 195

   67 DeclarationList: DeclarationList Declaration .

    $výchozí  reduce using rule 67 (DeclarationList)


State 196

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   29 BlockItem: . Declaration
   30          | . Statement
   31 BlockItemList: . BlockItem
   32              | . BlockItemList BlockItem
   33 BlockItemListOpt: . %empty  ['}']
   34                 | . BlockItemList
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   37                  | '{' . BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   65 Declaration: . DeclarationSpecifiers InitDeclaratorListOpt ';'
   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  134 FunctionSpecifier: . INLINE
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    AUTO               posunout a přejít do stavu 33
    BOOL               posunout a přejít do stavu 34
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHAR               posunout a přejít do stavu 35
    CHARCONST          posunout a přejít do stavu 166
    COMPLEX            posunout a přejít do stavu 36
    CONST              posunout a přejít do stavu 37
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    DOUBLE             posunout a přejít do stavu 38
    ENUM               posunout a přejít do stavu 39
    EXTERN             posunout a přejít do stavu 40
    FLOAT              posunout a přejít do stavu 41
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INLINE             posunout a přejít do stavu 42
    INT                posunout a přejít do stavu 43
    INTCONST           posunout a přejít do stavu 171
    LONG               posunout a přejít do stavu 44
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    REGISTER           posunout a přejít do stavu 45
    RESTRICT           posunout a přejít do stavu 46
    RETURN             posunout a přejít do stavu 282
    SHORT              posunout a přejít do stavu 47
    SIGNED             posunout a přejít do stavu 48
    SIZEOF             posunout a přejít do stavu 174
    STATIC             posunout a přejít do stavu 49
    STRINGLITERAL      posunout a přejít do stavu 175
    STRUCT             posunout a přejít do stavu 50
    SWITCH             posunout a přejít do stavu 283
    TYPEDEF            posunout a přejít do stavu 51
    TYPEDEFNAME        posunout a přejít do stavu 52
    UNION              posunout a přejít do stavu 53
    UNSIGNED           posunout a přejít do stavu 54
    VOID               posunout a přejít do stavu 55
    VOLATILE           posunout a přejít do stavu 56
    WHILE              posunout a přejít do stavu 284

    ';'         reduce using rule 128 (ExpressionOpt)
    $výchozí  reduce using rule 33 (BlockItemListOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    BlockItem                 přejít do stavu 285
    BlockItemList             přejít do stavu 286
    BlockItemListOpt          přejít do stavu 287
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    Declaration               přejít do stavu 289
    DeclarationSpecifiers     přejít do stavu 135
    EnumSpecifier             přejít do stavu 59
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    FunctionSpecifier         přejít do stavu 62
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 297
    StorageClassSpecifier     přejít do stavu 63
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeQualifier             přejít do stavu 67
    TypeSpecifier             přejít do stavu 68
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 197

  133 FunctionDefinition: DeclarationSpecifiers Declarator DeclarationListOpt CompoundStatement .

    $výchozí  reduce using rule 133 (FunctionDefinition)


State 198

  155 InitDeclarator: Declarator .  [',', ';']
  156               | Declarator . '=' Initializer

    '='  posunout a přejít do stavu 131

    $výchozí  reduce using rule 155 (InitDeclarator)


State 199

  158 InitDeclaratorList: InitDeclaratorList ',' InitDeclarator .

    $výchozí  reduce using rule 158 (InitDeclaratorList)


State 200

   97 DirectDeclarator: '(' Declarator . ')'

    ')'  posunout a přejít do stavu 298


State 201

   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
  102 DirectDeclarator: DirectDeclarator '(' . ParameterTypeList ')'
  103                 | DirectDeclarator '(' . IdentifierListOpt ')'
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  143 IdentifierList: . IDENTIFIER
  144               | . IdentifierList ',' IDENTIFIER
  145 IdentifierListOpt: . %empty  [')']
  146                  | . IdentifierList
  185 ParameterDeclaration: . DeclarationSpecifiers Declarator
  186                     | . DeclarationSpecifiers AbstractDeclaratorOpt
  187 ParameterList: . ParameterDeclaration
  188              | . ParameterList ',' ParameterDeclaration
  189 ParameterTypeList: . ParameterList
  190                  | . ParameterList ',' DDD
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    IDENTIFIER   posunout a přejít do stavu 299
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 145 (IdentifierListOpt)

    DeclarationSpecifiers   přejít do stavu 300
    EnumSpecifier           přejít do stavu 59
    FunctionSpecifier       přejít do stavu 62
    IdentifierList          přejít do stavu 301
    IdentifierListOpt       přejít do stavu 302
    ParameterDeclaration    přejít do stavu 303
    ParameterList           přejít do stavu 304
    ParameterTypeList       přejít do stavu 305
    StorageClassSpecifier   přejít do stavu 63
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 67
    TypeSpecifier           přejít do stavu 68


State 202

   98 DirectDeclarator: DirectDeclarator '[' . TypeQualifierListOpt AssignmentExpressionOpt ']'
   99                 | DirectDeclarator '[' . STATIC TypeQualifierListOpt AssignmentExpression ']'
  100                 | DirectDeclarator '[' . TypeQualifierList STATIC AssignmentExpression ']'
  101                 | DirectDeclarator '[' . TypeQualifierListOpt '*' ']'
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  262 TypeQualifierList: . TypeQualifier
  263                  | . TypeQualifierList TypeQualifier
  264 TypeQualifierListOpt: . %empty  ['!', '&', '(', '*', '+', '-', ']', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]
  265                     | . TypeQualifierList

    CONST     posunout a přejít do stavu 37
    RESTRICT  posunout a přejít do stavu 46
    STATIC    posunout a přejít do stavu 306
    VOLATILE  posunout a přejít do stavu 56

    $výchozí  reduce using rule 264 (TypeQualifierListOpt)

    TypeQualifier         přejít do stavu 128
    TypeQualifierList     přejít do stavu 307
    TypeQualifierListOpt  přejít do stavu 308


State 203

   76 Declarator: . PointerOpt DirectDeclarator
   77 DeclaratorOpt: . %empty  [':']
   78              | . Declarator
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', IDENTIFIER]
  196           | . Pointer
  244 StructDeclaration: SpecifierQualifierList . StructDeclaratorList ';'
  247 StructDeclarator: . Declarator
  248                 | . DeclaratorOpt ':' ConstantExpression
  249 StructDeclaratorList: . StructDeclarator
  250                     | . StructDeclaratorList ',' StructDeclarator

    '*'  posunout a přejít do stavu 96

    ':'         reduce using rule 77 (DeclaratorOpt)
    $výchozí  reduce using rule 195 (PointerOpt)

    Declarator            přejít do stavu 309
    DeclaratorOpt         přejít do stavu 310
    Pointer               přejít do stavu 101
    PointerOpt            přejít do stavu 102
    StructDeclarator      přejít do stavu 311
    StructDeclaratorList  přejít do stavu 312


State 204

  245 StructDeclarationList: StructDeclaration .

    $výchozí  reduce using rule 245 (StructDeclarationList)


State 205

  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  227 SpecifierQualifierList: . TypeSpecifier SpecifierQualifierListOpt
  228                       | . TypeQualifier SpecifierQualifierListOpt
  244 StructDeclaration: . SpecifierQualifierList StructDeclaratorList ';'
  246 StructDeclarationList: StructDeclarationList . StructDeclaration
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  253                       | StructOrUnion IdentifierOpt '{' StructDeclarationList . '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    '}'          posunout a přejít do stavu 313
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    FLOAT        posunout a přejít do stavu 41
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STRUCT       posunout a přejít do stavu 50
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    EnumSpecifier           přejít do stavu 59
    SpecifierQualifierList  přejít do stavu 203
    StructDeclaration       přejít do stavu 314
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 206
    TypeSpecifier           přejít do stavu 207


State 206

  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  227 SpecifierQualifierList: . TypeSpecifier SpecifierQualifierListOpt
  228                       | . TypeQualifier SpecifierQualifierListOpt
  228                       | TypeQualifier . SpecifierQualifierListOpt
  229 SpecifierQualifierListOpt: . %empty  ['(', ')', '*', ':', '[', IDENTIFIER]
  230                          | . SpecifierQualifierList
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    FLOAT        posunout a přejít do stavu 41
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STRUCT       posunout a přejít do stavu 50
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 229 (SpecifierQualifierListOpt)

    EnumSpecifier              přejít do stavu 59
    SpecifierQualifierList     přejít do stavu 315
    SpecifierQualifierListOpt  přejít do stavu 316
    StructOrUnion              přejít do stavu 64
    StructOrUnionSpecifier     přejít do stavu 65
    TypeQualifier              přejít do stavu 206
    TypeSpecifier              přejít do stavu 207


State 207

  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  227 SpecifierQualifierList: . TypeSpecifier SpecifierQualifierListOpt
  227                       | TypeSpecifier . SpecifierQualifierListOpt
  228                       | . TypeQualifier SpecifierQualifierListOpt
  229 SpecifierQualifierListOpt: . %empty  ['(', ')', '*', ':', '[', IDENTIFIER]
  230                          | . SpecifierQualifierList
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    FLOAT        posunout a přejít do stavu 41
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STRUCT       posunout a přejít do stavu 50
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 229 (SpecifierQualifierListOpt)

    EnumSpecifier              přejít do stavu 59
    SpecifierQualifierList     přejít do stavu 315
    SpecifierQualifierListOpt  přejít do stavu 317
    StructOrUnion              přejít do stavu 64
    StructOrUnionSpecifier     přejít do stavu 65
    TypeQualifier              přejít do stavu 206
    TypeSpecifier              přejít do stavu 207


State 208

   48 ControlLine: PPDEFINE IDENTIFIER_LPAREN DDD ')' ReplacementList .

    $výchozí  reduce using rule 48 (ControlLine)


State 209

   58 ControlLine: PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' . ReplacementList
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  220 ReplacementList: . PpTokenListOpt

    '\n'     posunout a přejít do stavu 4
    PPOTHER  posunout a přejít do stavu 18

    PpTokenList      přejít do stavu 28
    PpTokenListOpt   přejít do stavu 111
    PpTokens         přejít do stavu 30
    ReplacementList  přejít do stavu 318


State 210

   49 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD . ')' ReplacementList

    ')'  posunout a přejít do stavu 319


State 211

   59 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER . DDD ')' ReplacementList
  144 IdentifierList: IdentifierList ',' IDENTIFIER .  [')', ',']

    DDD  posunout a přejít do stavu 320

    $výchozí  reduce using rule 144 (IdentifierList)


State 212

   50 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierListOpt ')' ReplacementList .

    $výchozí  reduce using rule 50 (ControlLine)


State 213

  109 ElseGroup: PPELSE '\n' GroupListOpt .

    $výchozí  reduce using rule 109 (ElseGroup)


State 214

  112 EndifLine: PPENDIF PpTokenListOpt .

    $výchozí  reduce using rule 112 (EndifLine)


State 215

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   46 ConstantExpression: . ConditionalExpression
  118 Enumerator: EnumerationConstant '=' . ConstantExpression
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 321
    Constant                  přejít do stavu 181
    ConstantExpression        přejít do stavu 322
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 216

  114 EnumSpecifier: ENUM IdentifierOpt '{' EnumeratorList ',' . '}'
  116 EnumerationConstant: . IDENTIFIER
  117 Enumerator: . EnumerationConstant
  118           | . EnumerationConstant '=' ConstantExpression
  120 EnumeratorList: EnumeratorList ',' . Enumerator

    '}'         posunout a přejít do stavu 323
    IDENTIFIER  posunout a přejít do stavu 152

    EnumerationConstant  přejít do stavu 153
    Enumerator           přejít do stavu 324


State 217

  113 EnumSpecifier: ENUM IdentifierOpt '{' EnumeratorList '}' .

    $výchozí  reduce using rule 113 (EnumSpecifier)


State 218

  126 Expression: AssignmentExpression .

    $výchozí  reduce using rule 126 (Expression)


State 219

  127 Expression: Expression . ',' AssignmentExpression
  214 PrimaryExpression: '(' Expression . ')'

    ')'  posunout a přejít do stavu 325
    ','  posunout a přejít do stavu 326


State 220

    1 AbstractDeclarator: . Pointer
    2                   | . PointerOpt DirectAbstractDeclarator
    3 AbstractDeclaratorOpt: . %empty  [')']
    4                      | . AbstractDeclarator
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', '[']
  196           | . Pointer
  258 TypeName: SpecifierQualifierList . AbstractDeclaratorOpt

    '*'  posunout a přejít do stavu 96

    ')'         reduce using rule 3 (AbstractDeclaratorOpt)
    $výchozí  reduce using rule 195 (PointerOpt)

    AbstractDeclarator     přejít do stavu 327
    AbstractDeclaratorOpt  přejít do stavu 328
    Pointer                přejít do stavu 329
    PointerOpt             přejít do stavu 330


State 221

   36 CastExpression: '(' TypeName . ')' CastExpression
  204 PostfixExpression: '(' TypeName . ')' '{' InitializerList '}'
  205                  | '(' TypeName . ')' '{' InitializerList ',' '}'

    ')'  posunout a přejít do stavu 331


State 222

   83 Designator: '.' . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 332


State 223

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   46 ConstantExpression: . ConditionalExpression
   82 Designator: '[' . ConstantExpression ']'
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 321
    Constant                  přejít do stavu 181
    ConstantExpression        přejít do stavu 333
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 224

   81 DesignationOpt: Designation .

    $výchozí  reduce using rule 81 (DesignationOpt)


State 225

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  161 Initializer: . AssignmentExpression
  162            | . '{' InitializerList '}'
  163            | . '{' InitializerList ',' '}'
  164 InitializerList: DesignationOpt . Initializer
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 164
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 178
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    Initializer               přejít do stavu 334
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 226

   84 DesignatorList: Designator .

    $výchozí  reduce using rule 84 (DesignatorList)


State 227

   79 Designation: DesignatorList . '='
   82 Designator: . '[' ConstantExpression ']'
   83           | . '.' IDENTIFIER
   85 DesignatorList: DesignatorList . Designator

    '.'  posunout a přejít do stavu 222
    '='  posunout a přejít do stavu 335
    '['  posunout a přejít do stavu 223

    Designator  přejít do stavu 336


State 228

  162 Initializer: '{' InitializerList . '}'
  163            | '{' InitializerList . ',' '}'
  165 InitializerList: InitializerList . ',' DesignationOpt Initializer

    ','  posunout a přejít do stavu 337
    '}'  posunout a přejít do stavu 338


State 229

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  204                  | '(' . TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  205                  | '(' . TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  214                  | '(' . Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  227 SpecifierQualifierList: . TypeSpecifier SpecifierQualifierListOpt
  228                       | . TypeQualifier SpecifierQualifierListOpt
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  258 TypeName: . SpecifierQualifierList AbstractDeclaratorOpt
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    BOOL               posunout a přejít do stavu 34
    CHAR               posunout a přejít do stavu 35
    CHARCONST          posunout a přejít do stavu 166
    COMPLEX            posunout a přejít do stavu 36
    CONST              posunout a přejít do stavu 37
    DEC                posunout a přejít do stavu 167
    DOUBLE             posunout a přejít do stavu 38
    ENUM               posunout a přejít do stavu 39
    FLOAT              posunout a přejít do stavu 41
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INT                posunout a přejít do stavu 43
    INTCONST           posunout a přejít do stavu 171
    LONG               posunout a přejít do stavu 44
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RESTRICT           posunout a přejít do stavu 46
    SHORT              posunout a přejít do stavu 47
    SIGNED             posunout a přejít do stavu 48
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    STRUCT             posunout a přejít do stavu 50
    TYPEDEFNAME        posunout a přejít do stavu 52
    UNION              posunout a přejít do stavu 53
    UNSIGNED           posunout a přejít do stavu 54
    VOID               posunout a přejít do stavu 55
    VOLATILE           posunout a přejít do stavu 56

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EnumSpecifier             přejít do stavu 59
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 219
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    SpecifierQualifierList    přejít do stavu 220
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeName                  přejít do stavu 339
    TypeQualifier             přejít do stavu 206
    TypeSpecifier             přejít do stavu 207
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 230

  282 UnaryExpression: DEC UnaryExpression .

    $výchozí  reduce using rule 282 (UnaryExpression)


State 231

  281 UnaryExpression: INC UnaryExpression .

    $výchozí  reduce using rule 281 (UnaryExpression)


State 232

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  204                  | '(' . TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  205                  | '(' . TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  214                  | '(' . Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  227 SpecifierQualifierList: . TypeSpecifier SpecifierQualifierListOpt
  228                       | . TypeQualifier SpecifierQualifierListOpt
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  258 TypeName: . SpecifierQualifierList AbstractDeclaratorOpt
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  285                | SIZEOF '(' . TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    BOOL               posunout a přejít do stavu 34
    CHAR               posunout a přejít do stavu 35
    CHARCONST          posunout a přejít do stavu 166
    COMPLEX            posunout a přejít do stavu 36
    CONST              posunout a přejít do stavu 37
    DEC                posunout a přejít do stavu 167
    DOUBLE             posunout a přejít do stavu 38
    ENUM               posunout a přejít do stavu 39
    FLOAT              posunout a přejít do stavu 41
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INT                posunout a přejít do stavu 43
    INTCONST           posunout a přejít do stavu 171
    LONG               posunout a přejít do stavu 44
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RESTRICT           posunout a přejít do stavu 46
    SHORT              posunout a přejít do stavu 47
    SIGNED             posunout a přejít do stavu 48
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    STRUCT             posunout a přejít do stavu 50
    TYPEDEFNAME        posunout a přejít do stavu 52
    UNION              posunout a přejít do stavu 53
    UNSIGNED           posunout a přejít do stavu 54
    VOID               posunout a přejít do stavu 55
    VOLATILE           posunout a přejít do stavu 56

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EnumSpecifier             přejít do stavu 59
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 219
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    SpecifierQualifierList    přejít do stavu 220
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeName                  přejít do stavu 340
    TypeQualifier             přejít do stavu 206
    TypeSpecifier             přejít do stavu 207
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 233

  284 UnaryExpression: SIZEOF UnaryExpression .

    $výchozí  reduce using rule 284 (UnaryExpression)


State 234

    6 AdditiveExpression: AdditiveExpression '+' . MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 341
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 235

    7 AdditiveExpression: AdditiveExpression '-' . MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 342
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 236

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    9 AndExpression: AndExpression '&' . EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 343
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 237

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  122 EqualityExpression: EqualityExpression EQ . RelationalExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 344
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 238

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  123 EqualityExpression: EqualityExpression NEQ . RelationalExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 345
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 239

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  125 ExclusiveOrExpression: ExclusiveOrExpression '^' . AndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 346
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 240

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  154 InclusiveOrExpression: InclusiveOrExpression '|' . ExclusiveOrExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 347
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 241

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  178 LogicalAndExpression: LogicalAndExpression ANDAND . InclusiveOrExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 348
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 242

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   39                      | LogicalOrExpression '?' . Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 349
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 243

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  180 LogicalOrExpression: LogicalOrExpression OROR . LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 350
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 244

   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  184 MultiplicativeExpression: MultiplicativeExpression '%' . CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    CastExpression     přejít do stavu 351
    Constant           přejít do stavu 181
    PostfixExpression  přejít do stavu 189
    PrimaryExpression  přejít do stavu 190
    UnaryExpression    přejít do stavu 272
    UnaryOperator      přejít do stavu 194


State 245

   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  182 MultiplicativeExpression: MultiplicativeExpression '*' . CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    CastExpression     přejít do stavu 352
    Constant           přejít do stavu 181
    PostfixExpression  přejít do stavu 189
    PrimaryExpression  přejít do stavu 190
    UnaryExpression    přejít do stavu 272
    UnaryOperator      přejít do stavu 194


State 246

   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  183 MultiplicativeExpression: MultiplicativeExpression '/' . CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    CastExpression     přejít do stavu 353
    Constant           přejít do stavu 181
    PostfixExpression  přejít do stavu 189
    PrimaryExpression  přejít do stavu 190
    UnaryExpression    přejít do stavu 272
    UnaryOperator      přejít do stavu 194


State 247

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   10 ArgumentExpressionList: . AssignmentExpression
   11                       | . ArgumentExpressionList ',' AssignmentExpression
   12 ArgumentExpressionListOpt: . %empty  [')']
   13                          | . ArgumentExpressionList
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  199                  | PostfixExpression '(' . ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    $výchozí  reduce using rule 12 (ArgumentExpressionListOpt)

    AdditiveExpression         přejít do stavu 176
    AndExpression              přejít do stavu 177
    ArgumentExpressionList     přejít do stavu 354
    ArgumentExpressionListOpt  přejít do stavu 355
    AssignmentExpression       přejít do stavu 356
    CastExpression             přejít do stavu 179
    ConditionalExpression      přejít do stavu 180
    Constant                   přejít do stavu 181
    EqualityExpression         přejít do stavu 182
    ExclusiveOrExpression      přejít do stavu 183
    InclusiveOrExpression      přejít do stavu 184
    LogicalAndExpression       přejít do stavu 186
    LogicalOrExpression        přejít do stavu 187
    MultiplicativeExpression   přejít do stavu 188
    PostfixExpression          přejít do stavu 189
    PrimaryExpression          přejít do stavu 190
    RelationalExpression       přejít do stavu 191
    ShiftExpression            přejít do stavu 192
    UnaryExpression            přejít do stavu 193
    UnaryOperator              přejít do stavu 194


State 248

  200 PostfixExpression: PostfixExpression '.' . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 357


State 249

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  198                  | PostfixExpression '[' . Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 358
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 250

  201 PostfixExpression: PostfixExpression ARROW . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 359


State 251

  203 PostfixExpression: PostfixExpression DEC .

    $výchozí  reduce using rule 203 (PostfixExpression)


State 252

  202 PostfixExpression: PostfixExpression INC .

    $výchozí  reduce using rule 202 (PostfixExpression)


State 253

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  216 RelationalExpression: RelationalExpression '<' . ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    ShiftExpression           přejít do stavu 360
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 254

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  217 RelationalExpression: RelationalExpression '>' . ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    ShiftExpression           přejít do stavu 361
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 255

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  219 RelationalExpression: RelationalExpression GEQ . ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    ShiftExpression           přejít do stavu 362
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 256

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  218 RelationalExpression: RelationalExpression LEQ . ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    ShiftExpression           přejít do stavu 363
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 257

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  225 ShiftExpression: ShiftExpression LSH . AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 364
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 258

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  226 ShiftExpression: ShiftExpression RSH . AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 365
    CastExpression            přejít do stavu 179
    Constant                  přejít do stavu 181
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 259

   18 AssignmentOperator: '=' .

    $výchozí  reduce using rule 18 (AssignmentOperator)


State 260

   22 AssignmentOperator: ADDASSIGN .

    $výchozí  reduce using rule 22 (AssignmentOperator)


State 261

   26 AssignmentOperator: ANDASSIGN .

    $výchozí  reduce using rule 26 (AssignmentOperator)


State 262

   20 AssignmentOperator: DIVASSIGN .

    $výchozí  reduce using rule 20 (AssignmentOperator)


State 263

   24 AssignmentOperator: LSHASSIGN .

    $výchozí  reduce using rule 24 (AssignmentOperator)


State 264

   21 AssignmentOperator: MODASSIGN .

    $výchozí  reduce using rule 21 (AssignmentOperator)


State 265

   19 AssignmentOperator: MULASSIGN .

    $výchozí  reduce using rule 19 (AssignmentOperator)


State 266

   28 AssignmentOperator: ORASSIGN .

    $výchozí  reduce using rule 28 (AssignmentOperator)


State 267

   25 AssignmentOperator: RSHASSIGN .

    $výchozí  reduce using rule 25 (AssignmentOperator)


State 268

   23 AssignmentOperator: SUBASSIGN .

    $výchozí  reduce using rule 23 (AssignmentOperator)


State 269

   27 AssignmentOperator: XORASSIGN .

    $výchozí  reduce using rule 27 (AssignmentOperator)


State 270

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   15                     | UnaryExpression AssignmentOperator . AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 366
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 271

  283 UnaryExpression: UnaryOperator CastExpression .

    $výchozí  reduce using rule 283 (UnaryExpression)


State 272

   35 CastExpression: UnaryExpression .

    $výchozí  reduce using rule 35 (CastExpression)


State 273

  172 JumpStatement: BREAK . ';'

    ';'  posunout a přejít do stavu 367


State 274

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   46 ConstantExpression: . ConditionalExpression
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  175 LabeledStatement: CASE . ConstantExpression ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 321
    Constant                  přejít do stavu 181
    ConstantExpression        přejít do stavu 368
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 275

  171 JumpStatement: CONTINUE . ';'

    ';'  posunout a přejít do stavu 369


State 276

  176 LabeledStatement: DEFAULT . ':' Statement

    ':'  posunout a přejít do stavu 370


State 277

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  167                   | DO . Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 371
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 278

  168 IterationStatement: FOR . '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | FOR . '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement

    '('  posunout a přejít do stavu 372


State 279

  170 JumpStatement: GOTO . IDENTIFIER ';'

    IDENTIFIER  posunout a přejít do stavu 373


State 280

  174 LabeledStatement: IDENTIFIER . ':' Statement
  212 PrimaryExpression: IDENTIFIER .  ['%', '&', '(', '*', '+', ',', '-', '.', '/', ';', '<', '=', '>', '?', '[', '^', '|', ADDASSIGN, ANDAND, ANDASSIGN, ARROW, DEC, DIVASSIGN, EQ, GEQ, INC, LEQ, LSH, LSHASSIGN, MODASSIGN, MULASSIGN, NEQ, ORASSIGN, OROR, RSH, RSHASSIGN, SUBASSIGN, XORASSIGN]

    ':'  posunout a přejít do stavu 374

    $výchozí  reduce using rule 212 (PrimaryExpression)


State 281

  221 SelectionStatement: IF . '(' Expression ')' Statement
  222                   | IF . '(' Expression ')' Statement ELSE Statement

    '('  posunout a přejít do stavu 375


State 282

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  173 JumpStatement: RETURN . ExpressionOpt ';'
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 376
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 283

  223 SelectionStatement: SWITCH . '(' Expression ')' Statement

    '('  posunout a přejít do stavu 377


State 284

  166 IterationStatement: WHILE . '(' Expression ')' Statement

    '('  posunout a přejít do stavu 378


State 285

   31 BlockItemList: BlockItem .

    $výchozí  reduce using rule 31 (BlockItemList)


State 286

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   29 BlockItem: . Declaration
   30          | . Statement
   32 BlockItemList: BlockItemList . BlockItem
   34 BlockItemListOpt: BlockItemList .  ['}']
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   65 Declaration: . DeclarationSpecifiers InitDeclaratorListOpt ';'
   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  134 FunctionSpecifier: . INLINE
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    AUTO               posunout a přejít do stavu 33
    BOOL               posunout a přejít do stavu 34
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHAR               posunout a přejít do stavu 35
    CHARCONST          posunout a přejít do stavu 166
    COMPLEX            posunout a přejít do stavu 36
    CONST              posunout a přejít do stavu 37
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    DOUBLE             posunout a přejít do stavu 38
    ENUM               posunout a přejít do stavu 39
    EXTERN             posunout a přejít do stavu 40
    FLOAT              posunout a přejít do stavu 41
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INLINE             posunout a přejít do stavu 42
    INT                posunout a přejít do stavu 43
    INTCONST           posunout a přejít do stavu 171
    LONG               posunout a přejít do stavu 44
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    REGISTER           posunout a přejít do stavu 45
    RESTRICT           posunout a přejít do stavu 46
    RETURN             posunout a přejít do stavu 282
    SHORT              posunout a přejít do stavu 47
    SIGNED             posunout a přejít do stavu 48
    SIZEOF             posunout a přejít do stavu 174
    STATIC             posunout a přejít do stavu 49
    STRINGLITERAL      posunout a přejít do stavu 175
    STRUCT             posunout a přejít do stavu 50
    SWITCH             posunout a přejít do stavu 283
    TYPEDEF            posunout a přejít do stavu 51
    TYPEDEFNAME        posunout a přejít do stavu 52
    UNION              posunout a přejít do stavu 53
    UNSIGNED           posunout a přejít do stavu 54
    VOID               posunout a přejít do stavu 55
    VOLATILE           posunout a přejít do stavu 56
    WHILE              posunout a přejít do stavu 284

    ';'         reduce using rule 128 (ExpressionOpt)
    $výchozí  reduce using rule 34 (BlockItemListOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    BlockItem                 přejít do stavu 379
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    Declaration               přejít do stavu 289
    DeclarationSpecifiers     přejít do stavu 135
    EnumSpecifier             přejít do stavu 59
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    FunctionSpecifier         přejít do stavu 62
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 297
    StorageClassSpecifier     přejít do stavu 63
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeQualifier             přejít do stavu 67
    TypeSpecifier             přejít do stavu 68
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 287

   37 CompoundStatement: '{' BlockItemListOpt . '}'

    '}'  posunout a přejít do stavu 380


State 288

  234 Statement: CompoundStatement .

    $výchozí  reduce using rule 234 (Statement)


State 289

   29 BlockItem: Declaration .

    $výchozí  reduce using rule 29 (BlockItem)


State 290

  127 Expression: Expression . ',' AssignmentExpression
  129 ExpressionOpt: Expression .  [')', ';']

    ','  posunout a přejít do stavu 326

    $výchozí  reduce using rule 129 (ExpressionOpt)


State 291

  130 ExpressionStatement: ExpressionOpt . ';'

    ';'  posunout a přejít do stavu 381


State 292

  235 Statement: ExpressionStatement .

    $výchozí  reduce using rule 235 (Statement)


State 293

  237 Statement: IterationStatement .

    $výchozí  reduce using rule 237 (Statement)


State 294

  238 Statement: JumpStatement .

    $výchozí  reduce using rule 238 (Statement)


State 295

  233 Statement: LabeledStatement .

    $výchozí  reduce using rule 233 (Statement)


State 296

  236 Statement: SelectionStatement .

    $výchozí  reduce using rule 236 (Statement)


State 297

   30 BlockItem: Statement .

    $výchozí  reduce using rule 30 (BlockItem)


State 298

   97 DirectDeclarator: '(' Declarator ')' .

    $výchozí  reduce using rule 97 (DirectDeclarator)


State 299

  143 IdentifierList: IDENTIFIER .

    $výchozí  reduce using rule 143 (IdentifierList)


State 300

    1 AbstractDeclarator: . Pointer
    2                   | . PointerOpt DirectAbstractDeclarator
    3 AbstractDeclaratorOpt: . %empty  [')', ',']
    4                      | . AbstractDeclarator
   76 Declarator: . PointerOpt DirectDeclarator
  185 ParameterDeclaration: DeclarationSpecifiers . Declarator
  186                     | DeclarationSpecifiers . AbstractDeclaratorOpt
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', '[', IDENTIFIER]
  196           | . Pointer

    '*'  posunout a přejít do stavu 96

    ')'         reduce using rule 3 (AbstractDeclaratorOpt)
    ','         reduce using rule 3 (AbstractDeclaratorOpt)
    $výchozí  reduce using rule 195 (PointerOpt)

    AbstractDeclarator     přejít do stavu 327
    AbstractDeclaratorOpt  přejít do stavu 382
    Declarator             přejít do stavu 383
    Pointer                přejít do stavu 329
    PointerOpt             přejít do stavu 384


State 301

  144 IdentifierList: IdentifierList . ',' IDENTIFIER
  146 IdentifierListOpt: IdentifierList .  [')']

    ','  posunout a přejít do stavu 385

    $výchozí  reduce using rule 146 (IdentifierListOpt)


State 302

  103 DirectDeclarator: DirectDeclarator '(' IdentifierListOpt . ')'

    ')'  posunout a přejít do stavu 386


State 303

  187 ParameterList: ParameterDeclaration .

    $výchozí  reduce using rule 187 (ParameterList)


State 304

  188 ParameterList: ParameterList . ',' ParameterDeclaration
  189 ParameterTypeList: ParameterList .  [')']
  190                  | ParameterList . ',' DDD

    ','  posunout a přejít do stavu 387

    $výchozí  reduce using rule 189 (ParameterTypeList)


State 305

  102 DirectDeclarator: DirectDeclarator '(' ParameterTypeList . ')'

    ')'  posunout a přejít do stavu 388


State 306

   99 DirectDeclarator: DirectDeclarator '[' STATIC . TypeQualifierListOpt AssignmentExpression ']'
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  262 TypeQualifierList: . TypeQualifier
  263                  | . TypeQualifierList TypeQualifier
  264 TypeQualifierListOpt: . %empty  ['!', '&', '(', '*', '+', '-', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]
  265                     | . TypeQualifierList

    CONST     posunout a přejít do stavu 37
    RESTRICT  posunout a přejít do stavu 46
    VOLATILE  posunout a přejít do stavu 56

    $výchozí  reduce using rule 264 (TypeQualifierListOpt)

    TypeQualifier         přejít do stavu 128
    TypeQualifierList     přejít do stavu 129
    TypeQualifierListOpt  přejít do stavu 389


State 307

  100 DirectDeclarator: DirectDeclarator '[' TypeQualifierList . STATIC AssignmentExpression ']'
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  263 TypeQualifierList: TypeQualifierList . TypeQualifier
  265 TypeQualifierListOpt: TypeQualifierList .  ['!', '&', '(', '*', '+', '-', ']', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]

    CONST     posunout a přejít do stavu 37
    RESTRICT  posunout a přejít do stavu 46
    STATIC    posunout a přejít do stavu 390
    VOLATILE  posunout a přejít do stavu 56

    $výchozí  reduce using rule 265 (TypeQualifierListOpt)

    TypeQualifier  přejít do stavu 156


State 308

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   16 AssignmentExpressionOpt: . %empty  [']']
   17                        | . AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   98 DirectDeclarator: DirectDeclarator '[' TypeQualifierListOpt . AssignmentExpressionOpt ']'
  101                 | DirectDeclarator '[' TypeQualifierListOpt . '*' ']'
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 391
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    $výchozí  reduce using rule 16 (AssignmentExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 392
    AssignmentExpressionOpt   přejít do stavu 393
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 309

   78 DeclaratorOpt: Declarator .  [':']
  247 StructDeclarator: Declarator .  [',', ';']

    ':'         reduce using rule 78 (DeclaratorOpt)
    $výchozí  reduce using rule 247 (StructDeclarator)


State 310

  248 StructDeclarator: DeclaratorOpt . ':' ConstantExpression

    ':'  posunout a přejít do stavu 394


State 311

  249 StructDeclaratorList: StructDeclarator .

    $výchozí  reduce using rule 249 (StructDeclaratorList)


State 312

  244 StructDeclaration: SpecifierQualifierList StructDeclaratorList . ';'
  250 StructDeclaratorList: StructDeclaratorList . ',' StructDeclarator

    ','  posunout a přejít do stavu 395
    ';'  posunout a přejít do stavu 396


State 313

  253 StructOrUnionSpecifier: StructOrUnion IdentifierOpt '{' StructDeclarationList '}' .

    $výchozí  reduce using rule 253 (StructOrUnionSpecifier)


State 314

  246 StructDeclarationList: StructDeclarationList StructDeclaration .

    $výchozí  reduce using rule 246 (StructDeclarationList)


State 315

  230 SpecifierQualifierListOpt: SpecifierQualifierList .

    $výchozí  reduce using rule 230 (SpecifierQualifierListOpt)


State 316

  228 SpecifierQualifierList: TypeQualifier SpecifierQualifierListOpt .

    $výchozí  reduce using rule 228 (SpecifierQualifierList)


State 317

  227 SpecifierQualifierList: TypeSpecifier SpecifierQualifierListOpt .

    $výchozí  reduce using rule 227 (SpecifierQualifierList)


State 318

   58 ControlLine: PPDEFINE IDENTIFIER_LPAREN IDENTIFIER DDD ')' ReplacementList .

    $výchozí  reduce using rule 58 (ControlLine)


State 319

   49 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' . ReplacementList
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  220 ReplacementList: . PpTokenListOpt

    '\n'     posunout a přejít do stavu 4
    PPOTHER  posunout a přejít do stavu 18

    PpTokenList      přejít do stavu 28
    PpTokenListOpt   přejít do stavu 111
    PpTokens         přejít do stavu 30
    ReplacementList  přejít do stavu 397


State 320

   59 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD . ')' ReplacementList

    ')'  posunout a přejít do stavu 398


State 321

   46 ConstantExpression: ConditionalExpression .

    $výchozí  reduce using rule 46 (ConstantExpression)


State 322

  118 Enumerator: EnumerationConstant '=' ConstantExpression .

    $výchozí  reduce using rule 118 (Enumerator)


State 323

  114 EnumSpecifier: ENUM IdentifierOpt '{' EnumeratorList ',' '}' .

    $výchozí  reduce using rule 114 (EnumSpecifier)


State 324

  120 EnumeratorList: EnumeratorList ',' Enumerator .

    $výchozí  reduce using rule 120 (EnumeratorList)


State 325

  214 PrimaryExpression: '(' Expression ')' .

    $výchozí  reduce using rule 214 (PrimaryExpression)


State 326

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  127 Expression: Expression ',' . AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 399
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 327

    4 AbstractDeclaratorOpt: AbstractDeclarator .

    $výchozí  reduce using rule 4 (AbstractDeclaratorOpt)


State 328

  258 TypeName: SpecifierQualifierList AbstractDeclaratorOpt .

    $výchozí  reduce using rule 258 (TypeName)


State 329

    1 AbstractDeclarator: Pointer .  [')', ',']
  196 PointerOpt: Pointer .  ['(', '[', IDENTIFIER]

    ')'         reduce using rule 1 (AbstractDeclarator)
    ','         reduce using rule 1 (AbstractDeclarator)
    $výchozí  reduce using rule 196 (PointerOpt)


State 330

    2 AbstractDeclarator: PointerOpt . DirectAbstractDeclarator
   86 DirectAbstractDeclarator: . '(' AbstractDeclarator ')'
   87                         | . DirectAbstractDeclaratorOpt '[' AssignmentExpressionOpt ']'
   88                         | . DirectAbstractDeclaratorOpt '[' TypeQualifierList AssignmentExpressionOpt ']'
   89                         | . DirectAbstractDeclaratorOpt '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
   90                         | . DirectAbstractDeclaratorOpt '[' TypeQualifierList STATIC AssignmentExpression ']'
   91                         | . DirectAbstractDeclaratorOpt '[' '*' ']'
   92                         | . '(' ParameterTypeListOpt ')'
   93                         | . DirectAbstractDeclarator '(' ParameterTypeListOpt ')'
   94 DirectAbstractDeclaratorOpt: . %empty  ['[']
   95                            | . DirectAbstractDeclarator

    '('  posunout a přejít do stavu 400

    $výchozí  reduce using rule 94 (DirectAbstractDeclaratorOpt)

    DirectAbstractDeclarator     přejít do stavu 401
    DirectAbstractDeclaratorOpt  přejít do stavu 402


State 331

   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   36               | '(' TypeName ')' . CastExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  204                  | '(' TypeName ')' . '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  205                  | '(' TypeName ')' . '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 403
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    CastExpression     přejít do stavu 404
    Constant           přejít do stavu 181
    PostfixExpression  přejít do stavu 189
    PrimaryExpression  přejít do stavu 190
    UnaryExpression    přejít do stavu 272
    UnaryOperator      přejít do stavu 194


State 332

   83 Designator: '.' IDENTIFIER .

    $výchozí  reduce using rule 83 (Designator)


State 333

   82 Designator: '[' ConstantExpression . ']'

    ']'  posunout a přejít do stavu 405


State 334

  164 InitializerList: DesignationOpt Initializer .

    $výchozí  reduce using rule 164 (InitializerList)


State 335

   79 Designation: DesignatorList '=' .

    $výchozí  reduce using rule 79 (Designation)


State 336

   85 DesignatorList: DesignatorList Designator .

    $výchozí  reduce using rule 85 (DesignatorList)


State 337

   79 Designation: . DesignatorList '='
   80 DesignationOpt: . %empty  ['!', '&', '(', '*', '+', '-', '{', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]
   81               | . Designation
   82 Designator: . '[' ConstantExpression ']'
   83           | . '.' IDENTIFIER
   84 DesignatorList: . Designator
   85               | . DesignatorList Designator
  163 Initializer: '{' InitializerList ',' . '}'
  165 InitializerList: InitializerList ',' . DesignationOpt Initializer

    '.'  posunout a přejít do stavu 222
    '['  posunout a přejít do stavu 223
    '}'  posunout a přejít do stavu 406

    $výchozí  reduce using rule 80 (DesignationOpt)

    Designation     přejít do stavu 224
    DesignationOpt  přejít do stavu 407
    Designator      přejít do stavu 226
    DesignatorList  přejít do stavu 227


State 338

  162 Initializer: '{' InitializerList '}' .

    $výchozí  reduce using rule 162 (Initializer)


State 339

  204 PostfixExpression: '(' TypeName . ')' '{' InitializerList '}'
  205                  | '(' TypeName . ')' '{' InitializerList ',' '}'

    ')'  posunout a přejít do stavu 408


State 340

  204 PostfixExpression: '(' TypeName . ')' '{' InitializerList '}'
  205                  | '(' TypeName . ')' '{' InitializerList ',' '}'
  285 UnaryExpression: SIZEOF '(' TypeName . ')'

    ')'  posunout a přejít do stavu 409


State 341

    6 AdditiveExpression: AdditiveExpression '+' MultiplicativeExpression .  ['&', ')', '+', ',', '-', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, LSH, NEQ, OROR, RSH]
  182 MultiplicativeExpression: MultiplicativeExpression . '*' CastExpression
  183                         | MultiplicativeExpression . '/' CastExpression
  184                         | MultiplicativeExpression . '%' CastExpression

    '%'  posunout a přejít do stavu 244
    '*'  posunout a přejít do stavu 245
    '/'  posunout a přejít do stavu 246

    $výchozí  reduce using rule 6 (AdditiveExpression)


State 342

    7 AdditiveExpression: AdditiveExpression '-' MultiplicativeExpression .  ['&', ')', '+', ',', '-', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, LSH, NEQ, OROR, RSH]
  182 MultiplicativeExpression: MultiplicativeExpression . '*' CastExpression
  183                         | MultiplicativeExpression . '/' CastExpression
  184                         | MultiplicativeExpression . '%' CastExpression

    '%'  posunout a přejít do stavu 244
    '*'  posunout a přejít do stavu 245
    '/'  posunout a přejít do stavu 246

    $výchozí  reduce using rule 7 (AdditiveExpression)


State 343

    9 AndExpression: AndExpression '&' EqualityExpression .  ['&', ')', ',', ':', ';', '?', ']', '^', '|', '}', ANDAND, OROR]
  122 EqualityExpression: EqualityExpression . EQ RelationalExpression
  123                   | EqualityExpression . NEQ RelationalExpression

    EQ   posunout a přejít do stavu 237
    NEQ  posunout a přejít do stavu 238

    $výchozí  reduce using rule 9 (AndExpression)


State 344

  122 EqualityExpression: EqualityExpression EQ RelationalExpression .  ['&', ')', ',', ':', ';', '?', ']', '^', '|', '}', ANDAND, EQ, NEQ, OROR]
  216 RelationalExpression: RelationalExpression . '<' ShiftExpression
  217                     | RelationalExpression . '>' ShiftExpression
  218                     | RelationalExpression . LEQ ShiftExpression
  219                     | RelationalExpression . GEQ ShiftExpression

    '<'  posunout a přejít do stavu 253
    '>'  posunout a přejít do stavu 254
    GEQ  posunout a přejít do stavu 255
    LEQ  posunout a přejít do stavu 256

    $výchozí  reduce using rule 122 (EqualityExpression)


State 345

  123 EqualityExpression: EqualityExpression NEQ RelationalExpression .  ['&', ')', ',', ':', ';', '?', ']', '^', '|', '}', ANDAND, EQ, NEQ, OROR]
  216 RelationalExpression: RelationalExpression . '<' ShiftExpression
  217                     | RelationalExpression . '>' ShiftExpression
  218                     | RelationalExpression . LEQ ShiftExpression
  219                     | RelationalExpression . GEQ ShiftExpression

    '<'  posunout a přejít do stavu 253
    '>'  posunout a přejít do stavu 254
    GEQ  posunout a přejít do stavu 255
    LEQ  posunout a přejít do stavu 256

    $výchozí  reduce using rule 123 (EqualityExpression)


State 346

    9 AndExpression: AndExpression . '&' EqualityExpression
  125 ExclusiveOrExpression: ExclusiveOrExpression '^' AndExpression .  [')', ',', ':', ';', '?', ']', '^', '|', '}', ANDAND, OROR]

    '&'  posunout a přejít do stavu 236

    $výchozí  reduce using rule 125 (ExclusiveOrExpression)


State 347

  125 ExclusiveOrExpression: ExclusiveOrExpression . '^' AndExpression
  154 InclusiveOrExpression: InclusiveOrExpression '|' ExclusiveOrExpression .  [')', ',', ':', ';', '?', ']', '|', '}', ANDAND, OROR]

    '^'  posunout a přejít do stavu 239

    $výchozí  reduce using rule 154 (InclusiveOrExpression)


State 348

  154 InclusiveOrExpression: InclusiveOrExpression . '|' ExclusiveOrExpression
  178 LogicalAndExpression: LogicalAndExpression ANDAND InclusiveOrExpression .  [')', ',', ':', ';', '?', ']', '}', ANDAND, OROR]

    '|'  posunout a přejít do stavu 240

    $výchozí  reduce using rule 178 (LogicalAndExpression)


State 349

   39 ConditionalExpression: LogicalOrExpression '?' Expression . ':' ConditionalExpression
  127 Expression: Expression . ',' AssignmentExpression

    ','  posunout a přejít do stavu 326
    ':'  posunout a přejít do stavu 410


State 350

  178 LogicalAndExpression: LogicalAndExpression . ANDAND InclusiveOrExpression
  180 LogicalOrExpression: LogicalOrExpression OROR LogicalAndExpression .  [')', ',', ':', ';', '?', ']', '}', OROR]

    ANDAND  posunout a přejít do stavu 241

    $výchozí  reduce using rule 180 (LogicalOrExpression)


State 351

  184 MultiplicativeExpression: MultiplicativeExpression '%' CastExpression .

    $výchozí  reduce using rule 184 (MultiplicativeExpression)


State 352

  182 MultiplicativeExpression: MultiplicativeExpression '*' CastExpression .

    $výchozí  reduce using rule 182 (MultiplicativeExpression)


State 353

  183 MultiplicativeExpression: MultiplicativeExpression '/' CastExpression .

    $výchozí  reduce using rule 183 (MultiplicativeExpression)


State 354

   11 ArgumentExpressionList: ArgumentExpressionList . ',' AssignmentExpression
   13 ArgumentExpressionListOpt: ArgumentExpressionList .  [')']

    ','  posunout a přejít do stavu 411

    $výchozí  reduce using rule 13 (ArgumentExpressionListOpt)


State 355

  199 PostfixExpression: PostfixExpression '(' ArgumentExpressionListOpt . ')'

    ')'  posunout a přejít do stavu 412


State 356

   10 ArgumentExpressionList: AssignmentExpression .

    $výchozí  reduce using rule 10 (ArgumentExpressionList)


State 357

  200 PostfixExpression: PostfixExpression '.' IDENTIFIER .

    $výchozí  reduce using rule 200 (PostfixExpression)


State 358

  127 Expression: Expression . ',' AssignmentExpression
  198 PostfixExpression: PostfixExpression '[' Expression . ']'

    ','  posunout a přejít do stavu 326
    ']'  posunout a přejít do stavu 413


State 359

  201 PostfixExpression: PostfixExpression ARROW IDENTIFIER .

    $výchozí  reduce using rule 201 (PostfixExpression)


State 360

  216 RelationalExpression: RelationalExpression '<' ShiftExpression .  ['&', ')', ',', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, NEQ, OROR]
  225 ShiftExpression: ShiftExpression . LSH AdditiveExpression
  226                | ShiftExpression . RSH AdditiveExpression

    LSH  posunout a přejít do stavu 257
    RSH  posunout a přejít do stavu 258

    $výchozí  reduce using rule 216 (RelationalExpression)


State 361

  217 RelationalExpression: RelationalExpression '>' ShiftExpression .  ['&', ')', ',', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, NEQ, OROR]
  225 ShiftExpression: ShiftExpression . LSH AdditiveExpression
  226                | ShiftExpression . RSH AdditiveExpression

    LSH  posunout a přejít do stavu 257
    RSH  posunout a přejít do stavu 258

    $výchozí  reduce using rule 217 (RelationalExpression)


State 362

  219 RelationalExpression: RelationalExpression GEQ ShiftExpression .  ['&', ')', ',', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, NEQ, OROR]
  225 ShiftExpression: ShiftExpression . LSH AdditiveExpression
  226                | ShiftExpression . RSH AdditiveExpression

    LSH  posunout a přejít do stavu 257
    RSH  posunout a přejít do stavu 258

    $výchozí  reduce using rule 219 (RelationalExpression)


State 363

  218 RelationalExpression: RelationalExpression LEQ ShiftExpression .  ['&', ')', ',', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, NEQ, OROR]
  225 ShiftExpression: ShiftExpression . LSH AdditiveExpression
  226                | ShiftExpression . RSH AdditiveExpression

    LSH  posunout a přejít do stavu 257
    RSH  posunout a přejít do stavu 258

    $výchozí  reduce using rule 218 (RelationalExpression)


State 364

    6 AdditiveExpression: AdditiveExpression . '+' MultiplicativeExpression
    7                   | AdditiveExpression . '-' MultiplicativeExpression
  225 ShiftExpression: ShiftExpression LSH AdditiveExpression .  ['&', ')', ',', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, LSH, NEQ, OROR, RSH]

    '+'  posunout a přejít do stavu 234
    '-'  posunout a přejít do stavu 235

    $výchozí  reduce using rule 225 (ShiftExpression)


State 365

    6 AdditiveExpression: AdditiveExpression . '+' MultiplicativeExpression
    7                   | AdditiveExpression . '-' MultiplicativeExpression
  226 ShiftExpression: ShiftExpression RSH AdditiveExpression .  ['&', ')', ',', ':', ';', '<', '>', '?', ']', '^', '|', '}', ANDAND, EQ, GEQ, LEQ, LSH, NEQ, OROR, RSH]

    '+'  posunout a přejít do stavu 234
    '-'  posunout a přejít do stavu 235

    $výchozí  reduce using rule 226 (ShiftExpression)


State 366

   15 AssignmentExpression: UnaryExpression AssignmentOperator AssignmentExpression .

    $výchozí  reduce using rule 15 (AssignmentExpression)


State 367

  172 JumpStatement: BREAK ';' .

    $výchozí  reduce using rule 172 (JumpStatement)


State 368

  175 LabeledStatement: CASE ConstantExpression . ':' Statement

    ':'  posunout a přejít do stavu 414


State 369

  171 JumpStatement: CONTINUE ';' .

    $výchozí  reduce using rule 171 (JumpStatement)


State 370

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  176                 | DEFAULT ':' . Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 415
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 371

  167 IterationStatement: DO Statement . WHILE '(' Expression ')' ';'

    WHILE  posunout a přejít do stavu 416


State 372

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   65 Declaration: . DeclarationSpecifiers InitDeclaratorListOpt ';'
   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  134 FunctionSpecifier: . INLINE
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  168 IterationStatement: FOR '(' . ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | FOR '(' . Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    AUTO               posunout a přejít do stavu 33
    BOOL               posunout a přejít do stavu 34
    CHAR               posunout a přejít do stavu 35
    CHARCONST          posunout a přejít do stavu 166
    COMPLEX            posunout a přejít do stavu 36
    CONST              posunout a přejít do stavu 37
    DEC                posunout a přejít do stavu 167
    DOUBLE             posunout a přejít do stavu 38
    ENUM               posunout a přejít do stavu 39
    EXTERN             posunout a přejít do stavu 40
    FLOAT              posunout a přejít do stavu 41
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INLINE             posunout a přejít do stavu 42
    INT                posunout a přejít do stavu 43
    INTCONST           posunout a přejít do stavu 171
    LONG               posunout a přejít do stavu 44
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    REGISTER           posunout a přejít do stavu 45
    RESTRICT           posunout a přejít do stavu 46
    SHORT              posunout a přejít do stavu 47
    SIGNED             posunout a přejít do stavu 48
    SIZEOF             posunout a přejít do stavu 174
    STATIC             posunout a přejít do stavu 49
    STRINGLITERAL      posunout a přejít do stavu 175
    STRUCT             posunout a přejít do stavu 50
    TYPEDEF            posunout a přejít do stavu 51
    TYPEDEFNAME        posunout a přejít do stavu 52
    UNION              posunout a přejít do stavu 53
    UNSIGNED           posunout a přejít do stavu 54
    VOID               posunout a přejít do stavu 55
    VOLATILE           posunout a přejít do stavu 56

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    Declaration               přejít do stavu 417
    DeclarationSpecifiers     přejít do stavu 135
    EnumSpecifier             přejít do stavu 59
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 418
    FunctionSpecifier         přejít do stavu 62
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    StorageClassSpecifier     přejít do stavu 63
    StructOrUnion             přejít do stavu 64
    StructOrUnionSpecifier    přejít do stavu 65
    TypeQualifier             přejít do stavu 67
    TypeSpecifier             přejít do stavu 68
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 373

  170 JumpStatement: GOTO IDENTIFIER . ';'

    ';'  posunout a přejít do stavu 419


State 374

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  174                 | IDENTIFIER ':' . Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 420
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 375

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: IF '(' . Expression ')' Statement
  222                   | IF '(' . Expression ')' Statement ELSE Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 421
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 376

  173 JumpStatement: RETURN ExpressionOpt . ';'

    ';'  posunout a přejít do stavu 422


State 377

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  223 SelectionStatement: SWITCH '(' . Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 423
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 378

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: WHILE '(' . Expression ')' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 424
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 379

   32 BlockItemList: BlockItemList BlockItem .

    $výchozí  reduce using rule 32 (BlockItemList)


State 380

   37 CompoundStatement: '{' BlockItemListOpt '}' .

    $výchozí  reduce using rule 37 (CompoundStatement)


State 381

  130 ExpressionStatement: ExpressionOpt ';' .

    $výchozí  reduce using rule 130 (ExpressionStatement)


State 382

  186 ParameterDeclaration: DeclarationSpecifiers AbstractDeclaratorOpt .

    $výchozí  reduce using rule 186 (ParameterDeclaration)


State 383

  185 ParameterDeclaration: DeclarationSpecifiers Declarator .

    $výchozí  reduce using rule 185 (ParameterDeclaration)


State 384

    2 AbstractDeclarator: PointerOpt . DirectAbstractDeclarator
   76 Declarator: PointerOpt . DirectDeclarator
   86 DirectAbstractDeclarator: . '(' AbstractDeclarator ')'
   87                         | . DirectAbstractDeclaratorOpt '[' AssignmentExpressionOpt ']'
   88                         | . DirectAbstractDeclaratorOpt '[' TypeQualifierList AssignmentExpressionOpt ']'
   89                         | . DirectAbstractDeclaratorOpt '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
   90                         | . DirectAbstractDeclaratorOpt '[' TypeQualifierList STATIC AssignmentExpression ']'
   91                         | . DirectAbstractDeclaratorOpt '[' '*' ']'
   92                         | . '(' ParameterTypeListOpt ')'
   93                         | . DirectAbstractDeclarator '(' ParameterTypeListOpt ')'
   94 DirectAbstractDeclaratorOpt: . %empty  ['[']
   95                            | . DirectAbstractDeclarator
   96 DirectDeclarator: . IDENTIFIER
   97                 | . '(' Declarator ')'
   98                 | . DirectDeclarator '[' TypeQualifierListOpt AssignmentExpressionOpt ']'
   99                 | . DirectDeclarator '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
  100                 | . DirectDeclarator '[' TypeQualifierList STATIC AssignmentExpression ']'
  101                 | . DirectDeclarator '[' TypeQualifierListOpt '*' ']'
  102                 | . DirectDeclarator '(' ParameterTypeList ')'
  103                 | . DirectDeclarator '(' IdentifierListOpt ')'

    '('         posunout a přejít do stavu 425
    IDENTIFIER  posunout a přejít do stavu 139

    $výchozí  reduce using rule 94 (DirectAbstractDeclaratorOpt)

    DirectAbstractDeclarator     přejít do stavu 401
    DirectAbstractDeclaratorOpt  přejít do stavu 402
    DirectDeclarator             přejít do stavu 140


State 385

  144 IdentifierList: IdentifierList ',' . IDENTIFIER

    IDENTIFIER  posunout a přejít do stavu 426


State 386

  103 DirectDeclarator: DirectDeclarator '(' IdentifierListOpt ')' .

    $výchozí  reduce using rule 103 (DirectDeclarator)


State 387

   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  185 ParameterDeclaration: . DeclarationSpecifiers Declarator
  186                     | . DeclarationSpecifiers AbstractDeclaratorOpt
  188 ParameterList: ParameterList ',' . ParameterDeclaration
  190 ParameterTypeList: ParameterList ',' . DDD
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DDD          posunout a přejít do stavu 427
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    DeclarationSpecifiers   přejít do stavu 300
    EnumSpecifier           přejít do stavu 59
    FunctionSpecifier       přejít do stavu 62
    ParameterDeclaration    přejít do stavu 428
    StorageClassSpecifier   přejít do stavu 63
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 67
    TypeSpecifier           přejít do stavu 68


State 388

  102 DirectDeclarator: DirectDeclarator '(' ParameterTypeList ')' .

    $výchozí  reduce using rule 102 (DirectDeclarator)


State 389

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   99 DirectDeclarator: DirectDeclarator '[' STATIC TypeQualifierListOpt . AssignmentExpression ']'
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 429
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 390

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  100 DirectDeclarator: DirectDeclarator '[' TypeQualifierList STATIC . AssignmentExpression ']'
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 430
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 391

  101 DirectDeclarator: DirectDeclarator '[' TypeQualifierListOpt '*' . ']'
  287 UnaryOperator: '*' .  ['!', '&', '(', '*', '+', '-', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]

    ']'  posunout a přejít do stavu 431

    $výchozí  reduce using rule 287 (UnaryOperator)


State 392

   17 AssignmentExpressionOpt: AssignmentExpression .

    $výchozí  reduce using rule 17 (AssignmentExpressionOpt)


State 393

   98 DirectDeclarator: DirectDeclarator '[' TypeQualifierListOpt AssignmentExpressionOpt . ']'

    ']'  posunout a přejít do stavu 432


State 394

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   46 ConstantExpression: . ConditionalExpression
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  248 StructDeclarator: DeclaratorOpt ':' . ConstantExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 321
    Constant                  přejít do stavu 181
    ConstantExpression        přejít do stavu 433
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 395

   76 Declarator: . PointerOpt DirectDeclarator
   77 DeclaratorOpt: . %empty  [':']
   78              | . Declarator
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', IDENTIFIER]
  196           | . Pointer
  247 StructDeclarator: . Declarator
  248                 | . DeclaratorOpt ':' ConstantExpression
  250 StructDeclaratorList: StructDeclaratorList ',' . StructDeclarator

    '*'  posunout a přejít do stavu 96

    ':'         reduce using rule 77 (DeclaratorOpt)
    $výchozí  reduce using rule 195 (PointerOpt)

    Declarator        přejít do stavu 309
    DeclaratorOpt     přejít do stavu 310
    Pointer           přejít do stavu 101
    PointerOpt        přejít do stavu 102
    StructDeclarator  přejít do stavu 434


State 396

  244 StructDeclaration: SpecifierQualifierList StructDeclaratorList ';' .

    $výchozí  reduce using rule 244 (StructDeclaration)


State 397

   49 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' DDD ')' ReplacementList .

    $výchozí  reduce using rule 49 (ControlLine)


State 398

   59 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' . ReplacementList
  206 PpTokenList: . PpTokens '\n'
  207 PpTokenListOpt: . '\n'
  208               | . PpTokenList
  209 PpTokens: . PPOTHER
  210         | . PpTokens PPOTHER
  220 ReplacementList: . PpTokenListOpt

    '\n'     posunout a přejít do stavu 4
    PPOTHER  posunout a přejít do stavu 18

    PpTokenList      přejít do stavu 28
    PpTokenListOpt   přejít do stavu 111
    PpTokens         přejít do stavu 30
    ReplacementList  přejít do stavu 435


State 399

  127 Expression: Expression ',' AssignmentExpression .

    $výchozí  reduce using rule 127 (Expression)


State 400

    1 AbstractDeclarator: . Pointer
    2                   | . PointerOpt DirectAbstractDeclarator
   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
   86 DirectAbstractDeclarator: '(' . AbstractDeclarator ')'
   92                         | '(' . ParameterTypeListOpt ')'
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  185 ParameterDeclaration: . DeclarationSpecifiers Declarator
  186                     | . DeclarationSpecifiers AbstractDeclaratorOpt
  187 ParameterList: . ParameterDeclaration
  188              | . ParameterList ',' ParameterDeclaration
  189 ParameterTypeList: . ParameterList
  190                  | . ParameterList ',' DDD
  191 ParameterTypeListOpt: . %empty  [')']
  192                     | . ParameterTypeList
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', '[']
  196           | . Pointer
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    '*'          posunout a přejít do stavu 96
    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    ')'         reduce using rule 191 (ParameterTypeListOpt)
    $výchozí  reduce using rule 195 (PointerOpt)

    AbstractDeclarator      přejít do stavu 436
    DeclarationSpecifiers   přejít do stavu 300
    EnumSpecifier           přejít do stavu 59
    FunctionSpecifier       přejít do stavu 62
    ParameterDeclaration    přejít do stavu 303
    ParameterList           přejít do stavu 304
    ParameterTypeList       přejít do stavu 437
    ParameterTypeListOpt    přejít do stavu 438
    Pointer                 přejít do stavu 329
    PointerOpt              přejít do stavu 330
    StorageClassSpecifier   přejít do stavu 63
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 67
    TypeSpecifier           přejít do stavu 68


State 401

    2 AbstractDeclarator: PointerOpt DirectAbstractDeclarator .  [')', ',']
   93 DirectAbstractDeclarator: DirectAbstractDeclarator . '(' ParameterTypeListOpt ')'
   95 DirectAbstractDeclaratorOpt: DirectAbstractDeclarator .  ['[']

    '('  posunout a přejít do stavu 439

    '['         reduce using rule 95 (DirectAbstractDeclaratorOpt)
    $výchozí  reduce using rule 2 (AbstractDeclarator)


State 402

   87 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt . '[' AssignmentExpressionOpt ']'
   88                         | DirectAbstractDeclaratorOpt . '[' TypeQualifierList AssignmentExpressionOpt ']'
   89                         | DirectAbstractDeclaratorOpt . '[' STATIC TypeQualifierListOpt AssignmentExpression ']'
   90                         | DirectAbstractDeclaratorOpt . '[' TypeQualifierList STATIC AssignmentExpression ']'
   91                         | DirectAbstractDeclaratorOpt . '[' '*' ']'

    '['  posunout a přejít do stavu 440


State 403

   79 Designation: . DesignatorList '='
   80 DesignationOpt: . %empty  ['!', '&', '(', '*', '+', '-', '{', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]
   81               | . Designation
   82 Designator: . '[' ConstantExpression ']'
   83           | . '.' IDENTIFIER
   84 DesignatorList: . Designator
   85               | . DesignatorList Designator
  164 InitializerList: . DesignationOpt Initializer
  165                | . InitializerList ',' DesignationOpt Initializer
  204 PostfixExpression: '(' TypeName ')' '{' . InitializerList '}'
  205                  | '(' TypeName ')' '{' . InitializerList ',' '}'

    '.'  posunout a přejít do stavu 222
    '['  posunout a přejít do stavu 223

    $výchozí  reduce using rule 80 (DesignationOpt)

    Designation      přejít do stavu 224
    DesignationOpt   přejít do stavu 225
    Designator       přejít do stavu 226
    DesignatorList   přejít do stavu 227
    InitializerList  přejít do stavu 441


State 404

   36 CastExpression: '(' TypeName ')' CastExpression .

    $výchozí  reduce using rule 36 (CastExpression)


State 405

   82 Designator: '[' ConstantExpression ']' .

    $výchozí  reduce using rule 82 (Designator)


State 406

  163 Initializer: '{' InitializerList ',' '}' .

    $výchozí  reduce using rule 163 (Initializer)


State 407

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  161 Initializer: . AssignmentExpression
  162            | . '{' InitializerList '}'
  163            | . '{' InitializerList ',' '}'
  165 InitializerList: InitializerList ',' DesignationOpt . Initializer
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 164
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 178
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    Initializer               přejít do stavu 442
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 408

  204 PostfixExpression: '(' TypeName ')' . '{' InitializerList '}'
  205                  | '(' TypeName ')' . '{' InitializerList ',' '}'

    '{'  posunout a přejít do stavu 403


State 409

  204 PostfixExpression: '(' TypeName ')' . '{' InitializerList '}'
  205                  | '(' TypeName ')' . '{' InitializerList ',' '}'
  285 UnaryExpression: SIZEOF '(' TypeName ')' .  ['%', '&', ')', '*', '+', ',', '-', '/', ':', ';', '<', '=', '>', '?', ']', '^', '|', '}', ADDASSIGN, ANDAND, ANDASSIGN, DIVASSIGN, EQ, GEQ, LEQ, LSH, LSHASSIGN, MODASSIGN, MULASSIGN, NEQ, ORASSIGN, OROR, RSH, RSHASSIGN, SUBASSIGN, XORASSIGN]

    '{'  posunout a přejít do stavu 403

    $výchozí  reduce using rule 285 (UnaryExpression)


State 410

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   39                      | LogicalOrExpression '?' Expression ':' . ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 443
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 272
    UnaryOperator             přejít do stavu 194


State 411

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   11 ArgumentExpressionList: ArgumentExpressionList ',' . AssignmentExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 444
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 412

  199 PostfixExpression: PostfixExpression '(' ArgumentExpressionListOpt ')' .

    $výchozí  reduce using rule 199 (PostfixExpression)


State 413

  198 PostfixExpression: PostfixExpression '[' Expression ']' .

    $výchozí  reduce using rule 198 (PostfixExpression)


State 414

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  175                 | CASE ConstantExpression ':' . Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 445
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 415

  176 LabeledStatement: DEFAULT ':' Statement .

    $výchozí  reduce using rule 176 (LabeledStatement)


State 416

  167 IterationStatement: DO Statement WHILE . '(' Expression ')' ';'

    '('  posunout a přejít do stavu 446


State 417

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  169 IterationStatement: FOR '(' Declaration . ExpressionOpt ';' ExpressionOpt ')' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 447
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 418

  168 IterationStatement: FOR '(' ExpressionOpt . ';' ExpressionOpt ';' ExpressionOpt ')' Statement

    ';'  posunout a přejít do stavu 448


State 419

  170 JumpStatement: GOTO IDENTIFIER ';' .

    $výchozí  reduce using rule 170 (JumpStatement)


State 420

  174 LabeledStatement: IDENTIFIER ':' Statement .

    $výchozí  reduce using rule 174 (LabeledStatement)


State 421

  127 Expression: Expression . ',' AssignmentExpression
  221 SelectionStatement: IF '(' Expression . ')' Statement
  222                   | IF '(' Expression . ')' Statement ELSE Statement

    ')'  posunout a přejít do stavu 449
    ','  posunout a přejít do stavu 326


State 422

  173 JumpStatement: RETURN ExpressionOpt ';' .

    $výchozí  reduce using rule 173 (JumpStatement)


State 423

  127 Expression: Expression . ',' AssignmentExpression
  223 SelectionStatement: SWITCH '(' Expression . ')' Statement

    ')'  posunout a přejít do stavu 450
    ','  posunout a přejít do stavu 326


State 424

  127 Expression: Expression . ',' AssignmentExpression
  166 IterationStatement: WHILE '(' Expression . ')' Statement

    ')'  posunout a přejít do stavu 451
    ','  posunout a přejít do stavu 326


State 425

    1 AbstractDeclarator: . Pointer
    2                   | . PointerOpt DirectAbstractDeclarator
   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
   76 Declarator: . PointerOpt DirectDeclarator
   86 DirectAbstractDeclarator: '(' . AbstractDeclarator ')'
   92                         | '(' . ParameterTypeListOpt ')'
   97 DirectDeclarator: '(' . Declarator ')'
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  185 ParameterDeclaration: . DeclarationSpecifiers Declarator
  186                     | . DeclarationSpecifiers AbstractDeclaratorOpt
  187 ParameterList: . ParameterDeclaration
  188              | . ParameterList ',' ParameterDeclaration
  189 ParameterTypeList: . ParameterList
  190                  | . ParameterList ',' DDD
  191 ParameterTypeListOpt: . %empty  [')']
  192                     | . ParameterTypeList
  193 Pointer: . '*' TypeQualifierListOpt
  194        | . '*' TypeQualifierListOpt Pointer
  195 PointerOpt: . %empty  ['(', '[', IDENTIFIER]
  196           | . Pointer
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    '*'          posunout a přejít do stavu 96
    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    ')'         reduce using rule 191 (ParameterTypeListOpt)
    $výchozí  reduce using rule 195 (PointerOpt)

    AbstractDeclarator      přejít do stavu 436
    DeclarationSpecifiers   přejít do stavu 300
    Declarator              přejít do stavu 200
    EnumSpecifier           přejít do stavu 59
    FunctionSpecifier       přejít do stavu 62
    ParameterDeclaration    přejít do stavu 303
    ParameterList           přejít do stavu 304
    ParameterTypeList       přejít do stavu 437
    ParameterTypeListOpt    přejít do stavu 438
    Pointer                 přejít do stavu 329
    PointerOpt              přejít do stavu 384
    StorageClassSpecifier   přejít do stavu 63
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 67
    TypeSpecifier           přejít do stavu 68


State 426

  144 IdentifierList: IdentifierList ',' IDENTIFIER .

    $výchozí  reduce using rule 144 (IdentifierList)


State 427

  190 ParameterTypeList: ParameterList ',' DDD .

    $výchozí  reduce using rule 190 (ParameterTypeList)


State 428

  188 ParameterList: ParameterList ',' ParameterDeclaration .

    $výchozí  reduce using rule 188 (ParameterList)


State 429

   99 DirectDeclarator: DirectDeclarator '[' STATIC TypeQualifierListOpt AssignmentExpression . ']'

    ']'  posunout a přejít do stavu 452


State 430

  100 DirectDeclarator: DirectDeclarator '[' TypeQualifierList STATIC AssignmentExpression . ']'

    ']'  posunout a přejít do stavu 453


State 431

  101 DirectDeclarator: DirectDeclarator '[' TypeQualifierListOpt '*' ']' .

    $výchozí  reduce using rule 101 (DirectDeclarator)


State 432

   98 DirectDeclarator: DirectDeclarator '[' TypeQualifierListOpt AssignmentExpressionOpt ']' .

    $výchozí  reduce using rule 98 (DirectDeclarator)


State 433

  248 StructDeclarator: DeclaratorOpt ':' ConstantExpression .

    $výchozí  reduce using rule 248 (StructDeclarator)


State 434

  250 StructDeclaratorList: StructDeclaratorList ',' StructDeclarator .

    $výchozí  reduce using rule 250 (StructDeclaratorList)


State 435

   59 ControlLine: PPDEFINE IDENTIFIER_LPAREN IdentifierList ',' IDENTIFIER DDD ')' ReplacementList .

    $výchozí  reduce using rule 59 (ControlLine)


State 436

   86 DirectAbstractDeclarator: '(' AbstractDeclarator . ')'

    ')'  posunout a přejít do stavu 454


State 437

  192 ParameterTypeListOpt: ParameterTypeList .

    $výchozí  reduce using rule 192 (ParameterTypeListOpt)


State 438

   92 DirectAbstractDeclarator: '(' ParameterTypeListOpt . ')'

    ')'  posunout a přejít do stavu 455


State 439

   70 DeclarationSpecifiers: . StorageClassSpecifier DeclarationSpecifiersOpt
   71                      | . TypeSpecifier DeclarationSpecifiersOpt
   72                      | . TypeQualifier DeclarationSpecifiersOpt
   73                      | . FunctionSpecifier DeclarationSpecifiersOpt
   93 DirectAbstractDeclarator: DirectAbstractDeclarator '(' . ParameterTypeListOpt ')'
  113 EnumSpecifier: . ENUM IdentifierOpt '{' EnumeratorList '}'
  114              | . ENUM IdentifierOpt '{' EnumeratorList ',' '}'
  115              | . ENUM IDENTIFIER
  134 FunctionSpecifier: . INLINE
  185 ParameterDeclaration: . DeclarationSpecifiers Declarator
  186                     | . DeclarationSpecifiers AbstractDeclaratorOpt
  187 ParameterList: . ParameterDeclaration
  188              | . ParameterList ',' ParameterDeclaration
  189 ParameterTypeList: . ParameterList
  190                  | . ParameterList ',' DDD
  191 ParameterTypeListOpt: . %empty  [')']
  192                     | . ParameterTypeList
  239 StorageClassSpecifier: . TYPEDEF
  240                      | . EXTERN
  241                      | . STATIC
  242                      | . AUTO
  243                      | . REGISTER
  251 StructOrUnion: . STRUCT
  252              | . UNION
  253 StructOrUnionSpecifier: . StructOrUnion IdentifierOpt '{' StructDeclarationList '}'
  254                       | . StructOrUnion IDENTIFIER
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  266 TypeSpecifier: . VOID
  267              | . CHAR
  268              | . SHORT
  269              | . INT
  270              | . LONG
  271              | . FLOAT
  272              | . DOUBLE
  273              | . SIGNED
  274              | . UNSIGNED
  275              | . BOOL
  276              | . COMPLEX
  277              | . StructOrUnionSpecifier
  278              | . EnumSpecifier
  279              | . TYPEDEFNAME

    AUTO         posunout a přejít do stavu 33
    BOOL         posunout a přejít do stavu 34
    CHAR         posunout a přejít do stavu 35
    COMPLEX      posunout a přejít do stavu 36
    CONST        posunout a přejít do stavu 37
    DOUBLE       posunout a přejít do stavu 38
    ENUM         posunout a přejít do stavu 39
    EXTERN       posunout a přejít do stavu 40
    FLOAT        posunout a přejít do stavu 41
    INLINE       posunout a přejít do stavu 42
    INT          posunout a přejít do stavu 43
    LONG         posunout a přejít do stavu 44
    REGISTER     posunout a přejít do stavu 45
    RESTRICT     posunout a přejít do stavu 46
    SHORT        posunout a přejít do stavu 47
    SIGNED       posunout a přejít do stavu 48
    STATIC       posunout a přejít do stavu 49
    STRUCT       posunout a přejít do stavu 50
    TYPEDEF      posunout a přejít do stavu 51
    TYPEDEFNAME  posunout a přejít do stavu 52
    UNION        posunout a přejít do stavu 53
    UNSIGNED     posunout a přejít do stavu 54
    VOID         posunout a přejít do stavu 55
    VOLATILE     posunout a přejít do stavu 56

    $výchozí  reduce using rule 191 (ParameterTypeListOpt)

    DeclarationSpecifiers   přejít do stavu 300
    EnumSpecifier           přejít do stavu 59
    FunctionSpecifier       přejít do stavu 62
    ParameterDeclaration    přejít do stavu 303
    ParameterList           přejít do stavu 304
    ParameterTypeList       přejít do stavu 437
    ParameterTypeListOpt    přejít do stavu 456
    StorageClassSpecifier   přejít do stavu 63
    StructOrUnion           přejít do stavu 64
    StructOrUnionSpecifier  přejít do stavu 65
    TypeQualifier           přejít do stavu 67
    TypeSpecifier           přejít do stavu 68


State 440

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   16 AssignmentExpressionOpt: . %empty  [']']
   17                        | . AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   87 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' . AssignmentExpressionOpt ']'
   88                         | DirectAbstractDeclaratorOpt '[' . TypeQualifierList AssignmentExpressionOpt ']'
   89                         | DirectAbstractDeclaratorOpt '[' . STATIC TypeQualifierListOpt AssignmentExpression ']'
   90                         | DirectAbstractDeclaratorOpt '[' . TypeQualifierList STATIC AssignmentExpression ']'
   91                         | DirectAbstractDeclaratorOpt '[' . '*' ']'
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  262 TypeQualifierList: . TypeQualifier
  263                  | . TypeQualifierList TypeQualifier
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 457
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    CONST              posunout a přejít do stavu 37
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RESTRICT           posunout a přejít do stavu 46
    SIZEOF             posunout a přejít do stavu 174
    STATIC             posunout a přejít do stavu 458
    STRINGLITERAL      posunout a přejít do stavu 175
    VOLATILE           posunout a přejít do stavu 56

    $výchozí  reduce using rule 16 (AssignmentExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 392
    AssignmentExpressionOpt   přejít do stavu 459
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    TypeQualifier             přejít do stavu 128
    TypeQualifierList         přejít do stavu 460
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 441

  165 InitializerList: InitializerList . ',' DesignationOpt Initializer
  204 PostfixExpression: '(' TypeName ')' '{' InitializerList . '}'
  205                  | '(' TypeName ')' '{' InitializerList . ',' '}'

    ','  posunout a přejít do stavu 461
    '}'  posunout a přejít do stavu 462


State 442

  165 InitializerList: InitializerList ',' DesignationOpt Initializer .

    $výchozí  reduce using rule 165 (InitializerList)


State 443

   39 ConditionalExpression: LogicalOrExpression '?' Expression ':' ConditionalExpression .

    $výchozí  reduce using rule 39 (ConditionalExpression)


State 444

   11 ArgumentExpressionList: ArgumentExpressionList ',' AssignmentExpression .

    $výchozí  reduce using rule 11 (ArgumentExpressionList)


State 445

  175 LabeledStatement: CASE ConstantExpression ':' Statement .

    $výchozí  reduce using rule 175 (LabeledStatement)


State 446

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  167 IterationStatement: DO Statement WHILE '(' . Expression ')' ';'
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 463
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 447

  169 IterationStatement: FOR '(' Declaration ExpressionOpt . ';' ExpressionOpt ')' Statement

    ';'  posunout a přejít do stavu 464


State 448

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  168 IterationStatement: FOR '(' ExpressionOpt ';' . ExpressionOpt ';' ExpressionOpt ')' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 465
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 449

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  221                   | IF '(' Expression ')' . Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  222                   | IF '(' Expression ')' . Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 466
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 450

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  223                   | SWITCH '(' Expression ')' . Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 467
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 451

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  166                   | WHILE '(' Expression ')' . Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 468
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 452

   99 DirectDeclarator: DirectDeclarator '[' STATIC TypeQualifierListOpt AssignmentExpression ']' .

    $výchozí  reduce using rule 99 (DirectDeclarator)


State 453

  100 DirectDeclarator: DirectDeclarator '[' TypeQualifierList STATIC AssignmentExpression ']' .

    $výchozí  reduce using rule 100 (DirectDeclarator)


State 454

   86 DirectAbstractDeclarator: '(' AbstractDeclarator ')' .

    $výchozí  reduce using rule 86 (DirectAbstractDeclarator)


State 455

   92 DirectAbstractDeclarator: '(' ParameterTypeListOpt ')' .

    $výchozí  reduce using rule 92 (DirectAbstractDeclarator)


State 456

   93 DirectAbstractDeclarator: DirectAbstractDeclarator '(' ParameterTypeListOpt . ')'

    ')'  posunout a přejít do stavu 469


State 457

   91 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' '*' . ']'
  287 UnaryOperator: '*' .  ['!', '&', '(', '*', '+', '-', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]

    ']'  posunout a přejít do stavu 470

    $výchozí  reduce using rule 287 (UnaryOperator)


State 458

   89 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' STATIC . TypeQualifierListOpt AssignmentExpression ']'
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  262 TypeQualifierList: . TypeQualifier
  263                  | . TypeQualifierList TypeQualifier
  264 TypeQualifierListOpt: . %empty  ['!', '&', '(', '*', '+', '-', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]
  265                     | . TypeQualifierList

    CONST     posunout a přejít do stavu 37
    RESTRICT  posunout a přejít do stavu 46
    VOLATILE  posunout a přejít do stavu 56

    $výchozí  reduce using rule 264 (TypeQualifierListOpt)

    TypeQualifier         přejít do stavu 128
    TypeQualifierList     přejít do stavu 129
    TypeQualifierListOpt  přejít do stavu 471


State 459

   87 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' AssignmentExpressionOpt . ']'

    ']'  posunout a přejít do stavu 472


State 460

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   16 AssignmentExpressionOpt: . %empty  [']']
   17                        | . AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   88 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' TypeQualifierList . AssignmentExpressionOpt ']'
   90                         | DirectAbstractDeclaratorOpt '[' TypeQualifierList . STATIC AssignmentExpression ']'
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  259 TypeQualifier: . CONST
  260              | . RESTRICT
  261              | . VOLATILE
  263 TypeQualifierList: TypeQualifierList . TypeQualifier
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    CONST              posunout a přejít do stavu 37
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RESTRICT           posunout a přejít do stavu 46
    SIZEOF             posunout a přejít do stavu 174
    STATIC             posunout a přejít do stavu 473
    STRINGLITERAL      posunout a přejít do stavu 175
    VOLATILE           posunout a přejít do stavu 56

    $výchozí  reduce using rule 16 (AssignmentExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 392
    AssignmentExpressionOpt   přejít do stavu 474
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    TypeQualifier             přejít do stavu 156
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 461

   79 Designation: . DesignatorList '='
   80 DesignationOpt: . %empty  ['!', '&', '(', '*', '+', '-', '{', '~', CHARCONST, DEC, FLOATCONST, IDENTIFIER, INC, INTCONST, LONGCHARCONST, LONGSTRINGLITERAL, SIZEOF, STRINGLITERAL]
   81               | . Designation
   82 Designator: . '[' ConstantExpression ']'
   83           | . '.' IDENTIFIER
   84 DesignatorList: . Designator
   85               | . DesignatorList Designator
  165 InitializerList: InitializerList ',' . DesignationOpt Initializer
  205 PostfixExpression: '(' TypeName ')' '{' InitializerList ',' . '}'

    '.'  posunout a přejít do stavu 222
    '['  posunout a přejít do stavu 223
    '}'  posunout a přejít do stavu 475

    $výchozí  reduce using rule 80 (DesignationOpt)

    Designation     přejít do stavu 224
    DesignationOpt  přejít do stavu 407
    Designator      přejít do stavu 226
    DesignatorList  přejít do stavu 227


State 462

  204 PostfixExpression: '(' TypeName ')' '{' InitializerList '}' .

    $výchozí  reduce using rule 204 (PostfixExpression)


State 463

  127 Expression: Expression . ',' AssignmentExpression
  167 IterationStatement: DO Statement WHILE '(' Expression . ')' ';'

    ')'  posunout a přejít do stavu 476
    ','  posunout a přejít do stavu 326


State 464

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [')']
  129              | . Expression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  169 IterationStatement: FOR '(' Declaration ExpressionOpt ';' . ExpressionOpt ')' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 477
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 465

  168 IterationStatement: FOR '(' ExpressionOpt ';' ExpressionOpt . ';' ExpressionOpt ')' Statement

    ';'  posunout a přejít do stavu 478


State 466

  221 SelectionStatement: IF '(' Expression ')' Statement .  ['!', '&', '(', '*', '+', '-', ';', '{', '}', '~', AUTO, BOOL, BREAK, CASE, CHAR, CHARCONST, COMPLEX, CONST, CONTINUE, DEC, DEFAULT, DO, DOUBLE, ENUM, EXTERN, FLOAT, FLOATCONST, FOR, GOTO, IDENTIFIER, IF, INC, INLINE, INT, INTCONST, LONG, LONGCHARCONST, LONGSTRINGLITERAL, REGISTER, RESTRICT, RETURN, SHORT, SIGNED, SIZEOF, STATIC, STRINGLITERAL, STRUCT, SWITCH, TYPEDEF, TYPEDEFNAME, UNION, UNSIGNED, VOID, VOLATILE, WHILE]
  222                   | IF '(' Expression ')' Statement . ELSE Statement

    ELSE  posunout a přejít do stavu 479

    $výchozí  reduce using rule 221 (SelectionStatement)

    Conflict between rule 221 and token ELSE resolved as shift (NOELSE < ELSE).


State 467

  223 SelectionStatement: SWITCH '(' Expression ')' Statement .

    $výchozí  reduce using rule 223 (SelectionStatement)


State 468

  166 IterationStatement: WHILE '(' Expression ')' Statement .

    $výchozí  reduce using rule 166 (IterationStatement)


State 469

   93 DirectAbstractDeclarator: DirectAbstractDeclarator '(' ParameterTypeListOpt ')' .

    $výchozí  reduce using rule 93 (DirectAbstractDeclarator)


State 470

   91 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' '*' ']' .

    $výchozí  reduce using rule 91 (DirectAbstractDeclarator)


State 471

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   89 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' STATIC TypeQualifierListOpt . AssignmentExpression ']'
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 480
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 472

   87 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' AssignmentExpressionOpt ']' .

    $výchozí  reduce using rule 87 (DirectAbstractDeclarator)


State 473

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
   90 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' TypeQualifierList STATIC . AssignmentExpression ']'
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 481
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 474

   88 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' TypeQualifierList AssignmentExpressionOpt . ']'

    ']'  posunout a přejít do stavu 482


State 475

  205 PostfixExpression: '(' TypeName ')' '{' InitializerList ',' '}' .

    $výchozí  reduce using rule 205 (PostfixExpression)


State 476

  167 IterationStatement: DO Statement WHILE '(' Expression ')' . ';'

    ';'  posunout a přejít do stavu 483


State 477

  169 IterationStatement: FOR '(' Declaration ExpressionOpt ';' ExpressionOpt . ')' Statement

    ')'  posunout a přejít do stavu 484


State 478

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [')']
  129              | . Expression
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  168 IterationStatement: FOR '(' ExpressionOpt ';' ExpressionOpt ';' . ExpressionOpt ')' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '~'                posunout a přejít do stavu 165
    CHARCONST          posunout a přejít do stavu 166
    DEC                posunout a přejít do stavu 167
    FLOATCONST         posunout a přejít do stavu 168
    IDENTIFIER         posunout a přejít do stavu 169
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 485
    InclusiveOrExpression     přejít do stavu 184
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    ShiftExpression           přejít do stavu 192
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 479

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  222                   | IF '(' Expression ')' Statement ELSE . Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 486
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 480

   89 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' STATIC TypeQualifierListOpt AssignmentExpression . ']'

    ']'  posunout a přejít do stavu 487


State 481

   90 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' TypeQualifierList STATIC AssignmentExpression . ']'

    ']'  posunout a přejít do stavu 488


State 482

   88 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' TypeQualifierList AssignmentExpressionOpt ']' .

    $výchozí  reduce using rule 88 (DirectAbstractDeclarator)


State 483

  167 IterationStatement: DO Statement WHILE '(' Expression ')' ';' .

    $výchozí  reduce using rule 167 (IterationStatement)


State 484

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  169                   | FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' . Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 489
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 485

  168 IterationStatement: FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt . ')' Statement

    ')'  posunout a přejít do stavu 490


State 486

  222 SelectionStatement: IF '(' Expression ')' Statement ELSE Statement .

    $výchozí  reduce using rule 222 (SelectionStatement)


State 487

   89 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' STATIC TypeQualifierListOpt AssignmentExpression ']' .

    $výchozí  reduce using rule 89 (DirectAbstractDeclarator)


State 488

   90 DirectAbstractDeclarator: DirectAbstractDeclaratorOpt '[' TypeQualifierList STATIC AssignmentExpression ']' .

    $výchozí  reduce using rule 90 (DirectAbstractDeclarator)


State 489

  169 IterationStatement: FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement .

    $výchozí  reduce using rule 169 (IterationStatement)


State 490

    5 AdditiveExpression: . MultiplicativeExpression
    6                   | . AdditiveExpression '+' MultiplicativeExpression
    7                   | . AdditiveExpression '-' MultiplicativeExpression
    8 AndExpression: . EqualityExpression
    9              | . AndExpression '&' EqualityExpression
   14 AssignmentExpression: . ConditionalExpression
   15                     | . UnaryExpression AssignmentOperator AssignmentExpression
   35 CastExpression: . UnaryExpression
   36               | . '(' TypeName ')' CastExpression
   37 CompoundStatement: . '{' BlockItemListOpt '}'
   38 ConditionalExpression: . LogicalOrExpression
   39                      | . LogicalOrExpression '?' Expression ':' ConditionalExpression
   40 Constant: . CHARCONST
   41         | . FLOATCONST
   42         | . INTCONST
   43         | . LONGCHARCONST
   44         | . LONGSTRINGLITERAL
   45         | . STRINGLITERAL
  121 EqualityExpression: . RelationalExpression
  122                   | . EqualityExpression EQ RelationalExpression
  123                   | . EqualityExpression NEQ RelationalExpression
  124 ExclusiveOrExpression: . AndExpression
  125                      | . ExclusiveOrExpression '^' AndExpression
  126 Expression: . AssignmentExpression
  127           | . Expression ',' AssignmentExpression
  128 ExpressionOpt: . %empty  [';']
  129              | . Expression
  130 ExpressionStatement: . ExpressionOpt ';'
  153 InclusiveOrExpression: . ExclusiveOrExpression
  154                      | . InclusiveOrExpression '|' ExclusiveOrExpression
  166 IterationStatement: . WHILE '(' Expression ')' Statement
  167                   | . DO Statement WHILE '(' Expression ')' ';'
  168                   | . FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement
  168                   | FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' . Statement
  169                   | . FOR '(' Declaration ExpressionOpt ';' ExpressionOpt ')' Statement
  170 JumpStatement: . GOTO IDENTIFIER ';'
  171              | . CONTINUE ';'
  172              | . BREAK ';'
  173              | . RETURN ExpressionOpt ';'
  174 LabeledStatement: . IDENTIFIER ':' Statement
  175                 | . CASE ConstantExpression ':' Statement
  176                 | . DEFAULT ':' Statement
  177 LogicalAndExpression: . InclusiveOrExpression
  178                     | . LogicalAndExpression ANDAND InclusiveOrExpression
  179 LogicalOrExpression: . LogicalAndExpression
  180                    | . LogicalOrExpression OROR LogicalAndExpression
  181 MultiplicativeExpression: . CastExpression
  182                         | . MultiplicativeExpression '*' CastExpression
  183                         | . MultiplicativeExpression '/' CastExpression
  184                         | . MultiplicativeExpression '%' CastExpression
  197 PostfixExpression: . PrimaryExpression
  198                  | . PostfixExpression '[' Expression ']'
  199                  | . PostfixExpression '(' ArgumentExpressionListOpt ')'
  200                  | . PostfixExpression '.' IDENTIFIER
  201                  | . PostfixExpression ARROW IDENTIFIER
  202                  | . PostfixExpression INC
  203                  | . PostfixExpression DEC
  204                  | . '(' TypeName ')' '{' InitializerList '}'
  205                  | . '(' TypeName ')' '{' InitializerList ',' '}'
  212 PrimaryExpression: . IDENTIFIER
  213                  | . Constant
  214                  | . '(' Expression ')'
  215 RelationalExpression: . ShiftExpression
  216                     | . RelationalExpression '<' ShiftExpression
  217                     | . RelationalExpression '>' ShiftExpression
  218                     | . RelationalExpression LEQ ShiftExpression
  219                     | . RelationalExpression GEQ ShiftExpression
  221 SelectionStatement: . IF '(' Expression ')' Statement
  222                   | . IF '(' Expression ')' Statement ELSE Statement
  223                   | . SWITCH '(' Expression ')' Statement
  224 ShiftExpression: . AdditiveExpression
  225                | . ShiftExpression LSH AdditiveExpression
  226                | . ShiftExpression RSH AdditiveExpression
  233 Statement: . LabeledStatement
  234          | . CompoundStatement
  235          | . ExpressionStatement
  236          | . SelectionStatement
  237          | . IterationStatement
  238          | . JumpStatement
  280 UnaryExpression: . PostfixExpression
  281                | . INC UnaryExpression
  282                | . DEC UnaryExpression
  283                | . UnaryOperator CastExpression
  284                | . SIZEOF UnaryExpression
  285                | . SIZEOF '(' TypeName ')'
  286 UnaryOperator: . '&'
  287              | . '*'
  288              | . '+'
  289              | . '-'
  290              | . '~'
  291              | . '!'

    '!'                posunout a přejít do stavu 158
    '&'                posunout a přejít do stavu 159
    '('                posunout a přejít do stavu 160
    '*'                posunout a přejít do stavu 161
    '+'                posunout a přejít do stavu 162
    '-'                posunout a přejít do stavu 163
    '{'                posunout a přejít do stavu 196
    '~'                posunout a přejít do stavu 165
    BREAK              posunout a přejít do stavu 273
    CASE               posunout a přejít do stavu 274
    CHARCONST          posunout a přejít do stavu 166
    CONTINUE           posunout a přejít do stavu 275
    DEC                posunout a přejít do stavu 167
    DEFAULT            posunout a přejít do stavu 276
    DO                 posunout a přejít do stavu 277
    FLOATCONST         posunout a přejít do stavu 168
    FOR                posunout a přejít do stavu 278
    GOTO               posunout a přejít do stavu 279
    IDENTIFIER         posunout a přejít do stavu 280
    IF                 posunout a přejít do stavu 281
    INC                posunout a přejít do stavu 170
    INTCONST           posunout a přejít do stavu 171
    LONGCHARCONST      posunout a přejít do stavu 172
    LONGSTRINGLITERAL  posunout a přejít do stavu 173
    RETURN             posunout a přejít do stavu 282
    SIZEOF             posunout a přejít do stavu 174
    STRINGLITERAL      posunout a přejít do stavu 175
    SWITCH             posunout a přejít do stavu 283
    WHILE              posunout a přejít do stavu 284

    $výchozí  reduce using rule 128 (ExpressionOpt)

    AdditiveExpression        přejít do stavu 176
    AndExpression             přejít do stavu 177
    AssignmentExpression      přejít do stavu 218
    CastExpression            přejít do stavu 179
    CompoundStatement         přejít do stavu 288
    ConditionalExpression     přejít do stavu 180
    Constant                  přejít do stavu 181
    EqualityExpression        přejít do stavu 182
    ExclusiveOrExpression     přejít do stavu 183
    Expression                přejít do stavu 290
    ExpressionOpt             přejít do stavu 291
    ExpressionStatement       přejít do stavu 292
    InclusiveOrExpression     přejít do stavu 184
    IterationStatement        přejít do stavu 293
    JumpStatement             přejít do stavu 294
    LabeledStatement          přejít do stavu 295
    LogicalAndExpression      přejít do stavu 186
    LogicalOrExpression       přejít do stavu 187
    MultiplicativeExpression  přejít do stavu 188
    PostfixExpression         přejít do stavu 189
    PrimaryExpression         přejít do stavu 190
    RelationalExpression      přejít do stavu 191
    SelectionStatement        přejít do stavu 296
    ShiftExpression           přejít do stavu 192
    Statement                 přejít do stavu 491
    UnaryExpression           přejít do stavu 193
    UnaryOperator             přejít do stavu 194


State 491

  168 IterationStatement: FOR '(' ExpressionOpt ';' ExpressionOpt ';' ExpressionOpt ')' Statement .

    $výchozí  reduce using rule 168 (IterationStatement)
