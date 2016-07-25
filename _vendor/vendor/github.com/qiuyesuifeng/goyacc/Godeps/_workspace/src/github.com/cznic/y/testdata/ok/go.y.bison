Terminals unused in grammar

   LIGNORE


Gramatika

    0 $accept: file $end

    1 file: loadsys package imports xdcl_list

    2 package: %empty
    3        | LPACKAGE sym ';'

    4 $@1: %empty

    5 loadsys: $@1 import_package import_there

    6 imports: %empty
    7        | imports import ';'

    8 import: LIMPORT import_stmt
    9       | LIMPORT '(' import_stmt_list osemi ')'
   10       | LIMPORT '(' ')'

   11 import_stmt: import_here import_package import_there
   12            | import_here import_there

   13 import_stmt_list: import_stmt
   14                 | import_stmt_list ';' import_stmt

   15 import_here: LLITERAL
   16            | sym LLITERAL
   17            | '.' LLITERAL

   18 import_package: LPACKAGE LNAME import_safety ';'

   19 import_safety: %empty
   20              | LNAME

   21 $@2: %empty

   22 import_there: $@2 hidden_import_list '$' '$'

   23 xdcl: %empty
   24     | common_dcl
   25     | xfndcl
   26     | non_dcl_stmt
   27     | error

   28 common_dcl: LVAR vardcl
   29           | LVAR '(' vardcl_list osemi ')'
   30           | LVAR '(' ')'
   31           | lconst constdcl
   32           | lconst '(' constdcl osemi ')'
   33           | lconst '(' constdcl ';' constdcl_list osemi ')'
   34           | lconst '(' ')'
   35           | LTYPE typedcl
   36           | LTYPE '(' typedcl_list osemi ')'
   37           | LTYPE '(' ')'

   38 lconst: LCONST

   39 vardcl: dcl_name_list ntype
   40       | dcl_name_list ntype '=' expr_list
   41       | dcl_name_list '=' expr_list

   42 constdcl: dcl_name_list ntype '=' expr_list
   43         | dcl_name_list '=' expr_list

   44 constdcl1: constdcl
   45          | dcl_name_list ntype
   46          | dcl_name_list

   47 typedclname: sym

   48 typedcl: typedclname ntype

   49 simple_stmt: expr
   50            | expr LASOP expr
   51            | expr_list '=' expr_list
   52            | expr_list LCOLAS expr_list
   53            | expr LINC
   54            | expr LDEC

   55 case: LCASE expr_or_type_list ':'
   56     | LCASE expr_or_type_list '=' expr ':'
   57     | LCASE expr_or_type_list LCOLAS expr ':'
   58     | LDEFAULT ':'

   59 $@3: %empty

   60 compound_stmt: '{' $@3 stmt_list '}'

   61 $@4: %empty

   62 caseblock: case $@4 stmt_list

   63 caseblock_list: %empty
   64               | caseblock_list caseblock

   65 $@5: %empty

   66 loop_body: LBODY $@5 stmt_list '}'

   67 range_stmt: expr_list '=' LRANGE expr
   68           | expr_list LCOLAS LRANGE expr

   69 for_header: osimple_stmt ';' osimple_stmt ';' osimple_stmt
   70           | osimple_stmt
   71           | range_stmt

   72 for_body: for_header loop_body

   73 $@6: %empty

   74 for_stmt: LFOR $@6 for_body

   75 if_header: osimple_stmt
   76          | osimple_stmt ';' osimple_stmt

   77 $@7: %empty

   78 $@8: %empty

   79 $@9: %empty

   80 if_stmt: LIF $@7 if_header $@8 loop_body $@9 elseif_list else

   81 $@10: %empty

   82 elseif: LELSE LIF $@10 if_header loop_body

   83 elseif_list: %empty
   84            | elseif_list elseif

   85 else: %empty
   86     | LELSE compound_stmt

   87 $@11: %empty

   88 $@12: %empty

   89 switch_stmt: LSWITCH $@11 if_header $@12 LBODY caseblock_list '}'

   90 $@13: %empty

   91 select_stmt: LSELECT $@13 LBODY caseblock_list '}'

   92 expr: uexpr
   93     | expr LOROR expr
   94     | expr LANDAND expr
   95     | expr LEQ expr
   96     | expr LNE expr
   97     | expr LLT expr
   98     | expr LLE expr
   99     | expr LGE expr
  100     | expr LGT expr
  101     | expr '+' expr
  102     | expr '-' expr
  103     | expr '|' expr
  104     | expr '^' expr
  105     | expr '*' expr
  106     | expr '/' expr
  107     | expr '%' expr
  108     | expr '&' expr
  109     | expr LANDNOT expr
  110     | expr LLSH expr
  111     | expr LRSH expr
  112     | expr LCOMM expr

  113 uexpr: pexpr
  114      | '*' uexpr
  115      | '&' uexpr
  116      | '+' uexpr
  117      | '-' uexpr
  118      | '!' uexpr
  119      | '~' uexpr
  120      | '^' uexpr
  121      | LCOMM uexpr

  122 pseudocall: pexpr '(' ')'
  123           | pexpr '(' expr_or_type_list ocomma ')'
  124           | pexpr '(' expr_or_type_list LDDD ocomma ')'

  125 pexpr_no_paren: LLITERAL
  126               | name
  127               | pexpr '.' sym
  128               | pexpr '.' '(' expr_or_type ')'
  129               | pexpr '.' '(' LTYPE ')'
  130               | pexpr '[' expr ']'
  131               | pexpr '[' oexpr ':' oexpr ']'
  132               | pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | pseudocall
  134               | convtype '(' expr ocomma ')'
  135               | comptype lbrace start_complit braced_keyval_list '}'
  136               | pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | fnliteral

  139 start_complit: %empty

  140 keyval: expr ':' complitexpr

  141 bare_complitexpr: expr
  142                 | '{' start_complit braced_keyval_list '}'

  143 complitexpr: expr
  144            | '{' start_complit braced_keyval_list '}'

  145 pexpr: pexpr_no_paren
  146      | '(' expr_or_type ')'

  147 expr_or_type: expr
  148             | non_expr_type

  149 name_or_type: ntype

  150 lbrace: LBODY
  151       | '{'

  152 new_name: sym

  153 dcl_name: sym

  154 onew_name: %empty
  155          | new_name

  156 sym: LNAME
  157    | hidden_importsym
  158    | '?'

  159 hidden_importsym: '@' LLITERAL '.' LNAME
  160                 | '@' LLITERAL '.' '?'

  161 name: sym

  162 labelname: new_name

  163 dotdotdot: LDDD
  164          | LDDD ntype

  165 ntype: recvchantype
  166      | fntype
  167      | othertype
  168      | ptrtype
  169      | dotname
  170      | '(' ntype ')'

  171 non_expr_type: recvchantype
  172              | fntype
  173              | othertype
  174              | '*' non_expr_type

  175 non_recvchantype: fntype
  176                 | othertype
  177                 | ptrtype
  178                 | dotname
  179                 | '(' ntype ')'

  180 convtype: fntype
  181         | othertype

  182 comptype: othertype

  183 fnret_type: recvchantype
  184           | fntype
  185           | othertype
  186           | ptrtype
  187           | dotname

  188 dotname: name
  189        | name '.' sym

  190 othertype: '[' oexpr ']' ntype
  191          | '[' LDDD ']' ntype
  192          | LCHAN non_recvchantype
  193          | LCHAN LCOMM ntype
  194          | LMAP '[' ntype ']' ntype
  195          | structtype
  196          | interfacetype

  197 ptrtype: '*' ntype

  198 recvchantype: LCOMM LCHAN ntype

  199 structtype: LSTRUCT lbrace structdcl_list osemi '}'
  200           | LSTRUCT lbrace '}'

  201 interfacetype: LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | LINTERFACE lbrace '}'

  203 xfndcl: LFUNC fndcl fnbody

  204 fndcl: sym '(' oarg_type_list_ocomma ')' fnres
  205      | '(' oarg_type_list_ocomma ')' sym '(' oarg_type_list_ocomma ')' fnres

  206 hidden_fndcl: hidden_pkg_importsym '(' ohidden_funarg_list ')' ohidden_funres
  207             | '(' hidden_funarg_list ')' sym '(' ohidden_funarg_list ')' ohidden_funres

  208 fntype: LFUNC '(' oarg_type_list_ocomma ')' fnres

  209 fnbody: %empty
  210       | '{' stmt_list '}'

  211 fnres: %empty
  212      | fnret_type
  213      | '(' oarg_type_list_ocomma ')'

  214 fnlitdcl: fntype

  215 fnliteral: fnlitdcl lbrace stmt_list '}'
  216          | fnlitdcl error

  217 xdcl_list: %empty
  218          | xdcl_list xdcl ';'

  219 vardcl_list: vardcl
  220            | vardcl_list ';' vardcl

  221 constdcl_list: constdcl1
  222              | constdcl_list ';' constdcl1

  223 typedcl_list: typedcl
  224             | typedcl_list ';' typedcl

  225 structdcl_list: structdcl
  226               | structdcl_list ';' structdcl

  227 interfacedcl_list: interfacedcl
  228                  | interfacedcl_list ';' interfacedcl

  229 structdcl: new_name_list ntype oliteral
  230          | embed oliteral
  231          | '(' embed ')' oliteral
  232          | '*' embed oliteral
  233          | '(' '*' embed ')' oliteral
  234          | '*' '(' embed ')' oliteral

  235 packname: LNAME
  236         | LNAME '.' sym

  237 embed: packname

  238 interfacedcl: new_name indcl
  239             | packname
  240             | '(' packname ')'

  241 indcl: '(' oarg_type_list_ocomma ')' fnres

  242 arg_type: name_or_type
  243         | sym name_or_type
  244         | sym dotdotdot
  245         | dotdotdot

  246 arg_type_list: arg_type
  247              | arg_type_list ',' arg_type

  248 oarg_type_list_ocomma: %empty
  249                      | arg_type_list ocomma

  250 stmt: %empty
  251     | compound_stmt
  252     | common_dcl
  253     | non_dcl_stmt
  254     | error

  255 non_dcl_stmt: simple_stmt
  256             | for_stmt
  257             | switch_stmt
  258             | select_stmt
  259             | if_stmt

  260 $@14: %empty

  261 non_dcl_stmt: labelname ':' $@14 stmt
  262             | LFALL
  263             | LBREAK onew_name
  264             | LCONTINUE onew_name
  265             | LGO pseudocall
  266             | LDEFER pseudocall
  267             | LGOTO new_name
  268             | LRETURN oexpr_list

  269 stmt_list: stmt
  270          | stmt_list ';' stmt

  271 new_name_list: new_name
  272              | new_name_list ',' new_name

  273 dcl_name_list: dcl_name
  274              | dcl_name_list ',' dcl_name

  275 expr_list: expr
  276          | expr_list ',' expr

  277 expr_or_type_list: expr_or_type
  278                  | expr_or_type_list ',' expr_or_type

  279 keyval_list: keyval
  280            | bare_complitexpr
  281            | keyval_list ',' keyval
  282            | keyval_list ',' bare_complitexpr

  283 braced_keyval_list: %empty
  284                   | keyval_list ocomma

  285 osemi: %empty
  286      | ';'

  287 ocomma: %empty
  288       | ','

  289 oexpr: %empty
  290      | expr

  291 oexpr_list: %empty
  292           | expr_list

  293 osimple_stmt: %empty
  294             | simple_stmt

  295 ohidden_funarg_list: %empty
  296                    | hidden_funarg_list

  297 ohidden_structdcl_list: %empty
  298                       | hidden_structdcl_list

  299 ohidden_interfacedcl_list: %empty
  300                          | hidden_interfacedcl_list

  301 oliteral: %empty
  302         | LLITERAL

  303 hidden_import: LIMPORT LNAME LLITERAL ';'
  304              | LVAR hidden_pkg_importsym hidden_type ';'
  305              | LCONST hidden_pkg_importsym '=' hidden_constant ';'
  306              | LCONST hidden_pkg_importsym hidden_type '=' hidden_constant ';'
  307              | LTYPE hidden_pkgtype hidden_type ';'
  308              | LFUNC hidden_fndcl fnbody ';'

  309 hidden_pkg_importsym: hidden_importsym

  310 hidden_pkgtype: hidden_pkg_importsym

  311 hidden_type: hidden_type_misc
  312            | hidden_type_recv_chan
  313            | hidden_type_func

  314 hidden_type_non_recv_chan: hidden_type_misc
  315                          | hidden_type_func

  316 hidden_type_misc: hidden_importsym
  317                 | LNAME
  318                 | '[' ']' hidden_type
  319                 | '[' LLITERAL ']' hidden_type
  320                 | LMAP '[' hidden_type ']' hidden_type
  321                 | LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | '*' hidden_type
  324                 | LCHAN hidden_type_non_recv_chan
  325                 | LCHAN '(' hidden_type_recv_chan ')'
  326                 | LCHAN LCOMM hidden_type

  327 hidden_type_recv_chan: LCOMM LCHAN hidden_type

  328 hidden_type_func: LFUNC '(' ohidden_funarg_list ')' ohidden_funres

  329 hidden_funarg: sym hidden_type oliteral
  330              | sym LDDD hidden_type oliteral

  331 hidden_structdcl: sym hidden_type oliteral

  332 hidden_interfacedcl: sym '(' ohidden_funarg_list ')' ohidden_funres
  333                    | hidden_type

  334 ohidden_funres: %empty
  335               | hidden_funres

  336 hidden_funres: '(' ohidden_funarg_list ')'
  337              | hidden_type

  338 hidden_literal: LLITERAL
  339               | '-' LLITERAL
  340               | sym

  341 hidden_constant: hidden_literal
  342                | '(' hidden_literal '+' hidden_literal ')'

  343 hidden_import_list: %empty
  344                   | hidden_import_list hidden_import

  345 hidden_funarg_list: hidden_funarg
  346                   | hidden_funarg_list ',' hidden_funarg

  347 hidden_structdcl_list: hidden_structdcl
  348                      | hidden_structdcl_list ';' hidden_structdcl

  349 hidden_interfacedcl_list: hidden_interfacedcl
  350                         | hidden_interfacedcl_list ';' hidden_interfacedcl


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'!' (33) 118
'$' (36) 22
'%' (37) 107
'&' (38) 108 115
'(' (40) 9 10 29 30 32 33 34 36 37 122 123 124 128 129 134 137 146
    170 179 204 205 206 207 208 213 231 233 234 240 241 325 328 332
    336 342
')' (41) 9 10 29 30 32 33 34 36 37 122 123 124 128 129 134 137 146
    170 179 204 205 206 207 208 213 231 233 234 240 241 325 328 332
    336 342
'*' (42) 105 114 174 197 232 233 234 323
'+' (43) 101 116 342
',' (44) 247 272 274 276 278 281 282 288 346
'-' (45) 102 117 339
'.' (46) 17 127 128 129 159 160 189 236
'/' (47) 106
':' (58) 55 56 57 58 131 132 140 261
';' (59) 3 7 14 18 33 69 76 218 220 222 224 226 228 270 286 303 304
    305 306 307 308 348 350
'=' (61) 40 41 42 43 51 56 67 305 306
'?' (63) 158 160
'@' (64) 159 160
'[' (91) 130 131 132 190 191 194 318 319 320
']' (93) 130 131 132 190 191 194 318 319 320
'^' (94) 104 120
'{' (123) 60 136 137 142 144 151 210 321 322
'|' (124) 103
'}' (125) 60 66 89 91 135 136 137 142 144 199 200 201 202 210 215 321
    322
'~' (126) 119
error (256) 27 216 254
LLITERAL (258) 15 16 17 125 159 160 302 303 319 338 339
LASOP (259) 50
LCOLAS (260) 52 57 68
LBREAK (261) 263
LCASE (262) 55 56 57
LCHAN (263) 192 193 198 324 325 326 327
LCONST (264) 38 305 306
LCONTINUE (265) 264
LDDD (266) 124 163 164 191 330
LDEFAULT (267) 58
LDEFER (268) 266
LELSE (269) 82 86
LFALL (270) 262
LFOR (271) 74
LFUNC (272) 203 208 308 328
LGO (273) 265
LGOTO (274) 267
LIF (275) 80 82
LIMPORT (276) 8 9 10 303
LINTERFACE (277) 201 202 322
LMAP (278) 194 320
LNAME (279) 18 20 156 159 235 236 303 317
LPACKAGE (280) 3 18
LRANGE (281) 67 68
LRETURN (282) 268
LSELECT (283) 91
LSTRUCT (284) 199 200 321
LSWITCH (285) 89
LTYPE (286) 35 36 37 129 307
LVAR (287) 28 29 30 304
LANDAND (288) 94
LANDNOT (289) 109
LBODY (290) 66 89 91 150
LCOMM (291) 112 121 193 198 326 327
LDEC (292) 54
LEQ (293) 95
LGE (294) 99
LGT (295) 100
LIGNORE (296)
LINC (297) 53
LLE (298) 98
LLSH (299) 110
LLT (300) 97
LNE (301) 96
LOROR (302) 93
LRSH (303) 111
NotPackage (304)
NotParen (305)
PreferToRightParen (306)


Neterminály s pravidly, ve kterých se objevují

$accept (76)
    vlevo: 0
file (77)
    vlevo: 1, vpravo: 0
package (78)
    vlevo: 2 3, vpravo: 1
loadsys (79)
    vlevo: 5, vpravo: 1
$@1 (80)
    vlevo: 4, vpravo: 5
imports (81)
    vlevo: 6 7, vpravo: 1 7
import (82)
    vlevo: 8 9 10, vpravo: 7
import_stmt (83)
    vlevo: 11 12, vpravo: 8 13 14
import_stmt_list (84)
    vlevo: 13 14, vpravo: 9 14
import_here (85)
    vlevo: 15 16 17, vpravo: 11 12
import_package (86)
    vlevo: 18, vpravo: 5 11
import_safety (87)
    vlevo: 19 20, vpravo: 18
import_there (88)
    vlevo: 22, vpravo: 5 11 12
$@2 (89)
    vlevo: 21, vpravo: 22
xdcl (90)
    vlevo: 23 24 25 26 27, vpravo: 218
common_dcl (91)
    vlevo: 28 29 30 31 32 33 34 35 36 37, vpravo: 24 252
lconst (92)
    vlevo: 38, vpravo: 31 32 33 34
vardcl (93)
    vlevo: 39 40 41, vpravo: 28 219 220
constdcl (94)
    vlevo: 42 43, vpravo: 31 32 33 44
constdcl1 (95)
    vlevo: 44 45 46, vpravo: 221 222
typedclname (96)
    vlevo: 47, vpravo: 48
typedcl (97)
    vlevo: 48, vpravo: 35 223 224
simple_stmt (98)
    vlevo: 49 50 51 52 53 54, vpravo: 255 294
case (99)
    vlevo: 55 56 57 58, vpravo: 62
compound_stmt (100)
    vlevo: 60, vpravo: 86 251
$@3 (101)
    vlevo: 59, vpravo: 60
caseblock (102)
    vlevo: 62, vpravo: 64
$@4 (103)
    vlevo: 61, vpravo: 62
caseblock_list (104)
    vlevo: 63 64, vpravo: 64 89 91
loop_body (105)
    vlevo: 66, vpravo: 72 80 82
$@5 (106)
    vlevo: 65, vpravo: 66
range_stmt (107)
    vlevo: 67 68, vpravo: 71
for_header (108)
    vlevo: 69 70 71, vpravo: 72
for_body (109)
    vlevo: 72, vpravo: 74
for_stmt (110)
    vlevo: 74, vpravo: 256
$@6 (111)
    vlevo: 73, vpravo: 74
if_header (112)
    vlevo: 75 76, vpravo: 80 82 89
if_stmt (113)
    vlevo: 80, vpravo: 259
$@7 (114)
    vlevo: 77, vpravo: 80
$@8 (115)
    vlevo: 78, vpravo: 80
$@9 (116)
    vlevo: 79, vpravo: 80
elseif (117)
    vlevo: 82, vpravo: 84
$@10 (118)
    vlevo: 81, vpravo: 82
elseif_list (119)
    vlevo: 83 84, vpravo: 80 84
else (120)
    vlevo: 85 86, vpravo: 80
switch_stmt (121)
    vlevo: 89, vpravo: 257
$@11 (122)
    vlevo: 87, vpravo: 89
$@12 (123)
    vlevo: 88, vpravo: 89
select_stmt (124)
    vlevo: 91, vpravo: 258
$@13 (125)
    vlevo: 90, vpravo: 91
expr (126)
    vlevo: 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107
    108 109 110 111 112, vpravo: 49 50 53 54 56 57 67 68 93 94 95 96
    97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 130
    134 140 141 143 147 275 276 290
uexpr (127)
    vlevo: 113 114 115 116 117 118 119 120 121, vpravo: 92 114 115
    116 117 118 119 120 121
pseudocall (128)
    vlevo: 122 123 124, vpravo: 133 265 266
pexpr_no_paren (129)
    vlevo: 125 126 127 128 129 130 131 132 133 134 135 136 137 138,
    vpravo: 136 145
start_complit (130)
    vlevo: 139, vpravo: 135 136 137 142 144
keyval (131)
    vlevo: 140, vpravo: 279 281
bare_complitexpr (132)
    vlevo: 141 142, vpravo: 280 282
complitexpr (133)
    vlevo: 143 144, vpravo: 140
pexpr (134)
    vlevo: 145 146, vpravo: 113 122 123 124 127 128 129 130 131 132
expr_or_type (135)
    vlevo: 147 148, vpravo: 128 137 146 277 278
name_or_type (136)
    vlevo: 149, vpravo: 242 243
lbrace (137)
    vlevo: 150 151, vpravo: 135 199 200 201 202 215
new_name (138)
    vlevo: 152, vpravo: 155 162 238 267 271 272
dcl_name (139)
    vlevo: 153, vpravo: 273 274
onew_name (140)
    vlevo: 154 155, vpravo: 263 264
sym (141)
    vlevo: 156 157 158, vpravo: 3 16 47 127 152 153 161 189 204 205
    207 236 243 244 329 330 331 332 340
hidden_importsym (142)
    vlevo: 159 160, vpravo: 157 309 316
name (143)
    vlevo: 161, vpravo: 126 188 189
labelname (144)
    vlevo: 162, vpravo: 261
dotdotdot (145)
    vlevo: 163 164, vpravo: 244 245
ntype (146)
    vlevo: 165 166 167 168 169 170, vpravo: 39 40 42 45 48 149 164
    170 179 190 191 193 194 197 198 229
non_expr_type (147)
    vlevo: 171 172 173 174, vpravo: 148 174
non_recvchantype (148)
    vlevo: 175 176 177 178 179, vpravo: 192
convtype (149)
    vlevo: 180 181, vpravo: 134
comptype (150)
    vlevo: 182, vpravo: 135
fnret_type (151)
    vlevo: 183 184 185 186 187, vpravo: 212
dotname (152)
    vlevo: 188 189, vpravo: 169 178 187
othertype (153)
    vlevo: 190 191 192 193 194 195 196, vpravo: 167 173 176 181 182
    185
ptrtype (154)
    vlevo: 197, vpravo: 168 177 186
recvchantype (155)
    vlevo: 198, vpravo: 165 171 183
structtype (156)
    vlevo: 199 200, vpravo: 195
interfacetype (157)
    vlevo: 201 202, vpravo: 196
xfndcl (158)
    vlevo: 203, vpravo: 25
fndcl (159)
    vlevo: 204 205, vpravo: 203
hidden_fndcl (160)
    vlevo: 206 207, vpravo: 308
fntype (161)
    vlevo: 208, vpravo: 166 172 175 180 184 214
fnbody (162)
    vlevo: 209 210, vpravo: 203 308
fnres (163)
    vlevo: 211 212 213, vpravo: 204 205 208 241
fnlitdcl (164)
    vlevo: 214, vpravo: 215 216
fnliteral (165)
    vlevo: 215 216, vpravo: 138
xdcl_list (166)
    vlevo: 217 218, vpravo: 1 218
vardcl_list (167)
    vlevo: 219 220, vpravo: 29 220
constdcl_list (168)
    vlevo: 221 222, vpravo: 33 222
typedcl_list (169)
    vlevo: 223 224, vpravo: 36 224
structdcl_list (170)
    vlevo: 225 226, vpravo: 199 226
interfacedcl_list (171)
    vlevo: 227 228, vpravo: 201 228
structdcl (172)
    vlevo: 229 230 231 232 233 234, vpravo: 225 226
packname (173)
    vlevo: 235 236, vpravo: 237 239 240
embed (174)
    vlevo: 237, vpravo: 230 231 232 233 234
interfacedcl (175)
    vlevo: 238 239 240, vpravo: 227 228
indcl (176)
    vlevo: 241, vpravo: 238
arg_type (177)
    vlevo: 242 243 244 245, vpravo: 246 247
arg_type_list (178)
    vlevo: 246 247, vpravo: 247 249
oarg_type_list_ocomma (179)
    vlevo: 248 249, vpravo: 204 205 208 213 241
stmt (180)
    vlevo: 250 251 252 253 254, vpravo: 261 269 270
non_dcl_stmt (181)
    vlevo: 255 256 257 258 259 261 262 263 264 265 266 267 268, vpravo:
    26 253
$@14 (182)
    vlevo: 260, vpravo: 261
stmt_list (183)
    vlevo: 269 270, vpravo: 60 62 66 210 215 270
new_name_list (184)
    vlevo: 271 272, vpravo: 229 272
dcl_name_list (185)
    vlevo: 273 274, vpravo: 39 40 41 42 43 45 46 274
expr_list (186)
    vlevo: 275 276, vpravo: 40 41 42 43 51 52 67 68 276 292
expr_or_type_list (187)
    vlevo: 277 278, vpravo: 55 56 57 123 124 278
keyval_list (188)
    vlevo: 279 280 281 282, vpravo: 281 282 284
braced_keyval_list (189)
    vlevo: 283 284, vpravo: 135 136 137 142 144
osemi (190)
    vlevo: 285 286, vpravo: 9 29 32 33 36 199 201
ocomma (191)
    vlevo: 287 288, vpravo: 123 124 134 249 284
oexpr (192)
    vlevo: 289 290, vpravo: 131 132 190
oexpr_list (193)
    vlevo: 291 292, vpravo: 268
osimple_stmt (194)
    vlevo: 293 294, vpravo: 69 70 75 76
ohidden_funarg_list (195)
    vlevo: 295 296, vpravo: 206 207 328 332 336
ohidden_structdcl_list (196)
    vlevo: 297 298, vpravo: 321
ohidden_interfacedcl_list (197)
    vlevo: 299 300, vpravo: 322
oliteral (198)
    vlevo: 301 302, vpravo: 229 230 231 232 233 234 329 330 331
hidden_import (199)
    vlevo: 303 304 305 306 307 308, vpravo: 344
hidden_pkg_importsym (200)
    vlevo: 309, vpravo: 206 304 305 306 310
hidden_pkgtype (201)
    vlevo: 310, vpravo: 307
hidden_type (202)
    vlevo: 311 312 313, vpravo: 304 306 307 318 319 320 323 326 327
    329 330 331 333 337
hidden_type_non_recv_chan (203)
    vlevo: 314 315, vpravo: 324
hidden_type_misc (204)
    vlevo: 316 317 318 319 320 321 322 323 324 325 326, vpravo: 311
    314
hidden_type_recv_chan (205)
    vlevo: 327, vpravo: 312 325
hidden_type_func (206)
    vlevo: 328, vpravo: 313 315
hidden_funarg (207)
    vlevo: 329 330, vpravo: 345 346
hidden_structdcl (208)
    vlevo: 331, vpravo: 347 348
hidden_interfacedcl (209)
    vlevo: 332 333, vpravo: 349 350
ohidden_funres (210)
    vlevo: 334 335, vpravo: 206 207 328 332
hidden_funres (211)
    vlevo: 336 337, vpravo: 335
hidden_literal (212)
    vlevo: 338 339 340, vpravo: 341 342
hidden_constant (213)
    vlevo: 341 342, vpravo: 305 306
hidden_import_list (214)
    vlevo: 343 344, vpravo: 22 344
hidden_funarg_list (215)
    vlevo: 345 346, vpravo: 207 296 346
hidden_structdcl_list (216)
    vlevo: 347 348, vpravo: 298 348
hidden_interfacedcl_list (217)
    vlevo: 349 350, vpravo: 300 350


State 0

    0 $accept: . file $end
    1 file: . loadsys package imports xdcl_list
    4 $@1: . %empty
    5 loadsys: . $@1 import_package import_there

    $výchozí  reduce using rule 4 ($@1)

    file     přejít do stavu 1
    loadsys  přejít do stavu 2
    $@1      přejít do stavu 3


State 1

    0 $accept: file . $end

    $end  posunout a přejít do stavu 4


State 2

    1 file: loadsys . package imports xdcl_list
    2 package: . %empty  [$end, error, LLITERAL, LBREAK, LCHAN, LCONST, LCONTINUE, LDEFER, LFALL, LFOR, LFUNC, LGO, LGOTO, LIF, LIMPORT, LINTERFACE, LMAP, LNAME, LRETURN, LSELECT, LSTRUCT, LSWITCH, LTYPE, LVAR, LCOMM, '+', '-', '^', '*', '&', '(', ';', '!', '~', '[', '?', '@']
    3        | . LPACKAGE sym ';'

    LPACKAGE  posunout a přejít do stavu 5

    $výchozí  reduce using rule 2 (package)

    package  přejít do stavu 6


State 3

    5 loadsys: $@1 . import_package import_there
   18 import_package: . LPACKAGE LNAME import_safety ';'

    LPACKAGE  posunout a přejít do stavu 7

    import_package  přejít do stavu 8


State 4

    0 $accept: file $end .

    $výchozí  přijmout


State 5

    3 package: LPACKAGE . sym ';'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    sym               přejít do stavu 12
    hidden_importsym  přejít do stavu 13


State 6

    1 file: loadsys package . imports xdcl_list
    6 imports: . %empty
    7        | . imports import ';'

    $výchozí  reduce using rule 6 (imports)

    imports  přejít do stavu 14


State 7

   18 import_package: LPACKAGE . LNAME import_safety ';'

    LNAME  posunout a přejít do stavu 15


State 8

    5 loadsys: $@1 import_package . import_there
   21 $@2: . %empty
   22 import_there: . $@2 hidden_import_list '$' '$'

    $výchozí  reduce using rule 21 ($@2)

    import_there  přejít do stavu 16
    $@2           přejít do stavu 17


State 9

  156 sym: LNAME .

    $výchozí  reduce using rule 156 (sym)


State 10

  158 sym: '?' .

    $výchozí  reduce using rule 158 (sym)


State 11

  159 hidden_importsym: '@' . LLITERAL '.' LNAME
  160                 | '@' . LLITERAL '.' '?'

    LLITERAL  posunout a přejít do stavu 18


State 12

    3 package: LPACKAGE sym . ';'

    ';'  posunout a přejít do stavu 19


State 13

  157 sym: hidden_importsym .

    $výchozí  reduce using rule 157 (sym)


State 14

    1 file: loadsys package imports . xdcl_list
    7 imports: imports . import ';'
    8 import: . LIMPORT import_stmt
    9       | . LIMPORT '(' import_stmt_list osemi ')'
   10       | . LIMPORT '(' ')'
  217 xdcl_list: . %empty  [$end, error, LLITERAL, LBREAK, LCHAN, LCONST, LCONTINUE, LDEFER, LFALL, LFOR, LFUNC, LGO, LGOTO, LIF, LINTERFACE, LMAP, LNAME, LRETURN, LSELECT, LSTRUCT, LSWITCH, LTYPE, LVAR, LCOMM, '+', '-', '^', '*', '&', '(', ';', '!', '~', '[', '?', '@']
  218          | . xdcl_list xdcl ';'

    LIMPORT  posunout a přejít do stavu 20

    $výchozí  reduce using rule 217 (xdcl_list)

    import     přejít do stavu 21
    xdcl_list  přejít do stavu 22


State 15

   18 import_package: LPACKAGE LNAME . import_safety ';'
   19 import_safety: . %empty  [';']
   20              | . LNAME

    LNAME  posunout a přejít do stavu 23

    $výchozí  reduce using rule 19 (import_safety)

    import_safety  přejít do stavu 24


State 16

    5 loadsys: $@1 import_package import_there .

    $výchozí  reduce using rule 5 (loadsys)


State 17

   22 import_there: $@2 . hidden_import_list '$' '$'
  343 hidden_import_list: . %empty
  344                   | . hidden_import_list hidden_import

    $výchozí  reduce using rule 343 (hidden_import_list)

    hidden_import_list  přejít do stavu 25


State 18

  159 hidden_importsym: '@' LLITERAL . '.' LNAME
  160                 | '@' LLITERAL . '.' '?'

    '.'  posunout a přejít do stavu 26


State 19

    3 package: LPACKAGE sym ';' .

    $výchozí  reduce using rule 3 (package)


State 20

    8 import: LIMPORT . import_stmt
    9       | LIMPORT . '(' import_stmt_list osemi ')'
   10       | LIMPORT . '(' ')'
   11 import_stmt: . import_here import_package import_there
   12            | . import_here import_there
   15 import_here: . LLITERAL
   16            | . sym LLITERAL
   17            | . '.' LLITERAL
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'

    LLITERAL  posunout a přejít do stavu 27
    LNAME     posunout a přejít do stavu 9
    '('       posunout a přejít do stavu 28
    '.'       posunout a přejít do stavu 29
    '?'       posunout a přejít do stavu 10
    '@'       posunout a přejít do stavu 11

    import_stmt       přejít do stavu 30
    import_here       přejít do stavu 31
    sym               přejít do stavu 32
    hidden_importsym  přejít do stavu 13


State 21

    7 imports: imports import . ';'

    ';'  posunout a přejít do stavu 33


State 22

    1 file: loadsys package imports xdcl_list .  [$end]
   23 xdcl: . %empty  [';']
   24     | . common_dcl
   25     | . xfndcl
   26     | . non_dcl_stmt
   27     | . error
   28 common_dcl: . LVAR vardcl
   29           | . LVAR '(' vardcl_list osemi ')'
   30           | . LVAR '(' ')'
   31           | . lconst constdcl
   32           | . lconst '(' constdcl osemi ')'
   33           | . lconst '(' constdcl ';' constdcl_list osemi ')'
   34           | . lconst '(' ')'
   35           | . LTYPE typedcl
   36           | . LTYPE '(' typedcl_list osemi ')'
   37           | . LTYPE '(' ')'
   38 lconst: . LCONST
   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   74 for_stmt: . LFOR $@6 for_body
   80 if_stmt: . LIF $@7 if_header $@8 loop_body $@9 elseif_list else
   89 switch_stmt: . LSWITCH $@11 if_header $@12 LBODY caseblock_list '}'
   91 select_stmt: . LSELECT $@13 LBODY caseblock_list '}'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  162 labelname: . new_name
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  203 xfndcl: . LFUNC fndcl fnbody
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  218 xdcl_list: xdcl_list . xdcl ';'
  255 non_dcl_stmt: . simple_stmt
  256             | . for_stmt
  257             | . switch_stmt
  258             | . select_stmt
  259             | . if_stmt
  261             | . labelname ':' $@14 stmt
  262             | . LFALL
  263             | . LBREAK onew_name
  264             | . LCONTINUE onew_name
  265             | . LGO pseudocall
  266             | . LDEFER pseudocall
  267             | . LGOTO new_name
  268             | . LRETURN oexpr_list
  275 expr_list: . expr
  276          | . expr_list ',' expr

    error       posunout a přejít do stavu 34
    LLITERAL    posunout a přejít do stavu 35
    LBREAK      posunout a přejít do stavu 36
    LCHAN       posunout a přejít do stavu 37
    LCONST      posunout a přejít do stavu 38
    LCONTINUE   posunout a přejít do stavu 39
    LDEFER      posunout a přejít do stavu 40
    LFALL       posunout a přejít do stavu 41
    LFOR        posunout a přejít do stavu 42
    LFUNC       posunout a přejít do stavu 43
    LGO         posunout a přejít do stavu 44
    LGOTO       posunout a přejít do stavu 45
    LIF         posunout a přejít do stavu 46
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRETURN     posunout a přejít do stavu 49
    LSELECT     posunout a přejít do stavu 50
    LSTRUCT     posunout a přejít do stavu 51
    LSWITCH     posunout a přejít do stavu 52
    LTYPE       posunout a přejít do stavu 53
    LVAR        posunout a přejít do stavu 54
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $end  reduce using rule 1 (file)
    ';'   reduce using rule 23 (xdcl)

    xdcl              přejít do stavu 65
    common_dcl        přejít do stavu 66
    lconst            přejít do stavu 67
    simple_stmt       přejít do stavu 68
    for_stmt          přejít do stavu 69
    if_stmt           přejít do stavu 70
    switch_stmt       přejít do stavu 71
    select_stmt       přejít do stavu 72
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    new_name          přejít do stavu 78
    sym               přejít do stavu 79
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    labelname         přejít do stavu 81
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    xfndcl            přejít do stavu 87
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    non_dcl_stmt      přejít do stavu 91
    expr_list         přejít do stavu 92


State 23

   20 import_safety: LNAME .

    $výchozí  reduce using rule 20 (import_safety)


State 24

   18 import_package: LPACKAGE LNAME import_safety . ';'

    ';'  posunout a přejít do stavu 93


State 25

   22 import_there: $@2 hidden_import_list . '$' '$'
  303 hidden_import: . LIMPORT LNAME LLITERAL ';'
  304              | . LVAR hidden_pkg_importsym hidden_type ';'
  305              | . LCONST hidden_pkg_importsym '=' hidden_constant ';'
  306              | . LCONST hidden_pkg_importsym hidden_type '=' hidden_constant ';'
  307              | . LTYPE hidden_pkgtype hidden_type ';'
  308              | . LFUNC hidden_fndcl fnbody ';'
  344 hidden_import_list: hidden_import_list . hidden_import

    LCONST   posunout a přejít do stavu 94
    LFUNC    posunout a přejít do stavu 95
    LIMPORT  posunout a přejít do stavu 96
    LTYPE    posunout a přejít do stavu 97
    LVAR     posunout a přejít do stavu 98
    '$'      posunout a přejít do stavu 99

    hidden_import  přejít do stavu 100


State 26

  159 hidden_importsym: '@' LLITERAL '.' . LNAME
  160                 | '@' LLITERAL '.' . '?'

    LNAME  posunout a přejít do stavu 101
    '?'    posunout a přejít do stavu 102


State 27

   15 import_here: LLITERAL .

    $výchozí  reduce using rule 15 (import_here)


State 28

    9 import: LIMPORT '(' . import_stmt_list osemi ')'
   10       | LIMPORT '(' . ')'
   11 import_stmt: . import_here import_package import_there
   12            | . import_here import_there
   13 import_stmt_list: . import_stmt
   14                 | . import_stmt_list ';' import_stmt
   15 import_here: . LLITERAL
   16            | . sym LLITERAL
   17            | . '.' LLITERAL
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'

    LLITERAL  posunout a přejít do stavu 27
    LNAME     posunout a přejít do stavu 9
    ')'       posunout a přejít do stavu 103
    '.'       posunout a přejít do stavu 29
    '?'       posunout a přejít do stavu 10
    '@'       posunout a přejít do stavu 11

    import_stmt       přejít do stavu 104
    import_stmt_list  přejít do stavu 105
    import_here       přejít do stavu 31
    sym               přejít do stavu 32
    hidden_importsym  přejít do stavu 13


State 29

   17 import_here: '.' . LLITERAL

    LLITERAL  posunout a přejít do stavu 106


State 30

    8 import: LIMPORT import_stmt .

    $výchozí  reduce using rule 8 (import)


State 31

   11 import_stmt: import_here . import_package import_there
   12            | import_here . import_there
   18 import_package: . LPACKAGE LNAME import_safety ';'
   21 $@2: . %empty  [LCONST, LFUNC, LIMPORT, LTYPE, LVAR, '$']
   22 import_there: . $@2 hidden_import_list '$' '$'

    LPACKAGE  posunout a přejít do stavu 7

    $výchozí  reduce using rule 21 ($@2)

    import_package  přejít do stavu 107
    import_there    přejít do stavu 108
    $@2             přejít do stavu 17


State 32

   16 import_here: sym . LLITERAL

    LLITERAL  posunout a přejít do stavu 109


State 33

    7 imports: imports import ';' .

    $výchozí  reduce using rule 7 (imports)


State 34

   27 xdcl: error .

    $výchozí  reduce using rule 27 (xdcl)


State 35

  125 pexpr_no_paren: LLITERAL .

    $výchozí  reduce using rule 125 (pexpr_no_paren)


State 36

  152 new_name: . sym
  154 onew_name: . %empty  [LCASE, LDEFAULT, ';', '}']
  155          | . new_name
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  263 non_dcl_stmt: LBREAK . onew_name

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 154 (onew_name)

    new_name          přejít do stavu 110
    onew_name         přejít do stavu 111
    sym               přejít do stavu 112
    hidden_importsym  přejít do stavu 13


State 37

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  175 non_recvchantype: . fntype
  176                 | . othertype
  177                 | . ptrtype
  178                 | . dotname
  179                 | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  192          | LCHAN . non_recvchantype
  193          | . LCHAN LCOMM ntype
  193          | LCHAN . LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 114
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 116
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    non_recvchantype  přejít do stavu 119
    dotname           přejít do stavu 120
    othertype         přejít do stavu 121
    ptrtype           přejít do stavu 122
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 123


State 38

   38 lconst: LCONST .

    $výchozí  reduce using rule 38 (lconst)


State 39

  152 new_name: . sym
  154 onew_name: . %empty  [LCASE, LDEFAULT, ';', '}']
  155          | . new_name
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  264 non_dcl_stmt: LCONTINUE . onew_name

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 154 (onew_name)

    new_name          přejít do stavu 110
    onew_name         přejít do stavu 124
    sym               přejít do stavu 112
    hidden_importsym  přejít do stavu 13


State 40

  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  266 non_dcl_stmt: LDEFER . pseudocall

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    '('         posunout a přejít do stavu 61
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    pseudocall        přejít do stavu 125
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 126
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 41

  262 non_dcl_stmt: LFALL .

    $výchozí  reduce using rule 262 (non_dcl_stmt)


State 42

   73 $@6: . %empty
   74 for_stmt: LFOR . $@6 for_body

    $výchozí  reduce using rule 73 ($@6)

    $@6  přejít do stavu 127


State 43

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  203 xfndcl: LFUNC . fndcl fnbody
  204 fndcl: . sym '(' oarg_type_list_ocomma ')' fnres
  205      | . '(' oarg_type_list_ocomma ')' sym '(' oarg_type_list_ocomma ')' fnres
  208 fntype: LFUNC . '(' oarg_type_list_ocomma ')' fnres

    LNAME  posunout a přejít do stavu 9
    '('    posunout a přejít do stavu 128
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    sym               přejít do stavu 129
    hidden_importsym  přejít do stavu 13
    fndcl             přejít do stavu 130


State 44

  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  265 non_dcl_stmt: LGO . pseudocall

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    '('         posunout a přejít do stavu 61
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    pseudocall        přejít do stavu 131
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 126
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 45

  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  267 non_dcl_stmt: LGOTO . new_name

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    new_name          přejít do stavu 132
    sym               přejít do stavu 112
    hidden_importsym  přejít do stavu 13


State 46

   77 $@7: . %empty
   80 if_stmt: LIF . $@7 if_header $@8 loop_body $@9 elseif_list else

    $výchozí  reduce using rule 77 ($@7)

    $@7  přejít do stavu 133


State 47

  150 lbrace: . LBODY
  151       | . '{'
  201 interfacetype: LINTERFACE . lbrace interfacedcl_list osemi '}'
  202              | LINTERFACE . lbrace '}'

    LBODY  posunout a přejít do stavu 134
    '{'    posunout a přejít do stavu 135

    lbrace  přejít do stavu 136


State 48

  194 othertype: LMAP . '[' ntype ']' ntype

    '['  posunout a přejít do stavu 137


State 49

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  268 non_dcl_stmt: LRETURN . oexpr_list
  275 expr_list: . expr
  276          | . expr_list ',' expr
  291 oexpr_list: . %empty  [LCASE, LDEFAULT, ';', '}']
  292           | . expr_list

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 291 (oexpr_list)

    expr              přejít do stavu 138
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 139
    oexpr_list        přejít do stavu 140


State 50

   90 $@13: . %empty
   91 select_stmt: LSELECT . $@13 LBODY caseblock_list '}'

    $výchozí  reduce using rule 90 ($@13)

    $@13  přejít do stavu 141


State 51

  150 lbrace: . LBODY
  151       | . '{'
  199 structtype: LSTRUCT . lbrace structdcl_list osemi '}'
  200           | LSTRUCT . lbrace '}'

    LBODY  posunout a přejít do stavu 134
    '{'    posunout a přejít do stavu 135

    lbrace  přejít do stavu 142


State 52

   87 $@11: . %empty
   89 switch_stmt: LSWITCH . $@11 if_header $@12 LBODY caseblock_list '}'

    $výchozí  reduce using rule 87 ($@11)

    $@11  přejít do stavu 143


State 53

   35 common_dcl: LTYPE . typedcl
   36           | LTYPE . '(' typedcl_list osemi ')'
   37           | LTYPE . '(' ')'
   47 typedclname: . sym
   48 typedcl: . typedclname ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'

    LNAME  posunout a přejít do stavu 9
    '('    posunout a přejít do stavu 144
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    typedclname       přejít do stavu 145
    typedcl           přejít do stavu 146
    sym               přejít do stavu 147
    hidden_importsym  přejít do stavu 13


State 54

   28 common_dcl: LVAR . vardcl
   29           | LVAR . '(' vardcl_list osemi ')'
   30           | LVAR . '(' ')'
   39 vardcl: . dcl_name_list ntype
   40       | . dcl_name_list ntype '=' expr_list
   41       | . dcl_name_list '=' expr_list
  153 dcl_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  273 dcl_name_list: . dcl_name
  274              | . dcl_name_list ',' dcl_name

    LNAME  posunout a přejít do stavu 9
    '('    posunout a přejít do stavu 148
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    vardcl            přejít do stavu 149
    dcl_name          přejít do stavu 150
    sym               přejít do stavu 151
    hidden_importsym  přejít do stavu 13
    dcl_name_list     přejít do stavu 152


State 55

  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  121      | LCOMM . uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 153
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 56

  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  116      | '+' . uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 154
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 57

  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  117      | '-' . uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 155
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 58

  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  120      | '^' . uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 156
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 59

  113 uexpr: . pexpr
  114      | . '*' uexpr
  114      | '*' . uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 157
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 60

  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  115      | '&' . uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 158
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 61

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  137               | '(' . expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  146      | '(' . expr_or_type ')'
  147 expr_or_type: . expr
  148             | . non_expr_type
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  171 non_expr_type: . recvchantype
  172              | . fntype
  173              | . othertype
  174              | . '*' non_expr_type
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 159
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 160
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 161
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    expr_or_type      přejít do stavu 162
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    non_expr_type     přejít do stavu 163
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 164
    recvchantype      přejít do stavu 165
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 166
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 62

  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  118      | '!' . uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 167
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 63

  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  119      | '~' . uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 168
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 64

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  190          | '[' . oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  191          | '[' . LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  289 oexpr: . %empty  [']']
  290      | . expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LDDD        posunout a přejít do stavu 169
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 289 (oexpr)

    expr              přejít do stavu 170
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    oexpr             přejít do stavu 171


State 65

  218 xdcl_list: xdcl_list xdcl . ';'

    ';'  posunout a přejít do stavu 172


State 66

   24 xdcl: common_dcl .

    $výchozí  reduce using rule 24 (xdcl)


State 67

   31 common_dcl: lconst . constdcl
   32           | lconst . '(' constdcl osemi ')'
   33           | lconst . '(' constdcl ';' constdcl_list osemi ')'
   34           | lconst . '(' ')'
   42 constdcl: . dcl_name_list ntype '=' expr_list
   43         | . dcl_name_list '=' expr_list
  153 dcl_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  273 dcl_name_list: . dcl_name
  274              | . dcl_name_list ',' dcl_name

    LNAME  posunout a přejít do stavu 9
    '('    posunout a přejít do stavu 173
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    constdcl          přejít do stavu 174
    dcl_name          přejít do stavu 150
    sym               přejít do stavu 151
    hidden_importsym  přejít do stavu 13
    dcl_name_list     přejít do stavu 175


State 68

  255 non_dcl_stmt: simple_stmt .

    $výchozí  reduce using rule 255 (non_dcl_stmt)


State 69

  256 non_dcl_stmt: for_stmt .

    $výchozí  reduce using rule 256 (non_dcl_stmt)


State 70

  259 non_dcl_stmt: if_stmt .

    $výchozí  reduce using rule 259 (non_dcl_stmt)


State 71

  257 non_dcl_stmt: switch_stmt .

    $výchozí  reduce using rule 257 (non_dcl_stmt)


State 72

  258 non_dcl_stmt: select_stmt .

    $výchozí  reduce using rule 258 (non_dcl_stmt)


State 73

   49 simple_stmt: expr .  [LCASE, LDEFAULT, LBODY, ';', '}']
   50            | expr . LASOP expr
   53            | expr . LINC
   54            | expr . LDEC
   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  275 expr_list: expr .  [LCOLAS, '=', ',']

    LASOP    posunout a přejít do stavu 176
    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LDEC     posunout a přejít do stavu 180
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LINC     posunout a přejít do stavu 184
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    LCOLAS      reduce using rule 275 (expr_list)
    '='         reduce using rule 275 (expr_list)
    ','         reduce using rule 275 (expr_list)
    $výchozí  reduce using rule 49 (simple_stmt)


State 74

   92 expr: uexpr .

    $výchozí  reduce using rule 92 (expr)


State 75

  133 pexpr_no_paren: pseudocall .

    $výchozí  reduce using rule 133 (pexpr_no_paren)


State 76

  136 pexpr_no_paren: pexpr_no_paren . '{' start_complit braced_keyval_list '}'
  145 pexpr: pexpr_no_paren .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', '(', ')', ';', '.', '=', ':', '}', '[', ']', ',']

    '{'  posunout a přejít do stavu 199

    $výchozí  reduce using rule 145 (pexpr)


State 77

  113 uexpr: pexpr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', ')', ';', '=', ':', '}', ']', ',']
  122 pseudocall: pexpr . '(' ')'
  123           | pexpr . '(' expr_or_type_list ocomma ')'
  124           | pexpr . '(' expr_or_type_list LDDD ocomma ')'
  127 pexpr_no_paren: pexpr . '.' sym
  128               | pexpr . '.' '(' expr_or_type ')'
  129               | pexpr . '.' '(' LTYPE ')'
  130               | pexpr . '[' expr ']'
  131               | pexpr . '[' oexpr ':' oexpr ']'
  132               | pexpr . '[' oexpr ':' oexpr ':' oexpr ']'

    '('  posunout a přejít do stavu 200
    '.'  posunout a přejít do stavu 201
    '['  posunout a přejít do stavu 202

    $výchozí  reduce using rule 113 (uexpr)


State 78

  162 labelname: new_name .

    $výchozí  reduce using rule 162 (labelname)


State 79

  152 new_name: sym .  [':']
  161 name: sym .  [LASOP, LCOLAS, LCASE, LDEFAULT, LANDAND, LANDNOT, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', '(', ';', '.', '=', '{', '}', '[', ',']

    ':'         reduce using rule 152 (new_name)
    $výchozí  reduce using rule 161 (name)


State 80

  126 pexpr_no_paren: name .

    $výchozí  reduce using rule 126 (pexpr_no_paren)


State 81

  261 non_dcl_stmt: labelname . ':' $@14 stmt

    ':'  posunout a přejít do stavu 203


State 82

  134 pexpr_no_paren: convtype . '(' expr ocomma ')'

    '('  posunout a přejít do stavu 204


State 83

  135 pexpr_no_paren: comptype . lbrace start_complit braced_keyval_list '}'
  150 lbrace: . LBODY
  151       | . '{'

    LBODY  posunout a přejít do stavu 134
    '{'    posunout a přejít do stavu 135

    lbrace  přejít do stavu 205


State 84

  181 convtype: othertype .  ['(']
  182 comptype: othertype .  [LBODY, '{']

    '('         reduce using rule 181 (convtype)
    $výchozí  reduce using rule 182 (comptype)


State 85

  195 othertype: structtype .

    $výchozí  reduce using rule 195 (othertype)


State 86

  196 othertype: interfacetype .

    $výchozí  reduce using rule 196 (othertype)


State 87

   25 xdcl: xfndcl .

    $výchozí  reduce using rule 25 (xdcl)


State 88

  180 convtype: fntype .  ['(']
  214 fnlitdcl: fntype .  [error, LBODY, '{']

    '('         reduce using rule 180 (convtype)
    $výchozí  reduce using rule 214 (fnlitdcl)


State 89

  150 lbrace: . LBODY
  151       | . '{'
  215 fnliteral: fnlitdcl . lbrace stmt_list '}'
  216          | fnlitdcl . error

    error  posunout a přejít do stavu 206
    LBODY  posunout a přejít do stavu 134
    '{'    posunout a přejít do stavu 135

    lbrace  přejít do stavu 207


State 90

  138 pexpr_no_paren: fnliteral .

    $výchozí  reduce using rule 138 (pexpr_no_paren)


State 91

   26 xdcl: non_dcl_stmt .

    $výchozí  reduce using rule 26 (xdcl)


State 92

   51 simple_stmt: expr_list . '=' expr_list
   52            | expr_list . LCOLAS expr_list
  276 expr_list: expr_list . ',' expr

    LCOLAS  posunout a přejít do stavu 208
    '='     posunout a přejít do stavu 209
    ','     posunout a přejít do stavu 210


State 93

   18 import_package: LPACKAGE LNAME import_safety ';' .

    $výchozí  reduce using rule 18 (import_package)


State 94

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  305 hidden_import: LCONST . hidden_pkg_importsym '=' hidden_constant ';'
  306              | LCONST . hidden_pkg_importsym hidden_type '=' hidden_constant ';'
  309 hidden_pkg_importsym: . hidden_importsym

    '@'  posunout a přejít do stavu 11

    hidden_importsym      přejít do stavu 211
    hidden_pkg_importsym  přejít do stavu 212


State 95

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  206 hidden_fndcl: . hidden_pkg_importsym '(' ohidden_funarg_list ')' ohidden_funres
  207             | . '(' hidden_funarg_list ')' sym '(' ohidden_funarg_list ')' ohidden_funres
  308 hidden_import: LFUNC . hidden_fndcl fnbody ';'
  309 hidden_pkg_importsym: . hidden_importsym

    '('  posunout a přejít do stavu 213
    '@'  posunout a přejít do stavu 11

    hidden_importsym      přejít do stavu 211
    hidden_fndcl          přejít do stavu 214
    hidden_pkg_importsym  přejít do stavu 215


State 96

  303 hidden_import: LIMPORT . LNAME LLITERAL ';'

    LNAME  posunout a přejít do stavu 216


State 97

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  307 hidden_import: LTYPE . hidden_pkgtype hidden_type ';'
  309 hidden_pkg_importsym: . hidden_importsym
  310 hidden_pkgtype: . hidden_pkg_importsym

    '@'  posunout a přejít do stavu 11

    hidden_importsym      přejít do stavu 211
    hidden_pkg_importsym  přejít do stavu 217
    hidden_pkgtype        přejít do stavu 218


State 98

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  304 hidden_import: LVAR . hidden_pkg_importsym hidden_type ';'
  309 hidden_pkg_importsym: . hidden_importsym

    '@'  posunout a přejít do stavu 11

    hidden_importsym      přejít do stavu 211
    hidden_pkg_importsym  přejít do stavu 219


State 99

   22 import_there: $@2 hidden_import_list '$' . '$'

    '$'  posunout a přejít do stavu 220


State 100

  344 hidden_import_list: hidden_import_list hidden_import .

    $výchozí  reduce using rule 344 (hidden_import_list)


State 101

  159 hidden_importsym: '@' LLITERAL '.' LNAME .

    $výchozí  reduce using rule 159 (hidden_importsym)


State 102

  160 hidden_importsym: '@' LLITERAL '.' '?' .

    $výchozí  reduce using rule 160 (hidden_importsym)


State 103

   10 import: LIMPORT '(' ')' .

    $výchozí  reduce using rule 10 (import)


State 104

   13 import_stmt_list: import_stmt .

    $výchozí  reduce using rule 13 (import_stmt_list)


State 105

    9 import: LIMPORT '(' import_stmt_list . osemi ')'
   14 import_stmt_list: import_stmt_list . ';' import_stmt
  285 osemi: . %empty  [')']
  286      | . ';'

    ';'  posunout a přejít do stavu 221

    $výchozí  reduce using rule 285 (osemi)

    osemi  přejít do stavu 222


State 106

   17 import_here: '.' LLITERAL .

    $výchozí  reduce using rule 17 (import_here)


State 107

   11 import_stmt: import_here import_package . import_there
   21 $@2: . %empty
   22 import_there: . $@2 hidden_import_list '$' '$'

    $výchozí  reduce using rule 21 ($@2)

    import_there  přejít do stavu 223
    $@2           přejít do stavu 17


State 108

   12 import_stmt: import_here import_there .

    $výchozí  reduce using rule 12 (import_stmt)


State 109

   16 import_here: sym LLITERAL .

    $výchozí  reduce using rule 16 (import_here)


State 110

  155 onew_name: new_name .

    $výchozí  reduce using rule 155 (onew_name)


State 111

  263 non_dcl_stmt: LBREAK onew_name .

    $výchozí  reduce using rule 263 (non_dcl_stmt)


State 112

  152 new_name: sym .

    $výchozí  reduce using rule 152 (new_name)


State 113

  208 fntype: LFUNC . '(' oarg_type_list_ocomma ')' fnres

    '('  posunout a přejít do stavu 224


State 114

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  193          | LCHAN LCOMM . ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 227
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 115

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  197        | '*' . ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 233
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 116

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  179 non_recvchantype: '(' . ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 234
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 117

  161 name: sym .

    $výchozí  reduce using rule 161 (name)


State 118

  188 dotname: name .  [error, LLITERAL, LCOLAS, LCASE, LDDD, LDEFAULT, LBODY, '(', ')', ';', '=', ':', '{', '}', ']', ',']
  189        | name . '.' sym

    '.'  posunout a přejít do stavu 235

    $výchozí  reduce using rule 188 (dotname)


State 119

  192 othertype: LCHAN non_recvchantype .

    $výchozí  reduce using rule 192 (othertype)


State 120

  178 non_recvchantype: dotname .

    $výchozí  reduce using rule 178 (non_recvchantype)


State 121

  176 non_recvchantype: othertype .

    $výchozí  reduce using rule 176 (non_recvchantype)


State 122

  177 non_recvchantype: ptrtype .

    $výchozí  reduce using rule 177 (non_recvchantype)


State 123

  175 non_recvchantype: fntype .

    $výchozí  reduce using rule 175 (non_recvchantype)


State 124

  264 non_dcl_stmt: LCONTINUE onew_name .

    $výchozí  reduce using rule 264 (non_dcl_stmt)


State 125

  133 pexpr_no_paren: pseudocall .  ['(', '.', '{', '[']
  266 non_dcl_stmt: LDEFER pseudocall .  [LCASE, LDEFAULT, ';', '}']

    LCASE       reduce using rule 266 (non_dcl_stmt)
    LDEFAULT    reduce using rule 266 (non_dcl_stmt)
    ';'         reduce using rule 266 (non_dcl_stmt)
    '}'         reduce using rule 266 (non_dcl_stmt)
    $výchozí  reduce using rule 133 (pexpr_no_paren)


State 126

  122 pseudocall: pexpr . '(' ')'
  123           | pexpr . '(' expr_or_type_list ocomma ')'
  124           | pexpr . '(' expr_or_type_list LDDD ocomma ')'
  127 pexpr_no_paren: pexpr . '.' sym
  128               | pexpr . '.' '(' expr_or_type ')'
  129               | pexpr . '.' '(' LTYPE ')'
  130               | pexpr . '[' expr ']'
  131               | pexpr . '[' oexpr ':' oexpr ']'
  132               | pexpr . '[' oexpr ':' oexpr ':' oexpr ']'

    '('  posunout a přejít do stavu 200
    '.'  posunout a přejít do stavu 201
    '['  posunout a přejít do stavu 202


State 127

   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   67 range_stmt: . expr_list '=' LRANGE expr
   68           | . expr_list LCOLAS LRANGE expr
   69 for_header: . osimple_stmt ';' osimple_stmt ';' osimple_stmt
   70           | . osimple_stmt
   71           | . range_stmt
   72 for_body: . for_header loop_body
   74 for_stmt: LFOR $@6 . for_body
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr
  293 osimple_stmt: . %empty  [LBODY, ';']
  294             | . simple_stmt

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 293 (osimple_stmt)

    simple_stmt       přejít do stavu 236
    range_stmt        přejít do stavu 237
    for_header        přejít do stavu 238
    for_body          přejít do stavu 239
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 240
    osimple_stmt      přejít do stavu 241


State 128

  149 name_or_type: . ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  163 dotdotdot: . LDDD
  164          | . LDDD ntype
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  205 fndcl: '(' . oarg_type_list_ocomma ')' sym '(' oarg_type_list_ocomma ')' fnres
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  208       | LFUNC '(' . oarg_type_list_ocomma ')' fnres
  242 arg_type: . name_or_type
  243         | . sym name_or_type
  244         | . sym dotdotdot
  245         | . dotdotdot
  246 arg_type_list: . arg_type
  247              | . arg_type_list ',' arg_type
  248 oarg_type_list_ocomma: . %empty  [')']
  249                      | . arg_type_list ocomma

    LCHAN       posunout a přejít do stavu 37
    LDDD        posunout a přejít do stavu 242
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 248 (oarg_type_list_ocomma)

    name_or_type           přejít do stavu 243
    sym                    přejít do stavu 244
    hidden_importsym       přejít do stavu 13
    name                   přejít do stavu 118
    dotdotdot              přejít do stavu 245
    ntype                  přejít do stavu 246
    dotname                přejít do stavu 228
    othertype              přejít do stavu 229
    ptrtype                přejít do stavu 230
    recvchantype           přejít do stavu 231
    structtype             přejít do stavu 85
    interfacetype          přejít do stavu 86
    fntype                 přejít do stavu 232
    arg_type               přejít do stavu 247
    arg_type_list          přejít do stavu 248
    oarg_type_list_ocomma  přejít do stavu 249


State 129

  204 fndcl: sym . '(' oarg_type_list_ocomma ')' fnres

    '('  posunout a přejít do stavu 250


State 130

  203 xfndcl: LFUNC fndcl . fnbody
  209 fnbody: . %empty  [';']
  210       | . '{' stmt_list '}'

    '{'  posunout a přejít do stavu 251

    $výchozí  reduce using rule 209 (fnbody)

    fnbody  přejít do stavu 252


State 131

  133 pexpr_no_paren: pseudocall .  ['(', '.', '{', '[']
  265 non_dcl_stmt: LGO pseudocall .  [LCASE, LDEFAULT, ';', '}']

    LCASE       reduce using rule 265 (non_dcl_stmt)
    LDEFAULT    reduce using rule 265 (non_dcl_stmt)
    ';'         reduce using rule 265 (non_dcl_stmt)
    '}'         reduce using rule 265 (non_dcl_stmt)
    $výchozí  reduce using rule 133 (pexpr_no_paren)


State 132

  267 non_dcl_stmt: LGOTO new_name .

    $výchozí  reduce using rule 267 (non_dcl_stmt)


State 133

   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   75 if_header: . osimple_stmt
   76          | . osimple_stmt ';' osimple_stmt
   80 if_stmt: LIF $@7 . if_header $@8 loop_body $@9 elseif_list else
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr
  293 osimple_stmt: . %empty  [LBODY, ';']
  294             | . simple_stmt

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 293 (osimple_stmt)

    simple_stmt       přejít do stavu 236
    if_header         přejít do stavu 253
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 92
    osimple_stmt      přejít do stavu 254


State 134

  150 lbrace: LBODY .

    $výchozí  reduce using rule 150 (lbrace)


State 135

  151 lbrace: '{' .

    $výchozí  reduce using rule 151 (lbrace)


State 136

  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  201 interfacetype: LINTERFACE lbrace . interfacedcl_list osemi '}'
  202              | LINTERFACE lbrace . '}'
  227 interfacedcl_list: . interfacedcl
  228                  | . interfacedcl_list ';' interfacedcl
  235 packname: . LNAME
  236         | . LNAME '.' sym
  238 interfacedcl: . new_name indcl
  239             | . packname
  240             | . '(' packname ')'

    LNAME  posunout a přejít do stavu 255
    '('    posunout a přejít do stavu 256
    '}'    posunout a přejít do stavu 257
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    new_name           přejít do stavu 258
    sym                přejít do stavu 112
    hidden_importsym   přejít do stavu 13
    interfacedcl_list  přejít do stavu 259
    packname           přejít do stavu 260
    interfacedcl       přejít do stavu 261


State 137

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  194          | LMAP '[' . ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 262
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 138

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  275 expr_list: expr .  [LCASE, LDEFAULT, LBODY, ')', ';', '}', ',']

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 275 (expr_list)


State 139

  276 expr_list: expr_list . ',' expr
  292 oexpr_list: expr_list .  [LCASE, LDEFAULT, ';', '}']

    ','  posunout a přejít do stavu 210

    $výchozí  reduce using rule 292 (oexpr_list)


State 140

  268 non_dcl_stmt: LRETURN oexpr_list .

    $výchozí  reduce using rule 268 (non_dcl_stmt)


State 141

   91 select_stmt: LSELECT $@13 . LBODY caseblock_list '}'

    LBODY  posunout a přejít do stavu 263


State 142

  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  199 structtype: LSTRUCT lbrace . structdcl_list osemi '}'
  200           | LSTRUCT lbrace . '}'
  225 structdcl_list: . structdcl
  226               | . structdcl_list ';' structdcl
  229 structdcl: . new_name_list ntype oliteral
  230          | . embed oliteral
  231          | . '(' embed ')' oliteral
  232          | . '*' embed oliteral
  233          | . '(' '*' embed ')' oliteral
  234          | . '*' '(' embed ')' oliteral
  235 packname: . LNAME
  236         | . LNAME '.' sym
  237 embed: . packname
  271 new_name_list: . new_name
  272              | . new_name_list ',' new_name

    LNAME  posunout a přejít do stavu 255
    '*'    posunout a přejít do stavu 264
    '('    posunout a přejít do stavu 265
    '}'    posunout a přejít do stavu 266
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    new_name          přejít do stavu 267
    sym               přejít do stavu 112
    hidden_importsym  přejít do stavu 13
    structdcl_list    přejít do stavu 268
    structdcl         přejít do stavu 269
    packname          přejít do stavu 270
    embed             přejít do stavu 271
    new_name_list     přejít do stavu 272


State 143

   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   75 if_header: . osimple_stmt
   76          | . osimple_stmt ';' osimple_stmt
   89 switch_stmt: LSWITCH $@11 . if_header $@12 LBODY caseblock_list '}'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr
  293 osimple_stmt: . %empty  [LBODY, ';']
  294             | . simple_stmt

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 293 (osimple_stmt)

    simple_stmt       přejít do stavu 236
    if_header         přejít do stavu 273
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 92
    osimple_stmt      přejít do stavu 254


State 144

   36 common_dcl: LTYPE '(' . typedcl_list osemi ')'
   37           | LTYPE '(' . ')'
   47 typedclname: . sym
   48 typedcl: . typedclname ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  223 typedcl_list: . typedcl
  224             | . typedcl_list ';' typedcl

    LNAME  posunout a přejít do stavu 9
    ')'    posunout a přejít do stavu 274
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    typedclname       přejít do stavu 145
    typedcl           přejít do stavu 275
    sym               přejít do stavu 147
    hidden_importsym  přejít do stavu 13
    typedcl_list      přejít do stavu 276


State 145

   48 typedcl: typedclname . ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 277
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 146

   35 common_dcl: LTYPE typedcl .

    $výchozí  reduce using rule 35 (common_dcl)


State 147

   47 typedclname: sym .

    $výchozí  reduce using rule 47 (typedclname)


State 148

   29 common_dcl: LVAR '(' . vardcl_list osemi ')'
   30           | LVAR '(' . ')'
   39 vardcl: . dcl_name_list ntype
   40       | . dcl_name_list ntype '=' expr_list
   41       | . dcl_name_list '=' expr_list
  153 dcl_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  219 vardcl_list: . vardcl
  220            | . vardcl_list ';' vardcl
  273 dcl_name_list: . dcl_name
  274              | . dcl_name_list ',' dcl_name

    LNAME  posunout a přejít do stavu 9
    ')'    posunout a přejít do stavu 278
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    vardcl            přejít do stavu 279
    dcl_name          přejít do stavu 150
    sym               přejít do stavu 151
    hidden_importsym  přejít do stavu 13
    vardcl_list       přejít do stavu 280
    dcl_name_list     přejít do stavu 152


State 149

   28 common_dcl: LVAR vardcl .

    $výchozí  reduce using rule 28 (common_dcl)


State 150

  273 dcl_name_list: dcl_name .

    $výchozí  reduce using rule 273 (dcl_name_list)


State 151

  153 dcl_name: sym .

    $výchozí  reduce using rule 153 (dcl_name)


State 152

   39 vardcl: dcl_name_list . ntype
   40       | dcl_name_list . ntype '=' expr_list
   41       | dcl_name_list . '=' expr_list
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  274 dcl_name_list: dcl_name_list . ',' dcl_name

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '='         posunout a přejít do stavu 281
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11
    ','         posunout a přejít do stavu 282

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 283
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 153

  121 uexpr: LCOMM uexpr .

    $výchozí  reduce using rule 121 (uexpr)


State 154

  116 uexpr: '+' uexpr .

    $výchozí  reduce using rule 116 (uexpr)


State 155

  117 uexpr: '-' uexpr .

    $výchozí  reduce using rule 117 (uexpr)


State 156

  120 uexpr: '^' uexpr .

    $výchozí  reduce using rule 120 (uexpr)


State 157

  114 uexpr: '*' uexpr .

    $výchozí  reduce using rule 114 (uexpr)


State 158

  115 uexpr: '&' uexpr .

    $výchozí  reduce using rule 115 (uexpr)


State 159

  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  121      | LCOMM . uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  198 recvchantype: LCOMM . LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 284
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 153
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 160

  113 uexpr: . pexpr
  114      | . '*' uexpr
  114      | '*' . uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  171 non_expr_type: . recvchantype
  172              | . fntype
  173              | . othertype
  174              | . '*' non_expr_type
  174              | '*' . non_expr_type
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 159
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 160
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    uexpr             přejít do stavu 157
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    non_expr_type     přejít do stavu 285
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 164
    recvchantype      přejít do stavu 165
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 166
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 161

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  147 expr_or_type: expr .  [LCOLAS, LDDD, ')', '=', ':', ',']

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 147 (expr_or_type)


State 162

  137 pexpr_no_paren: '(' expr_or_type . ')' '{' start_complit braced_keyval_list '}'
  146 pexpr: '(' expr_or_type . ')'

    ')'  posunout a přejít do stavu 286


State 163

  148 expr_or_type: non_expr_type .

    $výchozí  reduce using rule 148 (expr_or_type)


State 164

  173 non_expr_type: othertype .  [LCOLAS, LDDD, ')', '=', ':', ',']
  181 convtype: othertype .  ['(']
  182 comptype: othertype .  [LBODY, '{']

    LBODY       reduce using rule 182 (comptype)
    '('         reduce using rule 181 (convtype)
    '{'         reduce using rule 182 (comptype)
    $výchozí  reduce using rule 173 (non_expr_type)


State 165

  171 non_expr_type: recvchantype .

    $výchozí  reduce using rule 171 (non_expr_type)


State 166

  172 non_expr_type: fntype .  [LCOLAS, LDDD, ')', '=', ':', ',']
  180 convtype: fntype .  ['(']
  214 fnlitdcl: fntype .  [error, LBODY, '{']

    error       reduce using rule 214 (fnlitdcl)
    LBODY       reduce using rule 214 (fnlitdcl)
    '('         reduce using rule 180 (convtype)
    '{'         reduce using rule 214 (fnlitdcl)
    $výchozí  reduce using rule 172 (non_expr_type)


State 167

  118 uexpr: '!' uexpr .

    $výchozí  reduce using rule 118 (uexpr)


State 168

  119 uexpr: '~' uexpr .

    $výchozí  reduce using rule 119 (uexpr)


State 169

  191 othertype: '[' LDDD . ']' ntype

    ']'  posunout a přejít do stavu 287


State 170

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  290 oexpr: expr .  [':', ']']

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 290 (oexpr)


State 171

  190 othertype: '[' oexpr . ']' ntype

    ']'  posunout a přejít do stavu 288


State 172

  218 xdcl_list: xdcl_list xdcl ';' .

    $výchozí  reduce using rule 218 (xdcl_list)


State 173

   32 common_dcl: lconst '(' . constdcl osemi ')'
   33           | lconst '(' . constdcl ';' constdcl_list osemi ')'
   34           | lconst '(' . ')'
   42 constdcl: . dcl_name_list ntype '=' expr_list
   43         | . dcl_name_list '=' expr_list
  153 dcl_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  273 dcl_name_list: . dcl_name
  274              | . dcl_name_list ',' dcl_name

    LNAME  posunout a přejít do stavu 9
    ')'    posunout a přejít do stavu 289
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    constdcl          přejít do stavu 290
    dcl_name          přejít do stavu 150
    sym               přejít do stavu 151
    hidden_importsym  přejít do stavu 13
    dcl_name_list     přejít do stavu 175


State 174

   31 common_dcl: lconst constdcl .

    $výchozí  reduce using rule 31 (common_dcl)


State 175

   42 constdcl: dcl_name_list . ntype '=' expr_list
   43         | dcl_name_list . '=' expr_list
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  274 dcl_name_list: dcl_name_list . ',' dcl_name

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '='         posunout a přejít do stavu 291
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11
    ','         posunout a přejít do stavu 282

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 292
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 176

   50 simple_stmt: expr LASOP . expr
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 293
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 177

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   94     | expr LANDAND . expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 294
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 178

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  109     | expr LANDNOT . expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 295
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 179

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  112     | expr LCOMM . expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 296
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 180

   54 simple_stmt: expr LDEC .

    $výchozí  reduce using rule 54 (simple_stmt)


State 181

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   95     | expr LEQ . expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 297
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 182

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
   99     | expr LGE . expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 298
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 183

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  100     | expr LGT . expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 299
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 184

   53 simple_stmt: expr LINC .

    $výchozí  reduce using rule 53 (simple_stmt)


State 185

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   98     | expr LLE . expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 300
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 186

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  110     | expr LLSH . expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 301
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 187

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   97     | expr LLT . expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 302
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 188

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   96     | expr LNE . expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 303
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 189

   92 expr: . uexpr
   93     | . expr LOROR expr
   93     | expr LOROR . expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 304
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 190

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  111     | expr LRSH . expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 305
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 191

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  101     | expr '+' . expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 306
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 192

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  102     | expr '-' . expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 307
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 193

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  103     | expr '|' . expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 308
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 194

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  104     | expr '^' . expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 309
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 195

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  105     | expr '*' . expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 310
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 196

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  106     | expr '/' . expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 311
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 197

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  107     | expr '%' . expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 312
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 198

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  108     | expr '&' . expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 313
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 199

  136 pexpr_no_paren: pexpr_no_paren '{' . start_complit braced_keyval_list '}'
  139 start_complit: . %empty

    $výchozí  reduce using rule 139 (start_complit)

    start_complit  přejít do stavu 314


State 200

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  122           | pexpr '(' . ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  123           | pexpr '(' . expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  124           | pexpr '(' . expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  147 expr_or_type: . expr
  148             | . non_expr_type
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  171 non_expr_type: . recvchantype
  172              | . fntype
  173              | . othertype
  174              | . '*' non_expr_type
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  277 expr_or_type_list: . expr_or_type
  278                  | . expr_or_type_list ',' expr_or_type

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 159
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 160
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    ')'         posunout a přejít do stavu 315
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr               přejít do stavu 161
    uexpr              přejít do stavu 74
    pseudocall         přejít do stavu 75
    pexpr_no_paren     přejít do stavu 76
    pexpr              přejít do stavu 77
    expr_or_type       přejít do stavu 316
    sym                přejít do stavu 117
    hidden_importsym   přejít do stavu 13
    name               přejít do stavu 80
    non_expr_type      přejít do stavu 163
    convtype           přejít do stavu 82
    comptype           přejít do stavu 83
    othertype          přejít do stavu 164
    recvchantype       přejít do stavu 165
    structtype         přejít do stavu 85
    interfacetype      přejít do stavu 86
    fntype             přejít do stavu 166
    fnlitdcl           přejít do stavu 89
    fnliteral          přejít do stavu 90
    expr_or_type_list  přejít do stavu 317


State 201

  127 pexpr_no_paren: pexpr '.' . sym
  128               | pexpr '.' . '(' expr_or_type ')'
  129               | pexpr '.' . '(' LTYPE ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'

    LNAME  posunout a přejít do stavu 9
    '('    posunout a přejít do stavu 318
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    sym               přejít do stavu 319
    hidden_importsym  přejít do stavu 13


State 202

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  130               | pexpr '[' . expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  131               | pexpr '[' . oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  132               | pexpr '[' . oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  289 oexpr: . %empty  [':']
  290      | . expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 289 (oexpr)

    expr              přejít do stavu 320
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    oexpr             přejít do stavu 321


State 203

  260 $@14: . %empty
  261 non_dcl_stmt: labelname ':' . $@14 stmt

    $výchozí  reduce using rule 260 ($@14)

    $@14  přejít do stavu 322


State 204

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  134               | convtype '(' . expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 323
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 205

  135 pexpr_no_paren: comptype lbrace . start_complit braced_keyval_list '}'
  139 start_complit: . %empty

    $výchozí  reduce using rule 139 (start_complit)

    start_complit  přejít do stavu 324


State 206

  216 fnliteral: fnlitdcl error .

    $výchozí  reduce using rule 216 (fnliteral)


State 207

   28 common_dcl: . LVAR vardcl
   29           | . LVAR '(' vardcl_list osemi ')'
   30           | . LVAR '(' ')'
   31           | . lconst constdcl
   32           | . lconst '(' constdcl osemi ')'
   33           | . lconst '(' constdcl ';' constdcl_list osemi ')'
   34           | . lconst '(' ')'
   35           | . LTYPE typedcl
   36           | . LTYPE '(' typedcl_list osemi ')'
   37           | . LTYPE '(' ')'
   38 lconst: . LCONST
   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   60 compound_stmt: . '{' $@3 stmt_list '}'
   74 for_stmt: . LFOR $@6 for_body
   80 if_stmt: . LIF $@7 if_header $@8 loop_body $@9 elseif_list else
   89 switch_stmt: . LSWITCH $@11 if_header $@12 LBODY caseblock_list '}'
   91 select_stmt: . LSELECT $@13 LBODY caseblock_list '}'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  162 labelname: . new_name
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  215          | fnlitdcl lbrace . stmt_list '}'
  216          | . fnlitdcl error
  250 stmt: . %empty  [';', '}']
  251     | . compound_stmt
  252     | . common_dcl
  253     | . non_dcl_stmt
  254     | . error
  255 non_dcl_stmt: . simple_stmt
  256             | . for_stmt
  257             | . switch_stmt
  258             | . select_stmt
  259             | . if_stmt
  261             | . labelname ':' $@14 stmt
  262             | . LFALL
  263             | . LBREAK onew_name
  264             | . LCONTINUE onew_name
  265             | . LGO pseudocall
  266             | . LDEFER pseudocall
  267             | . LGOTO new_name
  268             | . LRETURN oexpr_list
  269 stmt_list: . stmt
  270          | . stmt_list ';' stmt
  275 expr_list: . expr
  276          | . expr_list ',' expr

    error       posunout a přejít do stavu 325
    LLITERAL    posunout a přejít do stavu 35
    LBREAK      posunout a přejít do stavu 36
    LCHAN       posunout a přejít do stavu 37
    LCONST      posunout a přejít do stavu 38
    LCONTINUE   posunout a přejít do stavu 39
    LDEFER      posunout a přejít do stavu 40
    LFALL       posunout a přejít do stavu 41
    LFOR        posunout a přejít do stavu 42
    LFUNC       posunout a přejít do stavu 113
    LGO         posunout a přejít do stavu 44
    LGOTO       posunout a přejít do stavu 45
    LIF         posunout a přejít do stavu 46
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRETURN     posunout a přejít do stavu 49
    LSELECT     posunout a přejít do stavu 50
    LSTRUCT     posunout a přejít do stavu 51
    LSWITCH     posunout a přejít do stavu 52
    LTYPE       posunout a přejít do stavu 53
    LVAR        posunout a přejít do stavu 54
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 326
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    ';'  reduce using rule 250 (stmt)
    '}'  reduce using rule 250 (stmt)

    common_dcl        přejít do stavu 327
    lconst            přejít do stavu 67
    simple_stmt       přejít do stavu 68
    compound_stmt     přejít do stavu 328
    for_stmt          přejít do stavu 69
    if_stmt           přejít do stavu 70
    switch_stmt       přejít do stavu 71
    select_stmt       přejít do stavu 72
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    new_name          přejít do stavu 78
    sym               přejít do stavu 79
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    labelname         přejít do stavu 81
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    stmt              přejít do stavu 329
    non_dcl_stmt      přejít do stavu 330
    stmt_list         přejít do stavu 331
    expr_list         přejít do stavu 92


State 208

   52 simple_stmt: expr_list LCOLAS . expr_list
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 138
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 332


State 209

   51 simple_stmt: expr_list '=' . expr_list
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 138
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 333


State 210

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  276 expr_list: expr_list ',' . expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 334
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 211

  309 hidden_pkg_importsym: hidden_importsym .

    $výchozí  reduce using rule 309 (hidden_pkg_importsym)


State 212

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  305 hidden_import: LCONST hidden_pkg_importsym . '=' hidden_constant ';'
  306              | LCONST hidden_pkg_importsym . hidden_type '=' hidden_constant ';'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '='         posunout a přejít do stavu 343
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 346
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 213

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  207 hidden_fndcl: '(' . hidden_funarg_list ')' sym '(' ohidden_funarg_list ')' ohidden_funres
  329 hidden_funarg: . sym hidden_type oliteral
  330              | . sym LDDD hidden_type oliteral
  345 hidden_funarg_list: . hidden_funarg
  346                   | . hidden_funarg_list ',' hidden_funarg

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    sym                 přejít do stavu 350
    hidden_importsym    přejít do stavu 13
    hidden_funarg       přejít do stavu 351
    hidden_funarg_list  přejít do stavu 352


State 214

  209 fnbody: . %empty  [';']
  210       | . '{' stmt_list '}'
  308 hidden_import: LFUNC hidden_fndcl . fnbody ';'

    '{'  posunout a přejít do stavu 251

    $výchozí  reduce using rule 209 (fnbody)

    fnbody  přejít do stavu 353


State 215

  206 hidden_fndcl: hidden_pkg_importsym . '(' ohidden_funarg_list ')' ohidden_funres

    '('  posunout a přejít do stavu 354


State 216

  303 hidden_import: LIMPORT LNAME . LLITERAL ';'

    LLITERAL  posunout a přejít do stavu 355


State 217

  310 hidden_pkgtype: hidden_pkg_importsym .

    $výchozí  reduce using rule 310 (hidden_pkgtype)


State 218

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  307 hidden_import: LTYPE hidden_pkgtype . hidden_type ';'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 356
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 219

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  304 hidden_import: LVAR hidden_pkg_importsym . hidden_type ';'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 357
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 220

   22 import_there: $@2 hidden_import_list '$' '$' .

    $výchozí  reduce using rule 22 (import_there)


State 221

   11 import_stmt: . import_here import_package import_there
   12            | . import_here import_there
   14 import_stmt_list: import_stmt_list ';' . import_stmt
   15 import_here: . LLITERAL
   16            | . sym LLITERAL
   17            | . '.' LLITERAL
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  286 osemi: ';' .  [')']

    LLITERAL  posunout a přejít do stavu 27
    LNAME     posunout a přejít do stavu 9
    '.'       posunout a přejít do stavu 29
    '?'       posunout a přejít do stavu 10
    '@'       posunout a přejít do stavu 11

    $výchozí  reduce using rule 286 (osemi)

    import_stmt       přejít do stavu 358
    import_here       přejít do stavu 31
    sym               přejít do stavu 32
    hidden_importsym  přejít do stavu 13


State 222

    9 import: LIMPORT '(' import_stmt_list osemi . ')'

    ')'  posunout a přejít do stavu 359


State 223

   11 import_stmt: import_here import_package import_there .

    $výchozí  reduce using rule 11 (import_stmt)


State 224

  149 name_or_type: . ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  163 dotdotdot: . LDDD
  164          | . LDDD ntype
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  208       | LFUNC '(' . oarg_type_list_ocomma ')' fnres
  242 arg_type: . name_or_type
  243         | . sym name_or_type
  244         | . sym dotdotdot
  245         | . dotdotdot
  246 arg_type_list: . arg_type
  247              | . arg_type_list ',' arg_type
  248 oarg_type_list_ocomma: . %empty  [')']
  249                      | . arg_type_list ocomma

    LCHAN       posunout a přejít do stavu 37
    LDDD        posunout a přejít do stavu 242
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 248 (oarg_type_list_ocomma)

    name_or_type           přejít do stavu 243
    sym                    přejít do stavu 244
    hidden_importsym       přejít do stavu 13
    name                   přejít do stavu 118
    dotdotdot              přejít do stavu 245
    ntype                  přejít do stavu 246
    dotname                přejít do stavu 228
    othertype              přejít do stavu 229
    ptrtype                přejít do stavu 230
    recvchantype           přejít do stavu 231
    structtype             přejít do stavu 85
    interfacetype          přejít do stavu 86
    fntype                 přejít do stavu 232
    arg_type               přejít do stavu 247
    arg_type_list          přejít do stavu 248
    oarg_type_list_ocomma  přejít do stavu 360


State 225

  198 recvchantype: LCOMM . LCHAN ntype

    LCHAN  posunout a přejít do stavu 361


State 226

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  170      | '(' . ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 362
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 227

  193 othertype: LCHAN LCOMM ntype .

    $výchozí  reduce using rule 193 (othertype)


State 228

  169 ntype: dotname .

    $výchozí  reduce using rule 169 (ntype)


State 229

  167 ntype: othertype .

    $výchozí  reduce using rule 167 (ntype)


State 230

  168 ntype: ptrtype .

    $výchozí  reduce using rule 168 (ntype)


State 231

  165 ntype: recvchantype .

    $výchozí  reduce using rule 165 (ntype)


State 232

  166 ntype: fntype .

    $výchozí  reduce using rule 166 (ntype)


State 233

  197 ptrtype: '*' ntype .

    $výchozí  reduce using rule 197 (ptrtype)


State 234

  179 non_recvchantype: '(' ntype . ')'

    ')'  posunout a přejít do stavu 363


State 235

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  189 dotname: name '.' . sym

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    sym               přejít do stavu 364
    hidden_importsym  přejít do stavu 13


State 236

  294 osimple_stmt: simple_stmt .

    $výchozí  reduce using rule 294 (osimple_stmt)


State 237

   71 for_header: range_stmt .

    $výchozí  reduce using rule 71 (for_header)


State 238

   66 loop_body: . LBODY $@5 stmt_list '}'
   72 for_body: for_header . loop_body

    LBODY  posunout a přejít do stavu 365

    loop_body  přejít do stavu 366


State 239

   74 for_stmt: LFOR $@6 for_body .

    $výchozí  reduce using rule 74 (for_stmt)


State 240

   51 simple_stmt: expr_list . '=' expr_list
   52            | expr_list . LCOLAS expr_list
   67 range_stmt: expr_list . '=' LRANGE expr
   68           | expr_list . LCOLAS LRANGE expr
  276 expr_list: expr_list . ',' expr

    LCOLAS  posunout a přejít do stavu 367
    '='     posunout a přejít do stavu 368
    ','     posunout a přejít do stavu 210


State 241

   69 for_header: osimple_stmt . ';' osimple_stmt ';' osimple_stmt
   70           | osimple_stmt .  [LBODY]

    ';'  posunout a přejít do stavu 369

    $výchozí  reduce using rule 70 (for_header)


State 242

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  163 dotdotdot: LDDD .  [')', ',']
  164          | LDDD . ntype
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 163 (dotdotdot)

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 370
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 243

  242 arg_type: name_or_type .

    $výchozí  reduce using rule 242 (arg_type)


State 244

  149 name_or_type: . ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  161     | sym .  [')', '.', ',']
  163 dotdotdot: . LDDD
  164          | . LDDD ntype
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  243 arg_type: sym . name_or_type
  244         | sym . dotdotdot

    LCHAN       posunout a přejít do stavu 37
    LDDD        posunout a přejít do stavu 242
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 161 (name)

    name_or_type      přejít do stavu 371
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    dotdotdot         přejít do stavu 372
    ntype             přejít do stavu 246
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 245

  245 arg_type: dotdotdot .

    $výchozí  reduce using rule 245 (arg_type)


State 246

  149 name_or_type: ntype .

    $výchozí  reduce using rule 149 (name_or_type)


State 247

  246 arg_type_list: arg_type .

    $výchozí  reduce using rule 246 (arg_type_list)


State 248

  247 arg_type_list: arg_type_list . ',' arg_type
  249 oarg_type_list_ocomma: arg_type_list . ocomma
  287 ocomma: . %empty  [')']
  288       | . ','

    ','  posunout a přejít do stavu 373

    $výchozí  reduce using rule 287 (ocomma)

    ocomma  přejít do stavu 374


State 249

  205 fndcl: '(' oarg_type_list_ocomma . ')' sym '(' oarg_type_list_ocomma ')' fnres
  208 fntype: LFUNC '(' oarg_type_list_ocomma . ')' fnres

    ')'  posunout a přejít do stavu 375


State 250

  149 name_or_type: . ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  163 dotdotdot: . LDDD
  164          | . LDDD ntype
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  204 fndcl: sym '(' . oarg_type_list_ocomma ')' fnres
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  242 arg_type: . name_or_type
  243         | . sym name_or_type
  244         | . sym dotdotdot
  245         | . dotdotdot
  246 arg_type_list: . arg_type
  247              | . arg_type_list ',' arg_type
  248 oarg_type_list_ocomma: . %empty  [')']
  249                      | . arg_type_list ocomma

    LCHAN       posunout a přejít do stavu 37
    LDDD        posunout a přejít do stavu 242
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 248 (oarg_type_list_ocomma)

    name_or_type           přejít do stavu 243
    sym                    přejít do stavu 244
    hidden_importsym       přejít do stavu 13
    name                   přejít do stavu 118
    dotdotdot              přejít do stavu 245
    ntype                  přejít do stavu 246
    dotname                přejít do stavu 228
    othertype              přejít do stavu 229
    ptrtype                přejít do stavu 230
    recvchantype           přejít do stavu 231
    structtype             přejít do stavu 85
    interfacetype          přejít do stavu 86
    fntype                 přejít do stavu 232
    arg_type               přejít do stavu 247
    arg_type_list          přejít do stavu 248
    oarg_type_list_ocomma  přejít do stavu 376


State 251

   28 common_dcl: . LVAR vardcl
   29           | . LVAR '(' vardcl_list osemi ')'
   30           | . LVAR '(' ')'
   31           | . lconst constdcl
   32           | . lconst '(' constdcl osemi ')'
   33           | . lconst '(' constdcl ';' constdcl_list osemi ')'
   34           | . lconst '(' ')'
   35           | . LTYPE typedcl
   36           | . LTYPE '(' typedcl_list osemi ')'
   37           | . LTYPE '(' ')'
   38 lconst: . LCONST
   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   60 compound_stmt: . '{' $@3 stmt_list '}'
   74 for_stmt: . LFOR $@6 for_body
   80 if_stmt: . LIF $@7 if_header $@8 loop_body $@9 elseif_list else
   89 switch_stmt: . LSWITCH $@11 if_header $@12 LBODY caseblock_list '}'
   91 select_stmt: . LSELECT $@13 LBODY caseblock_list '}'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  162 labelname: . new_name
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  210 fnbody: '{' . stmt_list '}'
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  250 stmt: . %empty  [';', '}']
  251     | . compound_stmt
  252     | . common_dcl
  253     | . non_dcl_stmt
  254     | . error
  255 non_dcl_stmt: . simple_stmt
  256             | . for_stmt
  257             | . switch_stmt
  258             | . select_stmt
  259             | . if_stmt
  261             | . labelname ':' $@14 stmt
  262             | . LFALL
  263             | . LBREAK onew_name
  264             | . LCONTINUE onew_name
  265             | . LGO pseudocall
  266             | . LDEFER pseudocall
  267             | . LGOTO new_name
  268             | . LRETURN oexpr_list
  269 stmt_list: . stmt
  270          | . stmt_list ';' stmt
  275 expr_list: . expr
  276          | . expr_list ',' expr

    error       posunout a přejít do stavu 325
    LLITERAL    posunout a přejít do stavu 35
    LBREAK      posunout a přejít do stavu 36
    LCHAN       posunout a přejít do stavu 37
    LCONST      posunout a přejít do stavu 38
    LCONTINUE   posunout a přejít do stavu 39
    LDEFER      posunout a přejít do stavu 40
    LFALL       posunout a přejít do stavu 41
    LFOR        posunout a přejít do stavu 42
    LFUNC       posunout a přejít do stavu 113
    LGO         posunout a přejít do stavu 44
    LGOTO       posunout a přejít do stavu 45
    LIF         posunout a přejít do stavu 46
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRETURN     posunout a přejít do stavu 49
    LSELECT     posunout a přejít do stavu 50
    LSTRUCT     posunout a přejít do stavu 51
    LSWITCH     posunout a přejít do stavu 52
    LTYPE       posunout a přejít do stavu 53
    LVAR        posunout a přejít do stavu 54
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 326
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    ';'  reduce using rule 250 (stmt)
    '}'  reduce using rule 250 (stmt)

    common_dcl        přejít do stavu 327
    lconst            přejít do stavu 67
    simple_stmt       přejít do stavu 68
    compound_stmt     přejít do stavu 328
    for_stmt          přejít do stavu 69
    if_stmt           přejít do stavu 70
    switch_stmt       přejít do stavu 71
    select_stmt       přejít do stavu 72
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    new_name          přejít do stavu 78
    sym               přejít do stavu 79
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    labelname         přejít do stavu 81
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    stmt              přejít do stavu 329
    non_dcl_stmt      přejít do stavu 330
    stmt_list         přejít do stavu 377
    expr_list         přejít do stavu 92


State 252

  203 xfndcl: LFUNC fndcl fnbody .

    $výchozí  reduce using rule 203 (xfndcl)


State 253

   78 $@8: . %empty
   80 if_stmt: LIF $@7 if_header . $@8 loop_body $@9 elseif_list else

    $výchozí  reduce using rule 78 ($@8)

    $@8  přejít do stavu 378


State 254

   75 if_header: osimple_stmt .  [LBODY]
   76          | osimple_stmt . ';' osimple_stmt

    ';'  posunout a přejít do stavu 379

    $výchozí  reduce using rule 75 (if_header)


State 255

  156 sym: LNAME .  [LCHAN, LFUNC, LINTERFACE, LMAP, LNAME, LSTRUCT, LCOMM, '*', '(', '[', '?', '@', ',']
  235 packname: LNAME .  [LLITERAL, ';', '}']
  236         | LNAME . '.' sym

    '.'  posunout a přejít do stavu 380

    LLITERAL    reduce using rule 235 (packname)
    ';'         reduce using rule 235 (packname)
    '}'         reduce using rule 235 (packname)
    $výchozí  reduce using rule 156 (sym)


State 256

  235 packname: . LNAME
  236         | . LNAME '.' sym
  240 interfacedcl: '(' . packname ')'

    LNAME  posunout a přejít do stavu 381

    packname  přejít do stavu 382


State 257

  202 interfacetype: LINTERFACE lbrace '}' .

    $výchozí  reduce using rule 202 (interfacetype)


State 258

  238 interfacedcl: new_name . indcl
  241 indcl: . '(' oarg_type_list_ocomma ')' fnres

    '('  posunout a přejít do stavu 383

    indcl  přejít do stavu 384


State 259

  201 interfacetype: LINTERFACE lbrace interfacedcl_list . osemi '}'
  228 interfacedcl_list: interfacedcl_list . ';' interfacedcl
  285 osemi: . %empty  ['}']
  286      | . ';'

    ';'  posunout a přejít do stavu 385

    $výchozí  reduce using rule 285 (osemi)

    osemi  přejít do stavu 386


State 260

  239 interfacedcl: packname .

    $výchozí  reduce using rule 239 (interfacedcl)


State 261

  227 interfacedcl_list: interfacedcl .

    $výchozí  reduce using rule 227 (interfacedcl_list)


State 262

  194 othertype: LMAP '[' ntype . ']' ntype

    ']'  posunout a přejít do stavu 387


State 263

   63 caseblock_list: . %empty
   64               | . caseblock_list caseblock
   91 select_stmt: LSELECT $@13 LBODY . caseblock_list '}'

    $výchozí  reduce using rule 63 (caseblock_list)

    caseblock_list  přejít do stavu 388


State 264

  232 structdcl: '*' . embed oliteral
  234          | '*' . '(' embed ')' oliteral
  235 packname: . LNAME
  236         | . LNAME '.' sym
  237 embed: . packname

    LNAME  posunout a přejít do stavu 381
    '('    posunout a přejít do stavu 389

    packname  přejít do stavu 270
    embed     přejít do stavu 390


State 265

  231 structdcl: '(' . embed ')' oliteral
  233          | '(' . '*' embed ')' oliteral
  235 packname: . LNAME
  236         | . LNAME '.' sym
  237 embed: . packname

    LNAME  posunout a přejít do stavu 381
    '*'    posunout a přejít do stavu 391

    packname  přejít do stavu 270
    embed     přejít do stavu 392


State 266

  200 structtype: LSTRUCT lbrace '}' .

    $výchozí  reduce using rule 200 (structtype)


State 267

  271 new_name_list: new_name .

    $výchozí  reduce using rule 271 (new_name_list)


State 268

  199 structtype: LSTRUCT lbrace structdcl_list . osemi '}'
  226 structdcl_list: structdcl_list . ';' structdcl
  285 osemi: . %empty  ['}']
  286      | . ';'

    ';'  posunout a přejít do stavu 393

    $výchozí  reduce using rule 285 (osemi)

    osemi  přejít do stavu 394


State 269

  225 structdcl_list: structdcl .

    $výchozí  reduce using rule 225 (structdcl_list)


State 270

  237 embed: packname .

    $výchozí  reduce using rule 237 (embed)


State 271

  230 structdcl: embed . oliteral
  301 oliteral: . %empty  [';', '}']
  302         | . LLITERAL

    LLITERAL  posunout a přejít do stavu 395

    $výchozí  reduce using rule 301 (oliteral)

    oliteral  přejít do stavu 396


State 272

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  229 structdcl: new_name_list . ntype oliteral
  272 new_name_list: new_name_list . ',' new_name

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11
    ','         posunout a přejít do stavu 397

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 398
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 273

   88 $@12: . %empty
   89 switch_stmt: LSWITCH $@11 if_header . $@12 LBODY caseblock_list '}'

    $výchozí  reduce using rule 88 ($@12)

    $@12  přejít do stavu 399


State 274

   37 common_dcl: LTYPE '(' ')' .

    $výchozí  reduce using rule 37 (common_dcl)


State 275

  223 typedcl_list: typedcl .

    $výchozí  reduce using rule 223 (typedcl_list)


State 276

   36 common_dcl: LTYPE '(' typedcl_list . osemi ')'
  224 typedcl_list: typedcl_list . ';' typedcl
  285 osemi: . %empty  [')']
  286      | . ';'

    ';'  posunout a přejít do stavu 400

    $výchozí  reduce using rule 285 (osemi)

    osemi  přejít do stavu 401


State 277

   48 typedcl: typedclname ntype .

    $výchozí  reduce using rule 48 (typedcl)


State 278

   30 common_dcl: LVAR '(' ')' .

    $výchozí  reduce using rule 30 (common_dcl)


State 279

  219 vardcl_list: vardcl .

    $výchozí  reduce using rule 219 (vardcl_list)


State 280

   29 common_dcl: LVAR '(' vardcl_list . osemi ')'
  220 vardcl_list: vardcl_list . ';' vardcl
  285 osemi: . %empty  [')']
  286      | . ';'

    ';'  posunout a přejít do stavu 402

    $výchozí  reduce using rule 285 (osemi)

    osemi  přejít do stavu 403


State 281

   41 vardcl: dcl_name_list '=' . expr_list
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 138
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 404


State 282

  153 dcl_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  274 dcl_name_list: dcl_name_list ',' . dcl_name

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    dcl_name          přejít do stavu 405
    sym               přejít do stavu 151
    hidden_importsym  přejít do stavu 13


State 283

   39 vardcl: dcl_name_list ntype .  [LCASE, LDEFAULT, ')', ';', '}']
   40       | dcl_name_list ntype . '=' expr_list

    '='  posunout a přejít do stavu 406

    $výchozí  reduce using rule 39 (vardcl)


State 284

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  175 non_recvchantype: . fntype
  176                 | . othertype
  177                 | . ptrtype
  178                 | . dotname
  179                 | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  192          | LCHAN . non_recvchantype
  193          | . LCHAN LCOMM ntype
  193          | LCHAN . LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  198             | LCOMM LCHAN . ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 407
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 408
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 409
    non_recvchantype  přejít do stavu 119
    dotname           přejít do stavu 410
    othertype         přejít do stavu 411
    ptrtype           přejít do stavu 412
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 413


State 285

  174 non_expr_type: '*' non_expr_type .

    $výchozí  reduce using rule 174 (non_expr_type)


State 286

  137 pexpr_no_paren: '(' expr_or_type ')' . '{' start_complit braced_keyval_list '}'
  146 pexpr: '(' expr_or_type ')' .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', '(', ')', ';', '.', '=', ':', '}', '[', ']', ',']

    '{'  posunout a přejít do stavu 414

    $výchozí  reduce using rule 146 (pexpr)


State 287

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  191          | '[' LDDD ']' . ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 415
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 288

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  190          | '[' oexpr ']' . ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 416
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 289

   34 common_dcl: lconst '(' ')' .

    $výchozí  reduce using rule 34 (common_dcl)


State 290

   32 common_dcl: lconst '(' constdcl . osemi ')'
   33           | lconst '(' constdcl . ';' constdcl_list osemi ')'
  285 osemi: . %empty  [')']
  286      | . ';'

    ';'  posunout a přejít do stavu 417

    $výchozí  reduce using rule 285 (osemi)

    osemi  přejít do stavu 418


State 291

   43 constdcl: dcl_name_list '=' . expr_list
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 138
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 419


State 292

   42 constdcl: dcl_name_list ntype . '=' expr_list

    '='  posunout a přejít do stavu 420


State 293

   50 simple_stmt: expr LASOP expr .  [LCASE, LDEFAULT, LBODY, ';', '}']
   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 50 (simple_stmt)


State 294

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   94     | expr LANDAND expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LINC, LOROR, ')', ';', '=', ':', '}', ']', ',']
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 94 (expr)

    Conflict between rule 94 and token LANDAND resolved as reduce (%left LANDAND).
    Conflict between rule 94 and token LANDNOT resolved as shift (LANDAND < LANDNOT).
    Conflict between rule 94 and token LCOMM resolved as reduce (LCOMM < LANDAND).
    Conflict between rule 94 and token LEQ resolved as shift (LANDAND < LEQ).
    Conflict between rule 94 and token LGE resolved as shift (LANDAND < LGE).
    Conflict between rule 94 and token LGT resolved as shift (LANDAND < LGT).
    Conflict between rule 94 and token LLE resolved as shift (LANDAND < LLE).
    Conflict between rule 94 and token LLSH resolved as shift (LANDAND < LLSH).
    Conflict between rule 94 and token LLT resolved as shift (LANDAND < LLT).
    Conflict between rule 94 and token LNE resolved as shift (LANDAND < LNE).
    Conflict between rule 94 and token LOROR resolved as reduce (LOROR < LANDAND).
    Conflict between rule 94 and token LRSH resolved as shift (LANDAND < LRSH).
    Conflict between rule 94 and token '+' resolved as shift (LANDAND < '+').
    Conflict between rule 94 and token '-' resolved as shift (LANDAND < '-').
    Conflict between rule 94 and token '|' resolved as shift (LANDAND < '|').
    Conflict between rule 94 and token '^' resolved as shift (LANDAND < '^').
    Conflict between rule 94 and token '*' resolved as shift (LANDAND < '*').
    Conflict between rule 94 and token '/' resolved as shift (LANDAND < '/').
    Conflict between rule 94 and token '%' resolved as shift (LANDAND < '%').
    Conflict between rule 94 and token '&' resolved as shift (LANDAND < '&').


State 295

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  109     | expr LANDNOT expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', ')', ';', '=', ':', '}', ']', ',']
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    $výchozí  reduce using rule 109 (expr)

    Conflict between rule 109 and token LANDAND resolved as reduce (LANDAND < LANDNOT).
    Conflict between rule 109 and token LANDNOT resolved as reduce (%left LANDNOT).
    Conflict between rule 109 and token LCOMM resolved as reduce (LCOMM < LANDNOT).
    Conflict between rule 109 and token LEQ resolved as reduce (LEQ < LANDNOT).
    Conflict between rule 109 and token LGE resolved as reduce (LGE < LANDNOT).
    Conflict between rule 109 and token LGT resolved as reduce (LGT < LANDNOT).
    Conflict between rule 109 and token LLE resolved as reduce (LLE < LANDNOT).
    Conflict between rule 109 and token LLSH resolved as reduce (%left LLSH).
    Conflict between rule 109 and token LLT resolved as reduce (LLT < LANDNOT).
    Conflict between rule 109 and token LNE resolved as reduce (LNE < LANDNOT).
    Conflict between rule 109 and token LOROR resolved as reduce (LOROR < LANDNOT).
    Conflict between rule 109 and token LRSH resolved as reduce (%left LRSH).
    Conflict between rule 109 and token '+' resolved as reduce ('+' < LANDNOT).
    Conflict between rule 109 and token '-' resolved as reduce ('-' < LANDNOT).
    Conflict between rule 109 and token '|' resolved as reduce ('|' < LANDNOT).
    Conflict between rule 109 and token '^' resolved as reduce ('^' < LANDNOT).
    Conflict between rule 109 and token '*' resolved as reduce (%left '*').
    Conflict between rule 109 and token '/' resolved as reduce (%left '/').
    Conflict between rule 109 and token '%' resolved as reduce (%left '%').
    Conflict between rule 109 and token '&' resolved as reduce (%left '&').


State 296

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  112     | expr LCOMM expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LBODY, LCOMM, LDEC, LINC, ')', ';', '=', ':', '}', ']', ',']

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 112 (expr)

    Conflict between rule 112 and token LANDAND resolved as shift (LCOMM < LANDAND).
    Conflict between rule 112 and token LANDNOT resolved as shift (LCOMM < LANDNOT).
    Conflict between rule 112 and token LCOMM resolved as reduce (%left LCOMM).
    Conflict between rule 112 and token LEQ resolved as shift (LCOMM < LEQ).
    Conflict between rule 112 and token LGE resolved as shift (LCOMM < LGE).
    Conflict between rule 112 and token LGT resolved as shift (LCOMM < LGT).
    Conflict between rule 112 and token LLE resolved as shift (LCOMM < LLE).
    Conflict between rule 112 and token LLSH resolved as shift (LCOMM < LLSH).
    Conflict between rule 112 and token LLT resolved as shift (LCOMM < LLT).
    Conflict between rule 112 and token LNE resolved as shift (LCOMM < LNE).
    Conflict between rule 112 and token LOROR resolved as shift (LCOMM < LOROR).
    Conflict between rule 112 and token LRSH resolved as shift (LCOMM < LRSH).
    Conflict between rule 112 and token '+' resolved as shift (LCOMM < '+').
    Conflict between rule 112 and token '-' resolved as shift (LCOMM < '-').
    Conflict between rule 112 and token '|' resolved as shift (LCOMM < '|').
    Conflict between rule 112 and token '^' resolved as shift (LCOMM < '^').
    Conflict between rule 112 and token '*' resolved as shift (LCOMM < '*').
    Conflict between rule 112 and token '/' resolved as shift (LCOMM < '/').
    Conflict between rule 112 and token '%' resolved as shift (LCOMM < '%').
    Conflict between rule 112 and token '&' resolved as shift (LCOMM < '&').


State 297

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   95     | expr LEQ expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, ')', ';', '=', ':', '}', ']', ',']
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 95 (expr)

    Conflict between rule 95 and token LANDAND resolved as reduce (LANDAND < LEQ).
    Conflict between rule 95 and token LANDNOT resolved as shift (LEQ < LANDNOT).
    Conflict between rule 95 and token LCOMM resolved as reduce (LCOMM < LEQ).
    Conflict between rule 95 and token LEQ resolved as reduce (%left LEQ).
    Conflict between rule 95 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 95 and token LGT resolved as reduce (%left LGT).
    Conflict between rule 95 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 95 and token LLSH resolved as shift (LEQ < LLSH).
    Conflict between rule 95 and token LLT resolved as reduce (%left LLT).
    Conflict between rule 95 and token LNE resolved as reduce (%left LNE).
    Conflict between rule 95 and token LOROR resolved as reduce (LOROR < LEQ).
    Conflict between rule 95 and token LRSH resolved as shift (LEQ < LRSH).
    Conflict between rule 95 and token '+' resolved as shift (LEQ < '+').
    Conflict between rule 95 and token '-' resolved as shift (LEQ < '-').
    Conflict between rule 95 and token '|' resolved as shift (LEQ < '|').
    Conflict between rule 95 and token '^' resolved as shift (LEQ < '^').
    Conflict between rule 95 and token '*' resolved as shift (LEQ < '*').
    Conflict between rule 95 and token '/' resolved as shift (LEQ < '/').
    Conflict between rule 95 and token '%' resolved as shift (LEQ < '%').
    Conflict between rule 95 and token '&' resolved as shift (LEQ < '&').


State 298

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
   99     | expr LGE expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, ')', ';', '=', ':', '}', ']', ',']
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 99 (expr)

    Conflict between rule 99 and token LANDAND resolved as reduce (LANDAND < LGE).
    Conflict between rule 99 and token LANDNOT resolved as shift (LGE < LANDNOT).
    Conflict between rule 99 and token LCOMM resolved as reduce (LCOMM < LGE).
    Conflict between rule 99 and token LEQ resolved as reduce (%left LEQ).
    Conflict between rule 99 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 99 and token LGT resolved as reduce (%left LGT).
    Conflict between rule 99 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 99 and token LLSH resolved as shift (LGE < LLSH).
    Conflict between rule 99 and token LLT resolved as reduce (%left LLT).
    Conflict between rule 99 and token LNE resolved as reduce (%left LNE).
    Conflict between rule 99 and token LOROR resolved as reduce (LOROR < LGE).
    Conflict between rule 99 and token LRSH resolved as shift (LGE < LRSH).
    Conflict between rule 99 and token '+' resolved as shift (LGE < '+').
    Conflict between rule 99 and token '-' resolved as shift (LGE < '-').
    Conflict between rule 99 and token '|' resolved as shift (LGE < '|').
    Conflict between rule 99 and token '^' resolved as shift (LGE < '^').
    Conflict between rule 99 and token '*' resolved as shift (LGE < '*').
    Conflict between rule 99 and token '/' resolved as shift (LGE < '/').
    Conflict between rule 99 and token '%' resolved as shift (LGE < '%').
    Conflict between rule 99 and token '&' resolved as shift (LGE < '&').


State 299

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  100     | expr LGT expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, ')', ';', '=', ':', '}', ']', ',']
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 100 (expr)

    Conflict between rule 100 and token LANDAND resolved as reduce (LANDAND < LGT).
    Conflict between rule 100 and token LANDNOT resolved as shift (LGT < LANDNOT).
    Conflict between rule 100 and token LCOMM resolved as reduce (LCOMM < LGT).
    Conflict between rule 100 and token LEQ resolved as reduce (%left LEQ).
    Conflict between rule 100 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 100 and token LGT resolved as reduce (%left LGT).
    Conflict between rule 100 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 100 and token LLSH resolved as shift (LGT < LLSH).
    Conflict between rule 100 and token LLT resolved as reduce (%left LLT).
    Conflict between rule 100 and token LNE resolved as reduce (%left LNE).
    Conflict between rule 100 and token LOROR resolved as reduce (LOROR < LGT).
    Conflict between rule 100 and token LRSH resolved as shift (LGT < LRSH).
    Conflict between rule 100 and token '+' resolved as shift (LGT < '+').
    Conflict between rule 100 and token '-' resolved as shift (LGT < '-').
    Conflict between rule 100 and token '|' resolved as shift (LGT < '|').
    Conflict between rule 100 and token '^' resolved as shift (LGT < '^').
    Conflict between rule 100 and token '*' resolved as shift (LGT < '*').
    Conflict between rule 100 and token '/' resolved as shift (LGT < '/').
    Conflict between rule 100 and token '%' resolved as shift (LGT < '%').
    Conflict between rule 100 and token '&' resolved as shift (LGT < '&').


State 300

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   98     | expr LLE expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, ')', ';', '=', ':', '}', ']', ',']
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 98 (expr)

    Conflict between rule 98 and token LANDAND resolved as reduce (LANDAND < LLE).
    Conflict between rule 98 and token LANDNOT resolved as shift (LLE < LANDNOT).
    Conflict between rule 98 and token LCOMM resolved as reduce (LCOMM < LLE).
    Conflict between rule 98 and token LEQ resolved as reduce (%left LEQ).
    Conflict between rule 98 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 98 and token LGT resolved as reduce (%left LGT).
    Conflict between rule 98 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 98 and token LLSH resolved as shift (LLE < LLSH).
    Conflict between rule 98 and token LLT resolved as reduce (%left LLT).
    Conflict between rule 98 and token LNE resolved as reduce (%left LNE).
    Conflict between rule 98 and token LOROR resolved as reduce (LOROR < LLE).
    Conflict between rule 98 and token LRSH resolved as shift (LLE < LRSH).
    Conflict between rule 98 and token '+' resolved as shift (LLE < '+').
    Conflict between rule 98 and token '-' resolved as shift (LLE < '-').
    Conflict between rule 98 and token '|' resolved as shift (LLE < '|').
    Conflict between rule 98 and token '^' resolved as shift (LLE < '^').
    Conflict between rule 98 and token '*' resolved as shift (LLE < '*').
    Conflict between rule 98 and token '/' resolved as shift (LLE < '/').
    Conflict between rule 98 and token '%' resolved as shift (LLE < '%').
    Conflict between rule 98 and token '&' resolved as shift (LLE < '&').


State 301

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  110     | expr LLSH expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', ')', ';', '=', ':', '}', ']', ',']
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    $výchozí  reduce using rule 110 (expr)

    Conflict between rule 110 and token LANDAND resolved as reduce (LANDAND < LLSH).
    Conflict between rule 110 and token LANDNOT resolved as reduce (%left LANDNOT).
    Conflict between rule 110 and token LCOMM resolved as reduce (LCOMM < LLSH).
    Conflict between rule 110 and token LEQ resolved as reduce (LEQ < LLSH).
    Conflict between rule 110 and token LGE resolved as reduce (LGE < LLSH).
    Conflict between rule 110 and token LGT resolved as reduce (LGT < LLSH).
    Conflict between rule 110 and token LLE resolved as reduce (LLE < LLSH).
    Conflict between rule 110 and token LLSH resolved as reduce (%left LLSH).
    Conflict between rule 110 and token LLT resolved as reduce (LLT < LLSH).
    Conflict between rule 110 and token LNE resolved as reduce (LNE < LLSH).
    Conflict between rule 110 and token LOROR resolved as reduce (LOROR < LLSH).
    Conflict between rule 110 and token LRSH resolved as reduce (%left LRSH).
    Conflict between rule 110 and token '+' resolved as reduce ('+' < LLSH).
    Conflict between rule 110 and token '-' resolved as reduce ('-' < LLSH).
    Conflict between rule 110 and token '|' resolved as reduce ('|' < LLSH).
    Conflict between rule 110 and token '^' resolved as reduce ('^' < LLSH).
    Conflict between rule 110 and token '*' resolved as reduce (%left '*').
    Conflict between rule 110 and token '/' resolved as reduce (%left '/').
    Conflict between rule 110 and token '%' resolved as reduce (%left '%').
    Conflict between rule 110 and token '&' resolved as reduce (%left '&').


State 302

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   97     | expr LLT expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, ')', ';', '=', ':', '}', ']', ',']
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 97 (expr)

    Conflict between rule 97 and token LANDAND resolved as reduce (LANDAND < LLT).
    Conflict between rule 97 and token LANDNOT resolved as shift (LLT < LANDNOT).
    Conflict between rule 97 and token LCOMM resolved as reduce (LCOMM < LLT).
    Conflict between rule 97 and token LEQ resolved as reduce (%left LEQ).
    Conflict between rule 97 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 97 and token LGT resolved as reduce (%left LGT).
    Conflict between rule 97 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 97 and token LLSH resolved as shift (LLT < LLSH).
    Conflict between rule 97 and token LLT resolved as reduce (%left LLT).
    Conflict between rule 97 and token LNE resolved as reduce (%left LNE).
    Conflict between rule 97 and token LOROR resolved as reduce (LOROR < LLT).
    Conflict between rule 97 and token LRSH resolved as shift (LLT < LRSH).
    Conflict between rule 97 and token '+' resolved as shift (LLT < '+').
    Conflict between rule 97 and token '-' resolved as shift (LLT < '-').
    Conflict between rule 97 and token '|' resolved as shift (LLT < '|').
    Conflict between rule 97 and token '^' resolved as shift (LLT < '^').
    Conflict between rule 97 and token '*' resolved as shift (LLT < '*').
    Conflict between rule 97 and token '/' resolved as shift (LLT < '/').
    Conflict between rule 97 and token '%' resolved as shift (LLT < '%').
    Conflict between rule 97 and token '&' resolved as shift (LLT < '&').


State 303

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   96     | expr LNE expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, ')', ';', '=', ':', '}', ']', ',']
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 96 (expr)

    Conflict between rule 96 and token LANDAND resolved as reduce (LANDAND < LNE).
    Conflict between rule 96 and token LANDNOT resolved as shift (LNE < LANDNOT).
    Conflict between rule 96 and token LCOMM resolved as reduce (LCOMM < LNE).
    Conflict between rule 96 and token LEQ resolved as reduce (%left LEQ).
    Conflict between rule 96 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 96 and token LGT resolved as reduce (%left LGT).
    Conflict between rule 96 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 96 and token LLSH resolved as shift (LNE < LLSH).
    Conflict between rule 96 and token LLT resolved as reduce (%left LLT).
    Conflict between rule 96 and token LNE resolved as reduce (%left LNE).
    Conflict between rule 96 and token LOROR resolved as reduce (LOROR < LNE).
    Conflict between rule 96 and token LRSH resolved as shift (LNE < LRSH).
    Conflict between rule 96 and token '+' resolved as shift (LNE < '+').
    Conflict between rule 96 and token '-' resolved as shift (LNE < '-').
    Conflict between rule 96 and token '|' resolved as shift (LNE < '|').
    Conflict between rule 96 and token '^' resolved as shift (LNE < '^').
    Conflict between rule 96 and token '*' resolved as shift (LNE < '*').
    Conflict between rule 96 and token '/' resolved as shift (LNE < '/').
    Conflict between rule 96 and token '%' resolved as shift (LNE < '%').
    Conflict between rule 96 and token '&' resolved as shift (LNE < '&').


State 304

   93 expr: expr . LOROR expr
   93     | expr LOROR expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LBODY, LCOMM, LDEC, LINC, LOROR, ')', ';', '=', ':', '}', ']', ',']
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 93 (expr)

    Conflict between rule 93 and token LANDAND resolved as shift (LOROR < LANDAND).
    Conflict between rule 93 and token LANDNOT resolved as shift (LOROR < LANDNOT).
    Conflict between rule 93 and token LCOMM resolved as reduce (LCOMM < LOROR).
    Conflict between rule 93 and token LEQ resolved as shift (LOROR < LEQ).
    Conflict between rule 93 and token LGE resolved as shift (LOROR < LGE).
    Conflict between rule 93 and token LGT resolved as shift (LOROR < LGT).
    Conflict between rule 93 and token LLE resolved as shift (LOROR < LLE).
    Conflict between rule 93 and token LLSH resolved as shift (LOROR < LLSH).
    Conflict between rule 93 and token LLT resolved as shift (LOROR < LLT).
    Conflict between rule 93 and token LNE resolved as shift (LOROR < LNE).
    Conflict between rule 93 and token LOROR resolved as reduce (%left LOROR).
    Conflict between rule 93 and token LRSH resolved as shift (LOROR < LRSH).
    Conflict between rule 93 and token '+' resolved as shift (LOROR < '+').
    Conflict between rule 93 and token '-' resolved as shift (LOROR < '-').
    Conflict between rule 93 and token '|' resolved as shift (LOROR < '|').
    Conflict between rule 93 and token '^' resolved as shift (LOROR < '^').
    Conflict between rule 93 and token '*' resolved as shift (LOROR < '*').
    Conflict between rule 93 and token '/' resolved as shift (LOROR < '/').
    Conflict between rule 93 and token '%' resolved as shift (LOROR < '%').
    Conflict between rule 93 and token '&' resolved as shift (LOROR < '&').


State 305

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  111     | expr LRSH expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', ')', ';', '=', ':', '}', ']', ',']
  112     | expr . LCOMM expr

    $výchozí  reduce using rule 111 (expr)

    Conflict between rule 111 and token LANDAND resolved as reduce (LANDAND < LRSH).
    Conflict between rule 111 and token LANDNOT resolved as reduce (%left LANDNOT).
    Conflict between rule 111 and token LCOMM resolved as reduce (LCOMM < LRSH).
    Conflict between rule 111 and token LEQ resolved as reduce (LEQ < LRSH).
    Conflict between rule 111 and token LGE resolved as reduce (LGE < LRSH).
    Conflict between rule 111 and token LGT resolved as reduce (LGT < LRSH).
    Conflict between rule 111 and token LLE resolved as reduce (LLE < LRSH).
    Conflict between rule 111 and token LLSH resolved as reduce (%left LLSH).
    Conflict between rule 111 and token LLT resolved as reduce (LLT < LRSH).
    Conflict between rule 111 and token LNE resolved as reduce (LNE < LRSH).
    Conflict between rule 111 and token LOROR resolved as reduce (LOROR < LRSH).
    Conflict between rule 111 and token LRSH resolved as reduce (%left LRSH).
    Conflict between rule 111 and token '+' resolved as reduce ('+' < LRSH).
    Conflict between rule 111 and token '-' resolved as reduce ('-' < LRSH).
    Conflict between rule 111 and token '|' resolved as reduce ('|' < LRSH).
    Conflict between rule 111 and token '^' resolved as reduce ('^' < LRSH).
    Conflict between rule 111 and token '*' resolved as reduce (%left '*').
    Conflict between rule 111 and token '/' resolved as reduce (%left '/').
    Conflict between rule 111 and token '%' resolved as reduce (%left '%').
    Conflict between rule 111 and token '&' resolved as reduce (%left '&').


State 306

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  101     | expr '+' expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, '+', '-', '|', '^', ')', ';', '=', ':', '}', ']', ',']
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 101 (expr)

    Conflict between rule 101 and token LANDAND resolved as reduce (LANDAND < '+').
    Conflict between rule 101 and token LANDNOT resolved as shift ('+' < LANDNOT).
    Conflict between rule 101 and token LCOMM resolved as reduce (LCOMM < '+').
    Conflict between rule 101 and token LEQ resolved as reduce (LEQ < '+').
    Conflict between rule 101 and token LGE resolved as reduce (LGE < '+').
    Conflict between rule 101 and token LGT resolved as reduce (LGT < '+').
    Conflict between rule 101 and token LLE resolved as reduce (LLE < '+').
    Conflict between rule 101 and token LLSH resolved as shift ('+' < LLSH).
    Conflict between rule 101 and token LLT resolved as reduce (LLT < '+').
    Conflict between rule 101 and token LNE resolved as reduce (LNE < '+').
    Conflict between rule 101 and token LOROR resolved as reduce (LOROR < '+').
    Conflict between rule 101 and token LRSH resolved as shift ('+' < LRSH).
    Conflict between rule 101 and token '+' resolved as reduce (%left '+').
    Conflict between rule 101 and token '-' resolved as reduce (%left '-').
    Conflict between rule 101 and token '|' resolved as reduce (%left '|').
    Conflict between rule 101 and token '^' resolved as reduce (%left '^').
    Conflict between rule 101 and token '*' resolved as shift ('+' < '*').
    Conflict between rule 101 and token '/' resolved as shift ('+' < '/').
    Conflict between rule 101 and token '%' resolved as shift ('+' < '%').
    Conflict between rule 101 and token '&' resolved as shift ('+' < '&').


State 307

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  102     | expr '-' expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, '+', '-', '|', '^', ')', ';', '=', ':', '}', ']', ',']
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 102 (expr)

    Conflict between rule 102 and token LANDAND resolved as reduce (LANDAND < '-').
    Conflict between rule 102 and token LANDNOT resolved as shift ('-' < LANDNOT).
    Conflict between rule 102 and token LCOMM resolved as reduce (LCOMM < '-').
    Conflict between rule 102 and token LEQ resolved as reduce (LEQ < '-').
    Conflict between rule 102 and token LGE resolved as reduce (LGE < '-').
    Conflict between rule 102 and token LGT resolved as reduce (LGT < '-').
    Conflict between rule 102 and token LLE resolved as reduce (LLE < '-').
    Conflict between rule 102 and token LLSH resolved as shift ('-' < LLSH).
    Conflict between rule 102 and token LLT resolved as reduce (LLT < '-').
    Conflict between rule 102 and token LNE resolved as reduce (LNE < '-').
    Conflict between rule 102 and token LOROR resolved as reduce (LOROR < '-').
    Conflict between rule 102 and token LRSH resolved as shift ('-' < LRSH).
    Conflict between rule 102 and token '+' resolved as reduce (%left '+').
    Conflict between rule 102 and token '-' resolved as reduce (%left '-').
    Conflict between rule 102 and token '|' resolved as reduce (%left '|').
    Conflict between rule 102 and token '^' resolved as reduce (%left '^').
    Conflict between rule 102 and token '*' resolved as shift ('-' < '*').
    Conflict between rule 102 and token '/' resolved as shift ('-' < '/').
    Conflict between rule 102 and token '%' resolved as shift ('-' < '%').
    Conflict between rule 102 and token '&' resolved as shift ('-' < '&').


State 308

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  103     | expr '|' expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, '+', '-', '|', '^', ')', ';', '=', ':', '}', ']', ',']
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 103 (expr)

    Conflict between rule 103 and token LANDAND resolved as reduce (LANDAND < '|').
    Conflict between rule 103 and token LANDNOT resolved as shift ('|' < LANDNOT).
    Conflict between rule 103 and token LCOMM resolved as reduce (LCOMM < '|').
    Conflict between rule 103 and token LEQ resolved as reduce (LEQ < '|').
    Conflict between rule 103 and token LGE resolved as reduce (LGE < '|').
    Conflict between rule 103 and token LGT resolved as reduce (LGT < '|').
    Conflict between rule 103 and token LLE resolved as reduce (LLE < '|').
    Conflict between rule 103 and token LLSH resolved as shift ('|' < LLSH).
    Conflict between rule 103 and token LLT resolved as reduce (LLT < '|').
    Conflict between rule 103 and token LNE resolved as reduce (LNE < '|').
    Conflict between rule 103 and token LOROR resolved as reduce (LOROR < '|').
    Conflict between rule 103 and token LRSH resolved as shift ('|' < LRSH).
    Conflict between rule 103 and token '+' resolved as reduce (%left '+').
    Conflict between rule 103 and token '-' resolved as reduce (%left '-').
    Conflict between rule 103 and token '|' resolved as reduce (%left '|').
    Conflict between rule 103 and token '^' resolved as reduce (%left '^').
    Conflict between rule 103 and token '*' resolved as shift ('|' < '*').
    Conflict between rule 103 and token '/' resolved as shift ('|' < '/').
    Conflict between rule 103 and token '%' resolved as shift ('|' < '%').
    Conflict between rule 103 and token '&' resolved as shift ('|' < '&').


State 309

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  104     | expr '^' expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLT, LNE, LOROR, '+', '-', '|', '^', ')', ';', '=', ':', '}', ']', ',']
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDNOT  posunout a přejít do stavu 178
    LLSH     posunout a přejít do stavu 186
    LRSH     posunout a přejít do stavu 190
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 104 (expr)

    Conflict between rule 104 and token LANDAND resolved as reduce (LANDAND < '^').
    Conflict between rule 104 and token LANDNOT resolved as shift ('^' < LANDNOT).
    Conflict between rule 104 and token LCOMM resolved as reduce (LCOMM < '^').
    Conflict between rule 104 and token LEQ resolved as reduce (LEQ < '^').
    Conflict between rule 104 and token LGE resolved as reduce (LGE < '^').
    Conflict between rule 104 and token LGT resolved as reduce (LGT < '^').
    Conflict between rule 104 and token LLE resolved as reduce (LLE < '^').
    Conflict between rule 104 and token LLSH resolved as shift ('^' < LLSH).
    Conflict between rule 104 and token LLT resolved as reduce (LLT < '^').
    Conflict between rule 104 and token LNE resolved as reduce (LNE < '^').
    Conflict between rule 104 and token LOROR resolved as reduce (LOROR < '^').
    Conflict between rule 104 and token LRSH resolved as shift ('^' < LRSH).
    Conflict between rule 104 and token '+' resolved as reduce (%left '+').
    Conflict between rule 104 and token '-' resolved as reduce (%left '-').
    Conflict between rule 104 and token '|' resolved as reduce (%left '|').
    Conflict between rule 104 and token '^' resolved as reduce (%left '^').
    Conflict between rule 104 and token '*' resolved as shift ('^' < '*').
    Conflict between rule 104 and token '/' resolved as shift ('^' < '/').
    Conflict between rule 104 and token '%' resolved as shift ('^' < '%').
    Conflict between rule 104 and token '&' resolved as shift ('^' < '&').


State 310

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  105     | expr '*' expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', ')', ';', '=', ':', '}', ']', ',']
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    $výchozí  reduce using rule 105 (expr)

    Conflict between rule 105 and token LANDAND resolved as reduce (LANDAND < '*').
    Conflict between rule 105 and token LANDNOT resolved as reduce (%left LANDNOT).
    Conflict between rule 105 and token LCOMM resolved as reduce (LCOMM < '*').
    Conflict between rule 105 and token LEQ resolved as reduce (LEQ < '*').
    Conflict between rule 105 and token LGE resolved as reduce (LGE < '*').
    Conflict between rule 105 and token LGT resolved as reduce (LGT < '*').
    Conflict between rule 105 and token LLE resolved as reduce (LLE < '*').
    Conflict between rule 105 and token LLSH resolved as reduce (%left LLSH).
    Conflict between rule 105 and token LLT resolved as reduce (LLT < '*').
    Conflict between rule 105 and token LNE resolved as reduce (LNE < '*').
    Conflict between rule 105 and token LOROR resolved as reduce (LOROR < '*').
    Conflict between rule 105 and token LRSH resolved as reduce (%left LRSH).
    Conflict between rule 105 and token '+' resolved as reduce ('+' < '*').
    Conflict between rule 105 and token '-' resolved as reduce ('-' < '*').
    Conflict between rule 105 and token '|' resolved as reduce ('|' < '*').
    Conflict between rule 105 and token '^' resolved as reduce ('^' < '*').
    Conflict between rule 105 and token '*' resolved as reduce (%left '*').
    Conflict between rule 105 and token '/' resolved as reduce (%left '/').
    Conflict between rule 105 and token '%' resolved as reduce (%left '%').
    Conflict between rule 105 and token '&' resolved as reduce (%left '&').


State 311

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  106     | expr '/' expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', ')', ';', '=', ':', '}', ']', ',']
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    $výchozí  reduce using rule 106 (expr)

    Conflict between rule 106 and token LANDAND resolved as reduce (LANDAND < '/').
    Conflict between rule 106 and token LANDNOT resolved as reduce (%left LANDNOT).
    Conflict between rule 106 and token LCOMM resolved as reduce (LCOMM < '/').
    Conflict between rule 106 and token LEQ resolved as reduce (LEQ < '/').
    Conflict between rule 106 and token LGE resolved as reduce (LGE < '/').
    Conflict between rule 106 and token LGT resolved as reduce (LGT < '/').
    Conflict between rule 106 and token LLE resolved as reduce (LLE < '/').
    Conflict between rule 106 and token LLSH resolved as reduce (%left LLSH).
    Conflict between rule 106 and token LLT resolved as reduce (LLT < '/').
    Conflict between rule 106 and token LNE resolved as reduce (LNE < '/').
    Conflict between rule 106 and token LOROR resolved as reduce (LOROR < '/').
    Conflict between rule 106 and token LRSH resolved as reduce (%left LRSH).
    Conflict between rule 106 and token '+' resolved as reduce ('+' < '/').
    Conflict between rule 106 and token '-' resolved as reduce ('-' < '/').
    Conflict between rule 106 and token '|' resolved as reduce ('|' < '/').
    Conflict between rule 106 and token '^' resolved as reduce ('^' < '/').
    Conflict between rule 106 and token '*' resolved as reduce (%left '*').
    Conflict between rule 106 and token '/' resolved as reduce (%left '/').
    Conflict between rule 106 and token '%' resolved as reduce (%left '%').
    Conflict between rule 106 and token '&' resolved as reduce (%left '&').


State 312

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  107     | expr '%' expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', ')', ';', '=', ':', '}', ']', ',']
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    $výchozí  reduce using rule 107 (expr)

    Conflict between rule 107 and token LANDAND resolved as reduce (LANDAND < '%').
    Conflict between rule 107 and token LANDNOT resolved as reduce (%left LANDNOT).
    Conflict between rule 107 and token LCOMM resolved as reduce (LCOMM < '%').
    Conflict between rule 107 and token LEQ resolved as reduce (LEQ < '%').
    Conflict between rule 107 and token LGE resolved as reduce (LGE < '%').
    Conflict between rule 107 and token LGT resolved as reduce (LGT < '%').
    Conflict between rule 107 and token LLE resolved as reduce (LLE < '%').
    Conflict between rule 107 and token LLSH resolved as reduce (%left LLSH).
    Conflict between rule 107 and token LLT resolved as reduce (LLT < '%').
    Conflict between rule 107 and token LNE resolved as reduce (LNE < '%').
    Conflict between rule 107 and token LOROR resolved as reduce (LOROR < '%').
    Conflict between rule 107 and token LRSH resolved as reduce (%left LRSH).
    Conflict between rule 107 and token '+' resolved as reduce ('+' < '%').
    Conflict between rule 107 and token '-' resolved as reduce ('-' < '%').
    Conflict between rule 107 and token '|' resolved as reduce ('|' < '%').
    Conflict between rule 107 and token '^' resolved as reduce ('^' < '%').
    Conflict between rule 107 and token '*' resolved as reduce (%left '*').
    Conflict between rule 107 and token '/' resolved as reduce (%left '/').
    Conflict between rule 107 and token '%' resolved as reduce (%left '%').
    Conflict between rule 107 and token '&' resolved as reduce (%left '&').


State 313

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  108     | expr '&' expr .  [LASOP, LCOLAS, LCASE, LDDD, LDEFAULT, LANDAND, LANDNOT, LBODY, LCOMM, LDEC, LEQ, LGE, LGT, LINC, LLE, LLSH, LLT, LNE, LOROR, LRSH, '+', '-', '|', '^', '*', '/', '%', '&', ')', ';', '=', ':', '}', ']', ',']
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    $výchozí  reduce using rule 108 (expr)

    Conflict between rule 108 and token LANDAND resolved as reduce (LANDAND < '&').
    Conflict between rule 108 and token LANDNOT resolved as reduce (%left LANDNOT).
    Conflict between rule 108 and token LCOMM resolved as reduce (LCOMM < '&').
    Conflict between rule 108 and token LEQ resolved as reduce (LEQ < '&').
    Conflict between rule 108 and token LGE resolved as reduce (LGE < '&').
    Conflict between rule 108 and token LGT resolved as reduce (LGT < '&').
    Conflict between rule 108 and token LLE resolved as reduce (LLE < '&').
    Conflict between rule 108 and token LLSH resolved as reduce (%left LLSH).
    Conflict between rule 108 and token LLT resolved as reduce (LLT < '&').
    Conflict between rule 108 and token LNE resolved as reduce (LNE < '&').
    Conflict between rule 108 and token LOROR resolved as reduce (LOROR < '&').
    Conflict between rule 108 and token LRSH resolved as reduce (%left LRSH).
    Conflict between rule 108 and token '+' resolved as reduce ('+' < '&').
    Conflict between rule 108 and token '-' resolved as reduce ('-' < '&').
    Conflict between rule 108 and token '|' resolved as reduce ('|' < '&').
    Conflict between rule 108 and token '^' resolved as reduce ('^' < '&').
    Conflict between rule 108 and token '*' resolved as reduce (%left '*').
    Conflict between rule 108 and token '/' resolved as reduce (%left '/').
    Conflict between rule 108 and token '%' resolved as reduce (%left '%').
    Conflict between rule 108 and token '&' resolved as reduce (%left '&').


State 314

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  136               | pexpr_no_paren '{' start_complit . braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  140 keyval: . expr ':' complitexpr
  141 bare_complitexpr: . expr
  142                 | . '{' start_complit braced_keyval_list '}'
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  279 keyval_list: . keyval
  280            | . bare_complitexpr
  281            | . keyval_list ',' keyval
  282            | . keyval_list ',' bare_complitexpr
  283 braced_keyval_list: . %empty  ['}']
  284                   | . keyval_list ocomma

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 421
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 283 (braced_keyval_list)

    expr                přejít do stavu 422
    uexpr               přejít do stavu 74
    pseudocall          přejít do stavu 75
    pexpr_no_paren      přejít do stavu 76
    keyval              přejít do stavu 423
    bare_complitexpr    přejít do stavu 424
    pexpr               přejít do stavu 77
    sym                 přejít do stavu 117
    hidden_importsym    přejít do stavu 13
    name                přejít do stavu 80
    convtype            přejít do stavu 82
    comptype            přejít do stavu 83
    othertype           přejít do stavu 84
    structtype          přejít do stavu 85
    interfacetype       přejít do stavu 86
    fntype              přejít do stavu 88
    fnlitdcl            přejít do stavu 89
    fnliteral           přejít do stavu 90
    keyval_list         přejít do stavu 425
    braced_keyval_list  přejít do stavu 426


State 315

  122 pseudocall: pexpr '(' ')' .

    $výchozí  reduce using rule 122 (pseudocall)


State 316

  277 expr_or_type_list: expr_or_type .

    $výchozí  reduce using rule 277 (expr_or_type_list)


State 317

  123 pseudocall: pexpr '(' expr_or_type_list . ocomma ')'
  124           | pexpr '(' expr_or_type_list . LDDD ocomma ')'
  278 expr_or_type_list: expr_or_type_list . ',' expr_or_type
  287 ocomma: . %empty  [')']
  288       | . ','

    LDDD  posunout a přejít do stavu 427
    ','   posunout a přejít do stavu 428

    $výchozí  reduce using rule 287 (ocomma)

    ocomma  přejít do stavu 429


State 318

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  128               | pexpr '.' '(' . expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  129               | pexpr '.' '(' . LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  147 expr_or_type: . expr
  148             | . non_expr_type
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  171 non_expr_type: . recvchantype
  172              | . fntype
  173              | . othertype
  174              | . '*' non_expr_type
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LTYPE       posunout a přejít do stavu 430
    LCOMM       posunout a přejít do stavu 159
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 160
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 161
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    expr_or_type      přejít do stavu 431
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    non_expr_type     přejít do stavu 163
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 164
    recvchantype      přejít do stavu 165
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 166
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 319

  127 pexpr_no_paren: pexpr '.' sym .

    $výchozí  reduce using rule 127 (pexpr_no_paren)


State 320

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  130 pexpr_no_paren: pexpr '[' expr . ']'
  290 oexpr: expr .  [':']

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198
    ']'      posunout a přejít do stavu 432

    $výchozí  reduce using rule 290 (oexpr)


State 321

  131 pexpr_no_paren: pexpr '[' oexpr . ':' oexpr ']'
  132               | pexpr '[' oexpr . ':' oexpr ':' oexpr ']'

    ':'  posunout a přejít do stavu 433


State 322

   28 common_dcl: . LVAR vardcl
   29           | . LVAR '(' vardcl_list osemi ')'
   30           | . LVAR '(' ')'
   31           | . lconst constdcl
   32           | . lconst '(' constdcl osemi ')'
   33           | . lconst '(' constdcl ';' constdcl_list osemi ')'
   34           | . lconst '(' ')'
   35           | . LTYPE typedcl
   36           | . LTYPE '(' typedcl_list osemi ')'
   37           | . LTYPE '(' ')'
   38 lconst: . LCONST
   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   60 compound_stmt: . '{' $@3 stmt_list '}'
   74 for_stmt: . LFOR $@6 for_body
   80 if_stmt: . LIF $@7 if_header $@8 loop_body $@9 elseif_list else
   89 switch_stmt: . LSWITCH $@11 if_header $@12 LBODY caseblock_list '}'
   91 select_stmt: . LSELECT $@13 LBODY caseblock_list '}'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  162 labelname: . new_name
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  250 stmt: . %empty  [LCASE, LDEFAULT, ';', '}']
  251     | . compound_stmt
  252     | . common_dcl
  253     | . non_dcl_stmt
  254     | . error
  255 non_dcl_stmt: . simple_stmt
  256             | . for_stmt
  257             | . switch_stmt
  258             | . select_stmt
  259             | . if_stmt
  261             | . labelname ':' $@14 stmt
  261             | labelname ':' $@14 . stmt
  262             | . LFALL
  263             | . LBREAK onew_name
  264             | . LCONTINUE onew_name
  265             | . LGO pseudocall
  266             | . LDEFER pseudocall
  267             | . LGOTO new_name
  268             | . LRETURN oexpr_list
  275 expr_list: . expr
  276          | . expr_list ',' expr

    error       posunout a přejít do stavu 325
    LLITERAL    posunout a přejít do stavu 35
    LBREAK      posunout a přejít do stavu 36
    LCHAN       posunout a přejít do stavu 37
    LCONST      posunout a přejít do stavu 38
    LCONTINUE   posunout a přejít do stavu 39
    LDEFER      posunout a přejít do stavu 40
    LFALL       posunout a přejít do stavu 41
    LFOR        posunout a přejít do stavu 42
    LFUNC       posunout a přejít do stavu 113
    LGO         posunout a přejít do stavu 44
    LGOTO       posunout a přejít do stavu 45
    LIF         posunout a přejít do stavu 46
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRETURN     posunout a přejít do stavu 49
    LSELECT     posunout a přejít do stavu 50
    LSTRUCT     posunout a přejít do stavu 51
    LSWITCH     posunout a přejít do stavu 52
    LTYPE       posunout a přejít do stavu 53
    LVAR        posunout a přejít do stavu 54
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 326
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    LCASE     reduce using rule 250 (stmt)
    LDEFAULT  reduce using rule 250 (stmt)
    ';'       reduce using rule 250 (stmt)
    '}'       reduce using rule 250 (stmt)

    common_dcl        přejít do stavu 327
    lconst            přejít do stavu 67
    simple_stmt       přejít do stavu 68
    compound_stmt     přejít do stavu 328
    for_stmt          přejít do stavu 69
    if_stmt           přejít do stavu 70
    switch_stmt       přejít do stavu 71
    select_stmt       přejít do stavu 72
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    new_name          přejít do stavu 78
    sym               přejít do stavu 79
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    labelname         přejít do stavu 81
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    stmt              přejít do stavu 434
    non_dcl_stmt      přejít do stavu 330
    expr_list         přejít do stavu 92


State 323

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  134 pexpr_no_paren: convtype '(' expr . ocomma ')'
  287 ocomma: . %empty  [')']
  288       | . ','

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198
    ','      posunout a přejít do stavu 435

    $výchozí  reduce using rule 287 (ocomma)

    ocomma  přejít do stavu 436


State 324

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  135               | comptype lbrace start_complit . braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  140 keyval: . expr ':' complitexpr
  141 bare_complitexpr: . expr
  142                 | . '{' start_complit braced_keyval_list '}'
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  279 keyval_list: . keyval
  280            | . bare_complitexpr
  281            | . keyval_list ',' keyval
  282            | . keyval_list ',' bare_complitexpr
  283 braced_keyval_list: . %empty  ['}']
  284                   | . keyval_list ocomma

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 421
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 283 (braced_keyval_list)

    expr                přejít do stavu 422
    uexpr               přejít do stavu 74
    pseudocall          přejít do stavu 75
    pexpr_no_paren      přejít do stavu 76
    keyval              přejít do stavu 423
    bare_complitexpr    přejít do stavu 424
    pexpr               přejít do stavu 77
    sym                 přejít do stavu 117
    hidden_importsym    přejít do stavu 13
    name                přejít do stavu 80
    convtype            přejít do stavu 82
    comptype            přejít do stavu 83
    othertype           přejít do stavu 84
    structtype          přejít do stavu 85
    interfacetype       přejít do stavu 86
    fntype              přejít do stavu 88
    fnlitdcl            přejít do stavu 89
    fnliteral           přejít do stavu 90
    keyval_list         přejít do stavu 425
    braced_keyval_list  přejít do stavu 437


State 325

  254 stmt: error .

    $výchozí  reduce using rule 254 (stmt)


State 326

   59 $@3: . %empty
   60 compound_stmt: '{' . $@3 stmt_list '}'

    $výchozí  reduce using rule 59 ($@3)

    $@3  přejít do stavu 438


State 327

  252 stmt: common_dcl .

    $výchozí  reduce using rule 252 (stmt)


State 328

  251 stmt: compound_stmt .

    $výchozí  reduce using rule 251 (stmt)


State 329

  269 stmt_list: stmt .

    $výchozí  reduce using rule 269 (stmt_list)


State 330

  253 stmt: non_dcl_stmt .

    $výchozí  reduce using rule 253 (stmt)


State 331

  215 fnliteral: fnlitdcl lbrace stmt_list . '}'
  270 stmt_list: stmt_list . ';' stmt

    ';'  posunout a přejít do stavu 439
    '}'  posunout a přejít do stavu 440


State 332

   52 simple_stmt: expr_list LCOLAS expr_list .  [LCASE, LDEFAULT, LBODY, ';', '}']
  276 expr_list: expr_list . ',' expr

    ','  posunout a přejít do stavu 210

    $výchozí  reduce using rule 52 (simple_stmt)


State 333

   51 simple_stmt: expr_list '=' expr_list .  [LCASE, LDEFAULT, LBODY, ';', '}']
  276 expr_list: expr_list . ',' expr

    ','  posunout a přejít do stavu 210

    $výchozí  reduce using rule 51 (simple_stmt)


State 334

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  276 expr_list: expr_list ',' expr .  [LCOLAS, LCASE, LDEFAULT, LBODY, ')', ';', '=', '}', ',']

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 276 (expr_list)


State 335

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  314 hidden_type_non_recv_chan: . hidden_type_misc
  315                          | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  324                 | LCHAN . hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  325                 | LCHAN . '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  326                 | LCHAN . LCOMM hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 441
    '*'         posunout a přejít do stavu 342
    '('         posunout a přejít do stavu 442
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym           přejít do stavu 345
    hidden_type_non_recv_chan  přejít do stavu 443
    hidden_type_misc           přejít do stavu 444
    hidden_type_func           přejít do stavu 445


State 336

  328 hidden_type_func: LFUNC . '(' ohidden_funarg_list ')' ohidden_funres

    '('  posunout a přejít do stavu 446


State 337

  322 hidden_type_misc: LINTERFACE . '{' ohidden_interfacedcl_list '}'

    '{'  posunout a přejít do stavu 447


State 338

  320 hidden_type_misc: LMAP . '[' hidden_type ']' hidden_type

    '['  posunout a přejít do stavu 448


State 339

  317 hidden_type_misc: LNAME .

    $výchozí  reduce using rule 317 (hidden_type_misc)


State 340

  321 hidden_type_misc: LSTRUCT . '{' ohidden_structdcl_list '}'

    '{'  posunout a přejít do stavu 449


State 341

  327 hidden_type_recv_chan: LCOMM . LCHAN hidden_type

    LCHAN  posunout a přejít do stavu 450


State 342

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  323                 | '*' . hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 451
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 343

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  305 hidden_import: LCONST hidden_pkg_importsym '=' . hidden_constant ';'
  338 hidden_literal: . LLITERAL
  339               | . '-' LLITERAL
  340               | . sym
  341 hidden_constant: . hidden_literal
  342                | . '(' hidden_literal '+' hidden_literal ')'

    LLITERAL  posunout a přejít do stavu 452
    LNAME     posunout a přejít do stavu 9
    '-'       posunout a přejít do stavu 453
    '('       posunout a přejít do stavu 454
    '?'       posunout a přejít do stavu 10
    '@'       posunout a přejít do stavu 11

    sym               přejít do stavu 455
    hidden_importsym  přejít do stavu 13
    hidden_literal    přejít do stavu 456
    hidden_constant   přejít do stavu 457


State 344

  318 hidden_type_misc: '[' . ']' hidden_type
  319                 | '[' . LLITERAL ']' hidden_type

    LLITERAL  posunout a přejít do stavu 458
    ']'       posunout a přejít do stavu 459


State 345

  316 hidden_type_misc: hidden_importsym .

    $výchozí  reduce using rule 316 (hidden_type_misc)


State 346

  306 hidden_import: LCONST hidden_pkg_importsym hidden_type . '=' hidden_constant ';'

    '='  posunout a přejít do stavu 460


State 347

  311 hidden_type: hidden_type_misc .

    $výchozí  reduce using rule 311 (hidden_type)


State 348

  312 hidden_type: hidden_type_recv_chan .

    $výchozí  reduce using rule 312 (hidden_type)


State 349

  313 hidden_type: hidden_type_func .

    $výchozí  reduce using rule 313 (hidden_type)


State 350

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres
  329 hidden_funarg: sym . hidden_type oliteral
  330              | sym . LDDD hidden_type oliteral

    LCHAN       posunout a přejít do stavu 335
    LDDD        posunout a přejít do stavu 461
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 462
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 351

  345 hidden_funarg_list: hidden_funarg .

    $výchozí  reduce using rule 345 (hidden_funarg_list)


State 352

  207 hidden_fndcl: '(' hidden_funarg_list . ')' sym '(' ohidden_funarg_list ')' ohidden_funres
  346 hidden_funarg_list: hidden_funarg_list . ',' hidden_funarg

    ')'  posunout a přejít do stavu 463
    ','  posunout a přejít do stavu 464


State 353

  308 hidden_import: LFUNC hidden_fndcl fnbody . ';'

    ';'  posunout a přejít do stavu 465


State 354

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  206 hidden_fndcl: hidden_pkg_importsym '(' . ohidden_funarg_list ')' ohidden_funres
  295 ohidden_funarg_list: . %empty  [')']
  296                    | . hidden_funarg_list
  329 hidden_funarg: . sym hidden_type oliteral
  330              | . sym LDDD hidden_type oliteral
  345 hidden_funarg_list: . hidden_funarg
  346                   | . hidden_funarg_list ',' hidden_funarg

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 295 (ohidden_funarg_list)

    sym                  přejít do stavu 350
    hidden_importsym     přejít do stavu 13
    ohidden_funarg_list  přejít do stavu 466
    hidden_funarg        přejít do stavu 351
    hidden_funarg_list   přejít do stavu 467


State 355

  303 hidden_import: LIMPORT LNAME LLITERAL . ';'

    ';'  posunout a přejít do stavu 468


State 356

  307 hidden_import: LTYPE hidden_pkgtype hidden_type . ';'

    ';'  posunout a přejít do stavu 469


State 357

  304 hidden_import: LVAR hidden_pkg_importsym hidden_type . ';'

    ';'  posunout a přejít do stavu 470


State 358

   14 import_stmt_list: import_stmt_list ';' import_stmt .

    $výchozí  reduce using rule 14 (import_stmt_list)


State 359

    9 import: LIMPORT '(' import_stmt_list osemi ')' .

    $výchozí  reduce using rule 9 (import)


State 360

  208 fntype: LFUNC '(' oarg_type_list_ocomma . ')' fnres

    ')'  posunout a přejít do stavu 471


State 361

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  198             | LCOMM LCHAN . ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 409
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 362

  170 ntype: '(' ntype . ')'

    ')'  posunout a přejít do stavu 472


State 363

  179 non_recvchantype: '(' ntype ')' .

    $výchozí  reduce using rule 179 (non_recvchantype)


State 364

  189 dotname: name '.' sym .

    $výchozí  reduce using rule 189 (dotname)


State 365

   65 $@5: . %empty
   66 loop_body: LBODY . $@5 stmt_list '}'

    $výchozí  reduce using rule 65 ($@5)

    $@5  přejít do stavu 473


State 366

   72 for_body: for_header loop_body .

    $výchozí  reduce using rule 72 (for_body)


State 367

   52 simple_stmt: expr_list LCOLAS . expr_list
   68 range_stmt: expr_list LCOLAS . LRANGE expr
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRANGE      posunout a přejít do stavu 474
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 138
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 332


State 368

   51 simple_stmt: expr_list '=' . expr_list
   67 range_stmt: expr_list '=' . LRANGE expr
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRANGE      posunout a přejít do stavu 475
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 138
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 333


State 369

   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   69 for_header: osimple_stmt ';' . osimple_stmt ';' osimple_stmt
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr
  293 osimple_stmt: . %empty  [';']
  294             | . simple_stmt

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 293 (osimple_stmt)

    simple_stmt       přejít do stavu 236
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 92
    osimple_stmt      přejít do stavu 476


State 370

  164 dotdotdot: LDDD ntype .

    $výchozí  reduce using rule 164 (dotdotdot)


State 371

  243 arg_type: sym name_or_type .

    $výchozí  reduce using rule 243 (arg_type)


State 372

  244 arg_type: sym dotdotdot .

    $výchozí  reduce using rule 244 (arg_type)


State 373

  149 name_or_type: . ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  163 dotdotdot: . LDDD
  164          | . LDDD ntype
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  242 arg_type: . name_or_type
  243         | . sym name_or_type
  244         | . sym dotdotdot
  245         | . dotdotdot
  247 arg_type_list: arg_type_list ',' . arg_type
  288 ocomma: ',' .  [')']

    LCHAN       posunout a přejít do stavu 37
    LDDD        posunout a přejít do stavu 242
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 288 (ocomma)

    name_or_type      přejít do stavu 243
    sym               přejít do stavu 244
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    dotdotdot         přejít do stavu 245
    ntype             přejít do stavu 246
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232
    arg_type          přejít do stavu 477


State 374

  249 oarg_type_list_ocomma: arg_type_list ocomma .

    $výchozí  reduce using rule 249 (oarg_type_list_ocomma)


State 375

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  183 fnret_type: . recvchantype
  184           | . fntype
  185           | . othertype
  186           | . ptrtype
  187           | . dotname
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  205 fndcl: '(' oarg_type_list_ocomma ')' . sym '(' oarg_type_list_ocomma ')' fnres
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  208       | LFUNC '(' oarg_type_list_ocomma ')' . fnres
  211 fnres: . %empty  [error, LBODY, '{']
  212      | . fnret_type
  213      | . '(' oarg_type_list_ocomma ')'

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 478
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 211 (fnres)

    sym               přejít do stavu 479
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    fnret_type        přejít do stavu 480
    dotname           přejít do stavu 481
    othertype         přejít do stavu 482
    ptrtype           přejít do stavu 483
    recvchantype      přejít do stavu 484
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 485
    fnres             přejít do stavu 486

    Conflict between rule 211 and token '(' resolved as shift (NotParen < '(').


State 376

  204 fndcl: sym '(' oarg_type_list_ocomma . ')' fnres

    ')'  posunout a přejít do stavu 487


State 377

  210 fnbody: '{' stmt_list . '}'
  270 stmt_list: stmt_list . ';' stmt

    ';'  posunout a přejít do stavu 439
    '}'  posunout a přejít do stavu 488


State 378

   66 loop_body: . LBODY $@5 stmt_list '}'
   80 if_stmt: LIF $@7 if_header $@8 . loop_body $@9 elseif_list else

    LBODY  posunout a přejít do stavu 365

    loop_body  přejít do stavu 489


State 379

   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   76 if_header: osimple_stmt ';' . osimple_stmt
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr
  293 osimple_stmt: . %empty  [LBODY]
  294             | . simple_stmt

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 293 (osimple_stmt)

    simple_stmt       přejít do stavu 236
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 92
    osimple_stmt      přejít do stavu 490


State 380

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  236 packname: LNAME '.' . sym

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    sym               přejít do stavu 491
    hidden_importsym  přejít do stavu 13


State 381

  235 packname: LNAME .  [LLITERAL, ')', ';', '}']
  236         | LNAME . '.' sym

    '.'  posunout a přejít do stavu 380

    $výchozí  reduce using rule 235 (packname)


State 382

  240 interfacedcl: '(' packname . ')'

    ')'  posunout a přejít do stavu 492


State 383

  149 name_or_type: . ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  163 dotdotdot: . LDDD
  164          | . LDDD ntype
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  241 indcl: '(' . oarg_type_list_ocomma ')' fnres
  242 arg_type: . name_or_type
  243         | . sym name_or_type
  244         | . sym dotdotdot
  245         | . dotdotdot
  246 arg_type_list: . arg_type
  247              | . arg_type_list ',' arg_type
  248 oarg_type_list_ocomma: . %empty  [')']
  249                      | . arg_type_list ocomma

    LCHAN       posunout a přejít do stavu 37
    LDDD        posunout a přejít do stavu 242
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 248 (oarg_type_list_ocomma)

    name_or_type           přejít do stavu 243
    sym                    přejít do stavu 244
    hidden_importsym       přejít do stavu 13
    name                   přejít do stavu 118
    dotdotdot              přejít do stavu 245
    ntype                  přejít do stavu 246
    dotname                přejít do stavu 228
    othertype              přejít do stavu 229
    ptrtype                přejít do stavu 230
    recvchantype           přejít do stavu 231
    structtype             přejít do stavu 85
    interfacetype          přejít do stavu 86
    fntype                 přejít do stavu 232
    arg_type               přejít do stavu 247
    arg_type_list          přejít do stavu 248
    oarg_type_list_ocomma  přejít do stavu 493


State 384

  238 interfacedcl: new_name indcl .

    $výchozí  reduce using rule 238 (interfacedcl)


State 385

  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  228 interfacedcl_list: interfacedcl_list ';' . interfacedcl
  235 packname: . LNAME
  236         | . LNAME '.' sym
  238 interfacedcl: . new_name indcl
  239             | . packname
  240             | . '(' packname ')'
  286 osemi: ';' .  ['}']

    LNAME  posunout a přejít do stavu 255
    '('    posunout a přejít do stavu 256
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 286 (osemi)

    new_name          přejít do stavu 258
    sym               přejít do stavu 112
    hidden_importsym  přejít do stavu 13
    packname          přejít do stavu 260
    interfacedcl      přejít do stavu 494


State 386

  201 interfacetype: LINTERFACE lbrace interfacedcl_list osemi . '}'

    '}'  posunout a přejít do stavu 495


State 387

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  194          | LMAP '[' ntype ']' . ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 496
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 388

   55 case: . LCASE expr_or_type_list ':'
   56     | . LCASE expr_or_type_list '=' expr ':'
   57     | . LCASE expr_or_type_list LCOLAS expr ':'
   58     | . LDEFAULT ':'
   62 caseblock: . case $@4 stmt_list
   64 caseblock_list: caseblock_list . caseblock
   91 select_stmt: LSELECT $@13 LBODY caseblock_list . '}'

    LCASE     posunout a přejít do stavu 497
    LDEFAULT  posunout a přejít do stavu 498
    '}'       posunout a přejít do stavu 499

    case       přejít do stavu 500
    caseblock  přejít do stavu 501


State 389

  234 structdcl: '*' '(' . embed ')' oliteral
  235 packname: . LNAME
  236         | . LNAME '.' sym
  237 embed: . packname

    LNAME  posunout a přejít do stavu 381

    packname  přejít do stavu 270
    embed     přejít do stavu 502


State 390

  232 structdcl: '*' embed . oliteral
  301 oliteral: . %empty  [';', '}']
  302         | . LLITERAL

    LLITERAL  posunout a přejít do stavu 395

    $výchozí  reduce using rule 301 (oliteral)

    oliteral  přejít do stavu 503


State 391

  233 structdcl: '(' '*' . embed ')' oliteral
  235 packname: . LNAME
  236         | . LNAME '.' sym
  237 embed: . packname

    LNAME  posunout a přejít do stavu 381

    packname  přejít do stavu 270
    embed     přejít do stavu 504


State 392

  231 structdcl: '(' embed . ')' oliteral

    ')'  posunout a přejít do stavu 505


State 393

  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  226 structdcl_list: structdcl_list ';' . structdcl
  229 structdcl: . new_name_list ntype oliteral
  230          | . embed oliteral
  231          | . '(' embed ')' oliteral
  232          | . '*' embed oliteral
  233          | . '(' '*' embed ')' oliteral
  234          | . '*' '(' embed ')' oliteral
  235 packname: . LNAME
  236         | . LNAME '.' sym
  237 embed: . packname
  271 new_name_list: . new_name
  272              | . new_name_list ',' new_name
  286 osemi: ';' .  ['}']

    LNAME  posunout a přejít do stavu 255
    '*'    posunout a přejít do stavu 264
    '('    posunout a přejít do stavu 265
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 286 (osemi)

    new_name          přejít do stavu 267
    sym               přejít do stavu 112
    hidden_importsym  přejít do stavu 13
    structdcl         přejít do stavu 506
    packname          přejít do stavu 270
    embed             přejít do stavu 271
    new_name_list     přejít do stavu 272


State 394

  199 structtype: LSTRUCT lbrace structdcl_list osemi . '}'

    '}'  posunout a přejít do stavu 507


State 395

  302 oliteral: LLITERAL .

    $výchozí  reduce using rule 302 (oliteral)


State 396

  230 structdcl: embed oliteral .

    $výchozí  reduce using rule 230 (structdcl)


State 397

  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  272 new_name_list: new_name_list ',' . new_name

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    new_name          přejít do stavu 508
    sym               přejít do stavu 112
    hidden_importsym  přejít do stavu 13


State 398

  229 structdcl: new_name_list ntype . oliteral
  301 oliteral: . %empty  [';', '}']
  302         | . LLITERAL

    LLITERAL  posunout a přejít do stavu 395

    $výchozí  reduce using rule 301 (oliteral)

    oliteral  přejít do stavu 509


State 399

   89 switch_stmt: LSWITCH $@11 if_header $@12 . LBODY caseblock_list '}'

    LBODY  posunout a přejít do stavu 510


State 400

   47 typedclname: . sym
   48 typedcl: . typedclname ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  224 typedcl_list: typedcl_list ';' . typedcl
  286 osemi: ';' .  [')']

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 286 (osemi)

    typedclname       přejít do stavu 145
    typedcl           přejít do stavu 511
    sym               přejít do stavu 147
    hidden_importsym  přejít do stavu 13


State 401

   36 common_dcl: LTYPE '(' typedcl_list osemi . ')'

    ')'  posunout a přejít do stavu 512


State 402

   39 vardcl: . dcl_name_list ntype
   40       | . dcl_name_list ntype '=' expr_list
   41       | . dcl_name_list '=' expr_list
  153 dcl_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  220 vardcl_list: vardcl_list ';' . vardcl
  273 dcl_name_list: . dcl_name
  274              | . dcl_name_list ',' dcl_name
  286 osemi: ';' .  [')']

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 286 (osemi)

    vardcl            přejít do stavu 513
    dcl_name          přejít do stavu 150
    sym               přejít do stavu 151
    hidden_importsym  přejít do stavu 13
    dcl_name_list     přejít do stavu 152


State 403

   29 common_dcl: LVAR '(' vardcl_list osemi . ')'

    ')'  posunout a přejít do stavu 514


State 404

   41 vardcl: dcl_name_list '=' expr_list .  [LCASE, LDEFAULT, ')', ';', '}']
  276 expr_list: expr_list . ',' expr

    ','  posunout a přejít do stavu 210

    $výchozí  reduce using rule 41 (vardcl)


State 405

  274 dcl_name_list: dcl_name_list ',' dcl_name .

    $výchozí  reduce using rule 274 (dcl_name_list)


State 406

   40 vardcl: dcl_name_list ntype '=' . expr_list
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 138
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 515


State 407

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  193          | LCHAN LCOMM . ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  198             | LCOMM . LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 284
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 227
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 408

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  170      | '(' . ntype ')'
  179 non_recvchantype: '(' . ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 516
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 409

  198 recvchantype: LCOMM LCHAN ntype .

    $výchozí  reduce using rule 198 (recvchantype)


State 410

  169 ntype: dotname .  [LCOLAS, LDDD, ')', '=', ':', ',']
  178 non_recvchantype: dotname .  [LBODY, '(', '{']

    LBODY       reduce using rule 178 (non_recvchantype)
    '('         reduce using rule 178 (non_recvchantype)
    '{'         reduce using rule 178 (non_recvchantype)
    $výchozí  reduce using rule 169 (ntype)


State 411

  167 ntype: othertype .  [LCOLAS, LDDD, ')', '=', ':', ',']
  176 non_recvchantype: othertype .  [LBODY, '(', '{']

    LBODY       reduce using rule 176 (non_recvchantype)
    '('         reduce using rule 176 (non_recvchantype)
    '{'         reduce using rule 176 (non_recvchantype)
    $výchozí  reduce using rule 167 (ntype)


State 412

  168 ntype: ptrtype .  [LCOLAS, LDDD, ')', '=', ':', ',']
  177 non_recvchantype: ptrtype .  [LBODY, '(', '{']

    LBODY       reduce using rule 177 (non_recvchantype)
    '('         reduce using rule 177 (non_recvchantype)
    '{'         reduce using rule 177 (non_recvchantype)
    $výchozí  reduce using rule 168 (ntype)


State 413

  166 ntype: fntype .  [LCOLAS, LDDD, ')', '=', ':', ',']
  175 non_recvchantype: fntype .  [LBODY, '(', '{']

    LBODY       reduce using rule 175 (non_recvchantype)
    '('         reduce using rule 175 (non_recvchantype)
    '{'         reduce using rule 175 (non_recvchantype)
    $výchozí  reduce using rule 166 (ntype)


State 414

  137 pexpr_no_paren: '(' expr_or_type ')' '{' . start_complit braced_keyval_list '}'
  139 start_complit: . %empty

    $výchozí  reduce using rule 139 (start_complit)

    start_complit  přejít do stavu 517


State 415

  191 othertype: '[' LDDD ']' ntype .

    $výchozí  reduce using rule 191 (othertype)


State 416

  190 othertype: '[' oexpr ']' ntype .

    $výchozí  reduce using rule 190 (othertype)


State 417

   33 common_dcl: lconst '(' constdcl ';' . constdcl_list osemi ')'
   42 constdcl: . dcl_name_list ntype '=' expr_list
   43         | . dcl_name_list '=' expr_list
   44 constdcl1: . constdcl
   45          | . dcl_name_list ntype
   46          | . dcl_name_list
  153 dcl_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  221 constdcl_list: . constdcl1
  222              | . constdcl_list ';' constdcl1
  273 dcl_name_list: . dcl_name
  274              | . dcl_name_list ',' dcl_name
  286 osemi: ';' .  [')']

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 286 (osemi)

    constdcl          přejít do stavu 518
    constdcl1         přejít do stavu 519
    dcl_name          přejít do stavu 150
    sym               přejít do stavu 151
    hidden_importsym  přejít do stavu 13
    constdcl_list     přejít do stavu 520
    dcl_name_list     přejít do stavu 521


State 418

   32 common_dcl: lconst '(' constdcl osemi . ')'

    ')'  posunout a přejít do stavu 522


State 419

   43 constdcl: dcl_name_list '=' expr_list .  [LCASE, LDEFAULT, ')', ';', '}']
  276 expr_list: expr_list . ',' expr

    ','  posunout a přejít do stavu 210

    $výchozí  reduce using rule 43 (constdcl)


State 420

   42 constdcl: dcl_name_list ntype '=' . expr_list
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 138
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 523


State 421

  139 start_complit: . %empty
  142 bare_complitexpr: '{' . start_complit braced_keyval_list '}'

    $výchozí  reduce using rule 139 (start_complit)

    start_complit  přejít do stavu 524


State 422

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  140 keyval: expr . ':' complitexpr
  141 bare_complitexpr: expr .  ['}', ',']

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198
    ':'      posunout a přejít do stavu 525

    $výchozí  reduce using rule 141 (bare_complitexpr)


State 423

  279 keyval_list: keyval .

    $výchozí  reduce using rule 279 (keyval_list)


State 424

  280 keyval_list: bare_complitexpr .

    $výchozí  reduce using rule 280 (keyval_list)


State 425

  281 keyval_list: keyval_list . ',' keyval
  282            | keyval_list . ',' bare_complitexpr
  284 braced_keyval_list: keyval_list . ocomma
  287 ocomma: . %empty  ['}']
  288       | . ','

    ','  posunout a přejít do stavu 526

    $výchozí  reduce using rule 287 (ocomma)

    ocomma  přejít do stavu 527


State 426

  136 pexpr_no_paren: pexpr_no_paren '{' start_complit braced_keyval_list . '}'

    '}'  posunout a přejít do stavu 528


State 427

  124 pseudocall: pexpr '(' expr_or_type_list LDDD . ocomma ')'
  287 ocomma: . %empty  [')']
  288       | . ','

    ','  posunout a přejít do stavu 435

    $výchozí  reduce using rule 287 (ocomma)

    ocomma  přejít do stavu 529


State 428

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  147 expr_or_type: . expr
  148             | . non_expr_type
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  171 non_expr_type: . recvchantype
  172              | . fntype
  173              | . othertype
  174              | . '*' non_expr_type
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  278 expr_or_type_list: expr_or_type_list ',' . expr_or_type
  288 ocomma: ',' .  [')']

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 159
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 160
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 288 (ocomma)

    expr              přejít do stavu 161
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    expr_or_type      přejít do stavu 530
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    non_expr_type     přejít do stavu 163
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 164
    recvchantype      přejít do stavu 165
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 166
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 429

  123 pseudocall: pexpr '(' expr_or_type_list ocomma . ')'

    ')'  posunout a přejít do stavu 531


State 430

  129 pexpr_no_paren: pexpr '.' '(' LTYPE . ')'

    ')'  posunout a přejít do stavu 532


State 431

  128 pexpr_no_paren: pexpr '.' '(' expr_or_type . ')'

    ')'  posunout a přejít do stavu 533


State 432

  130 pexpr_no_paren: pexpr '[' expr ']' .

    $výchozí  reduce using rule 130 (pexpr_no_paren)


State 433

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  131               | pexpr '[' oexpr ':' . oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  132               | pexpr '[' oexpr ':' . oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  289 oexpr: . %empty  [':', ']']
  290      | . expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 289 (oexpr)

    expr              přejít do stavu 170
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    oexpr             přejít do stavu 534


State 434

  261 non_dcl_stmt: labelname ':' $@14 stmt .

    $výchozí  reduce using rule 261 (non_dcl_stmt)


State 435

  288 ocomma: ',' .

    $výchozí  reduce using rule 288 (ocomma)


State 436

  134 pexpr_no_paren: convtype '(' expr ocomma . ')'

    ')'  posunout a přejít do stavu 535


State 437

  135 pexpr_no_paren: comptype lbrace start_complit braced_keyval_list . '}'

    '}'  posunout a přejít do stavu 536


State 438

   28 common_dcl: . LVAR vardcl
   29           | . LVAR '(' vardcl_list osemi ')'
   30           | . LVAR '(' ')'
   31           | . lconst constdcl
   32           | . lconst '(' constdcl osemi ')'
   33           | . lconst '(' constdcl ';' constdcl_list osemi ')'
   34           | . lconst '(' ')'
   35           | . LTYPE typedcl
   36           | . LTYPE '(' typedcl_list osemi ')'
   37           | . LTYPE '(' ')'
   38 lconst: . LCONST
   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   60 compound_stmt: . '{' $@3 stmt_list '}'
   60              | '{' $@3 . stmt_list '}'
   74 for_stmt: . LFOR $@6 for_body
   80 if_stmt: . LIF $@7 if_header $@8 loop_body $@9 elseif_list else
   89 switch_stmt: . LSWITCH $@11 if_header $@12 LBODY caseblock_list '}'
   91 select_stmt: . LSELECT $@13 LBODY caseblock_list '}'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  162 labelname: . new_name
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  250 stmt: . %empty  [';', '}']
  251     | . compound_stmt
  252     | . common_dcl
  253     | . non_dcl_stmt
  254     | . error
  255 non_dcl_stmt: . simple_stmt
  256             | . for_stmt
  257             | . switch_stmt
  258             | . select_stmt
  259             | . if_stmt
  261             | . labelname ':' $@14 stmt
  262             | . LFALL
  263             | . LBREAK onew_name
  264             | . LCONTINUE onew_name
  265             | . LGO pseudocall
  266             | . LDEFER pseudocall
  267             | . LGOTO new_name
  268             | . LRETURN oexpr_list
  269 stmt_list: . stmt
  270          | . stmt_list ';' stmt
  275 expr_list: . expr
  276          | . expr_list ',' expr

    error       posunout a přejít do stavu 325
    LLITERAL    posunout a přejít do stavu 35
    LBREAK      posunout a přejít do stavu 36
    LCHAN       posunout a přejít do stavu 37
    LCONST      posunout a přejít do stavu 38
    LCONTINUE   posunout a přejít do stavu 39
    LDEFER      posunout a přejít do stavu 40
    LFALL       posunout a přejít do stavu 41
    LFOR        posunout a přejít do stavu 42
    LFUNC       posunout a přejít do stavu 113
    LGO         posunout a přejít do stavu 44
    LGOTO       posunout a přejít do stavu 45
    LIF         posunout a přejít do stavu 46
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRETURN     posunout a přejít do stavu 49
    LSELECT     posunout a přejít do stavu 50
    LSTRUCT     posunout a přejít do stavu 51
    LSWITCH     posunout a přejít do stavu 52
    LTYPE       posunout a přejít do stavu 53
    LVAR        posunout a přejít do stavu 54
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 326
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    ';'  reduce using rule 250 (stmt)
    '}'  reduce using rule 250 (stmt)

    common_dcl        přejít do stavu 327
    lconst            přejít do stavu 67
    simple_stmt       přejít do stavu 68
    compound_stmt     přejít do stavu 328
    for_stmt          přejít do stavu 69
    if_stmt           přejít do stavu 70
    switch_stmt       přejít do stavu 71
    select_stmt       přejít do stavu 72
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    new_name          přejít do stavu 78
    sym               přejít do stavu 79
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    labelname         přejít do stavu 81
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    stmt              přejít do stavu 329
    non_dcl_stmt      přejít do stavu 330
    stmt_list         přejít do stavu 537
    expr_list         přejít do stavu 92


State 439

   28 common_dcl: . LVAR vardcl
   29           | . LVAR '(' vardcl_list osemi ')'
   30           | . LVAR '(' ')'
   31           | . lconst constdcl
   32           | . lconst '(' constdcl osemi ')'
   33           | . lconst '(' constdcl ';' constdcl_list osemi ')'
   34           | . lconst '(' ')'
   35           | . LTYPE typedcl
   36           | . LTYPE '(' typedcl_list osemi ')'
   37           | . LTYPE '(' ')'
   38 lconst: . LCONST
   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   60 compound_stmt: . '{' $@3 stmt_list '}'
   74 for_stmt: . LFOR $@6 for_body
   80 if_stmt: . LIF $@7 if_header $@8 loop_body $@9 elseif_list else
   89 switch_stmt: . LSWITCH $@11 if_header $@12 LBODY caseblock_list '}'
   91 select_stmt: . LSELECT $@13 LBODY caseblock_list '}'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  162 labelname: . new_name
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  250 stmt: . %empty  [LCASE, LDEFAULT, ';', '}']
  251     | . compound_stmt
  252     | . common_dcl
  253     | . non_dcl_stmt
  254     | . error
  255 non_dcl_stmt: . simple_stmt
  256             | . for_stmt
  257             | . switch_stmt
  258             | . select_stmt
  259             | . if_stmt
  261             | . labelname ':' $@14 stmt
  262             | . LFALL
  263             | . LBREAK onew_name
  264             | . LCONTINUE onew_name
  265             | . LGO pseudocall
  266             | . LDEFER pseudocall
  267             | . LGOTO new_name
  268             | . LRETURN oexpr_list
  270 stmt_list: stmt_list ';' . stmt
  275 expr_list: . expr
  276          | . expr_list ',' expr

    error       posunout a přejít do stavu 325
    LLITERAL    posunout a přejít do stavu 35
    LBREAK      posunout a přejít do stavu 36
    LCHAN       posunout a přejít do stavu 37
    LCONST      posunout a přejít do stavu 38
    LCONTINUE   posunout a přejít do stavu 39
    LDEFER      posunout a přejít do stavu 40
    LFALL       posunout a přejít do stavu 41
    LFOR        posunout a přejít do stavu 42
    LFUNC       posunout a přejít do stavu 113
    LGO         posunout a přejít do stavu 44
    LGOTO       posunout a přejít do stavu 45
    LIF         posunout a přejít do stavu 46
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRETURN     posunout a přejít do stavu 49
    LSELECT     posunout a přejít do stavu 50
    LSTRUCT     posunout a přejít do stavu 51
    LSWITCH     posunout a přejít do stavu 52
    LTYPE       posunout a přejít do stavu 53
    LVAR        posunout a přejít do stavu 54
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 326
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    LCASE     reduce using rule 250 (stmt)
    LDEFAULT  reduce using rule 250 (stmt)
    ';'       reduce using rule 250 (stmt)
    '}'       reduce using rule 250 (stmt)

    common_dcl        přejít do stavu 327
    lconst            přejít do stavu 67
    simple_stmt       přejít do stavu 68
    compound_stmt     přejít do stavu 328
    for_stmt          přejít do stavu 69
    if_stmt           přejít do stavu 70
    switch_stmt       přejít do stavu 71
    select_stmt       přejít do stavu 72
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    new_name          přejít do stavu 78
    sym               přejít do stavu 79
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    labelname         přejít do stavu 81
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    stmt              přejít do stavu 538
    non_dcl_stmt      přejít do stavu 330
    expr_list         přejít do stavu 92


State 440

  215 fnliteral: fnlitdcl lbrace stmt_list '}' .

    $výchozí  reduce using rule 215 (fnliteral)


State 441

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  326                 | LCHAN LCOMM . hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 539
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 442

  325 hidden_type_misc: LCHAN '(' . hidden_type_recv_chan ')'
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type

    LCOMM  posunout a přejít do stavu 341

    hidden_type_recv_chan  přejít do stavu 540


State 443

  324 hidden_type_misc: LCHAN hidden_type_non_recv_chan .

    $výchozí  reduce using rule 324 (hidden_type_misc)


State 444

  314 hidden_type_non_recv_chan: hidden_type_misc .

    $výchozí  reduce using rule 314 (hidden_type_non_recv_chan)


State 445

  315 hidden_type_non_recv_chan: hidden_type_func .

    $výchozí  reduce using rule 315 (hidden_type_non_recv_chan)


State 446

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  295 ohidden_funarg_list: . %empty  [')']
  296                    | . hidden_funarg_list
  328 hidden_type_func: LFUNC '(' . ohidden_funarg_list ')' ohidden_funres
  329 hidden_funarg: . sym hidden_type oliteral
  330              | . sym LDDD hidden_type oliteral
  345 hidden_funarg_list: . hidden_funarg
  346                   | . hidden_funarg_list ',' hidden_funarg

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 295 (ohidden_funarg_list)

    sym                  přejít do stavu 350
    hidden_importsym     přejít do stavu 13
    ohidden_funarg_list  přejít do stavu 541
    hidden_funarg        přejít do stavu 351
    hidden_funarg_list   přejít do stavu 467


State 447

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  299 ohidden_interfacedcl_list: . %empty  ['}']
  300                          | . hidden_interfacedcl_list
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  322                 | LINTERFACE '{' . ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres
  332 hidden_interfacedcl: . sym '(' ohidden_funarg_list ')' ohidden_funres
  333                    | . hidden_type
  349 hidden_interfacedcl_list: . hidden_interfacedcl
  350                         | . hidden_interfacedcl_list ';' hidden_interfacedcl

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 542
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 299 (ohidden_interfacedcl_list)

    sym                        přejít do stavu 543
    hidden_importsym           přejít do stavu 544
    ohidden_interfacedcl_list  přejít do stavu 545
    hidden_type                přejít do stavu 546
    hidden_type_misc           přejít do stavu 347
    hidden_type_recv_chan      přejít do stavu 348
    hidden_type_func           přejít do stavu 349
    hidden_interfacedcl        přejít do stavu 547
    hidden_interfacedcl_list   přejít do stavu 548


State 448

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  320                 | LMAP '[' . hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 549
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 449

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  297 ohidden_structdcl_list: . %empty  ['}']
  298                       | . hidden_structdcl_list
  321 hidden_type_misc: LSTRUCT '{' . ohidden_structdcl_list '}'
  331 hidden_structdcl: . sym hidden_type oliteral
  347 hidden_structdcl_list: . hidden_structdcl
  348                      | . hidden_structdcl_list ';' hidden_structdcl

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 297 (ohidden_structdcl_list)

    sym                     přejít do stavu 550
    hidden_importsym        přejít do stavu 13
    ohidden_structdcl_list  přejít do stavu 551
    hidden_structdcl        přejít do stavu 552
    hidden_structdcl_list   přejít do stavu 553


State 450

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  327                      | LCOMM LCHAN . hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 554
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 451

  323 hidden_type_misc: '*' hidden_type .

    $výchozí  reduce using rule 323 (hidden_type_misc)


State 452

  338 hidden_literal: LLITERAL .

    $výchozí  reduce using rule 338 (hidden_literal)


State 453

  339 hidden_literal: '-' . LLITERAL

    LLITERAL  posunout a přejít do stavu 555


State 454

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  338 hidden_literal: . LLITERAL
  339               | . '-' LLITERAL
  340               | . sym
  342 hidden_constant: '(' . hidden_literal '+' hidden_literal ')'

    LLITERAL  posunout a přejít do stavu 452
    LNAME     posunout a přejít do stavu 9
    '-'       posunout a přejít do stavu 453
    '?'       posunout a přejít do stavu 10
    '@'       posunout a přejít do stavu 11

    sym               přejít do stavu 455
    hidden_importsym  přejít do stavu 13
    hidden_literal    přejít do stavu 556


State 455

  340 hidden_literal: sym .

    $výchozí  reduce using rule 340 (hidden_literal)


State 456

  341 hidden_constant: hidden_literal .

    $výchozí  reduce using rule 341 (hidden_constant)


State 457

  305 hidden_import: LCONST hidden_pkg_importsym '=' hidden_constant . ';'

    ';'  posunout a přejít do stavu 557


State 458

  319 hidden_type_misc: '[' LLITERAL . ']' hidden_type

    ']'  posunout a přejít do stavu 558


State 459

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  318                 | '[' ']' . hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 559
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 460

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  306 hidden_import: LCONST hidden_pkg_importsym hidden_type '=' . hidden_constant ';'
  338 hidden_literal: . LLITERAL
  339               | . '-' LLITERAL
  340               | . sym
  341 hidden_constant: . hidden_literal
  342                | . '(' hidden_literal '+' hidden_literal ')'

    LLITERAL  posunout a přejít do stavu 452
    LNAME     posunout a přejít do stavu 9
    '-'       posunout a přejít do stavu 453
    '('       posunout a přejít do stavu 454
    '?'       posunout a přejít do stavu 10
    '@'       posunout a přejít do stavu 11

    sym               přejít do stavu 455
    hidden_importsym  přejít do stavu 13
    hidden_literal    přejít do stavu 456
    hidden_constant   přejít do stavu 560


State 461

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres
  330 hidden_funarg: sym LDDD . hidden_type oliteral

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 561
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 462

  301 oliteral: . %empty  [')', ',']
  302         | . LLITERAL
  329 hidden_funarg: sym hidden_type . oliteral

    LLITERAL  posunout a přejít do stavu 395

    $výchozí  reduce using rule 301 (oliteral)

    oliteral  přejít do stavu 562


State 463

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  207 hidden_fndcl: '(' hidden_funarg_list ')' . sym '(' ohidden_funarg_list ')' ohidden_funres

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    sym               přejít do stavu 563
    hidden_importsym  přejít do stavu 13


State 464

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  329 hidden_funarg: . sym hidden_type oliteral
  330              | . sym LDDD hidden_type oliteral
  346 hidden_funarg_list: hidden_funarg_list ',' . hidden_funarg

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    sym               přejít do stavu 350
    hidden_importsym  přejít do stavu 13
    hidden_funarg     přejít do stavu 564


State 465

  308 hidden_import: LFUNC hidden_fndcl fnbody ';' .

    $výchozí  reduce using rule 308 (hidden_import)


State 466

  206 hidden_fndcl: hidden_pkg_importsym '(' ohidden_funarg_list . ')' ohidden_funres

    ')'  posunout a přejít do stavu 565


State 467

  296 ohidden_funarg_list: hidden_funarg_list .  [')']
  346 hidden_funarg_list: hidden_funarg_list . ',' hidden_funarg

    ','  posunout a přejít do stavu 464

    $výchozí  reduce using rule 296 (ohidden_funarg_list)


State 468

  303 hidden_import: LIMPORT LNAME LLITERAL ';' .

    $výchozí  reduce using rule 303 (hidden_import)


State 469

  307 hidden_import: LTYPE hidden_pkgtype hidden_type ';' .

    $výchozí  reduce using rule 307 (hidden_import)


State 470

  304 hidden_import: LVAR hidden_pkg_importsym hidden_type ';' .

    $výchozí  reduce using rule 304 (hidden_import)


State 471

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  183 fnret_type: . recvchantype
  184           | . fntype
  185           | . othertype
  186           | . ptrtype
  187           | . dotname
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  208       | LFUNC '(' oarg_type_list_ocomma ')' . fnres
  211 fnres: . %empty  [error, LLITERAL, LCOLAS, LCASE, LDDD, LDEFAULT, LBODY, ')', ';', '=', ':', '{', '}', ']', ',']
  212      | . fnret_type
  213      | . '(' oarg_type_list_ocomma ')'

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 478
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 211 (fnres)

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    fnret_type        přejít do stavu 480
    dotname           přejít do stavu 481
    othertype         přejít do stavu 482
    ptrtype           přejít do stavu 483
    recvchantype      přejít do stavu 484
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 485
    fnres             přejít do stavu 486

    Conflict between rule 211 and token '(' resolved as shift (NotParen < '(').


State 472

  170 ntype: '(' ntype ')' .

    $výchozí  reduce using rule 170 (ntype)


State 473

   28 common_dcl: . LVAR vardcl
   29           | . LVAR '(' vardcl_list osemi ')'
   30           | . LVAR '(' ')'
   31           | . lconst constdcl
   32           | . lconst '(' constdcl osemi ')'
   33           | . lconst '(' constdcl ';' constdcl_list osemi ')'
   34           | . lconst '(' ')'
   35           | . LTYPE typedcl
   36           | . LTYPE '(' typedcl_list osemi ')'
   37           | . LTYPE '(' ')'
   38 lconst: . LCONST
   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   60 compound_stmt: . '{' $@3 stmt_list '}'
   66 loop_body: LBODY $@5 . stmt_list '}'
   74 for_stmt: . LFOR $@6 for_body
   80 if_stmt: . LIF $@7 if_header $@8 loop_body $@9 elseif_list else
   89 switch_stmt: . LSWITCH $@11 if_header $@12 LBODY caseblock_list '}'
   91 select_stmt: . LSELECT $@13 LBODY caseblock_list '}'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  162 labelname: . new_name
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  250 stmt: . %empty  [';', '}']
  251     | . compound_stmt
  252     | . common_dcl
  253     | . non_dcl_stmt
  254     | . error
  255 non_dcl_stmt: . simple_stmt
  256             | . for_stmt
  257             | . switch_stmt
  258             | . select_stmt
  259             | . if_stmt
  261             | . labelname ':' $@14 stmt
  262             | . LFALL
  263             | . LBREAK onew_name
  264             | . LCONTINUE onew_name
  265             | . LGO pseudocall
  266             | . LDEFER pseudocall
  267             | . LGOTO new_name
  268             | . LRETURN oexpr_list
  269 stmt_list: . stmt
  270          | . stmt_list ';' stmt
  275 expr_list: . expr
  276          | . expr_list ',' expr

    error       posunout a přejít do stavu 325
    LLITERAL    posunout a přejít do stavu 35
    LBREAK      posunout a přejít do stavu 36
    LCHAN       posunout a přejít do stavu 37
    LCONST      posunout a přejít do stavu 38
    LCONTINUE   posunout a přejít do stavu 39
    LDEFER      posunout a přejít do stavu 40
    LFALL       posunout a přejít do stavu 41
    LFOR        posunout a přejít do stavu 42
    LFUNC       posunout a přejít do stavu 113
    LGO         posunout a přejít do stavu 44
    LGOTO       posunout a přejít do stavu 45
    LIF         posunout a přejít do stavu 46
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRETURN     posunout a přejít do stavu 49
    LSELECT     posunout a přejít do stavu 50
    LSTRUCT     posunout a přejít do stavu 51
    LSWITCH     posunout a přejít do stavu 52
    LTYPE       posunout a přejít do stavu 53
    LVAR        posunout a přejít do stavu 54
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 326
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    ';'  reduce using rule 250 (stmt)
    '}'  reduce using rule 250 (stmt)

    common_dcl        přejít do stavu 327
    lconst            přejít do stavu 67
    simple_stmt       přejít do stavu 68
    compound_stmt     přejít do stavu 328
    for_stmt          přejít do stavu 69
    if_stmt           přejít do stavu 70
    switch_stmt       přejít do stavu 71
    select_stmt       přejít do stavu 72
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    new_name          přejít do stavu 78
    sym               přejít do stavu 79
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    labelname         přejít do stavu 81
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    stmt              přejít do stavu 329
    non_dcl_stmt      přejít do stavu 330
    stmt_list         přejít do stavu 566
    expr_list         přejít do stavu 92


State 474

   68 range_stmt: expr_list LCOLAS LRANGE . expr
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 567
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 475

   67 range_stmt: expr_list '=' LRANGE . expr
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 568
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 476

   69 for_header: osimple_stmt ';' osimple_stmt . ';' osimple_stmt

    ';'  posunout a přejít do stavu 569


State 477

  247 arg_type_list: arg_type_list ',' arg_type .

    $výchozí  reduce using rule 247 (arg_type_list)


State 478

  149 name_or_type: . ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  163 dotdotdot: . LDDD
  164          | . LDDD ntype
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  213 fnres: '(' . oarg_type_list_ocomma ')'
  242 arg_type: . name_or_type
  243         | . sym name_or_type
  244         | . sym dotdotdot
  245         | . dotdotdot
  246 arg_type_list: . arg_type
  247              | . arg_type_list ',' arg_type
  248 oarg_type_list_ocomma: . %empty  [')']
  249                      | . arg_type_list ocomma

    LCHAN       posunout a přejít do stavu 37
    LDDD        posunout a přejít do stavu 242
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 248 (oarg_type_list_ocomma)

    name_or_type           přejít do stavu 243
    sym                    přejít do stavu 244
    hidden_importsym       přejít do stavu 13
    name                   přejít do stavu 118
    dotdotdot              přejít do stavu 245
    ntype                  přejít do stavu 246
    dotname                přejít do stavu 228
    othertype              přejít do stavu 229
    ptrtype                přejít do stavu 230
    recvchantype           přejít do stavu 231
    structtype             přejít do stavu 85
    interfacetype          přejít do stavu 86
    fntype                 přejít do stavu 232
    arg_type               přejít do stavu 247
    arg_type_list          přejít do stavu 248
    oarg_type_list_ocomma  přejít do stavu 570


State 479

  161 name: sym .  [error, LBODY, '.', '{']
  205 fndcl: '(' oarg_type_list_ocomma ')' sym . '(' oarg_type_list_ocomma ')' fnres

    '('  posunout a přejít do stavu 571

    $výchozí  reduce using rule 161 (name)

    Conflict between rule 161 and token '(' resolved as shift (NotParen < '(').


State 480

  212 fnres: fnret_type .

    $výchozí  reduce using rule 212 (fnres)


State 481

  187 fnret_type: dotname .

    $výchozí  reduce using rule 187 (fnret_type)


State 482

  185 fnret_type: othertype .

    $výchozí  reduce using rule 185 (fnret_type)


State 483

  186 fnret_type: ptrtype .

    $výchozí  reduce using rule 186 (fnret_type)


State 484

  183 fnret_type: recvchantype .

    $výchozí  reduce using rule 183 (fnret_type)


State 485

  184 fnret_type: fntype .

    $výchozí  reduce using rule 184 (fnret_type)


State 486

  208 fntype: LFUNC '(' oarg_type_list_ocomma ')' fnres .

    $výchozí  reduce using rule 208 (fntype)


State 487

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  183 fnret_type: . recvchantype
  184           | . fntype
  185           | . othertype
  186           | . ptrtype
  187           | . dotname
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  204 fndcl: sym '(' oarg_type_list_ocomma ')' . fnres
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  211 fnres: . %empty  [';', '{']
  212      | . fnret_type
  213      | . '(' oarg_type_list_ocomma ')'

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 478
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 211 (fnres)

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    fnret_type        přejít do stavu 480
    dotname           přejít do stavu 481
    othertype         přejít do stavu 482
    ptrtype           přejít do stavu 483
    recvchantype      přejít do stavu 484
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 485
    fnres             přejít do stavu 572


State 488

  210 fnbody: '{' stmt_list '}' .

    $výchozí  reduce using rule 210 (fnbody)


State 489

   79 $@9: . %empty
   80 if_stmt: LIF $@7 if_header $@8 loop_body . $@9 elseif_list else

    $výchozí  reduce using rule 79 ($@9)

    $@9  přejít do stavu 573


State 490

   76 if_header: osimple_stmt ';' osimple_stmt .

    $výchozí  reduce using rule 76 (if_header)


State 491

  236 packname: LNAME '.' sym .

    $výchozí  reduce using rule 236 (packname)


State 492

  240 interfacedcl: '(' packname ')' .

    $výchozí  reduce using rule 240 (interfacedcl)


State 493

  241 indcl: '(' oarg_type_list_ocomma . ')' fnres

    ')'  posunout a přejít do stavu 574


State 494

  228 interfacedcl_list: interfacedcl_list ';' interfacedcl .

    $výchozí  reduce using rule 228 (interfacedcl_list)


State 495

  201 interfacetype: LINTERFACE lbrace interfacedcl_list osemi '}' .

    $výchozí  reduce using rule 201 (interfacetype)


State 496

  194 othertype: LMAP '[' ntype ']' ntype .

    $výchozí  reduce using rule 194 (othertype)


State 497

   55 case: LCASE . expr_or_type_list ':'
   56     | LCASE . expr_or_type_list '=' expr ':'
   57     | LCASE . expr_or_type_list LCOLAS expr ':'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  147 expr_or_type: . expr
  148             | . non_expr_type
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  171 non_expr_type: . recvchantype
  172              | . fntype
  173              | . othertype
  174              | . '*' non_expr_type
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  277 expr_or_type_list: . expr_or_type
  278                  | . expr_or_type_list ',' expr_or_type

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 159
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 160
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr               přejít do stavu 161
    uexpr              přejít do stavu 74
    pseudocall         přejít do stavu 75
    pexpr_no_paren     přejít do stavu 76
    pexpr              přejít do stavu 77
    expr_or_type       přejít do stavu 316
    sym                přejít do stavu 117
    hidden_importsym   přejít do stavu 13
    name               přejít do stavu 80
    non_expr_type      přejít do stavu 163
    convtype           přejít do stavu 82
    comptype           přejít do stavu 83
    othertype          přejít do stavu 164
    recvchantype       přejít do stavu 165
    structtype         přejít do stavu 85
    interfacetype      přejít do stavu 86
    fntype             přejít do stavu 166
    fnlitdcl           přejít do stavu 89
    fnliteral          přejít do stavu 90
    expr_or_type_list  přejít do stavu 575


State 498

   58 case: LDEFAULT . ':'

    ':'  posunout a přejít do stavu 576


State 499

   91 select_stmt: LSELECT $@13 LBODY caseblock_list '}' .

    $výchozí  reduce using rule 91 (select_stmt)


State 500

   61 $@4: . %empty
   62 caseblock: case . $@4 stmt_list

    $výchozí  reduce using rule 61 ($@4)

    $@4  přejít do stavu 577


State 501

   64 caseblock_list: caseblock_list caseblock .

    $výchozí  reduce using rule 64 (caseblock_list)


State 502

  234 structdcl: '*' '(' embed . ')' oliteral

    ')'  posunout a přejít do stavu 578


State 503

  232 structdcl: '*' embed oliteral .

    $výchozí  reduce using rule 232 (structdcl)


State 504

  233 structdcl: '(' '*' embed . ')' oliteral

    ')'  posunout a přejít do stavu 579


State 505

  231 structdcl: '(' embed ')' . oliteral
  301 oliteral: . %empty  [';', '}']
  302         | . LLITERAL

    LLITERAL  posunout a přejít do stavu 395

    $výchozí  reduce using rule 301 (oliteral)

    oliteral  přejít do stavu 580


State 506

  226 structdcl_list: structdcl_list ';' structdcl .

    $výchozí  reduce using rule 226 (structdcl_list)


State 507

  199 structtype: LSTRUCT lbrace structdcl_list osemi '}' .

    $výchozí  reduce using rule 199 (structtype)


State 508

  272 new_name_list: new_name_list ',' new_name .

    $výchozí  reduce using rule 272 (new_name_list)


State 509

  229 structdcl: new_name_list ntype oliteral .

    $výchozí  reduce using rule 229 (structdcl)


State 510

   63 caseblock_list: . %empty
   64               | . caseblock_list caseblock
   89 switch_stmt: LSWITCH $@11 if_header $@12 LBODY . caseblock_list '}'

    $výchozí  reduce using rule 63 (caseblock_list)

    caseblock_list  přejít do stavu 581


State 511

  224 typedcl_list: typedcl_list ';' typedcl .

    $výchozí  reduce using rule 224 (typedcl_list)


State 512

   36 common_dcl: LTYPE '(' typedcl_list osemi ')' .

    $výchozí  reduce using rule 36 (common_dcl)


State 513

  220 vardcl_list: vardcl_list ';' vardcl .

    $výchozí  reduce using rule 220 (vardcl_list)


State 514

   29 common_dcl: LVAR '(' vardcl_list osemi ')' .

    $výchozí  reduce using rule 29 (common_dcl)


State 515

   40 vardcl: dcl_name_list ntype '=' expr_list .  [LCASE, LDEFAULT, ')', ';', '}']
  276 expr_list: expr_list . ',' expr

    ','  posunout a přejít do stavu 210

    $výchozí  reduce using rule 40 (vardcl)


State 516

  170 ntype: '(' ntype . ')'
  179 non_recvchantype: '(' ntype . ')'

    ')'  posunout a přejít do stavu 582


State 517

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  137               | '(' expr_or_type ')' '{' start_complit . braced_keyval_list '}'
  138               | . fnliteral
  140 keyval: . expr ':' complitexpr
  141 bare_complitexpr: . expr
  142                 | . '{' start_complit braced_keyval_list '}'
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  279 keyval_list: . keyval
  280            | . bare_complitexpr
  281            | . keyval_list ',' keyval
  282            | . keyval_list ',' bare_complitexpr
  283 braced_keyval_list: . %empty  ['}']
  284                   | . keyval_list ocomma

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 421
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 283 (braced_keyval_list)

    expr                přejít do stavu 422
    uexpr               přejít do stavu 74
    pseudocall          přejít do stavu 75
    pexpr_no_paren      přejít do stavu 76
    keyval              přejít do stavu 423
    bare_complitexpr    přejít do stavu 424
    pexpr               přejít do stavu 77
    sym                 přejít do stavu 117
    hidden_importsym    přejít do stavu 13
    name                přejít do stavu 80
    convtype            přejít do stavu 82
    comptype            přejít do stavu 83
    othertype           přejít do stavu 84
    structtype          přejít do stavu 85
    interfacetype       přejít do stavu 86
    fntype              přejít do stavu 88
    fnlitdcl            přejít do stavu 89
    fnliteral           přejít do stavu 90
    keyval_list         přejít do stavu 425
    braced_keyval_list  přejít do stavu 583


State 518

   44 constdcl1: constdcl .

    $výchozí  reduce using rule 44 (constdcl1)


State 519

  221 constdcl_list: constdcl1 .

    $výchozí  reduce using rule 221 (constdcl_list)


State 520

   33 common_dcl: lconst '(' constdcl ';' constdcl_list . osemi ')'
  222 constdcl_list: constdcl_list . ';' constdcl1
  285 osemi: . %empty  [')']
  286      | . ';'

    ';'  posunout a přejít do stavu 584

    $výchozí  reduce using rule 285 (osemi)

    osemi  přejít do stavu 585


State 521

   42 constdcl: dcl_name_list . ntype '=' expr_list
   43         | dcl_name_list . '=' expr_list
   45 constdcl1: dcl_name_list . ntype
   46          | dcl_name_list .  [')', ';']
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  274 dcl_name_list: dcl_name_list . ',' dcl_name

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '='         posunout a přejít do stavu 291
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11
    ','         posunout a přejít do stavu 282

    $výchozí  reduce using rule 46 (constdcl1)

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    ntype             přejít do stavu 586
    dotname           přejít do stavu 228
    othertype         přejít do stavu 229
    ptrtype           přejít do stavu 230
    recvchantype      přejít do stavu 231
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 232


State 522

   32 common_dcl: lconst '(' constdcl osemi ')' .

    $výchozí  reduce using rule 32 (common_dcl)


State 523

   42 constdcl: dcl_name_list ntype '=' expr_list .  [LCASE, LDEFAULT, ')', ';', '}']
  276 expr_list: expr_list . ',' expr

    ','  posunout a přejít do stavu 210

    $výchozí  reduce using rule 42 (constdcl)


State 524

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  140 keyval: . expr ':' complitexpr
  141 bare_complitexpr: . expr
  142                 | . '{' start_complit braced_keyval_list '}'
  142                 | '{' start_complit . braced_keyval_list '}'
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  279 keyval_list: . keyval
  280            | . bare_complitexpr
  281            | . keyval_list ',' keyval
  282            | . keyval_list ',' bare_complitexpr
  283 braced_keyval_list: . %empty  ['}']
  284                   | . keyval_list ocomma

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 421
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 283 (braced_keyval_list)

    expr                přejít do stavu 422
    uexpr               přejít do stavu 74
    pseudocall          přejít do stavu 75
    pexpr_no_paren      přejít do stavu 76
    keyval              přejít do stavu 423
    bare_complitexpr    přejít do stavu 424
    pexpr               přejít do stavu 77
    sym                 přejít do stavu 117
    hidden_importsym    přejít do stavu 13
    name                přejít do stavu 80
    convtype            přejít do stavu 82
    comptype            přejít do stavu 83
    othertype           přejít do stavu 84
    structtype          přejít do stavu 85
    interfacetype       přejít do stavu 86
    fntype              přejít do stavu 88
    fnlitdcl            přejít do stavu 89
    fnliteral           přejít do stavu 90
    keyval_list         přejít do stavu 425
    braced_keyval_list  přejít do stavu 587


State 525

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  140 keyval: expr ':' . complitexpr
  143 complitexpr: . expr
  144            | . '{' start_complit braced_keyval_list '}'
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 588
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 589
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    complitexpr       přejít do stavu 590
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 526

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  140 keyval: . expr ':' complitexpr
  141 bare_complitexpr: . expr
  142                 | . '{' start_complit braced_keyval_list '}'
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  281 keyval_list: keyval_list ',' . keyval
  282            | keyval_list ',' . bare_complitexpr
  288 ocomma: ',' .  ['}']

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 421
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 288 (ocomma)

    expr              přejít do stavu 422
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    keyval            přejít do stavu 591
    bare_complitexpr  přejít do stavu 592
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 527

  284 braced_keyval_list: keyval_list ocomma .

    $výchozí  reduce using rule 284 (braced_keyval_list)


State 528

  136 pexpr_no_paren: pexpr_no_paren '{' start_complit braced_keyval_list '}' .

    $výchozí  reduce using rule 136 (pexpr_no_paren)


State 529

  124 pseudocall: pexpr '(' expr_or_type_list LDDD ocomma . ')'

    ')'  posunout a přejít do stavu 593


State 530

  278 expr_or_type_list: expr_or_type_list ',' expr_or_type .

    $výchozí  reduce using rule 278 (expr_or_type_list)


State 531

  123 pseudocall: pexpr '(' expr_or_type_list ocomma ')' .

    $výchozí  reduce using rule 123 (pseudocall)


State 532

  129 pexpr_no_paren: pexpr '.' '(' LTYPE ')' .

    $výchozí  reduce using rule 129 (pexpr_no_paren)


State 533

  128 pexpr_no_paren: pexpr '.' '(' expr_or_type ')' .

    $výchozí  reduce using rule 128 (pexpr_no_paren)


State 534

  131 pexpr_no_paren: pexpr '[' oexpr ':' oexpr . ']'
  132               | pexpr '[' oexpr ':' oexpr . ':' oexpr ']'

    ':'  posunout a přejít do stavu 594
    ']'  posunout a přejít do stavu 595


State 535

  134 pexpr_no_paren: convtype '(' expr ocomma ')' .

    $výchozí  reduce using rule 134 (pexpr_no_paren)


State 536

  135 pexpr_no_paren: comptype lbrace start_complit braced_keyval_list '}' .

    $výchozí  reduce using rule 135 (pexpr_no_paren)


State 537

   60 compound_stmt: '{' $@3 stmt_list . '}'
  270 stmt_list: stmt_list . ';' stmt

    ';'  posunout a přejít do stavu 439
    '}'  posunout a přejít do stavu 596


State 538

  270 stmt_list: stmt_list ';' stmt .

    $výchozí  reduce using rule 270 (stmt_list)


State 539

  326 hidden_type_misc: LCHAN LCOMM hidden_type .

    $výchozí  reduce using rule 326 (hidden_type_misc)


State 540

  325 hidden_type_misc: LCHAN '(' hidden_type_recv_chan . ')'

    ')'  posunout a přejít do stavu 597


State 541

  328 hidden_type_func: LFUNC '(' ohidden_funarg_list . ')' ohidden_funres

    ')'  posunout a přejít do stavu 598


State 542

  156 sym: LNAME .  ['(']
  317 hidden_type_misc: LNAME .  [';', '}']

    '('         reduce using rule 156 (sym)
    $výchozí  reduce using rule 317 (hidden_type_misc)


State 543

  332 hidden_interfacedcl: sym . '(' ohidden_funarg_list ')' ohidden_funres

    '('  posunout a přejít do stavu 599


State 544

  157 sym: hidden_importsym .  ['(']
  316 hidden_type_misc: hidden_importsym .  [';', '}']

    '('         reduce using rule 157 (sym)
    $výchozí  reduce using rule 316 (hidden_type_misc)


State 545

  322 hidden_type_misc: LINTERFACE '{' ohidden_interfacedcl_list . '}'

    '}'  posunout a přejít do stavu 600


State 546

  333 hidden_interfacedcl: hidden_type .

    $výchozí  reduce using rule 333 (hidden_interfacedcl)


State 547

  349 hidden_interfacedcl_list: hidden_interfacedcl .

    $výchozí  reduce using rule 349 (hidden_interfacedcl_list)


State 548

  300 ohidden_interfacedcl_list: hidden_interfacedcl_list .  ['}']
  350 hidden_interfacedcl_list: hidden_interfacedcl_list . ';' hidden_interfacedcl

    ';'  posunout a přejít do stavu 601

    $výchozí  reduce using rule 300 (ohidden_interfacedcl_list)


State 549

  320 hidden_type_misc: LMAP '[' hidden_type . ']' hidden_type

    ']'  posunout a přejít do stavu 602


State 550

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres
  331 hidden_structdcl: sym . hidden_type oliteral

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 603
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 551

  321 hidden_type_misc: LSTRUCT '{' ohidden_structdcl_list . '}'

    '}'  posunout a přejít do stavu 604


State 552

  347 hidden_structdcl_list: hidden_structdcl .

    $výchozí  reduce using rule 347 (hidden_structdcl_list)


State 553

  298 ohidden_structdcl_list: hidden_structdcl_list .  ['}']
  348 hidden_structdcl_list: hidden_structdcl_list . ';' hidden_structdcl

    ';'  posunout a přejít do stavu 605

    $výchozí  reduce using rule 298 (ohidden_structdcl_list)


State 554

  327 hidden_type_recv_chan: LCOMM LCHAN hidden_type .

    $výchozí  reduce using rule 327 (hidden_type_recv_chan)


State 555

  339 hidden_literal: '-' LLITERAL .

    $výchozí  reduce using rule 339 (hidden_literal)


State 556

  342 hidden_constant: '(' hidden_literal . '+' hidden_literal ')'

    '+'  posunout a přejít do stavu 606


State 557

  305 hidden_import: LCONST hidden_pkg_importsym '=' hidden_constant ';' .

    $výchozí  reduce using rule 305 (hidden_import)


State 558

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  319                 | '[' LLITERAL ']' . hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 607
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 559

  318 hidden_type_misc: '[' ']' hidden_type .

    $výchozí  reduce using rule 318 (hidden_type_misc)


State 560

  306 hidden_import: LCONST hidden_pkg_importsym hidden_type '=' hidden_constant . ';'

    ';'  posunout a přejít do stavu 608


State 561

  301 oliteral: . %empty  [')', ',']
  302         | . LLITERAL
  330 hidden_funarg: sym LDDD hidden_type . oliteral

    LLITERAL  posunout a přejít do stavu 395

    $výchozí  reduce using rule 301 (oliteral)

    oliteral  přejít do stavu 609


State 562

  329 hidden_funarg: sym hidden_type oliteral .

    $výchozí  reduce using rule 329 (hidden_funarg)


State 563

  207 hidden_fndcl: '(' hidden_funarg_list ')' sym . '(' ohidden_funarg_list ')' ohidden_funres

    '('  posunout a přejít do stavu 610


State 564

  346 hidden_funarg_list: hidden_funarg_list ',' hidden_funarg .

    $výchozí  reduce using rule 346 (hidden_funarg_list)


State 565

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  206 hidden_fndcl: hidden_pkg_importsym '(' ohidden_funarg_list ')' . ohidden_funres
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres
  334 ohidden_funres: . %empty  [';', '{']
  335               | . hidden_funres
  336 hidden_funres: . '(' ohidden_funarg_list ')'
  337              | . hidden_type

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '('         posunout a přejít do stavu 611
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 334 (ohidden_funres)

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 612
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349
    ohidden_funres         přejít do stavu 613
    hidden_funres          přejít do stavu 614


State 566

   66 loop_body: LBODY $@5 stmt_list . '}'
  270 stmt_list: stmt_list . ';' stmt

    ';'  posunout a přejít do stavu 439
    '}'  posunout a přejít do stavu 615


State 567

   68 range_stmt: expr_list LCOLAS LRANGE expr .  [LBODY]
   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 68 (range_stmt)


State 568

   67 range_stmt: expr_list '=' LRANGE expr .  [LBODY]
   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 67 (range_stmt)


State 569

   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   69 for_header: osimple_stmt ';' osimple_stmt ';' . osimple_stmt
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr
  293 osimple_stmt: . %empty  [LBODY]
  294             | . simple_stmt

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 293 (osimple_stmt)

    simple_stmt       přejít do stavu 236
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 92
    osimple_stmt      přejít do stavu 616


State 570

  213 fnres: '(' oarg_type_list_ocomma . ')'

    ')'  posunout a přejít do stavu 617


State 571

  149 name_or_type: . ntype
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  163 dotdotdot: . LDDD
  164          | . LDDD ntype
  165 ntype: . recvchantype
  166      | . fntype
  167      | . othertype
  168      | . ptrtype
  169      | . dotname
  170      | . '(' ntype ')'
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  205 fndcl: '(' oarg_type_list_ocomma ')' sym '(' . oarg_type_list_ocomma ')' fnres
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  242 arg_type: . name_or_type
  243         | . sym name_or_type
  244         | . sym dotdotdot
  245         | . dotdotdot
  246 arg_type_list: . arg_type
  247              | . arg_type_list ',' arg_type
  248 oarg_type_list_ocomma: . %empty  [')']
  249                      | . arg_type_list ocomma

    LCHAN       posunout a přejít do stavu 37
    LDDD        posunout a přejít do stavu 242
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 226
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 248 (oarg_type_list_ocomma)

    name_or_type           přejít do stavu 243
    sym                    přejít do stavu 244
    hidden_importsym       přejít do stavu 13
    name                   přejít do stavu 118
    dotdotdot              přejít do stavu 245
    ntype                  přejít do stavu 246
    dotname                přejít do stavu 228
    othertype              přejít do stavu 229
    ptrtype                přejít do stavu 230
    recvchantype           přejít do stavu 231
    structtype             přejít do stavu 85
    interfacetype          přejít do stavu 86
    fntype                 přejít do stavu 232
    arg_type               přejít do stavu 247
    arg_type_list          přejít do stavu 248
    oarg_type_list_ocomma  přejít do stavu 618


State 572

  204 fndcl: sym '(' oarg_type_list_ocomma ')' fnres .

    $výchozí  reduce using rule 204 (fndcl)


State 573

   80 if_stmt: LIF $@7 if_header $@8 loop_body $@9 . elseif_list else
   83 elseif_list: . %empty
   84            | . elseif_list elseif

    $výchozí  reduce using rule 83 (elseif_list)

    elseif_list  přejít do stavu 619


State 574

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  183 fnret_type: . recvchantype
  184           | . fntype
  185           | . othertype
  186           | . ptrtype
  187           | . dotname
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  211 fnres: . %empty  [';', '}']
  212      | . fnret_type
  213      | . '(' oarg_type_list_ocomma ')'
  241 indcl: '(' oarg_type_list_ocomma ')' . fnres

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 478
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 211 (fnres)

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    fnret_type        přejít do stavu 480
    dotname           přejít do stavu 481
    othertype         přejít do stavu 482
    ptrtype           přejít do stavu 483
    recvchantype      přejít do stavu 484
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 485
    fnres             přejít do stavu 620


State 575

   55 case: LCASE expr_or_type_list . ':'
   56     | LCASE expr_or_type_list . '=' expr ':'
   57     | LCASE expr_or_type_list . LCOLAS expr ':'
  278 expr_or_type_list: expr_or_type_list . ',' expr_or_type

    LCOLAS  posunout a přejít do stavu 621
    '='     posunout a přejít do stavu 622
    ':'     posunout a přejít do stavu 623
    ','     posunout a přejít do stavu 624


State 576

   58 case: LDEFAULT ':' .

    $výchozí  reduce using rule 58 (case)


State 577

   28 common_dcl: . LVAR vardcl
   29           | . LVAR '(' vardcl_list osemi ')'
   30           | . LVAR '(' ')'
   31           | . lconst constdcl
   32           | . lconst '(' constdcl osemi ')'
   33           | . lconst '(' constdcl ';' constdcl_list osemi ')'
   34           | . lconst '(' ')'
   35           | . LTYPE typedcl
   36           | . LTYPE '(' typedcl_list osemi ')'
   37           | . LTYPE '(' ')'
   38 lconst: . LCONST
   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   60 compound_stmt: . '{' $@3 stmt_list '}'
   62 caseblock: case $@4 . stmt_list
   74 for_stmt: . LFOR $@6 for_body
   80 if_stmt: . LIF $@7 if_header $@8 loop_body $@9 elseif_list else
   89 switch_stmt: . LSWITCH $@11 if_header $@12 LBODY caseblock_list '}'
   91 select_stmt: . LSELECT $@13 LBODY caseblock_list '}'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  152 new_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  162 labelname: . new_name
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  250 stmt: . %empty  [LCASE, LDEFAULT, ';', '}']
  251     | . compound_stmt
  252     | . common_dcl
  253     | . non_dcl_stmt
  254     | . error
  255 non_dcl_stmt: . simple_stmt
  256             | . for_stmt
  257             | . switch_stmt
  258             | . select_stmt
  259             | . if_stmt
  261             | . labelname ':' $@14 stmt
  262             | . LFALL
  263             | . LBREAK onew_name
  264             | . LCONTINUE onew_name
  265             | . LGO pseudocall
  266             | . LDEFER pseudocall
  267             | . LGOTO new_name
  268             | . LRETURN oexpr_list
  269 stmt_list: . stmt
  270          | . stmt_list ';' stmt
  275 expr_list: . expr
  276          | . expr_list ',' expr

    error       posunout a přejít do stavu 325
    LLITERAL    posunout a přejít do stavu 35
    LBREAK      posunout a přejít do stavu 36
    LCHAN       posunout a přejít do stavu 37
    LCONST      posunout a přejít do stavu 38
    LCONTINUE   posunout a přejít do stavu 39
    LDEFER      posunout a přejít do stavu 40
    LFALL       posunout a přejít do stavu 41
    LFOR        posunout a přejít do stavu 42
    LFUNC       posunout a přejít do stavu 113
    LGO         posunout a přejít do stavu 44
    LGOTO       posunout a přejít do stavu 45
    LIF         posunout a přejít do stavu 46
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LRETURN     posunout a přejít do stavu 49
    LSELECT     posunout a přejít do stavu 50
    LSTRUCT     posunout a přejít do stavu 51
    LSWITCH     posunout a přejít do stavu 52
    LTYPE       posunout a přejít do stavu 53
    LVAR        posunout a přejít do stavu 54
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 326
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    LCASE     reduce using rule 250 (stmt)
    LDEFAULT  reduce using rule 250 (stmt)
    ';'       reduce using rule 250 (stmt)
    '}'       reduce using rule 250 (stmt)

    common_dcl        přejít do stavu 327
    lconst            přejít do stavu 67
    simple_stmt       přejít do stavu 68
    compound_stmt     přejít do stavu 328
    for_stmt          přejít do stavu 69
    if_stmt           přejít do stavu 70
    switch_stmt       přejít do stavu 71
    select_stmt       přejít do stavu 72
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    new_name          přejít do stavu 78
    sym               přejít do stavu 79
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    labelname         přejít do stavu 81
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    stmt              přejít do stavu 329
    non_dcl_stmt      přejít do stavu 330
    stmt_list         přejít do stavu 625
    expr_list         přejít do stavu 92


State 578

  234 structdcl: '*' '(' embed ')' . oliteral
  301 oliteral: . %empty  [';', '}']
  302         | . LLITERAL

    LLITERAL  posunout a přejít do stavu 395

    $výchozí  reduce using rule 301 (oliteral)

    oliteral  přejít do stavu 626


State 579

  233 structdcl: '(' '*' embed ')' . oliteral
  301 oliteral: . %empty  [';', '}']
  302         | . LLITERAL

    LLITERAL  posunout a přejít do stavu 395

    $výchozí  reduce using rule 301 (oliteral)

    oliteral  přejít do stavu 627


State 580

  231 structdcl: '(' embed ')' oliteral .

    $výchozí  reduce using rule 231 (structdcl)


State 581

   55 case: . LCASE expr_or_type_list ':'
   56     | . LCASE expr_or_type_list '=' expr ':'
   57     | . LCASE expr_or_type_list LCOLAS expr ':'
   58     | . LDEFAULT ':'
   62 caseblock: . case $@4 stmt_list
   64 caseblock_list: caseblock_list . caseblock
   89 switch_stmt: LSWITCH $@11 if_header $@12 LBODY caseblock_list . '}'

    LCASE     posunout a přejít do stavu 497
    LDEFAULT  posunout a přejít do stavu 498
    '}'       posunout a přejít do stavu 628

    case       přejít do stavu 500
    caseblock  přejít do stavu 501


State 582

  170 ntype: '(' ntype ')' .  [LCOLAS, LDDD, ')', '=', ':', ',']
  179 non_recvchantype: '(' ntype ')' .  [LBODY, '(', '{']

    LBODY       reduce using rule 179 (non_recvchantype)
    '('         reduce using rule 179 (non_recvchantype)
    '{'         reduce using rule 179 (non_recvchantype)
    $výchozí  reduce using rule 170 (ntype)


State 583

  137 pexpr_no_paren: '(' expr_or_type ')' '{' start_complit braced_keyval_list . '}'

    '}'  posunout a přejít do stavu 629


State 584

   42 constdcl: . dcl_name_list ntype '=' expr_list
   43         | . dcl_name_list '=' expr_list
   44 constdcl1: . constdcl
   45          | . dcl_name_list ntype
   46          | . dcl_name_list
  153 dcl_name: . sym
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  222 constdcl_list: constdcl_list ';' . constdcl1
  273 dcl_name_list: . dcl_name
  274              | . dcl_name_list ',' dcl_name
  286 osemi: ';' .  [')']

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 286 (osemi)

    constdcl          přejít do stavu 518
    constdcl1         přejít do stavu 630
    dcl_name          přejít do stavu 150
    sym               přejít do stavu 151
    hidden_importsym  přejít do stavu 13
    dcl_name_list     přejít do stavu 521


State 585

   33 common_dcl: lconst '(' constdcl ';' constdcl_list osemi . ')'

    ')'  posunout a přejít do stavu 631


State 586

   42 constdcl: dcl_name_list ntype . '=' expr_list
   45 constdcl1: dcl_name_list ntype .  [')', ';']

    '='  posunout a přejít do stavu 420

    $výchozí  reduce using rule 45 (constdcl1)


State 587

  142 bare_complitexpr: '{' start_complit braced_keyval_list . '}'

    '}'  posunout a přejít do stavu 632


State 588

  139 start_complit: . %empty
  144 complitexpr: '{' . start_complit braced_keyval_list '}'

    $výchozí  reduce using rule 139 (start_complit)

    start_complit  přejít do stavu 633


State 589

   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr
  143 complitexpr: expr .  ['}', ',']

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198

    $výchozí  reduce using rule 143 (complitexpr)


State 590

  140 keyval: expr ':' complitexpr .

    $výchozí  reduce using rule 140 (keyval)


State 591

  281 keyval_list: keyval_list ',' keyval .

    $výchozí  reduce using rule 281 (keyval_list)


State 592

  282 keyval_list: keyval_list ',' bare_complitexpr .

    $výchozí  reduce using rule 282 (keyval_list)


State 593

  124 pseudocall: pexpr '(' expr_or_type_list LDDD ocomma ')' .

    $výchozí  reduce using rule 124 (pseudocall)


State 594

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  132               | pexpr '[' oexpr ':' oexpr ':' . oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  289 oexpr: . %empty  [']']
  290      | . expr

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 289 (oexpr)

    expr              přejít do stavu 170
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    oexpr             přejít do stavu 634


State 595

  131 pexpr_no_paren: pexpr '[' oexpr ':' oexpr ']' .

    $výchozí  reduce using rule 131 (pexpr_no_paren)


State 596

   60 compound_stmt: '{' $@3 stmt_list '}' .

    $výchozí  reduce using rule 60 (compound_stmt)


State 597

  325 hidden_type_misc: LCHAN '(' hidden_type_recv_chan ')' .

    $výchozí  reduce using rule 325 (hidden_type_misc)


State 598

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres
  328                 | LFUNC '(' ohidden_funarg_list ')' . ohidden_funres
  334 ohidden_funres: . %empty  [LLITERAL, ')', ';', '=', '{', '}', ']', ',']
  335               | . hidden_funres
  336 hidden_funres: . '(' ohidden_funarg_list ')'
  337              | . hidden_type

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '('         posunout a přejít do stavu 611
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 334 (ohidden_funres)

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 612
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349
    ohidden_funres         přejít do stavu 635
    hidden_funres          přejít do stavu 614


State 599

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  295 ohidden_funarg_list: . %empty  [')']
  296                    | . hidden_funarg_list
  329 hidden_funarg: . sym hidden_type oliteral
  330              | . sym LDDD hidden_type oliteral
  332 hidden_interfacedcl: sym '(' . ohidden_funarg_list ')' ohidden_funres
  345 hidden_funarg_list: . hidden_funarg
  346                   | . hidden_funarg_list ',' hidden_funarg

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 295 (ohidden_funarg_list)

    sym                  přejít do stavu 350
    hidden_importsym     přejít do stavu 13
    ohidden_funarg_list  přejít do stavu 636
    hidden_funarg        přejít do stavu 351
    hidden_funarg_list   přejít do stavu 467


State 600

  322 hidden_type_misc: LINTERFACE '{' ohidden_interfacedcl_list '}' .

    $výchozí  reduce using rule 322 (hidden_type_misc)


State 601

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres
  332 hidden_interfacedcl: . sym '(' ohidden_funarg_list ')' ohidden_funres
  333                    | . hidden_type
  350 hidden_interfacedcl_list: hidden_interfacedcl_list ';' . hidden_interfacedcl

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 542
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    sym                    přejít do stavu 543
    hidden_importsym       přejít do stavu 544
    hidden_type            přejít do stavu 546
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349
    hidden_interfacedcl    přejít do stavu 637


State 602

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  320                 | LMAP '[' hidden_type ']' . hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 638
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349


State 603

  301 oliteral: . %empty  [';', '}']
  302         | . LLITERAL
  331 hidden_structdcl: sym hidden_type . oliteral

    LLITERAL  posunout a přejít do stavu 395

    $výchozí  reduce using rule 301 (oliteral)

    oliteral  přejít do stavu 639


State 604

  321 hidden_type_misc: LSTRUCT '{' ohidden_structdcl_list '}' .

    $výchozí  reduce using rule 321 (hidden_type_misc)


State 605

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  331 hidden_structdcl: . sym hidden_type oliteral
  348 hidden_structdcl_list: hidden_structdcl_list ';' . hidden_structdcl

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    sym               přejít do stavu 550
    hidden_importsym  přejít do stavu 13
    hidden_structdcl  přejít do stavu 640


State 606

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  338 hidden_literal: . LLITERAL
  339               | . '-' LLITERAL
  340               | . sym
  342 hidden_constant: '(' hidden_literal '+' . hidden_literal ')'

    LLITERAL  posunout a přejít do stavu 452
    LNAME     posunout a přejít do stavu 9
    '-'       posunout a přejít do stavu 453
    '?'       posunout a přejít do stavu 10
    '@'       posunout a přejít do stavu 11

    sym               přejít do stavu 455
    hidden_importsym  přejít do stavu 13
    hidden_literal    přejít do stavu 641


State 607

  319 hidden_type_misc: '[' LLITERAL ']' hidden_type .

    $výchozí  reduce using rule 319 (hidden_type_misc)


State 608

  306 hidden_import: LCONST hidden_pkg_importsym hidden_type '=' hidden_constant ';' .

    $výchozí  reduce using rule 306 (hidden_import)


State 609

  330 hidden_funarg: sym LDDD hidden_type oliteral .

    $výchozí  reduce using rule 330 (hidden_funarg)


State 610

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  207 hidden_fndcl: '(' hidden_funarg_list ')' sym '(' . ohidden_funarg_list ')' ohidden_funres
  295 ohidden_funarg_list: . %empty  [')']
  296                    | . hidden_funarg_list
  329 hidden_funarg: . sym hidden_type oliteral
  330              | . sym LDDD hidden_type oliteral
  345 hidden_funarg_list: . hidden_funarg
  346                   | . hidden_funarg_list ',' hidden_funarg

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 295 (ohidden_funarg_list)

    sym                  přejít do stavu 350
    hidden_importsym     přejít do stavu 13
    ohidden_funarg_list  přejít do stavu 642
    hidden_funarg        přejít do stavu 351
    hidden_funarg_list   přejít do stavu 467


State 611

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  295 ohidden_funarg_list: . %empty  [')']
  296                    | . hidden_funarg_list
  329 hidden_funarg: . sym hidden_type oliteral
  330              | . sym LDDD hidden_type oliteral
  336 hidden_funres: '(' . ohidden_funarg_list ')'
  345 hidden_funarg_list: . hidden_funarg
  346                   | . hidden_funarg_list ',' hidden_funarg

    LNAME  posunout a přejít do stavu 9
    '?'    posunout a přejít do stavu 10
    '@'    posunout a přejít do stavu 11

    $výchozí  reduce using rule 295 (ohidden_funarg_list)

    sym                  přejít do stavu 350
    hidden_importsym     přejít do stavu 13
    ohidden_funarg_list  přejít do stavu 643
    hidden_funarg        přejít do stavu 351
    hidden_funarg_list   přejít do stavu 467


State 612

  337 hidden_funres: hidden_type .

    $výchozí  reduce using rule 337 (hidden_funres)


State 613

  206 hidden_fndcl: hidden_pkg_importsym '(' ohidden_funarg_list ')' ohidden_funres .

    $výchozí  reduce using rule 206 (hidden_fndcl)


State 614

  335 ohidden_funres: hidden_funres .

    $výchozí  reduce using rule 335 (ohidden_funres)


State 615

   66 loop_body: LBODY $@5 stmt_list '}' .

    $výchozí  reduce using rule 66 (loop_body)


State 616

   69 for_header: osimple_stmt ';' osimple_stmt ';' osimple_stmt .

    $výchozí  reduce using rule 69 (for_header)


State 617

  213 fnres: '(' oarg_type_list_ocomma ')' .

    $výchozí  reduce using rule 213 (fnres)


State 618

  205 fndcl: '(' oarg_type_list_ocomma ')' sym '(' oarg_type_list_ocomma . ')' fnres

    ')'  posunout a přejít do stavu 644


State 619

   80 if_stmt: LIF $@7 if_header $@8 loop_body $@9 elseif_list . else
   82 elseif: . LELSE LIF $@10 if_header loop_body
   84 elseif_list: elseif_list . elseif
   85 else: . %empty  [LCASE, LDEFAULT, ';', '}']
   86     | . LELSE compound_stmt

    LELSE  posunout a přejít do stavu 645

    $výchozí  reduce using rule 85 (else)

    elseif  přejít do stavu 646
    else    přejít do stavu 647


State 620

  241 indcl: '(' oarg_type_list_ocomma ')' fnres .

    $výchozí  reduce using rule 241 (indcl)


State 621

   57 case: LCASE expr_or_type_list LCOLAS . expr ':'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 648
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 622

   56 case: LCASE expr_or_type_list '=' . expr ':'
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 649
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 623

   55 case: LCASE expr_or_type_list ':' .

    $výchozí  reduce using rule 55 (case)


State 624

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  147 expr_or_type: . expr
  148             | . non_expr_type
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  171 non_expr_type: . recvchantype
  172              | . fntype
  173              | . othertype
  174              | . '*' non_expr_type
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  278 expr_or_type_list: expr_or_type_list ',' . expr_or_type

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 159
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 160
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    expr              přejít do stavu 161
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    expr_or_type      přejít do stavu 530
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    non_expr_type     přejít do stavu 163
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 164
    recvchantype      přejít do stavu 165
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 166
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90


State 625

   62 caseblock: case $@4 stmt_list .  [LCASE, LDEFAULT, '}']
  270 stmt_list: stmt_list . ';' stmt

    ';'  posunout a přejít do stavu 439

    $výchozí  reduce using rule 62 (caseblock)


State 626

  234 structdcl: '*' '(' embed ')' oliteral .

    $výchozí  reduce using rule 234 (structdcl)


State 627

  233 structdcl: '(' '*' embed ')' oliteral .

    $výchozí  reduce using rule 233 (structdcl)


State 628

   89 switch_stmt: LSWITCH $@11 if_header $@12 LBODY caseblock_list '}' .

    $výchozí  reduce using rule 89 (switch_stmt)


State 629

  137 pexpr_no_paren: '(' expr_or_type ')' '{' start_complit braced_keyval_list '}' .

    $výchozí  reduce using rule 137 (pexpr_no_paren)


State 630

  222 constdcl_list: constdcl_list ';' constdcl1 .

    $výchozí  reduce using rule 222 (constdcl_list)


State 631

   33 common_dcl: lconst '(' constdcl ';' constdcl_list osemi ')' .

    $výchozí  reduce using rule 33 (common_dcl)


State 632

  142 bare_complitexpr: '{' start_complit braced_keyval_list '}' .

    $výchozí  reduce using rule 142 (bare_complitexpr)


State 633

   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  140 keyval: . expr ':' complitexpr
  141 bare_complitexpr: . expr
  142                 | . '{' start_complit braced_keyval_list '}'
  144 complitexpr: '{' start_complit . braced_keyval_list '}'
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  279 keyval_list: . keyval
  280            | . bare_complitexpr
  281            | . keyval_list ',' keyval
  282            | . keyval_list ',' bare_complitexpr
  283 braced_keyval_list: . %empty  ['}']
  284                   | . keyval_list ocomma

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '{'         posunout a přejít do stavu 421
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 283 (braced_keyval_list)

    expr                přejít do stavu 422
    uexpr               přejít do stavu 74
    pseudocall          přejít do stavu 75
    pexpr_no_paren      přejít do stavu 76
    keyval              přejít do stavu 423
    bare_complitexpr    přejít do stavu 424
    pexpr               přejít do stavu 77
    sym                 přejít do stavu 117
    hidden_importsym    přejít do stavu 13
    name                přejít do stavu 80
    convtype            přejít do stavu 82
    comptype            přejít do stavu 83
    othertype           přejít do stavu 84
    structtype          přejít do stavu 85
    interfacetype       přejít do stavu 86
    fntype              přejít do stavu 88
    fnlitdcl            přejít do stavu 89
    fnliteral           přejít do stavu 90
    keyval_list         přejít do stavu 425
    braced_keyval_list  přejít do stavu 650


State 634

  132 pexpr_no_paren: pexpr '[' oexpr ':' oexpr ':' oexpr . ']'

    ']'  posunout a přejít do stavu 651


State 635

  328 hidden_type_func: LFUNC '(' ohidden_funarg_list ')' ohidden_funres .

    $výchozí  reduce using rule 328 (hidden_type_func)


State 636

  332 hidden_interfacedcl: sym '(' ohidden_funarg_list . ')' ohidden_funres

    ')'  posunout a přejít do stavu 652


State 637

  350 hidden_interfacedcl_list: hidden_interfacedcl_list ';' hidden_interfacedcl .

    $výchozí  reduce using rule 350 (hidden_interfacedcl_list)


State 638

  320 hidden_type_misc: LMAP '[' hidden_type ']' hidden_type .

    $výchozí  reduce using rule 320 (hidden_type_misc)


State 639

  331 hidden_structdcl: sym hidden_type oliteral .

    $výchozí  reduce using rule 331 (hidden_structdcl)


State 640

  348 hidden_structdcl_list: hidden_structdcl_list ';' hidden_structdcl .

    $výchozí  reduce using rule 348 (hidden_structdcl_list)


State 641

  342 hidden_constant: '(' hidden_literal '+' hidden_literal . ')'

    ')'  posunout a přejít do stavu 653


State 642

  207 hidden_fndcl: '(' hidden_funarg_list ')' sym '(' ohidden_funarg_list . ')' ohidden_funres

    ')'  posunout a přejít do stavu 654


State 643

  336 hidden_funres: '(' ohidden_funarg_list . ')'

    ')'  posunout a přejít do stavu 655


State 644

  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  183 fnret_type: . recvchantype
  184           | . fntype
  185           | . othertype
  186           | . ptrtype
  187           | . dotname
  188 dotname: . name
  189        | . name '.' sym
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  197 ptrtype: . '*' ntype
  198 recvchantype: . LCOMM LCHAN ntype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  205 fndcl: '(' oarg_type_list_ocomma ')' sym '(' oarg_type_list_ocomma ')' . fnres
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  211 fnres: . %empty  [';', '{']
  212      | . fnret_type
  213      | . '(' oarg_type_list_ocomma ')'

    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 225
    '*'         posunout a přejít do stavu 115
    '('         posunout a přejít do stavu 478
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 211 (fnres)

    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 118
    fnret_type        přejít do stavu 480
    dotname           přejít do stavu 481
    othertype         přejít do stavu 482
    ptrtype           přejít do stavu 483
    recvchantype      přejít do stavu 484
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 485
    fnres             přejít do stavu 656


State 645

   60 compound_stmt: . '{' $@3 stmt_list '}'
   82 elseif: LELSE . LIF $@10 if_header loop_body
   86 else: LELSE . compound_stmt

    LIF  posunout a přejít do stavu 657
    '{'  posunout a přejít do stavu 326

    compound_stmt  přejít do stavu 658


State 646

   84 elseif_list: elseif_list elseif .

    $výchozí  reduce using rule 84 (elseif_list)


State 647

   80 if_stmt: LIF $@7 if_header $@8 loop_body $@9 elseif_list else .

    $výchozí  reduce using rule 80 (if_stmt)


State 648

   57 case: LCASE expr_or_type_list LCOLAS expr . ':'
   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198
    ':'      posunout a přejít do stavu 659


State 649

   56 case: LCASE expr_or_type_list '=' expr . ':'
   93 expr: expr . LOROR expr
   94     | expr . LANDAND expr
   95     | expr . LEQ expr
   96     | expr . LNE expr
   97     | expr . LLT expr
   98     | expr . LLE expr
   99     | expr . LGE expr
  100     | expr . LGT expr
  101     | expr . '+' expr
  102     | expr . '-' expr
  103     | expr . '|' expr
  104     | expr . '^' expr
  105     | expr . '*' expr
  106     | expr . '/' expr
  107     | expr . '%' expr
  108     | expr . '&' expr
  109     | expr . LANDNOT expr
  110     | expr . LLSH expr
  111     | expr . LRSH expr
  112     | expr . LCOMM expr

    LANDAND  posunout a přejít do stavu 177
    LANDNOT  posunout a přejít do stavu 178
    LCOMM    posunout a přejít do stavu 179
    LEQ      posunout a přejít do stavu 181
    LGE      posunout a přejít do stavu 182
    LGT      posunout a přejít do stavu 183
    LLE      posunout a přejít do stavu 185
    LLSH     posunout a přejít do stavu 186
    LLT      posunout a přejít do stavu 187
    LNE      posunout a přejít do stavu 188
    LOROR    posunout a přejít do stavu 189
    LRSH     posunout a přejít do stavu 190
    '+'      posunout a přejít do stavu 191
    '-'      posunout a přejít do stavu 192
    '|'      posunout a přejít do stavu 193
    '^'      posunout a přejít do stavu 194
    '*'      posunout a přejít do stavu 195
    '/'      posunout a přejít do stavu 196
    '%'      posunout a přejít do stavu 197
    '&'      posunout a přejít do stavu 198
    ':'      posunout a přejít do stavu 660


State 650

  144 complitexpr: '{' start_complit braced_keyval_list . '}'

    '}'  posunout a přejít do stavu 661


State 651

  132 pexpr_no_paren: pexpr '[' oexpr ':' oexpr ':' oexpr ']' .

    $výchozí  reduce using rule 132 (pexpr_no_paren)


State 652

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres
  332 hidden_interfacedcl: sym '(' ohidden_funarg_list ')' . ohidden_funres
  334 ohidden_funres: . %empty  [';', '}']
  335               | . hidden_funres
  336 hidden_funres: . '(' ohidden_funarg_list ')'
  337              | . hidden_type

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '('         posunout a přejít do stavu 611
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 334 (ohidden_funres)

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 612
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349
    ohidden_funres         přejít do stavu 662
    hidden_funres          přejít do stavu 614


State 653

  342 hidden_constant: '(' hidden_literal '+' hidden_literal ')' .

    $výchozí  reduce using rule 342 (hidden_constant)


State 654

  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  207 hidden_fndcl: '(' hidden_funarg_list ')' sym '(' ohidden_funarg_list ')' . ohidden_funres
  311 hidden_type: . hidden_type_misc
  312            | . hidden_type_recv_chan
  313            | . hidden_type_func
  316 hidden_type_misc: . hidden_importsym
  317                 | . LNAME
  318                 | . '[' ']' hidden_type
  319                 | . '[' LLITERAL ']' hidden_type
  320                 | . LMAP '[' hidden_type ']' hidden_type
  321                 | . LSTRUCT '{' ohidden_structdcl_list '}'
  322                 | . LINTERFACE '{' ohidden_interfacedcl_list '}'
  323                 | . '*' hidden_type
  324                 | . LCHAN hidden_type_non_recv_chan
  325                 | . LCHAN '(' hidden_type_recv_chan ')'
  326                 | . LCHAN LCOMM hidden_type
  327 hidden_type_recv_chan: . LCOMM LCHAN hidden_type
  328 hidden_type_func: . LFUNC '(' ohidden_funarg_list ')' ohidden_funres
  334 ohidden_funres: . %empty  [';', '{']
  335               | . hidden_funres
  336 hidden_funres: . '(' ohidden_funarg_list ')'
  337              | . hidden_type

    LCHAN       posunout a přejít do stavu 335
    LFUNC       posunout a přejít do stavu 336
    LINTERFACE  posunout a přejít do stavu 337
    LMAP        posunout a přejít do stavu 338
    LNAME       posunout a přejít do stavu 339
    LSTRUCT     posunout a přejít do stavu 340
    LCOMM       posunout a přejít do stavu 341
    '*'         posunout a přejít do stavu 342
    '('         posunout a přejít do stavu 611
    '['         posunout a přejít do stavu 344
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 334 (ohidden_funres)

    hidden_importsym       přejít do stavu 345
    hidden_type            přejít do stavu 612
    hidden_type_misc       přejít do stavu 347
    hidden_type_recv_chan  přejít do stavu 348
    hidden_type_func       přejít do stavu 349
    ohidden_funres         přejít do stavu 663
    hidden_funres          přejít do stavu 614


State 655

  336 hidden_funres: '(' ohidden_funarg_list ')' .

    $výchozí  reduce using rule 336 (hidden_funres)


State 656

  205 fndcl: '(' oarg_type_list_ocomma ')' sym '(' oarg_type_list_ocomma ')' fnres .

    $výchozí  reduce using rule 205 (fndcl)


State 657

   81 $@10: . %empty
   82 elseif: LELSE LIF . $@10 if_header loop_body

    $výchozí  reduce using rule 81 ($@10)

    $@10  přejít do stavu 664


State 658

   86 else: LELSE compound_stmt .

    $výchozí  reduce using rule 86 (else)


State 659

   57 case: LCASE expr_or_type_list LCOLAS expr ':' .

    $výchozí  reduce using rule 57 (case)


State 660

   56 case: LCASE expr_or_type_list '=' expr ':' .

    $výchozí  reduce using rule 56 (case)


State 661

  144 complitexpr: '{' start_complit braced_keyval_list '}' .

    $výchozí  reduce using rule 144 (complitexpr)


State 662

  332 hidden_interfacedcl: sym '(' ohidden_funarg_list ')' ohidden_funres .

    $výchozí  reduce using rule 332 (hidden_interfacedcl)


State 663

  207 hidden_fndcl: '(' hidden_funarg_list ')' sym '(' ohidden_funarg_list ')' ohidden_funres .

    $výchozí  reduce using rule 207 (hidden_fndcl)


State 664

   49 simple_stmt: . expr
   50            | . expr LASOP expr
   51            | . expr_list '=' expr_list
   52            | . expr_list LCOLAS expr_list
   53            | . expr LINC
   54            | . expr LDEC
   75 if_header: . osimple_stmt
   76          | . osimple_stmt ';' osimple_stmt
   82 elseif: LELSE LIF $@10 . if_header loop_body
   92 expr: . uexpr
   93     | . expr LOROR expr
   94     | . expr LANDAND expr
   95     | . expr LEQ expr
   96     | . expr LNE expr
   97     | . expr LLT expr
   98     | . expr LLE expr
   99     | . expr LGE expr
  100     | . expr LGT expr
  101     | . expr '+' expr
  102     | . expr '-' expr
  103     | . expr '|' expr
  104     | . expr '^' expr
  105     | . expr '*' expr
  106     | . expr '/' expr
  107     | . expr '%' expr
  108     | . expr '&' expr
  109     | . expr LANDNOT expr
  110     | . expr LLSH expr
  111     | . expr LRSH expr
  112     | . expr LCOMM expr
  113 uexpr: . pexpr
  114      | . '*' uexpr
  115      | . '&' uexpr
  116      | . '+' uexpr
  117      | . '-' uexpr
  118      | . '!' uexpr
  119      | . '~' uexpr
  120      | . '^' uexpr
  121      | . LCOMM uexpr
  122 pseudocall: . pexpr '(' ')'
  123           | . pexpr '(' expr_or_type_list ocomma ')'
  124           | . pexpr '(' expr_or_type_list LDDD ocomma ')'
  125 pexpr_no_paren: . LLITERAL
  126               | . name
  127               | . pexpr '.' sym
  128               | . pexpr '.' '(' expr_or_type ')'
  129               | . pexpr '.' '(' LTYPE ')'
  130               | . pexpr '[' expr ']'
  131               | . pexpr '[' oexpr ':' oexpr ']'
  132               | . pexpr '[' oexpr ':' oexpr ':' oexpr ']'
  133               | . pseudocall
  134               | . convtype '(' expr ocomma ')'
  135               | . comptype lbrace start_complit braced_keyval_list '}'
  136               | . pexpr_no_paren '{' start_complit braced_keyval_list '}'
  137               | . '(' expr_or_type ')' '{' start_complit braced_keyval_list '}'
  138               | . fnliteral
  145 pexpr: . pexpr_no_paren
  146      | . '(' expr_or_type ')'
  156 sym: . LNAME
  157    | . hidden_importsym
  158    | . '?'
  159 hidden_importsym: . '@' LLITERAL '.' LNAME
  160                 | . '@' LLITERAL '.' '?'
  161 name: . sym
  180 convtype: . fntype
  181         | . othertype
  182 comptype: . othertype
  190 othertype: . '[' oexpr ']' ntype
  191          | . '[' LDDD ']' ntype
  192          | . LCHAN non_recvchantype
  193          | . LCHAN LCOMM ntype
  194          | . LMAP '[' ntype ']' ntype
  195          | . structtype
  196          | . interfacetype
  199 structtype: . LSTRUCT lbrace structdcl_list osemi '}'
  200           | . LSTRUCT lbrace '}'
  201 interfacetype: . LINTERFACE lbrace interfacedcl_list osemi '}'
  202              | . LINTERFACE lbrace '}'
  208 fntype: . LFUNC '(' oarg_type_list_ocomma ')' fnres
  214 fnlitdcl: . fntype
  215 fnliteral: . fnlitdcl lbrace stmt_list '}'
  216          | . fnlitdcl error
  275 expr_list: . expr
  276          | . expr_list ',' expr
  293 osimple_stmt: . %empty  [LBODY, ';']
  294             | . simple_stmt

    LLITERAL    posunout a přejít do stavu 35
    LCHAN       posunout a přejít do stavu 37
    LFUNC       posunout a přejít do stavu 113
    LINTERFACE  posunout a přejít do stavu 47
    LMAP        posunout a přejít do stavu 48
    LNAME       posunout a přejít do stavu 9
    LSTRUCT     posunout a přejít do stavu 51
    LCOMM       posunout a přejít do stavu 55
    '+'         posunout a přejít do stavu 56
    '-'         posunout a přejít do stavu 57
    '^'         posunout a přejít do stavu 58
    '*'         posunout a přejít do stavu 59
    '&'         posunout a přejít do stavu 60
    '('         posunout a přejít do stavu 61
    '!'         posunout a přejít do stavu 62
    '~'         posunout a přejít do stavu 63
    '['         posunout a přejít do stavu 64
    '?'         posunout a přejít do stavu 10
    '@'         posunout a přejít do stavu 11

    $výchozí  reduce using rule 293 (osimple_stmt)

    simple_stmt       přejít do stavu 236
    if_header         přejít do stavu 665
    expr              přejít do stavu 73
    uexpr             přejít do stavu 74
    pseudocall        přejít do stavu 75
    pexpr_no_paren    přejít do stavu 76
    pexpr             přejít do stavu 77
    sym               přejít do stavu 117
    hidden_importsym  přejít do stavu 13
    name              přejít do stavu 80
    convtype          přejít do stavu 82
    comptype          přejít do stavu 83
    othertype         přejít do stavu 84
    structtype        přejít do stavu 85
    interfacetype     přejít do stavu 86
    fntype            přejít do stavu 88
    fnlitdcl          přejít do stavu 89
    fnliteral         přejít do stavu 90
    expr_list         přejít do stavu 92
    osimple_stmt      přejít do stavu 254


State 665

   66 loop_body: . LBODY $@5 stmt_list '}'
   82 elseif: LELSE LIF $@10 if_header . loop_body

    LBODY  posunout a přejít do stavu 365

    loop_body  přejít do stavu 666


State 666

   82 elseif: LELSE LIF $@10 if_header loop_body .

    $výchozí  reduce using rule 82 (elseif)
