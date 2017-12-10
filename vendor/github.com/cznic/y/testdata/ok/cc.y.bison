Stav 1 conflicts: 1 shift/reduce
Stav 31 conflicts: 1 shift/reduce
Stav 399 conflicts: 1 shift/reduce


Gramatika

    0 $accept: prog $end

    1 prog: %empty
    2     | prog xdecl

    3 xdecl: zctlist ';'
    4      | zctlist xdlist ';'

    5 $@1: %empty

    6 $@2: %empty

    7 xdecl: zctlist xdecor $@1 pdecl $@2 block

    8 xdlist: xdecor

    9 $@3: %empty

   10 xdlist: xdecor $@3 '=' init
   11       | xdlist ',' xdlist

   12 xdecor: xdecor2
   13       | '*' zgnlist xdecor

   14 xdecor2: tag
   15        | '(' xdecor ')'
   16        | xdecor2 '(' zarglist ')'
   17        | xdecor2 '[' zexpr ']'

   18 adecl: ctlist ';'
   19      | ctlist adlist ';'

   20 adlist: xdecor

   21 $@4: %empty

   22 adlist: xdecor $@4 '=' init
   23       | adlist ',' adlist

   24 pdecl: %empty
   25      | pdecl ctlist pdlist ';'

   26 pdlist: xdecor
   27       | pdlist ',' pdlist

   28 $@5: %empty

   29 edecl: tlist $@5 zedlist ';'

   30 $@6: %empty

   31 edecl: edecl tlist $@6 zedlist ';'

   32 zedlist: %empty
   33        | edlist

   34 edlist: edecor
   35       | edlist ',' edlist

   36 edecor: xdecor
   37       | tag ':' lexpr
   38       | ':' lexpr

   39 abdecor: %empty
   40        | abdecor1

   41 abdecor1: '*' zgnlist
   42         | '*' zgnlist abdecor1
   43         | abdecor2

   44 abdecor2: abdecor3
   45         | abdecor2 '(' zarglist ')'
   46         | abdecor2 '[' zexpr ']'

   47 abdecor3: '(' ')'
   48         | '[' zexpr ']'
   49         | '(' abdecor1 ')'

   50 init: expr
   51     | '{' ilist '}'

   52 qual: '[' lexpr ']'
   53     | '.' ltag
   54     | qual '='

   55 qlist: init ','
   56      | qlist init ','
   57      | qual
   58      | qlist qual

   59 ilist: qlist
   60      | init
   61      | qlist init

   62 zarglist: %empty
   63         | arglist

   64 arglist: name
   65        | tlist abdecor
   66        | tlist xdecor
   67        | '.' '.' '.'
   68        | arglist ',' arglist

   69 block: '{' slist '}'

   70 slist: %empty
   71      | slist adecl
   72      | slist stmnt

   73 labels: label
   74       | labels label

   75 label: LCASE expr ':'
   76      | LDEFAULT ':'
   77      | LNAME ':'

   78 stmnt: error ';'
   79      | ulstmnt
   80      | labels ulstmnt

   81 forexpr: zcexpr
   82        | ctlist adlist

   83 ulstmnt: zcexpr ';'

   84 $@7: %empty

   85 ulstmnt: $@7 block
   86        | LIF '(' cexpr ')' stmnt
   87        | LIF '(' cexpr ')' stmnt LELSE stmnt

   88 $@8: %empty

   89 ulstmnt: $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt
   90        | LWHILE '(' cexpr ')' stmnt
   91        | LDO stmnt LWHILE '(' cexpr ')' ';'
   92        | LRETURN zcexpr ';'
   93        | LSWITCH '(' cexpr ')' stmnt
   94        | LBREAK ';'
   95        | LCONTINUE ';'
   96        | LGOTO ltag ';'
   97        | LUSED '(' zelist ')' ';'
   98        | LPREFETCH '(' zelist ')' ';'
   99        | LSET '(' zelist ')' ';'

  100 zcexpr: %empty
  101       | cexpr

  102 zexpr: %empty
  103      | lexpr

  104 lexpr: expr

  105 cexpr: expr
  106      | cexpr ',' cexpr

  107 expr: xuexpr
  108     | expr '*' expr
  109     | expr '/' expr
  110     | expr '%' expr
  111     | expr '+' expr
  112     | expr '-' expr
  113     | expr LRSH expr
  114     | expr LLSH expr
  115     | expr '<' expr
  116     | expr '>' expr
  117     | expr LLE expr
  118     | expr LGE expr
  119     | expr LEQ expr
  120     | expr LNE expr
  121     | expr '&' expr
  122     | expr '^' expr
  123     | expr '|' expr
  124     | expr LANDAND expr
  125     | expr LOROR expr
  126     | expr '?' cexpr ':' expr
  127     | expr '=' expr
  128     | expr LPE expr
  129     | expr LME expr
  130     | expr LMLE expr
  131     | expr LDVE expr
  132     | expr LMDE expr
  133     | expr LLSHE expr
  134     | expr LRSHE expr
  135     | expr LANDE expr
  136     | expr LXORE expr
  137     | expr LORE expr

  138 xuexpr: uexpr
  139       | '(' tlist abdecor ')' xuexpr
  140       | '(' tlist abdecor ')' '{' ilist '}'

  141 uexpr: pexpr
  142      | '*' xuexpr
  143      | '&' xuexpr
  144      | '+' xuexpr
  145      | '-' xuexpr
  146      | '!' xuexpr
  147      | '~' xuexpr
  148      | LPP xuexpr
  149      | LMM xuexpr
  150      | LSIZEOF uexpr
  151      | LSIGNOF uexpr

  152 pexpr: '(' cexpr ')'
  153      | LSIZEOF '(' tlist abdecor ')'
  154      | LSIGNOF '(' tlist abdecor ')'
  155      | pexpr '(' zelist ')'
  156      | pexpr '[' cexpr ']'
  157      | pexpr LMG ltag
  158      | pexpr '.' ltag
  159      | pexpr LPP
  160      | pexpr LMM
  161      | name
  162      | LCONST
  163      | LLCONST
  164      | LUCONST
  165      | LULCONST
  166      | LDCONST
  167      | LFCONST
  168      | LVLCONST
  169      | LUVLCONST
  170      | string
  171      | lstring

  172 string: LSTRING
  173       | string LSTRING

  174 lstring: LLSTRING
  175        | lstring LLSTRING

  176 zelist: %empty
  177       | elist

  178 elist: expr
  179      | elist ',' elist

  180 @9: %empty

  181 sbody: '{' @9 edecl '}'

  182 zctlist: %empty
  183        | ctlist

  184 types: complex
  185      | tname
  186      | gcnlist
  187      | complex gctnlist
  188      | tname gctnlist
  189      | gcnlist complex zgnlist
  190      | gcnlist tname
  191      | gcnlist tname gctnlist

  192 tlist: types

  193 ctlist: types

  194 complex: LSTRUCT ltag

  195 $@10: %empty

  196 complex: LSTRUCT ltag $@10 sbody
  197        | LSTRUCT sbody
  198        | LUNION ltag

  199 $@11: %empty

  200 complex: LUNION ltag $@11 sbody
  201        | LUNION sbody
  202        | LENUM ltag

  203 $@12: %empty

  204 $@13: %empty

  205 complex: LENUM ltag $@12 '{' $@13 enum '}'

  206 $@14: %empty

  207 complex: LENUM '{' $@14 enum '}'
  208        | LTYPE

  209 gctnlist: gctname
  210         | gctnlist gctname

  211 zgnlist: %empty
  212        | zgnlist gname

  213 gctname: tname
  214        | gname
  215        | cname

  216 gcnlist: gcname
  217        | gcnlist gcname

  218 gcname: gname
  219       | cname

  220 enum: LNAME
  221     | LNAME '=' expr
  222     | enum ','
  223     | enum ',' enum

  224 tname: LCHAR
  225      | LSHORT
  226      | LINT
  227      | LLONG
  228      | LSIGNED
  229      | LUNSIGNED
  230      | LFLOAT
  231      | LDOUBLE
  232      | LVOID

  233 cname: LAUTO
  234      | LSTATIC
  235      | LEXTERN
  236      | LTYPEDEF
  237      | LTYPESTR
  238      | LREGISTER
  239      | LINLINE

  240 gname: LCONSTNT
  241      | LVOLATILE
  242      | LRESTRICT

  243 name: LNAME

  244 tag: ltag

  245 ltag: LNAME
  246     | LTYPE


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'!' (33) 146
'%' (37) 110
'&' (38) 121 143
'(' (40) 15 16 45 47 49 86 87 89 90 91 93 97 98 99 139 140 152 153
    154 155
')' (41) 15 16 45 47 49 86 87 89 90 91 93 97 98 99 139 140 152 153
    154 155
'*' (42) 13 41 42 108 142
'+' (43) 111 144
',' (44) 11 23 27 35 55 56 68 106 179 222 223
'-' (45) 112 145
'.' (46) 53 67 158
'/' (47) 109
':' (58) 37 38 75 76 77 126
';' (59) 3 4 18 19 25 29 31 78 83 89 91 92 94 95 96 97 98 99
'<' (60) 115
'=' (61) 10 22 54 127 221
'>' (62) 116
'?' (63) 126
'[' (91) 17 46 48 52 156
']' (93) 17 46 48 52 156
'^' (94) 122
'{' (123) 51 69 140 181 205 207
'|' (124) 123
'}' (125) 51 69 140 181 205 207
'~' (126) 147
error (256) 78
LPE (258) 128
LME (259) 129
LMLE (260) 130
LDVE (261) 131
LMDE (262) 132
LRSHE (263) 134
LLSHE (264) 133
LANDE (265) 135
LXORE (266) 136
LORE (267) 137
LOROR (268) 125
LANDAND (269) 124
LEQ (270) 119
LNE (271) 120
LLE (272) 117
LGE (273) 118
LLSH (274) 114
LRSH (275) 113
LMM (276) 149 160
LPP (277) 148 159
LMG (278) 157
LNAME (279) 77 220 221 243 245
LTYPE (280) 208 246
LFCONST (281) 167
LDCONST (282) 166
LCONST (283) 162
LLCONST (284) 163
LUCONST (285) 164
LULCONST (286) 165
LVLCONST (287) 168
LUVLCONST (288) 169
LSTRING (289) 172 173
LLSTRING (290) 174 175
LAUTO (291) 233
LBREAK (292) 94
LCASE (293) 75
LCHAR (294) 224
LCONTINUE (295) 95
LDEFAULT (296) 76
LDO (297) 91
LDOUBLE (298) 231
LELSE (299) 87
LEXTERN (300) 235
LFLOAT (301) 230
LFOR (302) 89
LGOTO (303) 96
LIF (304) 86 87
LINT (305) 226
LLONG (306) 227
LPREFETCH (307) 98
LREGISTER (308) 238
LRETURN (309) 92
LSHORT (310) 225
LSIZEOF (311) 150 153
LUSED (312) 97
LSTATIC (313) 234
LSTRUCT (314) 194 196 197
LSWITCH (315) 93
LTYPEDEF (316) 236
LTYPESTR (317) 237
LUNION (318) 198 200 201
LUNSIGNED (319) 229
LWHILE (320) 90 91
LVOID (321) 232
LENUM (322) 202 205 207
LSIGNED (323) 228
LCONSTNT (324) 240
LVOLATILE (325) 241
LSET (326) 99
LSIGNOF (327) 151 154
LRESTRICT (328) 242
LINLINE (329) 239


Neterminály s pravidly, ve kterých se objevují

$accept (99)
    vlevo: 0
prog (100)
    vlevo: 1 2, vpravo: 0 2
xdecl (101)
    vlevo: 3 4 7, vpravo: 2
$@1 (102)
    vlevo: 5, vpravo: 7
$@2 (103)
    vlevo: 6, vpravo: 7
xdlist (104)
    vlevo: 8 10 11, vpravo: 4 11
$@3 (105)
    vlevo: 9, vpravo: 10
xdecor (106)
    vlevo: 12 13, vpravo: 7 8 10 13 15 20 22 26 36 66
xdecor2 (107)
    vlevo: 14 15 16 17, vpravo: 12 16 17
adecl (108)
    vlevo: 18 19, vpravo: 71
adlist (109)
    vlevo: 20 22 23, vpravo: 19 23 82
$@4 (110)
    vlevo: 21, vpravo: 22
pdecl (111)
    vlevo: 24 25, vpravo: 7 25
pdlist (112)
    vlevo: 26 27, vpravo: 25 27
edecl (113)
    vlevo: 29 31, vpravo: 31 181
$@5 (114)
    vlevo: 28, vpravo: 29
$@6 (115)
    vlevo: 30, vpravo: 31
zedlist (116)
    vlevo: 32 33, vpravo: 29 31
edlist (117)
    vlevo: 34 35, vpravo: 33 35
edecor (118)
    vlevo: 36 37 38, vpravo: 34
abdecor (119)
    vlevo: 39 40, vpravo: 65 139 140 153 154
abdecor1 (120)
    vlevo: 41 42 43, vpravo: 40 42 49
abdecor2 (121)
    vlevo: 44 45 46, vpravo: 43 45 46
abdecor3 (122)
    vlevo: 47 48 49, vpravo: 44
init (123)
    vlevo: 50 51, vpravo: 10 22 55 56 60 61
qual (124)
    vlevo: 52 53 54, vpravo: 54 57 58
qlist (125)
    vlevo: 55 56 57 58, vpravo: 56 58 59 61
ilist (126)
    vlevo: 59 60 61, vpravo: 51 140
zarglist (127)
    vlevo: 62 63, vpravo: 16 45
arglist (128)
    vlevo: 64 65 66 67 68, vpravo: 63 68
block (129)
    vlevo: 69, vpravo: 7 85
slist (130)
    vlevo: 70 71 72, vpravo: 69 71 72
labels (131)
    vlevo: 73 74, vpravo: 74 80
label (132)
    vlevo: 75 76 77, vpravo: 73 74
stmnt (133)
    vlevo: 78 79 80, vpravo: 72 86 87 89 90 91 93
forexpr (134)
    vlevo: 81 82, vpravo: 89
ulstmnt (135)
    vlevo: 83 85 86 87 89 90 91 92 93 94 95 96 97 98 99, vpravo: 79
    80
$@7 (136)
    vlevo: 84, vpravo: 85
$@8 (137)
    vlevo: 88, vpravo: 89
zcexpr (138)
    vlevo: 100 101, vpravo: 81 83 89 92
zexpr (139)
    vlevo: 102 103, vpravo: 17 46 48
lexpr (140)
    vlevo: 104, vpravo: 37 38 52 103
cexpr (141)
    vlevo: 105 106, vpravo: 86 87 90 91 93 101 106 126 152 156
expr (142)
    vlevo: 107 108 109 110 111 112 113 114 115 116 117 118 119 120
    121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136
    137, vpravo: 50 75 104 105 108 109 110 111 112 113 114 115 116
    117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132
    133 134 135 136 137 178 221
xuexpr (143)
    vlevo: 138 139 140, vpravo: 107 139 142 143 144 145 146 147 148
    149
uexpr (144)
    vlevo: 141 142 143 144 145 146 147 148 149 150 151, vpravo: 138
    150 151
pexpr (145)
    vlevo: 152 153 154 155 156 157 158 159 160 161 162 163 164 165
    166 167 168 169 170 171, vpravo: 141 155 156 157 158 159 160
string (146)
    vlevo: 172 173, vpravo: 170 173
lstring (147)
    vlevo: 174 175, vpravo: 171 175
zelist (148)
    vlevo: 176 177, vpravo: 97 98 99 155
elist (149)
    vlevo: 178 179, vpravo: 177 179
sbody (150)
    vlevo: 181, vpravo: 196 197 200 201
@9 (151)
    vlevo: 180, vpravo: 181
zctlist (152)
    vlevo: 182 183, vpravo: 3 4 7
types (153)
    vlevo: 184 185 186 187 188 189 190 191, vpravo: 192 193
tlist (154)
    vlevo: 192, vpravo: 29 31 65 66 139 140 153 154
ctlist (155)
    vlevo: 193, vpravo: 18 19 25 82 183
complex (156)
    vlevo: 194 196 197 198 200 201 202 205 207 208, vpravo: 184 187
    189
$@10 (157)
    vlevo: 195, vpravo: 196
$@11 (158)
    vlevo: 199, vpravo: 200
$@12 (159)
    vlevo: 203, vpravo: 205
$@13 (160)
    vlevo: 204, vpravo: 205
$@14 (161)
    vlevo: 206, vpravo: 207
gctnlist (162)
    vlevo: 209 210, vpravo: 187 188 191 210
zgnlist (163)
    vlevo: 211 212, vpravo: 13 41 42 189 212
gctname (164)
    vlevo: 213 214 215, vpravo: 209 210
gcnlist (165)
    vlevo: 216 217, vpravo: 186 189 190 191 217
gcname (166)
    vlevo: 218 219, vpravo: 216 217
enum (167)
    vlevo: 220 221 222 223, vpravo: 205 207 222 223
tname (168)
    vlevo: 224 225 226 227 228 229 230 231 232, vpravo: 185 188 190
    191 213
cname (169)
    vlevo: 233 234 235 236 237 238 239, vpravo: 215 219
gname (170)
    vlevo: 240 241 242, vpravo: 212 214 218
name (171)
    vlevo: 243, vpravo: 64 161
tag (172)
    vlevo: 244, vpravo: 14 37
ltag (173)
    vlevo: 245 246, vpravo: 53 96 157 158 194 196 198 200 202 205 244


State 0

    0 $accept: . prog $end
    1 prog: . %empty
    2     | . prog xdecl

    $výchozí  reduce using rule 1 (prog)

    prog  přejít do stavu 1


State 1

    0 $accept: prog . $end
    2 prog: prog . xdecl
    3 xdecl: . zctlist ';'
    4      | . zctlist xdlist ';'
    7      | . zctlist xdecor $@1 pdecl $@2 block
  182 zctlist: . %empty  [';', '*', '(', LNAME, LTYPE]
  183        | . ctlist
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  193 ctlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    $end       posunout a přejít do stavu 2
    LTYPE      posunout a přejít do stavu 3
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    LTYPE       [reduce using rule 182 (zctlist)]
    $výchozí  reduce using rule 182 (zctlist)

    xdecl    přejít do stavu 26
    zctlist  přejít do stavu 27
    types    přejít do stavu 28
    ctlist   přejít do stavu 29
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35


State 2

    0 $accept: prog $end .

    $výchozí  přijmout


State 3

  208 complex: LTYPE .

    $výchozí  reduce using rule 208 (complex)


State 4

  233 cname: LAUTO .

    $výchozí  reduce using rule 233 (cname)


State 5

  224 tname: LCHAR .

    $výchozí  reduce using rule 224 (tname)


State 6

  231 tname: LDOUBLE .

    $výchozí  reduce using rule 231 (tname)


State 7

  235 cname: LEXTERN .

    $výchozí  reduce using rule 235 (cname)


State 8

  230 tname: LFLOAT .

    $výchozí  reduce using rule 230 (tname)


State 9

  226 tname: LINT .

    $výchozí  reduce using rule 226 (tname)


State 10

  227 tname: LLONG .

    $výchozí  reduce using rule 227 (tname)


State 11

  238 cname: LREGISTER .

    $výchozí  reduce using rule 238 (cname)


State 12

  225 tname: LSHORT .

    $výchozí  reduce using rule 225 (tname)


State 13

  234 cname: LSTATIC .

    $výchozí  reduce using rule 234 (cname)


State 14

  181 sbody: . '{' @9 edecl '}'
  194 complex: LSTRUCT . ltag
  196        | LSTRUCT . ltag $@10 sbody
  197        | LSTRUCT . sbody
  245 ltag: . LNAME
  246     | . LTYPE

    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37
    '{'    posunout a přejít do stavu 38

    sbody  přejít do stavu 39
    ltag   přejít do stavu 40


State 15

  236 cname: LTYPEDEF .

    $výchozí  reduce using rule 236 (cname)


State 16

  237 cname: LTYPESTR .

    $výchozí  reduce using rule 237 (cname)


State 17

  181 sbody: . '{' @9 edecl '}'
  198 complex: LUNION . ltag
  200        | LUNION . ltag $@11 sbody
  201        | LUNION . sbody
  245 ltag: . LNAME
  246     | . LTYPE

    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37
    '{'    posunout a přejít do stavu 38

    sbody  přejít do stavu 41
    ltag   přejít do stavu 42


State 18

  229 tname: LUNSIGNED .

    $výchozí  reduce using rule 229 (tname)


State 19

  232 tname: LVOID .

    $výchozí  reduce using rule 232 (tname)


State 20

  202 complex: LENUM . ltag
  205        | LENUM . ltag $@12 '{' $@13 enum '}'
  207        | LENUM . '{' $@14 enum '}'
  245 ltag: . LNAME
  246     | . LTYPE

    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37
    '{'    posunout a přejít do stavu 43

    ltag  přejít do stavu 44


State 21

  228 tname: LSIGNED .

    $výchozí  reduce using rule 228 (tname)


State 22

  240 gname: LCONSTNT .

    $výchozí  reduce using rule 240 (gname)


State 23

  241 gname: LVOLATILE .

    $výchozí  reduce using rule 241 (gname)


State 24

  242 gname: LRESTRICT .

    $výchozí  reduce using rule 242 (gname)


State 25

  239 cname: LINLINE .

    $výchozí  reduce using rule 239 (cname)


State 26

    2 prog: prog xdecl .

    $výchozí  reduce using rule 2 (prog)


State 27

    3 xdecl: zctlist . ';'
    4      | zctlist . xdlist ';'
    7      | zctlist . xdecor $@1 pdecl $@2 block
    8 xdlist: . xdecor
   10       | . xdecor $@3 '=' init
   11       | . xdlist ',' xdlist
   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    ';'    posunout a přejít do stavu 45
    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    xdlist   přejít do stavu 48
    xdecor   přejít do stavu 49
    xdecor2  přejít do stavu 50
    tag      přejít do stavu 51
    ltag     přejít do stavu 52


State 28

  193 ctlist: types .

    $výchozí  reduce using rule 193 (ctlist)


State 29

  183 zctlist: ctlist .

    $výchozí  reduce using rule 183 (zctlist)


State 30

  184 types: complex .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, ')']
  187      | complex . gctnlist
  209 gctnlist: . gctname
  210         | . gctnlist gctname
  213 gctname: . tname
  214        | . gname
  215        | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    $výchozí  reduce using rule 184 (types)

    gctnlist  přejít do stavu 53
    gctname   přejít do stavu 54
    tname     přejít do stavu 55
    cname     přejít do stavu 56
    gname     přejít do stavu 57


State 31

  186 types: gcnlist .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, ')']
  189      | gcnlist . complex zgnlist
  190      | gcnlist . tname
  191      | gcnlist . tname gctnlist
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  217 gcnlist: gcnlist . gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LTYPE      posunout a přejít do stavu 3
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    LTYPE       [reduce using rule 186 (types)]
    $výchozí  reduce using rule 186 (types)

    complex  přejít do stavu 58
    gcname   přejít do stavu 59
    tname    přejít do stavu 60
    cname    přejít do stavu 34
    gname    přejít do stavu 35


State 32

  216 gcnlist: gcname .

    $výchozí  reduce using rule 216 (gcnlist)


State 33

  185 types: tname .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, ')']
  188      | tname . gctnlist
  209 gctnlist: . gctname
  210         | . gctnlist gctname
  213 gctname: . tname
  214        | . gname
  215        | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    $výchozí  reduce using rule 185 (types)

    gctnlist  přejít do stavu 61
    gctname   přejít do stavu 54
    tname     přejít do stavu 55
    cname     přejít do stavu 56
    gname     přejít do stavu 57


State 34

  219 gcname: cname .

    $výchozí  reduce using rule 219 (gcname)


State 35

  218 gcname: gname .

    $výchozí  reduce using rule 218 (gcname)


State 36

  245 ltag: LNAME .

    $výchozí  reduce using rule 245 (ltag)


State 37

  246 ltag: LTYPE .

    $výchozí  reduce using rule 246 (ltag)


State 38

  180 @9: . %empty
  181 sbody: '{' . @9 edecl '}'

    $výchozí  reduce using rule 180 (@9)

    @9  přejít do stavu 62


State 39

  197 complex: LSTRUCT sbody .

    $výchozí  reduce using rule 197 (complex)


State 40

  194 complex: LSTRUCT ltag .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, LAUTO, LCHAR, LDOUBLE, LEXTERN, LFLOAT, LINT, LLONG, LREGISTER, LSHORT, LSTATIC, LTYPEDEF, LTYPESTR, LUNSIGNED, LVOID, LSIGNED, LCONSTNT, LVOLATILE, LRESTRICT, LINLINE, ')']
  195 $@10: . %empty  ['{']
  196 complex: LSTRUCT ltag . $@10 sbody

    '{'         reduce using rule 195 ($@10)
    $výchozí  reduce using rule 194 (complex)

    $@10  přejít do stavu 63


State 41

  201 complex: LUNION sbody .

    $výchozí  reduce using rule 201 (complex)


State 42

  198 complex: LUNION ltag .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, LAUTO, LCHAR, LDOUBLE, LEXTERN, LFLOAT, LINT, LLONG, LREGISTER, LSHORT, LSTATIC, LTYPEDEF, LTYPESTR, LUNSIGNED, LVOID, LSIGNED, LCONSTNT, LVOLATILE, LRESTRICT, LINLINE, ')']
  199 $@11: . %empty  ['{']
  200 complex: LUNION ltag . $@11 sbody

    '{'         reduce using rule 199 ($@11)
    $výchozí  reduce using rule 198 (complex)

    $@11  přejít do stavu 64


State 43

  206 $@14: . %empty
  207 complex: LENUM '{' . $@14 enum '}'

    $výchozí  reduce using rule 206 ($@14)

    $@14  přejít do stavu 65


State 44

  202 complex: LENUM ltag .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, LAUTO, LCHAR, LDOUBLE, LEXTERN, LFLOAT, LINT, LLONG, LREGISTER, LSHORT, LSTATIC, LTYPEDEF, LTYPESTR, LUNSIGNED, LVOID, LSIGNED, LCONSTNT, LVOLATILE, LRESTRICT, LINLINE, ')']
  203 $@12: . %empty  ['{']
  205 complex: LENUM ltag . $@12 '{' $@13 enum '}'

    '{'         reduce using rule 203 ($@12)
    $výchozí  reduce using rule 202 (complex)

    $@12  přejít do stavu 66


State 45

    3 xdecl: zctlist ';' .

    $výchozí  reduce using rule 3 (xdecl)


State 46

   13 xdecor: '*' . zgnlist xdecor
  211 zgnlist: . %empty
  212        | . zgnlist gname

    $výchozí  reduce using rule 211 (zgnlist)

    zgnlist  přejít do stavu 67


State 47

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   15        | '(' . xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    xdecor   přejít do stavu 68
    xdecor2  přejít do stavu 50
    tag      přejít do stavu 51
    ltag     přejít do stavu 52


State 48

    4 xdecl: zctlist xdlist . ';'
   11 xdlist: xdlist . ',' xdlist

    ';'  posunout a přejít do stavu 69
    ','  posunout a přejít do stavu 70


State 49

    5 $@1: . %empty  [LTYPE, LAUTO, LCHAR, LDOUBLE, LEXTERN, LFLOAT, LINT, LLONG, LREGISTER, LSHORT, LSTATIC, LSTRUCT, LTYPEDEF, LTYPESTR, LUNION, LUNSIGNED, LVOID, LENUM, LSIGNED, LCONSTNT, LVOLATILE, LRESTRICT, LINLINE, '{']
    7 xdecl: zctlist xdecor . $@1 pdecl $@2 block
    8 xdlist: xdecor .  [';', ',']
    9 $@3: . %empty  ['=']
   10 xdlist: xdecor . $@3 '=' init

    ';'         reduce using rule 8 (xdlist)
    ','         reduce using rule 8 (xdlist)
    '='         reduce using rule 9 ($@3)
    $výchozí  reduce using rule 5 ($@1)

    $@1  přejít do stavu 71
    $@3  přejít do stavu 72


State 50

   12 xdecor: xdecor2 .  [';', ',', '=', LTYPE, LAUTO, LCHAR, LDOUBLE, LEXTERN, LFLOAT, LINT, LLONG, LREGISTER, LSHORT, LSTATIC, LSTRUCT, LTYPEDEF, LTYPESTR, LUNION, LUNSIGNED, LVOID, LENUM, LSIGNED, LCONSTNT, LVOLATILE, LRESTRICT, LINLINE, ')', '{']
   16 xdecor2: xdecor2 . '(' zarglist ')'
   17        | xdecor2 . '[' zexpr ']'

    '['  posunout a přejít do stavu 73
    '('  posunout a přejít do stavu 74

    $výchozí  reduce using rule 12 (xdecor)


State 51

   14 xdecor2: tag .

    $výchozí  reduce using rule 14 (xdecor2)


State 52

  244 tag: ltag .

    $výchozí  reduce using rule 244 (tag)


State 53

  187 types: complex gctnlist .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, ')']
  210 gctnlist: gctnlist . gctname
  213 gctname: . tname
  214        | . gname
  215        | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    $výchozí  reduce using rule 187 (types)

    gctname  přejít do stavu 75
    tname    přejít do stavu 55
    cname    přejít do stavu 56
    gname    přejít do stavu 57


State 54

  209 gctnlist: gctname .

    $výchozí  reduce using rule 209 (gctnlist)


State 55

  213 gctname: tname .

    $výchozí  reduce using rule 213 (gctname)


State 56

  215 gctname: cname .

    $výchozí  reduce using rule 215 (gctname)


State 57

  214 gctname: gname .

    $výchozí  reduce using rule 214 (gctname)


State 58

  189 types: gcnlist complex . zgnlist
  211 zgnlist: . %empty
  212        | . zgnlist gname

    $výchozí  reduce using rule 211 (zgnlist)

    zgnlist  přejít do stavu 76


State 59

  217 gcnlist: gcnlist gcname .

    $výchozí  reduce using rule 217 (gcnlist)


State 60

  190 types: gcnlist tname .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, ')']
  191      | gcnlist tname . gctnlist
  209 gctnlist: . gctname
  210         | . gctnlist gctname
  213 gctname: . tname
  214        | . gname
  215        | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    $výchozí  reduce using rule 190 (types)

    gctnlist  přejít do stavu 77
    gctname   přejít do stavu 54
    tname     přejít do stavu 55
    cname     přejít do stavu 56
    gname     přejít do stavu 57


State 61

  188 types: tname gctnlist .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, ')']
  210 gctnlist: gctnlist . gctname
  213 gctname: . tname
  214        | . gname
  215        | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    $výchozí  reduce using rule 188 (types)

    gctname  přejít do stavu 75
    tname    přejít do stavu 55
    cname    přejít do stavu 56
    gname    přejít do stavu 57


State 62

   29 edecl: . tlist $@5 zedlist ';'
   31      | . edecl tlist $@6 zedlist ';'
  181 sbody: '{' @9 . edecl '}'
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  192 tlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LTYPE      posunout a přejít do stavu 3
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    edecl    přejít do stavu 78
    types    přejít do stavu 79
    tlist    přejít do stavu 80
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35


State 63

  181 sbody: . '{' @9 edecl '}'
  196 complex: LSTRUCT ltag $@10 . sbody

    '{'  posunout a přejít do stavu 38

    sbody  přejít do stavu 81


State 64

  181 sbody: . '{' @9 edecl '}'
  200 complex: LUNION ltag $@11 . sbody

    '{'  posunout a přejít do stavu 38

    sbody  přejít do stavu 82


State 65

  207 complex: LENUM '{' $@14 . enum '}'
  220 enum: . LNAME
  221     | . LNAME '=' expr
  222     | . enum ','
  223     | . enum ',' enum

    LNAME  posunout a přejít do stavu 83

    enum  přejít do stavu 84


State 66

  205 complex: LENUM ltag $@12 . '{' $@13 enum '}'

    '{'  posunout a přejít do stavu 85


State 67

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   13       | '*' zgnlist . xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
  212 zgnlist: zgnlist . gname
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'        posunout a přejít do stavu 46
    '('        posunout a přejít do stavu 47
    LNAME      posunout a přejít do stavu 36
    LTYPE      posunout a přejít do stavu 37
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24

    xdecor   přejít do stavu 86
    xdecor2  přejít do stavu 50
    gname    přejít do stavu 87
    tag      přejít do stavu 51
    ltag     přejít do stavu 52


State 68

   15 xdecor2: '(' xdecor . ')'

    ')'  posunout a přejít do stavu 88


State 69

    4 xdecl: zctlist xdlist ';' .

    $výchozí  reduce using rule 4 (xdecl)


State 70

    8 xdlist: . xdecor
   10       | . xdecor $@3 '=' init
   11       | . xdlist ',' xdlist
   11       | xdlist ',' . xdlist
   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    xdlist   přejít do stavu 89
    xdecor   přejít do stavu 90
    xdecor2  přejít do stavu 50
    tag      přejít do stavu 51
    ltag     přejít do stavu 52


State 71

    7 xdecl: zctlist xdecor $@1 . pdecl $@2 block
   24 pdecl: . %empty
   25      | . pdecl ctlist pdlist ';'

    $výchozí  reduce using rule 24 (pdecl)

    pdecl  přejít do stavu 91


State 72

   10 xdlist: xdecor $@3 . '=' init

    '='  posunout a přejít do stavu 92


State 73

   17 xdecor2: xdecor2 '[' . zexpr ']'
  102 zexpr: . %empty  [']']
  103      | . lexpr
  104 lexpr: . expr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 102 (zexpr)

    zexpr    přejít do stavu 115
    lexpr    přejít do stavu 116
    expr     přejít do stavu 117
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 74

   16 xdecor2: xdecor2 '(' . zarglist ')'
   62 zarglist: . %empty  [')']
   63         | . arglist
   64 arglist: . name
   65        | . tlist abdecor
   66        | . tlist xdecor
   67        | . '.' '.' '.'
   68        | . arglist ',' arglist
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  192 tlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  243 name: . LNAME

    '.'        posunout a přejít do stavu 124
    LNAME      posunout a přejít do stavu 100
    LTYPE      posunout a přejít do stavu 3
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    $výchozí  reduce using rule 62 (zarglist)

    zarglist  přejít do stavu 125
    arglist   přejít do stavu 126
    types     přejít do stavu 79
    tlist     přejít do stavu 127
    complex   přejít do stavu 30
    gcnlist   přejít do stavu 31
    gcname    přejít do stavu 32
    tname     přejít do stavu 33
    cname     přejít do stavu 34
    gname     přejít do stavu 35
    name      přejít do stavu 128


State 75

  210 gctnlist: gctnlist gctname .

    $výchozí  reduce using rule 210 (gctnlist)


State 76

  189 types: gcnlist complex zgnlist .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, ')']
  212 zgnlist: zgnlist . gname
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24

    $výchozí  reduce using rule 189 (types)

    gname  přejít do stavu 87


State 77

  191 types: gcnlist tname gctnlist .  [';', ',', ':', '*', '[', '(', LNAME, LTYPE, ')']
  210 gctnlist: gctnlist . gctname
  213 gctname: . tname
  214        | . gname
  215        | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    $výchozí  reduce using rule 191 (types)

    gctname  přejít do stavu 75
    tname    přejít do stavu 55
    cname    přejít do stavu 56
    gname    přejít do stavu 57


State 78

   31 edecl: edecl . tlist $@6 zedlist ';'
  181 sbody: '{' @9 edecl . '}'
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  192 tlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LTYPE      posunout a přejít do stavu 3
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25
    '}'        posunout a přejít do stavu 129

    types    přejít do stavu 79
    tlist    přejít do stavu 130
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35


State 79

  192 tlist: types .

    $výchozí  reduce using rule 192 (tlist)


State 80

   28 $@5: . %empty
   29 edecl: tlist . $@5 zedlist ';'

    $výchozí  reduce using rule 28 ($@5)

    $@5  přejít do stavu 131


State 81

  196 complex: LSTRUCT ltag $@10 sbody .

    $výchozí  reduce using rule 196 (complex)


State 82

  200 complex: LUNION ltag $@11 sbody .

    $výchozí  reduce using rule 200 (complex)


State 83

  220 enum: LNAME .  [',', '}']
  221     | LNAME . '=' expr

    '='  posunout a přejít do stavu 132

    $výchozí  reduce using rule 220 (enum)


State 84

  207 complex: LENUM '{' $@14 enum . '}'
  222 enum: enum . ','
  223     | enum . ',' enum

    ','  posunout a přejít do stavu 133
    '}'  posunout a přejít do stavu 134


State 85

  204 $@13: . %empty
  205 complex: LENUM ltag $@12 '{' . $@13 enum '}'

    $výchozí  reduce using rule 204 ($@13)

    $@13  přejít do stavu 135


State 86

   13 xdecor: '*' zgnlist xdecor .

    $výchozí  reduce using rule 13 (xdecor)


State 87

  212 zgnlist: zgnlist gname .

    $výchozí  reduce using rule 212 (zgnlist)


State 88

   15 xdecor2: '(' xdecor ')' .

    $výchozí  reduce using rule 15 (xdecor2)


State 89

   11 xdlist: xdlist . ',' xdlist
   11       | xdlist ',' xdlist .  [';', ',']

    $výchozí  reduce using rule 11 (xdlist)

    Conflict between rule 11 and token ',' resolved as reduce (%left ',').


State 90

    8 xdlist: xdecor .  [';', ',']
    9 $@3: . %empty  ['=']
   10 xdlist: xdecor . $@3 '=' init

    '='         reduce using rule 9 ($@3)
    $výchozí  reduce using rule 8 (xdlist)

    $@3  přejít do stavu 72


State 91

    6 $@2: . %empty  ['{']
    7 xdecl: zctlist xdecor $@1 pdecl . $@2 block
   25 pdecl: pdecl . ctlist pdlist ';'
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  193 ctlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    LTYPE      posunout a přejít do stavu 3
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    $výchozí  reduce using rule 6 ($@2)

    $@2      přejít do stavu 136
    types    přejít do stavu 28
    ctlist   přejít do stavu 137
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35


State 92

   10 xdlist: xdecor $@3 '=' . init
   50 init: . expr
   51     | . '{' ilist '}'
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '{'        posunout a přejít do stavu 138
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    init     přejít do stavu 139
    expr     přejít do stavu 140
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 93

  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  143      | '&' . xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    xuexpr   přejít do stavu 141
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 94

  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  144      | '+' . xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    xuexpr   přejít do stavu 142
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 95

  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  145      | '-' . xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    xuexpr   přejít do stavu 143
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 96

  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  142      | '*' . xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    xuexpr   přejít do stavu 144
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 97

  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  149      | LMM . xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    xuexpr   přejít do stavu 145
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 98

  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  148      | LPP . xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    xuexpr   přejít do stavu 146
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 99

  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  139       | '(' . tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  140       | '(' . tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  152      | '(' . cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  192 tlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LTYPE      posunout a přejít do stavu 3
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSIZEOF    posunout a přejít do stavu 111
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LSIGNOF    posunout a přejít do stavu 112
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 147
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    types    přejít do stavu 79
    tlist    přejít do stavu 149
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35
    name     přejít do stavu 123


State 100

  243 name: LNAME .

    $výchozí  reduce using rule 243 (name)


State 101

  167 pexpr: LFCONST .

    $výchozí  reduce using rule 167 (pexpr)


State 102

  166 pexpr: LDCONST .

    $výchozí  reduce using rule 166 (pexpr)


State 103

  162 pexpr: LCONST .

    $výchozí  reduce using rule 162 (pexpr)


State 104

  163 pexpr: LLCONST .

    $výchozí  reduce using rule 163 (pexpr)


State 105

  164 pexpr: LUCONST .

    $výchozí  reduce using rule 164 (pexpr)


State 106

  165 pexpr: LULCONST .

    $výchozí  reduce using rule 165 (pexpr)


State 107

  168 pexpr: LVLCONST .

    $výchozí  reduce using rule 168 (pexpr)


State 108

  169 pexpr: LUVLCONST .

    $výchozí  reduce using rule 169 (pexpr)


State 109

  172 string: LSTRING .

    $výchozí  reduce using rule 172 (string)


State 110

  174 lstring: LLSTRING .

    $výchozí  reduce using rule 174 (lstring)


State 111

  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  150      | LSIZEOF . uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  153      | LSIZEOF . '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 150
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    uexpr    přejít do stavu 151
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 112

  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  151      | LSIGNOF . uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  154      | LSIGNOF . '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 152
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    uexpr    přejít do stavu 153
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 113

  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  146      | '!' . xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    xuexpr   přejít do stavu 154
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 114

  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  147      | '~' . xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    xuexpr   přejít do stavu 155
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 115

   17 xdecor2: xdecor2 '[' zexpr . ']'

    ']'  posunout a přejít do stavu 156


State 116

  103 zexpr: lexpr .

    $výchozí  reduce using rule 103 (zexpr)


State 117

  104 lexpr: expr .  [';', ',', ']']
  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 104 (lexpr)


State 118

  107 expr: xuexpr .

    $výchozí  reduce using rule 107 (expr)


State 119

  138 xuexpr: uexpr .

    $výchozí  reduce using rule 138 (xuexpr)


State 120

  141 uexpr: pexpr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, '+', '-', '*', '/', '%', ')', ']', '}']
  155 pexpr: pexpr . '(' zelist ')'
  156      | pexpr . '[' cexpr ']'
  157      | pexpr . LMG ltag
  158      | pexpr . '.' ltag
  159      | pexpr . LPP
  160      | pexpr . LMM

    LMM  posunout a přejít do stavu 187
    LPP  posunout a přejít do stavu 188
    LMG  posunout a přejít do stavu 189
    '.'  posunout a přejít do stavu 190
    '['  posunout a přejít do stavu 191
    '('  posunout a přejít do stavu 192

    $výchozí  reduce using rule 141 (uexpr)


State 121

  170 pexpr: string .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, '+', '-', '*', '/', '%', LMM, LPP, LMG, '.', '[', '(', ')', ']', '}']
  173 string: string . LSTRING

    LSTRING  posunout a přejít do stavu 193

    $výchozí  reduce using rule 170 (pexpr)


State 122

  171 pexpr: lstring .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, '+', '-', '*', '/', '%', LMM, LPP, LMG, '.', '[', '(', ')', ']', '}']
  175 lstring: lstring . LLSTRING

    LLSTRING  posunout a přejít do stavu 194

    $výchozí  reduce using rule 171 (pexpr)


State 123

  161 pexpr: name .

    $výchozí  reduce using rule 161 (pexpr)


State 124

   67 arglist: '.' . '.' '.'

    '.'  posunout a přejít do stavu 195


State 125

   16 xdecor2: xdecor2 '(' zarglist . ')'

    ')'  posunout a přejít do stavu 196


State 126

   63 zarglist: arglist .  [')']
   68 arglist: arglist . ',' arglist

    ','  posunout a přejít do stavu 197

    $výchozí  reduce using rule 63 (zarglist)


State 127

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   39 abdecor: . %empty  [',', ')']
   40        | . abdecor1
   41 abdecor1: . '*' zgnlist
   42         | . '*' zgnlist abdecor1
   43         | . abdecor2
   44 abdecor2: . abdecor3
   45         | . abdecor2 '(' zarglist ')'
   46         | . abdecor2 '[' zexpr ']'
   47 abdecor3: . '(' ')'
   48         | . '[' zexpr ']'
   49         | . '(' abdecor1 ')'
   65 arglist: tlist . abdecor
   66        | tlist . xdecor
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'    posunout a přejít do stavu 198
    '['    posunout a přejít do stavu 199
    '('    posunout a přejít do stavu 200
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    $výchozí  reduce using rule 39 (abdecor)

    xdecor    přejít do stavu 201
    xdecor2   přejít do stavu 50
    abdecor   přejít do stavu 202
    abdecor1  přejít do stavu 203
    abdecor2  přejít do stavu 204
    abdecor3  přejít do stavu 205
    tag       přejít do stavu 51
    ltag      přejít do stavu 52


State 128

   64 arglist: name .

    $výchozí  reduce using rule 64 (arglist)


State 129

  181 sbody: '{' @9 edecl '}' .

    $výchozí  reduce using rule 181 (sbody)


State 130

   30 $@6: . %empty
   31 edecl: edecl tlist . $@6 zedlist ';'

    $výchozí  reduce using rule 30 ($@6)

    $@6  přejít do stavu 206


State 131

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   29 edecl: tlist $@5 . zedlist ';'
   32 zedlist: . %empty  [';']
   33        | . edlist
   34 edlist: . edecor
   35       | . edlist ',' edlist
   36 edecor: . xdecor
   37       | . tag ':' lexpr
   38       | . ':' lexpr
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    ':'    posunout a přejít do stavu 207
    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    $výchozí  reduce using rule 32 (zedlist)

    xdecor   přejít do stavu 208
    xdecor2  přejít do stavu 50
    zedlist  přejít do stavu 209
    edlist   přejít do stavu 210
    edecor   přejít do stavu 211
    tag      přejít do stavu 212
    ltag     přejít do stavu 52


State 132

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  221 enum: LNAME '=' . expr
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 213
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 133

  220 enum: . LNAME
  221     | . LNAME '=' expr
  222     | . enum ','
  222     | enum ',' .  [',', '}']
  223     | . enum ',' enum
  223     | enum ',' . enum

    LNAME  posunout a přejít do stavu 83

    $výchozí  reduce using rule 222 (enum)

    enum  přejít do stavu 214


State 134

  207 complex: LENUM '{' $@14 enum '}' .

    $výchozí  reduce using rule 207 (complex)


State 135

  205 complex: LENUM ltag $@12 '{' $@13 . enum '}'
  220 enum: . LNAME
  221     | . LNAME '=' expr
  222     | . enum ','
  223     | . enum ',' enum

    LNAME  posunout a přejít do stavu 83

    enum  přejít do stavu 215


State 136

    7 xdecl: zctlist xdecor $@1 pdecl $@2 . block
   69 block: . '{' slist '}'

    '{'  posunout a přejít do stavu 216

    block  přejít do stavu 217


State 137

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   25 pdecl: pdecl ctlist . pdlist ';'
   26 pdlist: . xdecor
   27       | . pdlist ',' pdlist
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    xdecor   přejít do stavu 218
    xdecor2  přejít do stavu 50
    pdlist   přejít do stavu 219
    tag      přejít do stavu 51
    ltag     přejít do stavu 52


State 138

   50 init: . expr
   51     | . '{' ilist '}'
   51     | '{' . ilist '}'
   52 qual: . '[' lexpr ']'
   53     | . '.' ltag
   54     | . qual '='
   55 qlist: . init ','
   56      | . qlist init ','
   57      | . qual
   58      | . qlist qual
   59 ilist: . qlist
   60      | . init
   61      | . qlist init
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '.'        posunout a přejít do stavu 220
    '['        posunout a přejít do stavu 221
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '{'        posunout a přejít do stavu 138
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    init     přejít do stavu 222
    qual     přejít do stavu 223
    qlist    přejít do stavu 224
    ilist    přejít do stavu 225
    expr     přejít do stavu 140
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 139

   10 xdlist: xdecor $@3 '=' init .

    $výchozí  reduce using rule 10 (xdlist)


State 140

   50 init: expr .  [';', ',', '}']
  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 50 (init)


State 141

  143 uexpr: '&' xuexpr .

    $výchozí  reduce using rule 143 (uexpr)


State 142

  144 uexpr: '+' xuexpr .

    $výchozí  reduce using rule 144 (uexpr)


State 143

  145 uexpr: '-' xuexpr .

    $výchozí  reduce using rule 145 (uexpr)


State 144

  142 uexpr: '*' xuexpr .

    $výchozí  reduce using rule 142 (uexpr)


State 145

  149 uexpr: LMM xuexpr .

    $výchozí  reduce using rule 149 (uexpr)


State 146

  148 uexpr: LPP xuexpr .

    $výchozí  reduce using rule 148 (uexpr)


State 147

  106 cexpr: cexpr . ',' cexpr
  152 pexpr: '(' cexpr . ')'

    ','  posunout a přejít do stavu 226
    ')'  posunout a přejít do stavu 227


State 148

  105 cexpr: expr .  [';', ',', ':', ')', ']']
  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 105 (cexpr)


State 149

   39 abdecor: . %empty  [')']
   40        | . abdecor1
   41 abdecor1: . '*' zgnlist
   42         | . '*' zgnlist abdecor1
   43         | . abdecor2
   44 abdecor2: . abdecor3
   45         | . abdecor2 '(' zarglist ')'
   46         | . abdecor2 '[' zexpr ']'
   47 abdecor3: . '(' ')'
   48         | . '[' zexpr ']'
   49         | . '(' abdecor1 ')'
  139 xuexpr: '(' tlist . abdecor ')' xuexpr
  140       | '(' tlist . abdecor ')' '{' ilist '}'

    '*'  posunout a přejít do stavu 228
    '['  posunout a přejít do stavu 199
    '('  posunout a přejít do stavu 229

    $výchozí  reduce using rule 39 (abdecor)

    abdecor   přejít do stavu 230
    abdecor1  přejít do stavu 203
    abdecor2  přejít do stavu 204
    abdecor3  přejít do stavu 205


State 150

  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  152      | '(' . cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  153      | LSIZEOF '(' . tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  192 tlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LTYPE      posunout a přejít do stavu 3
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSIZEOF    posunout a přejít do stavu 111
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LSIGNOF    posunout a přejít do stavu 112
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 147
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    types    přejít do stavu 79
    tlist    přejít do stavu 231
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35
    name     přejít do stavu 123


State 151

  150 uexpr: LSIZEOF uexpr .

    $výchozí  reduce using rule 150 (uexpr)


State 152

  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  152      | '(' . cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  154      | LSIGNOF '(' . tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  192 tlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LTYPE      posunout a přejít do stavu 3
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSIZEOF    posunout a přejít do stavu 111
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LSIGNOF    posunout a přejít do stavu 112
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 147
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    types    přejít do stavu 79
    tlist    přejít do stavu 232
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35
    name     přejít do stavu 123


State 153

  151 uexpr: LSIGNOF uexpr .

    $výchozí  reduce using rule 151 (uexpr)


State 154

  146 uexpr: '!' xuexpr .

    $výchozí  reduce using rule 146 (uexpr)


State 155

  147 uexpr: '~' xuexpr .

    $výchozí  reduce using rule 147 (uexpr)


State 156

   17 xdecor2: xdecor2 '[' zexpr ']' .

    $výchozí  reduce using rule 17 (xdecor2)


State 157

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  127     | expr '=' . expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 233
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 158

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  128     | expr LPE . expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 234
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 159

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  129     | expr LME . expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 235
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 160

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  130     | expr LMLE . expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 236
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 161

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  131     | expr LDVE . expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 237
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 162

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  132     | expr LMDE . expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 238
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 163

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  134     | expr LRSHE . expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 239
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 164

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  133     | expr LLSHE . expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 240
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 165

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  135     | expr LANDE . expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 241
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 166

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  136     | expr LXORE . expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 242
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 167

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  137     | expr LORE . expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 243
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 168

  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  126     | expr '?' . cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 244
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 169

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  125     | expr LOROR . expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 245
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 170

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  124     | expr LANDAND . expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 246
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 171

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  123     | expr '|' . expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 247
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 172

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  122     | expr '^' . expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 248
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 173

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  121     | expr '&' . expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 249
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 174

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  119     | expr LEQ . expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 250
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 175

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  120     | expr LNE . expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 251
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 176

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  115     | expr '<' . expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 252
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 177

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  116     | expr '>' . expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 253
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 178

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  117     | expr LLE . expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 254
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 179

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  118     | expr LGE . expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 255
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 180

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  114     | expr LLSH . expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 256
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 181

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  113     | expr LRSH . expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 257
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 182

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  111     | expr '+' . expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 258
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 183

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  112     | expr '-' . expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 259
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 184

  107 expr: . xuexpr
  108     | . expr '*' expr
  108     | expr '*' . expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 260
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 185

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  109     | expr '/' . expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 261
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 186

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  110     | expr '%' . expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 262
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 187

  160 pexpr: pexpr LMM .

    $výchozí  reduce using rule 160 (pexpr)


State 188

  159 pexpr: pexpr LPP .

    $výchozí  reduce using rule 159 (pexpr)


State 189

  157 pexpr: pexpr LMG . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    ltag  přejít do stavu 263


State 190

  158 pexpr: pexpr '.' . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    ltag  přejít do stavu 264


State 191

  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  156      | pexpr '[' . cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 265
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 192

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  155      | pexpr '(' . zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  176 zelist: . %empty  [')']
  177       | . elist
  178 elist: . expr
  179      | . elist ',' elist
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 176 (zelist)

    expr     přejít do stavu 266
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    zelist   přejít do stavu 267
    elist    přejít do stavu 268
    name     přejít do stavu 123


State 193

  173 string: string LSTRING .

    $výchozí  reduce using rule 173 (string)


State 194

  175 lstring: lstring LLSTRING .

    $výchozí  reduce using rule 175 (lstring)


State 195

   67 arglist: '.' '.' . '.'

    '.'  posunout a přejít do stavu 269


State 196

   16 xdecor2: xdecor2 '(' zarglist ')' .

    $výchozí  reduce using rule 16 (xdecor2)


State 197

   64 arglist: . name
   65        | . tlist abdecor
   66        | . tlist xdecor
   67        | . '.' '.' '.'
   68        | . arglist ',' arglist
   68        | arglist ',' . arglist
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  192 tlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  243 name: . LNAME

    '.'        posunout a přejít do stavu 124
    LNAME      posunout a přejít do stavu 100
    LTYPE      posunout a přejít do stavu 3
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    arglist  přejít do stavu 270
    types    přejít do stavu 79
    tlist    přejít do stavu 127
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35
    name     přejít do stavu 128


State 198

   13 xdecor: '*' . zgnlist xdecor
   41 abdecor1: '*' . zgnlist
   42         | '*' . zgnlist abdecor1
  211 zgnlist: . %empty
  212        | . zgnlist gname

    $výchozí  reduce using rule 211 (zgnlist)

    zgnlist  přejít do stavu 271


State 199

   48 abdecor3: '[' . zexpr ']'
  102 zexpr: . %empty  [']']
  103      | . lexpr
  104 lexpr: . expr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 102 (zexpr)

    zexpr    přejít do stavu 272
    lexpr    přejít do stavu 116
    expr     přejít do stavu 117
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 200

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   15        | '(' . xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   41 abdecor1: . '*' zgnlist
   42         | . '*' zgnlist abdecor1
   43         | . abdecor2
   44 abdecor2: . abdecor3
   45         | . abdecor2 '(' zarglist ')'
   46         | . abdecor2 '[' zexpr ']'
   47 abdecor3: . '(' ')'
   47         | '(' . ')'
   48         | . '[' zexpr ']'
   49         | . '(' abdecor1 ')'
   49         | '(' . abdecor1 ')'
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'    posunout a přejít do stavu 198
    '['    posunout a přejít do stavu 199
    '('    posunout a přejít do stavu 200
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37
    ')'    posunout a přejít do stavu 273

    xdecor    přejít do stavu 68
    xdecor2   přejít do stavu 50
    abdecor1  přejít do stavu 274
    abdecor2  přejít do stavu 204
    abdecor3  přejít do stavu 205
    tag       přejít do stavu 51
    ltag      přejít do stavu 52


State 201

   66 arglist: tlist xdecor .

    $výchozí  reduce using rule 66 (arglist)


State 202

   65 arglist: tlist abdecor .

    $výchozí  reduce using rule 65 (arglist)


State 203

   40 abdecor: abdecor1 .

    $výchozí  reduce using rule 40 (abdecor)


State 204

   43 abdecor1: abdecor2 .  [',', ')']
   45 abdecor2: abdecor2 . '(' zarglist ')'
   46         | abdecor2 . '[' zexpr ']'

    '['  posunout a přejít do stavu 275
    '('  posunout a přejít do stavu 276

    $výchozí  reduce using rule 43 (abdecor1)


State 205

   44 abdecor2: abdecor3 .

    $výchozí  reduce using rule 44 (abdecor2)


State 206

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   31 edecl: edecl tlist $@6 . zedlist ';'
   32 zedlist: . %empty  [';']
   33        | . edlist
   34 edlist: . edecor
   35       | . edlist ',' edlist
   36 edecor: . xdecor
   37       | . tag ':' lexpr
   38       | . ':' lexpr
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    ':'    posunout a přejít do stavu 207
    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    $výchozí  reduce using rule 32 (zedlist)

    xdecor   přejít do stavu 208
    xdecor2  přejít do stavu 50
    zedlist  přejít do stavu 277
    edlist   přejít do stavu 210
    edecor   přejít do stavu 211
    tag      přejít do stavu 212
    ltag     přejít do stavu 52


State 207

   38 edecor: ':' . lexpr
  104 lexpr: . expr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    lexpr    přejít do stavu 278
    expr     přejít do stavu 117
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 208

   36 edecor: xdecor .

    $výchozí  reduce using rule 36 (edecor)


State 209

   29 edecl: tlist $@5 zedlist . ';'

    ';'  posunout a přejít do stavu 279


State 210

   33 zedlist: edlist .  [';']
   35 edlist: edlist . ',' edlist

    ','  posunout a přejít do stavu 280

    $výchozí  reduce using rule 33 (zedlist)


State 211

   34 edlist: edecor .

    $výchozí  reduce using rule 34 (edlist)


State 212

   14 xdecor2: tag .  [';', ',', '[', '(']
   37 edecor: tag . ':' lexpr

    ':'  posunout a přejít do stavu 281

    $výchozí  reduce using rule 14 (xdecor2)


State 213

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr
  221 enum: LNAME '=' expr .  [',', '}']

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 221 (enum)


State 214

  222 enum: enum . ','
  223     | enum . ',' enum
  223     | enum ',' enum .  [',', '}']

    $výchozí  reduce using rule 223 (enum)

    Conflict between rule 223 and token ',' resolved as reduce (%left ',').


State 215

  205 complex: LENUM ltag $@12 '{' $@13 enum . '}'
  222 enum: enum . ','
  223     | enum . ',' enum

    ','  posunout a přejít do stavu 133
    '}'  posunout a přejít do stavu 282


State 216

   69 block: '{' . slist '}'
   70 slist: . %empty
   71      | . slist adecl
   72      | . slist stmnt

    $výchozí  reduce using rule 70 (slist)

    slist  přejít do stavu 283


State 217

    7 xdecl: zctlist xdecor $@1 pdecl $@2 block .

    $výchozí  reduce using rule 7 (xdecl)


State 218

   26 pdlist: xdecor .

    $výchozí  reduce using rule 26 (pdlist)


State 219

   25 pdecl: pdecl ctlist pdlist . ';'
   27 pdlist: pdlist . ',' pdlist

    ';'  posunout a přejít do stavu 284
    ','  posunout a přejít do stavu 285


State 220

   53 qual: '.' . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    ltag  přejít do stavu 286


State 221

   52 qual: '[' . lexpr ']'
  104 lexpr: . expr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    lexpr    přejít do stavu 287
    expr     přejít do stavu 117
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 222

   55 qlist: init . ','
   60 ilist: init .  ['}']

    ','  posunout a přejít do stavu 288

    $výchozí  reduce using rule 60 (ilist)


State 223

   54 qual: qual . '='
   57 qlist: qual .  ['&', '+', '-', '*', LMM, LPP, '.', '[', '(', LNAME, LFCONST, LDCONST, LCONST, LLCONST, LUCONST, LULCONST, LVLCONST, LUVLCONST, LSTRING, LLSTRING, LSIZEOF, LSIGNOF, '{', '}', '!', '~']

    '='  posunout a přejít do stavu 289

    $výchozí  reduce using rule 57 (qlist)


State 224

   50 init: . expr
   51     | . '{' ilist '}'
   52 qual: . '[' lexpr ']'
   53     | . '.' ltag
   54     | . qual '='
   56 qlist: qlist . init ','
   58      | qlist . qual
   59 ilist: qlist .  ['}']
   61      | qlist . init
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '.'        posunout a přejít do stavu 220
    '['        posunout a přejít do stavu 221
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '{'        posunout a přejít do stavu 138
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 59 (ilist)

    init     přejít do stavu 290
    qual     přejít do stavu 291
    expr     přejít do stavu 140
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 225

   51 init: '{' ilist . '}'

    '}'  posunout a přejít do stavu 292


State 226

  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  106      | cexpr ',' . cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 293
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 227

  152 pexpr: '(' cexpr ')' .

    $výchozí  reduce using rule 152 (pexpr)


State 228

   41 abdecor1: '*' . zgnlist
   42         | '*' . zgnlist abdecor1
  211 zgnlist: . %empty
  212        | . zgnlist gname

    $výchozí  reduce using rule 211 (zgnlist)

    zgnlist  přejít do stavu 294


State 229

   41 abdecor1: . '*' zgnlist
   42         | . '*' zgnlist abdecor1
   43         | . abdecor2
   44 abdecor2: . abdecor3
   45         | . abdecor2 '(' zarglist ')'
   46         | . abdecor2 '[' zexpr ']'
   47 abdecor3: . '(' ')'
   47         | '(' . ')'
   48         | . '[' zexpr ']'
   49         | . '(' abdecor1 ')'
   49         | '(' . abdecor1 ')'

    '*'  posunout a přejít do stavu 228
    '['  posunout a přejít do stavu 199
    '('  posunout a přejít do stavu 229
    ')'  posunout a přejít do stavu 273

    abdecor1  přejít do stavu 274
    abdecor2  přejít do stavu 204
    abdecor3  přejít do stavu 205


State 230

  139 xuexpr: '(' tlist abdecor . ')' xuexpr
  140       | '(' tlist abdecor . ')' '{' ilist '}'

    ')'  posunout a přejít do stavu 295


State 231

   39 abdecor: . %empty  [')']
   40        | . abdecor1
   41 abdecor1: . '*' zgnlist
   42         | . '*' zgnlist abdecor1
   43         | . abdecor2
   44 abdecor2: . abdecor3
   45         | . abdecor2 '(' zarglist ')'
   46         | . abdecor2 '[' zexpr ']'
   47 abdecor3: . '(' ')'
   48         | . '[' zexpr ']'
   49         | . '(' abdecor1 ')'
  153 pexpr: LSIZEOF '(' tlist . abdecor ')'

    '*'  posunout a přejít do stavu 228
    '['  posunout a přejít do stavu 199
    '('  posunout a přejít do stavu 229

    $výchozí  reduce using rule 39 (abdecor)

    abdecor   přejít do stavu 296
    abdecor1  přejít do stavu 203
    abdecor2  přejít do stavu 204
    abdecor3  přejít do stavu 205


State 232

   39 abdecor: . %empty  [')']
   40        | . abdecor1
   41 abdecor1: . '*' zgnlist
   42         | . '*' zgnlist abdecor1
   43         | . abdecor2
   44 abdecor2: . abdecor3
   45         | . abdecor2 '(' zarglist ')'
   46         | . abdecor2 '[' zexpr ']'
   47 abdecor3: . '(' ')'
   48         | . '[' zexpr ']'
   49         | . '(' abdecor1 ')'
  154 pexpr: LSIGNOF '(' tlist . abdecor ')'

    '*'  posunout a přejít do stavu 228
    '['  posunout a přejít do stavu 199
    '('  posunout a přejít do stavu 229

    $výchozí  reduce using rule 39 (abdecor)

    abdecor   přejít do stavu 297
    abdecor1  přejít do stavu 203
    abdecor2  přejít do stavu 204
    abdecor3  přejít do stavu 205


State 233

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  127     | expr '=' expr .  [';', ',', ':', ')', ']', '}']
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 127 (expr)

    Conflict between rule 127 and token '=' resolved as shift (%right '=').
    Conflict between rule 127 and token LPE resolved as shift (%right LPE).
    Conflict between rule 127 and token LME resolved as shift (%right LME).
    Conflict between rule 127 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 127 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 127 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 127 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 127 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 127 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 127 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 127 and token LORE resolved as shift (%right LORE).
    Conflict between rule 127 and token '?' resolved as shift ('=' < '?').
    Conflict between rule 127 and token LOROR resolved as shift ('=' < LOROR).
    Conflict between rule 127 and token LANDAND resolved as shift ('=' < LANDAND).
    Conflict between rule 127 and token '|' resolved as shift ('=' < '|').
    Conflict between rule 127 and token '^' resolved as shift ('=' < '^').
    Conflict between rule 127 and token '&' resolved as shift ('=' < '&').
    Conflict between rule 127 and token LEQ resolved as shift ('=' < LEQ).
    Conflict between rule 127 and token LNE resolved as shift ('=' < LNE).
    Conflict between rule 127 and token '<' resolved as shift ('=' < '<').
    Conflict between rule 127 and token '>' resolved as shift ('=' < '>').
    Conflict between rule 127 and token LLE resolved as shift ('=' < LLE).
    Conflict between rule 127 and token LGE resolved as shift ('=' < LGE).
    Conflict between rule 127 and token LLSH resolved as shift ('=' < LLSH).
    Conflict between rule 127 and token LRSH resolved as shift ('=' < LRSH).
    Conflict between rule 127 and token '+' resolved as shift ('=' < '+').
    Conflict between rule 127 and token '-' resolved as shift ('=' < '-').
    Conflict between rule 127 and token '*' resolved as shift ('=' < '*').
    Conflict between rule 127 and token '/' resolved as shift ('=' < '/').
    Conflict between rule 127 and token '%' resolved as shift ('=' < '%').


State 234

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  128     | expr LPE expr .  [';', ',', ':', ')', ']', '}']
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 128 (expr)

    Conflict between rule 128 and token '=' resolved as shift (%right '=').
    Conflict between rule 128 and token LPE resolved as shift (%right LPE).
    Conflict between rule 128 and token LME resolved as shift (%right LME).
    Conflict between rule 128 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 128 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 128 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 128 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 128 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 128 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 128 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 128 and token LORE resolved as shift (%right LORE).
    Conflict between rule 128 and token '?' resolved as shift (LPE < '?').
    Conflict between rule 128 and token LOROR resolved as shift (LPE < LOROR).
    Conflict between rule 128 and token LANDAND resolved as shift (LPE < LANDAND).
    Conflict between rule 128 and token '|' resolved as shift (LPE < '|').
    Conflict between rule 128 and token '^' resolved as shift (LPE < '^').
    Conflict between rule 128 and token '&' resolved as shift (LPE < '&').
    Conflict between rule 128 and token LEQ resolved as shift (LPE < LEQ).
    Conflict between rule 128 and token LNE resolved as shift (LPE < LNE).
    Conflict between rule 128 and token '<' resolved as shift (LPE < '<').
    Conflict between rule 128 and token '>' resolved as shift (LPE < '>').
    Conflict between rule 128 and token LLE resolved as shift (LPE < LLE).
    Conflict between rule 128 and token LGE resolved as shift (LPE < LGE).
    Conflict between rule 128 and token LLSH resolved as shift (LPE < LLSH).
    Conflict between rule 128 and token LRSH resolved as shift (LPE < LRSH).
    Conflict between rule 128 and token '+' resolved as shift (LPE < '+').
    Conflict between rule 128 and token '-' resolved as shift (LPE < '-').
    Conflict between rule 128 and token '*' resolved as shift (LPE < '*').
    Conflict between rule 128 and token '/' resolved as shift (LPE < '/').
    Conflict between rule 128 and token '%' resolved as shift (LPE < '%').


State 235

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  129     | expr LME expr .  [';', ',', ':', ')', ']', '}']
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 129 (expr)

    Conflict between rule 129 and token '=' resolved as shift (%right '=').
    Conflict between rule 129 and token LPE resolved as shift (%right LPE).
    Conflict between rule 129 and token LME resolved as shift (%right LME).
    Conflict between rule 129 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 129 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 129 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 129 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 129 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 129 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 129 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 129 and token LORE resolved as shift (%right LORE).
    Conflict between rule 129 and token '?' resolved as shift (LME < '?').
    Conflict between rule 129 and token LOROR resolved as shift (LME < LOROR).
    Conflict between rule 129 and token LANDAND resolved as shift (LME < LANDAND).
    Conflict between rule 129 and token '|' resolved as shift (LME < '|').
    Conflict between rule 129 and token '^' resolved as shift (LME < '^').
    Conflict between rule 129 and token '&' resolved as shift (LME < '&').
    Conflict between rule 129 and token LEQ resolved as shift (LME < LEQ).
    Conflict between rule 129 and token LNE resolved as shift (LME < LNE).
    Conflict between rule 129 and token '<' resolved as shift (LME < '<').
    Conflict between rule 129 and token '>' resolved as shift (LME < '>').
    Conflict between rule 129 and token LLE resolved as shift (LME < LLE).
    Conflict between rule 129 and token LGE resolved as shift (LME < LGE).
    Conflict between rule 129 and token LLSH resolved as shift (LME < LLSH).
    Conflict between rule 129 and token LRSH resolved as shift (LME < LRSH).
    Conflict between rule 129 and token '+' resolved as shift (LME < '+').
    Conflict between rule 129 and token '-' resolved as shift (LME < '-').
    Conflict between rule 129 and token '*' resolved as shift (LME < '*').
    Conflict between rule 129 and token '/' resolved as shift (LME < '/').
    Conflict between rule 129 and token '%' resolved as shift (LME < '%').


State 236

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  130     | expr LMLE expr .  [';', ',', ':', ')', ']', '}']
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 130 (expr)

    Conflict between rule 130 and token '=' resolved as shift (%right '=').
    Conflict between rule 130 and token LPE resolved as shift (%right LPE).
    Conflict between rule 130 and token LME resolved as shift (%right LME).
    Conflict between rule 130 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 130 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 130 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 130 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 130 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 130 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 130 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 130 and token LORE resolved as shift (%right LORE).
    Conflict between rule 130 and token '?' resolved as shift (LMLE < '?').
    Conflict between rule 130 and token LOROR resolved as shift (LMLE < LOROR).
    Conflict between rule 130 and token LANDAND resolved as shift (LMLE < LANDAND).
    Conflict between rule 130 and token '|' resolved as shift (LMLE < '|').
    Conflict between rule 130 and token '^' resolved as shift (LMLE < '^').
    Conflict between rule 130 and token '&' resolved as shift (LMLE < '&').
    Conflict between rule 130 and token LEQ resolved as shift (LMLE < LEQ).
    Conflict between rule 130 and token LNE resolved as shift (LMLE < LNE).
    Conflict between rule 130 and token '<' resolved as shift (LMLE < '<').
    Conflict between rule 130 and token '>' resolved as shift (LMLE < '>').
    Conflict between rule 130 and token LLE resolved as shift (LMLE < LLE).
    Conflict between rule 130 and token LGE resolved as shift (LMLE < LGE).
    Conflict between rule 130 and token LLSH resolved as shift (LMLE < LLSH).
    Conflict between rule 130 and token LRSH resolved as shift (LMLE < LRSH).
    Conflict between rule 130 and token '+' resolved as shift (LMLE < '+').
    Conflict between rule 130 and token '-' resolved as shift (LMLE < '-').
    Conflict between rule 130 and token '*' resolved as shift (LMLE < '*').
    Conflict between rule 130 and token '/' resolved as shift (LMLE < '/').
    Conflict between rule 130 and token '%' resolved as shift (LMLE < '%').


State 237

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  131     | expr LDVE expr .  [';', ',', ':', ')', ']', '}']
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 131 (expr)

    Conflict between rule 131 and token '=' resolved as shift (%right '=').
    Conflict between rule 131 and token LPE resolved as shift (%right LPE).
    Conflict between rule 131 and token LME resolved as shift (%right LME).
    Conflict between rule 131 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 131 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 131 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 131 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 131 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 131 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 131 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 131 and token LORE resolved as shift (%right LORE).
    Conflict between rule 131 and token '?' resolved as shift (LDVE < '?').
    Conflict between rule 131 and token LOROR resolved as shift (LDVE < LOROR).
    Conflict between rule 131 and token LANDAND resolved as shift (LDVE < LANDAND).
    Conflict between rule 131 and token '|' resolved as shift (LDVE < '|').
    Conflict between rule 131 and token '^' resolved as shift (LDVE < '^').
    Conflict between rule 131 and token '&' resolved as shift (LDVE < '&').
    Conflict between rule 131 and token LEQ resolved as shift (LDVE < LEQ).
    Conflict between rule 131 and token LNE resolved as shift (LDVE < LNE).
    Conflict between rule 131 and token '<' resolved as shift (LDVE < '<').
    Conflict between rule 131 and token '>' resolved as shift (LDVE < '>').
    Conflict between rule 131 and token LLE resolved as shift (LDVE < LLE).
    Conflict between rule 131 and token LGE resolved as shift (LDVE < LGE).
    Conflict between rule 131 and token LLSH resolved as shift (LDVE < LLSH).
    Conflict between rule 131 and token LRSH resolved as shift (LDVE < LRSH).
    Conflict between rule 131 and token '+' resolved as shift (LDVE < '+').
    Conflict between rule 131 and token '-' resolved as shift (LDVE < '-').
    Conflict between rule 131 and token '*' resolved as shift (LDVE < '*').
    Conflict between rule 131 and token '/' resolved as shift (LDVE < '/').
    Conflict between rule 131 and token '%' resolved as shift (LDVE < '%').


State 238

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  132     | expr LMDE expr .  [';', ',', ':', ')', ']', '}']
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 132 (expr)

    Conflict between rule 132 and token '=' resolved as shift (%right '=').
    Conflict between rule 132 and token LPE resolved as shift (%right LPE).
    Conflict between rule 132 and token LME resolved as shift (%right LME).
    Conflict between rule 132 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 132 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 132 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 132 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 132 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 132 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 132 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 132 and token LORE resolved as shift (%right LORE).
    Conflict between rule 132 and token '?' resolved as shift (LMDE < '?').
    Conflict between rule 132 and token LOROR resolved as shift (LMDE < LOROR).
    Conflict between rule 132 and token LANDAND resolved as shift (LMDE < LANDAND).
    Conflict between rule 132 and token '|' resolved as shift (LMDE < '|').
    Conflict between rule 132 and token '^' resolved as shift (LMDE < '^').
    Conflict between rule 132 and token '&' resolved as shift (LMDE < '&').
    Conflict between rule 132 and token LEQ resolved as shift (LMDE < LEQ).
    Conflict between rule 132 and token LNE resolved as shift (LMDE < LNE).
    Conflict between rule 132 and token '<' resolved as shift (LMDE < '<').
    Conflict between rule 132 and token '>' resolved as shift (LMDE < '>').
    Conflict between rule 132 and token LLE resolved as shift (LMDE < LLE).
    Conflict between rule 132 and token LGE resolved as shift (LMDE < LGE).
    Conflict between rule 132 and token LLSH resolved as shift (LMDE < LLSH).
    Conflict between rule 132 and token LRSH resolved as shift (LMDE < LRSH).
    Conflict between rule 132 and token '+' resolved as shift (LMDE < '+').
    Conflict between rule 132 and token '-' resolved as shift (LMDE < '-').
    Conflict between rule 132 and token '*' resolved as shift (LMDE < '*').
    Conflict between rule 132 and token '/' resolved as shift (LMDE < '/').
    Conflict between rule 132 and token '%' resolved as shift (LMDE < '%').


State 239

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  134     | expr LRSHE expr .  [';', ',', ':', ')', ']', '}']
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 134 (expr)

    Conflict between rule 134 and token '=' resolved as shift (%right '=').
    Conflict between rule 134 and token LPE resolved as shift (%right LPE).
    Conflict between rule 134 and token LME resolved as shift (%right LME).
    Conflict between rule 134 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 134 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 134 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 134 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 134 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 134 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 134 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 134 and token LORE resolved as shift (%right LORE).
    Conflict between rule 134 and token '?' resolved as shift (LRSHE < '?').
    Conflict between rule 134 and token LOROR resolved as shift (LRSHE < LOROR).
    Conflict between rule 134 and token LANDAND resolved as shift (LRSHE < LANDAND).
    Conflict between rule 134 and token '|' resolved as shift (LRSHE < '|').
    Conflict between rule 134 and token '^' resolved as shift (LRSHE < '^').
    Conflict between rule 134 and token '&' resolved as shift (LRSHE < '&').
    Conflict between rule 134 and token LEQ resolved as shift (LRSHE < LEQ).
    Conflict between rule 134 and token LNE resolved as shift (LRSHE < LNE).
    Conflict between rule 134 and token '<' resolved as shift (LRSHE < '<').
    Conflict between rule 134 and token '>' resolved as shift (LRSHE < '>').
    Conflict between rule 134 and token LLE resolved as shift (LRSHE < LLE).
    Conflict between rule 134 and token LGE resolved as shift (LRSHE < LGE).
    Conflict between rule 134 and token LLSH resolved as shift (LRSHE < LLSH).
    Conflict between rule 134 and token LRSH resolved as shift (LRSHE < LRSH).
    Conflict between rule 134 and token '+' resolved as shift (LRSHE < '+').
    Conflict between rule 134 and token '-' resolved as shift (LRSHE < '-').
    Conflict between rule 134 and token '*' resolved as shift (LRSHE < '*').
    Conflict between rule 134 and token '/' resolved as shift (LRSHE < '/').
    Conflict between rule 134 and token '%' resolved as shift (LRSHE < '%').


State 240

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  133     | expr LLSHE expr .  [';', ',', ':', ')', ']', '}']
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 133 (expr)

    Conflict between rule 133 and token '=' resolved as shift (%right '=').
    Conflict between rule 133 and token LPE resolved as shift (%right LPE).
    Conflict between rule 133 and token LME resolved as shift (%right LME).
    Conflict between rule 133 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 133 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 133 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 133 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 133 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 133 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 133 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 133 and token LORE resolved as shift (%right LORE).
    Conflict between rule 133 and token '?' resolved as shift (LLSHE < '?').
    Conflict between rule 133 and token LOROR resolved as shift (LLSHE < LOROR).
    Conflict between rule 133 and token LANDAND resolved as shift (LLSHE < LANDAND).
    Conflict between rule 133 and token '|' resolved as shift (LLSHE < '|').
    Conflict between rule 133 and token '^' resolved as shift (LLSHE < '^').
    Conflict between rule 133 and token '&' resolved as shift (LLSHE < '&').
    Conflict between rule 133 and token LEQ resolved as shift (LLSHE < LEQ).
    Conflict between rule 133 and token LNE resolved as shift (LLSHE < LNE).
    Conflict between rule 133 and token '<' resolved as shift (LLSHE < '<').
    Conflict between rule 133 and token '>' resolved as shift (LLSHE < '>').
    Conflict between rule 133 and token LLE resolved as shift (LLSHE < LLE).
    Conflict between rule 133 and token LGE resolved as shift (LLSHE < LGE).
    Conflict between rule 133 and token LLSH resolved as shift (LLSHE < LLSH).
    Conflict between rule 133 and token LRSH resolved as shift (LLSHE < LRSH).
    Conflict between rule 133 and token '+' resolved as shift (LLSHE < '+').
    Conflict between rule 133 and token '-' resolved as shift (LLSHE < '-').
    Conflict between rule 133 and token '*' resolved as shift (LLSHE < '*').
    Conflict between rule 133 and token '/' resolved as shift (LLSHE < '/').
    Conflict between rule 133 and token '%' resolved as shift (LLSHE < '%').


State 241

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  135     | expr LANDE expr .  [';', ',', ':', ')', ']', '}']
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 135 (expr)

    Conflict between rule 135 and token '=' resolved as shift (%right '=').
    Conflict between rule 135 and token LPE resolved as shift (%right LPE).
    Conflict between rule 135 and token LME resolved as shift (%right LME).
    Conflict between rule 135 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 135 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 135 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 135 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 135 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 135 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 135 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 135 and token LORE resolved as shift (%right LORE).
    Conflict between rule 135 and token '?' resolved as shift (LANDE < '?').
    Conflict between rule 135 and token LOROR resolved as shift (LANDE < LOROR).
    Conflict between rule 135 and token LANDAND resolved as shift (LANDE < LANDAND).
    Conflict between rule 135 and token '|' resolved as shift (LANDE < '|').
    Conflict between rule 135 and token '^' resolved as shift (LANDE < '^').
    Conflict between rule 135 and token '&' resolved as shift (LANDE < '&').
    Conflict between rule 135 and token LEQ resolved as shift (LANDE < LEQ).
    Conflict between rule 135 and token LNE resolved as shift (LANDE < LNE).
    Conflict between rule 135 and token '<' resolved as shift (LANDE < '<').
    Conflict between rule 135 and token '>' resolved as shift (LANDE < '>').
    Conflict between rule 135 and token LLE resolved as shift (LANDE < LLE).
    Conflict between rule 135 and token LGE resolved as shift (LANDE < LGE).
    Conflict between rule 135 and token LLSH resolved as shift (LANDE < LLSH).
    Conflict between rule 135 and token LRSH resolved as shift (LANDE < LRSH).
    Conflict between rule 135 and token '+' resolved as shift (LANDE < '+').
    Conflict between rule 135 and token '-' resolved as shift (LANDE < '-').
    Conflict between rule 135 and token '*' resolved as shift (LANDE < '*').
    Conflict between rule 135 and token '/' resolved as shift (LANDE < '/').
    Conflict between rule 135 and token '%' resolved as shift (LANDE < '%').


State 242

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  136     | expr LXORE expr .  [';', ',', ':', ')', ']', '}']
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 136 (expr)

    Conflict between rule 136 and token '=' resolved as shift (%right '=').
    Conflict between rule 136 and token LPE resolved as shift (%right LPE).
    Conflict between rule 136 and token LME resolved as shift (%right LME).
    Conflict between rule 136 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 136 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 136 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 136 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 136 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 136 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 136 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 136 and token LORE resolved as shift (%right LORE).
    Conflict between rule 136 and token '?' resolved as shift (LXORE < '?').
    Conflict between rule 136 and token LOROR resolved as shift (LXORE < LOROR).
    Conflict between rule 136 and token LANDAND resolved as shift (LXORE < LANDAND).
    Conflict between rule 136 and token '|' resolved as shift (LXORE < '|').
    Conflict between rule 136 and token '^' resolved as shift (LXORE < '^').
    Conflict between rule 136 and token '&' resolved as shift (LXORE < '&').
    Conflict between rule 136 and token LEQ resolved as shift (LXORE < LEQ).
    Conflict between rule 136 and token LNE resolved as shift (LXORE < LNE).
    Conflict between rule 136 and token '<' resolved as shift (LXORE < '<').
    Conflict between rule 136 and token '>' resolved as shift (LXORE < '>').
    Conflict between rule 136 and token LLE resolved as shift (LXORE < LLE).
    Conflict between rule 136 and token LGE resolved as shift (LXORE < LGE).
    Conflict between rule 136 and token LLSH resolved as shift (LXORE < LLSH).
    Conflict between rule 136 and token LRSH resolved as shift (LXORE < LRSH).
    Conflict between rule 136 and token '+' resolved as shift (LXORE < '+').
    Conflict between rule 136 and token '-' resolved as shift (LXORE < '-').
    Conflict between rule 136 and token '*' resolved as shift (LXORE < '*').
    Conflict between rule 136 and token '/' resolved as shift (LXORE < '/').
    Conflict between rule 136 and token '%' resolved as shift (LXORE < '%').


State 243

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr
  137     | expr LORE expr .  [';', ',', ':', ')', ']', '}']

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 137 (expr)

    Conflict between rule 137 and token '=' resolved as shift (%right '=').
    Conflict between rule 137 and token LPE resolved as shift (%right LPE).
    Conflict between rule 137 and token LME resolved as shift (%right LME).
    Conflict between rule 137 and token LMLE resolved as shift (%right LMLE).
    Conflict between rule 137 and token LDVE resolved as shift (%right LDVE).
    Conflict between rule 137 and token LMDE resolved as shift (%right LMDE).
    Conflict between rule 137 and token LRSHE resolved as shift (%right LRSHE).
    Conflict between rule 137 and token LLSHE resolved as shift (%right LLSHE).
    Conflict between rule 137 and token LANDE resolved as shift (%right LANDE).
    Conflict between rule 137 and token LXORE resolved as shift (%right LXORE).
    Conflict between rule 137 and token LORE resolved as shift (%right LORE).
    Conflict between rule 137 and token '?' resolved as shift (LORE < '?').
    Conflict between rule 137 and token LOROR resolved as shift (LORE < LOROR).
    Conflict between rule 137 and token LANDAND resolved as shift (LORE < LANDAND).
    Conflict between rule 137 and token '|' resolved as shift (LORE < '|').
    Conflict between rule 137 and token '^' resolved as shift (LORE < '^').
    Conflict between rule 137 and token '&' resolved as shift (LORE < '&').
    Conflict between rule 137 and token LEQ resolved as shift (LORE < LEQ).
    Conflict between rule 137 and token LNE resolved as shift (LORE < LNE).
    Conflict between rule 137 and token '<' resolved as shift (LORE < '<').
    Conflict between rule 137 and token '>' resolved as shift (LORE < '>').
    Conflict between rule 137 and token LLE resolved as shift (LORE < LLE).
    Conflict between rule 137 and token LGE resolved as shift (LORE < LGE).
    Conflict between rule 137 and token LLSH resolved as shift (LORE < LLSH).
    Conflict between rule 137 and token LRSH resolved as shift (LORE < LRSH).
    Conflict between rule 137 and token '+' resolved as shift (LORE < '+').
    Conflict between rule 137 and token '-' resolved as shift (LORE < '-').
    Conflict between rule 137 and token '*' resolved as shift (LORE < '*').
    Conflict between rule 137 and token '/' resolved as shift (LORE < '/').
    Conflict between rule 137 and token '%' resolved as shift (LORE < '%').


State 244

  106 cexpr: cexpr . ',' cexpr
  126 expr: expr '?' cexpr . ':' expr

    ','  posunout a přejít do stavu 226
    ':'  posunout a přejít do stavu 298


State 245

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  125     | expr LOROR expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, ')', ']', '}']
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 125 (expr)

    Conflict between rule 125 and token '=' resolved as reduce ('=' < LOROR).
    Conflict between rule 125 and token LPE resolved as reduce (LPE < LOROR).
    Conflict between rule 125 and token LME resolved as reduce (LME < LOROR).
    Conflict between rule 125 and token LMLE resolved as reduce (LMLE < LOROR).
    Conflict between rule 125 and token LDVE resolved as reduce (LDVE < LOROR).
    Conflict between rule 125 and token LMDE resolved as reduce (LMDE < LOROR).
    Conflict between rule 125 and token LRSHE resolved as reduce (LRSHE < LOROR).
    Conflict between rule 125 and token LLSHE resolved as reduce (LLSHE < LOROR).
    Conflict between rule 125 and token LANDE resolved as reduce (LANDE < LOROR).
    Conflict between rule 125 and token LXORE resolved as reduce (LXORE < LOROR).
    Conflict between rule 125 and token LORE resolved as reduce (LORE < LOROR).
    Conflict between rule 125 and token '?' resolved as reduce ('?' < LOROR).
    Conflict between rule 125 and token LOROR resolved as reduce (%left LOROR).
    Conflict between rule 125 and token LANDAND resolved as shift (LOROR < LANDAND).
    Conflict between rule 125 and token '|' resolved as shift (LOROR < '|').
    Conflict between rule 125 and token '^' resolved as shift (LOROR < '^').
    Conflict between rule 125 and token '&' resolved as shift (LOROR < '&').
    Conflict between rule 125 and token LEQ resolved as shift (LOROR < LEQ).
    Conflict between rule 125 and token LNE resolved as shift (LOROR < LNE).
    Conflict between rule 125 and token '<' resolved as shift (LOROR < '<').
    Conflict between rule 125 and token '>' resolved as shift (LOROR < '>').
    Conflict between rule 125 and token LLE resolved as shift (LOROR < LLE).
    Conflict between rule 125 and token LGE resolved as shift (LOROR < LGE).
    Conflict between rule 125 and token LLSH resolved as shift (LOROR < LLSH).
    Conflict between rule 125 and token LRSH resolved as shift (LOROR < LRSH).
    Conflict between rule 125 and token '+' resolved as shift (LOROR < '+').
    Conflict between rule 125 and token '-' resolved as shift (LOROR < '-').
    Conflict between rule 125 and token '*' resolved as shift (LOROR < '*').
    Conflict between rule 125 and token '/' resolved as shift (LOROR < '/').
    Conflict between rule 125 and token '%' resolved as shift (LOROR < '%').


State 246

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  124     | expr LANDAND expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, ')', ']', '}']
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '|'   posunout a přejít do stavu 171
    '^'   posunout a přejít do stavu 172
    '&'   posunout a přejít do stavu 173
    LEQ   posunout a přejít do stavu 174
    LNE   posunout a přejít do stavu 175
    '<'   posunout a přejít do stavu 176
    '>'   posunout a přejít do stavu 177
    LLE   posunout a přejít do stavu 178
    LGE   posunout a přejít do stavu 179
    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 124 (expr)

    Conflict between rule 124 and token '=' resolved as reduce ('=' < LANDAND).
    Conflict between rule 124 and token LPE resolved as reduce (LPE < LANDAND).
    Conflict between rule 124 and token LME resolved as reduce (LME < LANDAND).
    Conflict between rule 124 and token LMLE resolved as reduce (LMLE < LANDAND).
    Conflict between rule 124 and token LDVE resolved as reduce (LDVE < LANDAND).
    Conflict between rule 124 and token LMDE resolved as reduce (LMDE < LANDAND).
    Conflict between rule 124 and token LRSHE resolved as reduce (LRSHE < LANDAND).
    Conflict between rule 124 and token LLSHE resolved as reduce (LLSHE < LANDAND).
    Conflict between rule 124 and token LANDE resolved as reduce (LANDE < LANDAND).
    Conflict between rule 124 and token LXORE resolved as reduce (LXORE < LANDAND).
    Conflict between rule 124 and token LORE resolved as reduce (LORE < LANDAND).
    Conflict between rule 124 and token '?' resolved as reduce ('?' < LANDAND).
    Conflict between rule 124 and token LOROR resolved as reduce (LOROR < LANDAND).
    Conflict between rule 124 and token LANDAND resolved as reduce (%left LANDAND).
    Conflict between rule 124 and token '|' resolved as shift (LANDAND < '|').
    Conflict between rule 124 and token '^' resolved as shift (LANDAND < '^').
    Conflict between rule 124 and token '&' resolved as shift (LANDAND < '&').
    Conflict between rule 124 and token LEQ resolved as shift (LANDAND < LEQ).
    Conflict between rule 124 and token LNE resolved as shift (LANDAND < LNE).
    Conflict between rule 124 and token '<' resolved as shift (LANDAND < '<').
    Conflict between rule 124 and token '>' resolved as shift (LANDAND < '>').
    Conflict between rule 124 and token LLE resolved as shift (LANDAND < LLE).
    Conflict between rule 124 and token LGE resolved as shift (LANDAND < LGE).
    Conflict between rule 124 and token LLSH resolved as shift (LANDAND < LLSH).
    Conflict between rule 124 and token LRSH resolved as shift (LANDAND < LRSH).
    Conflict between rule 124 and token '+' resolved as shift (LANDAND < '+').
    Conflict between rule 124 and token '-' resolved as shift (LANDAND < '-').
    Conflict between rule 124 and token '*' resolved as shift (LANDAND < '*').
    Conflict between rule 124 and token '/' resolved as shift (LANDAND < '/').
    Conflict between rule 124 and token '%' resolved as shift (LANDAND < '%').


State 247

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  123     | expr '|' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', ')', ']', '}']
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '^'   posunout a přejít do stavu 172
    '&'   posunout a přejít do stavu 173
    LEQ   posunout a přejít do stavu 174
    LNE   posunout a přejít do stavu 175
    '<'   posunout a přejít do stavu 176
    '>'   posunout a přejít do stavu 177
    LLE   posunout a přejít do stavu 178
    LGE   posunout a přejít do stavu 179
    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 123 (expr)

    Conflict between rule 123 and token '=' resolved as reduce ('=' < '|').
    Conflict between rule 123 and token LPE resolved as reduce (LPE < '|').
    Conflict between rule 123 and token LME resolved as reduce (LME < '|').
    Conflict between rule 123 and token LMLE resolved as reduce (LMLE < '|').
    Conflict between rule 123 and token LDVE resolved as reduce (LDVE < '|').
    Conflict between rule 123 and token LMDE resolved as reduce (LMDE < '|').
    Conflict between rule 123 and token LRSHE resolved as reduce (LRSHE < '|').
    Conflict between rule 123 and token LLSHE resolved as reduce (LLSHE < '|').
    Conflict between rule 123 and token LANDE resolved as reduce (LANDE < '|').
    Conflict between rule 123 and token LXORE resolved as reduce (LXORE < '|').
    Conflict between rule 123 and token LORE resolved as reduce (LORE < '|').
    Conflict between rule 123 and token '?' resolved as reduce ('?' < '|').
    Conflict between rule 123 and token LOROR resolved as reduce (LOROR < '|').
    Conflict between rule 123 and token LANDAND resolved as reduce (LANDAND < '|').
    Conflict between rule 123 and token '|' resolved as reduce (%left '|').
    Conflict between rule 123 and token '^' resolved as shift ('|' < '^').
    Conflict between rule 123 and token '&' resolved as shift ('|' < '&').
    Conflict between rule 123 and token LEQ resolved as shift ('|' < LEQ).
    Conflict between rule 123 and token LNE resolved as shift ('|' < LNE).
    Conflict between rule 123 and token '<' resolved as shift ('|' < '<').
    Conflict between rule 123 and token '>' resolved as shift ('|' < '>').
    Conflict between rule 123 and token LLE resolved as shift ('|' < LLE).
    Conflict between rule 123 and token LGE resolved as shift ('|' < LGE).
    Conflict between rule 123 and token LLSH resolved as shift ('|' < LLSH).
    Conflict between rule 123 and token LRSH resolved as shift ('|' < LRSH).
    Conflict between rule 123 and token '+' resolved as shift ('|' < '+').
    Conflict between rule 123 and token '-' resolved as shift ('|' < '-').
    Conflict between rule 123 and token '*' resolved as shift ('|' < '*').
    Conflict between rule 123 and token '/' resolved as shift ('|' < '/').
    Conflict between rule 123 and token '%' resolved as shift ('|' < '%').


State 248

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  122     | expr '^' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', ')', ']', '}']
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '&'   posunout a přejít do stavu 173
    LEQ   posunout a přejít do stavu 174
    LNE   posunout a přejít do stavu 175
    '<'   posunout a přejít do stavu 176
    '>'   posunout a přejít do stavu 177
    LLE   posunout a přejít do stavu 178
    LGE   posunout a přejít do stavu 179
    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 122 (expr)

    Conflict between rule 122 and token '=' resolved as reduce ('=' < '^').
    Conflict between rule 122 and token LPE resolved as reduce (LPE < '^').
    Conflict between rule 122 and token LME resolved as reduce (LME < '^').
    Conflict between rule 122 and token LMLE resolved as reduce (LMLE < '^').
    Conflict between rule 122 and token LDVE resolved as reduce (LDVE < '^').
    Conflict between rule 122 and token LMDE resolved as reduce (LMDE < '^').
    Conflict between rule 122 and token LRSHE resolved as reduce (LRSHE < '^').
    Conflict between rule 122 and token LLSHE resolved as reduce (LLSHE < '^').
    Conflict between rule 122 and token LANDE resolved as reduce (LANDE < '^').
    Conflict between rule 122 and token LXORE resolved as reduce (LXORE < '^').
    Conflict between rule 122 and token LORE resolved as reduce (LORE < '^').
    Conflict between rule 122 and token '?' resolved as reduce ('?' < '^').
    Conflict between rule 122 and token LOROR resolved as reduce (LOROR < '^').
    Conflict between rule 122 and token LANDAND resolved as reduce (LANDAND < '^').
    Conflict between rule 122 and token '|' resolved as reduce ('|' < '^').
    Conflict between rule 122 and token '^' resolved as reduce (%left '^').
    Conflict between rule 122 and token '&' resolved as shift ('^' < '&').
    Conflict between rule 122 and token LEQ resolved as shift ('^' < LEQ).
    Conflict between rule 122 and token LNE resolved as shift ('^' < LNE).
    Conflict between rule 122 and token '<' resolved as shift ('^' < '<').
    Conflict between rule 122 and token '>' resolved as shift ('^' < '>').
    Conflict between rule 122 and token LLE resolved as shift ('^' < LLE).
    Conflict between rule 122 and token LGE resolved as shift ('^' < LGE).
    Conflict between rule 122 and token LLSH resolved as shift ('^' < LLSH).
    Conflict between rule 122 and token LRSH resolved as shift ('^' < LRSH).
    Conflict between rule 122 and token '+' resolved as shift ('^' < '+').
    Conflict between rule 122 and token '-' resolved as shift ('^' < '-').
    Conflict between rule 122 and token '*' resolved as shift ('^' < '*').
    Conflict between rule 122 and token '/' resolved as shift ('^' < '/').
    Conflict between rule 122 and token '%' resolved as shift ('^' < '%').


State 249

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  121     | expr '&' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', ')', ']', '}']
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    LEQ   posunout a přejít do stavu 174
    LNE   posunout a přejít do stavu 175
    '<'   posunout a přejít do stavu 176
    '>'   posunout a přejít do stavu 177
    LLE   posunout a přejít do stavu 178
    LGE   posunout a přejít do stavu 179
    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 121 (expr)

    Conflict between rule 121 and token '=' resolved as reduce ('=' < '&').
    Conflict between rule 121 and token LPE resolved as reduce (LPE < '&').
    Conflict between rule 121 and token LME resolved as reduce (LME < '&').
    Conflict between rule 121 and token LMLE resolved as reduce (LMLE < '&').
    Conflict between rule 121 and token LDVE resolved as reduce (LDVE < '&').
    Conflict between rule 121 and token LMDE resolved as reduce (LMDE < '&').
    Conflict between rule 121 and token LRSHE resolved as reduce (LRSHE < '&').
    Conflict between rule 121 and token LLSHE resolved as reduce (LLSHE < '&').
    Conflict between rule 121 and token LANDE resolved as reduce (LANDE < '&').
    Conflict between rule 121 and token LXORE resolved as reduce (LXORE < '&').
    Conflict between rule 121 and token LORE resolved as reduce (LORE < '&').
    Conflict between rule 121 and token '?' resolved as reduce ('?' < '&').
    Conflict between rule 121 and token LOROR resolved as reduce (LOROR < '&').
    Conflict between rule 121 and token LANDAND resolved as reduce (LANDAND < '&').
    Conflict between rule 121 and token '|' resolved as reduce ('|' < '&').
    Conflict between rule 121 and token '^' resolved as reduce ('^' < '&').
    Conflict between rule 121 and token '&' resolved as reduce (%left '&').
    Conflict between rule 121 and token LEQ resolved as shift ('&' < LEQ).
    Conflict between rule 121 and token LNE resolved as shift ('&' < LNE).
    Conflict between rule 121 and token '<' resolved as shift ('&' < '<').
    Conflict between rule 121 and token '>' resolved as shift ('&' < '>').
    Conflict between rule 121 and token LLE resolved as shift ('&' < LLE).
    Conflict between rule 121 and token LGE resolved as shift ('&' < LGE).
    Conflict between rule 121 and token LLSH resolved as shift ('&' < LLSH).
    Conflict between rule 121 and token LRSH resolved as shift ('&' < LRSH).
    Conflict between rule 121 and token '+' resolved as shift ('&' < '+').
    Conflict between rule 121 and token '-' resolved as shift ('&' < '-').
    Conflict between rule 121 and token '*' resolved as shift ('&' < '*').
    Conflict between rule 121 and token '/' resolved as shift ('&' < '/').
    Conflict between rule 121 and token '%' resolved as shift ('&' < '%').


State 250

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  119     | expr LEQ expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, ')', ']', '}']
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '<'   posunout a přejít do stavu 176
    '>'   posunout a přejít do stavu 177
    LLE   posunout a přejít do stavu 178
    LGE   posunout a přejít do stavu 179
    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 119 (expr)

    Conflict between rule 119 and token '=' resolved as reduce ('=' < LEQ).
    Conflict between rule 119 and token LPE resolved as reduce (LPE < LEQ).
    Conflict between rule 119 and token LME resolved as reduce (LME < LEQ).
    Conflict between rule 119 and token LMLE resolved as reduce (LMLE < LEQ).
    Conflict between rule 119 and token LDVE resolved as reduce (LDVE < LEQ).
    Conflict between rule 119 and token LMDE resolved as reduce (LMDE < LEQ).
    Conflict between rule 119 and token LRSHE resolved as reduce (LRSHE < LEQ).
    Conflict between rule 119 and token LLSHE resolved as reduce (LLSHE < LEQ).
    Conflict between rule 119 and token LANDE resolved as reduce (LANDE < LEQ).
    Conflict between rule 119 and token LXORE resolved as reduce (LXORE < LEQ).
    Conflict between rule 119 and token LORE resolved as reduce (LORE < LEQ).
    Conflict between rule 119 and token '?' resolved as reduce ('?' < LEQ).
    Conflict between rule 119 and token LOROR resolved as reduce (LOROR < LEQ).
    Conflict between rule 119 and token LANDAND resolved as reduce (LANDAND < LEQ).
    Conflict between rule 119 and token '|' resolved as reduce ('|' < LEQ).
    Conflict between rule 119 and token '^' resolved as reduce ('^' < LEQ).
    Conflict between rule 119 and token '&' resolved as reduce ('&' < LEQ).
    Conflict between rule 119 and token LEQ resolved as reduce (%left LEQ).
    Conflict between rule 119 and token LNE resolved as reduce (%left LNE).
    Conflict between rule 119 and token '<' resolved as shift (LEQ < '<').
    Conflict between rule 119 and token '>' resolved as shift (LEQ < '>').
    Conflict between rule 119 and token LLE resolved as shift (LEQ < LLE).
    Conflict between rule 119 and token LGE resolved as shift (LEQ < LGE).
    Conflict between rule 119 and token LLSH resolved as shift (LEQ < LLSH).
    Conflict between rule 119 and token LRSH resolved as shift (LEQ < LRSH).
    Conflict between rule 119 and token '+' resolved as shift (LEQ < '+').
    Conflict between rule 119 and token '-' resolved as shift (LEQ < '-').
    Conflict between rule 119 and token '*' resolved as shift (LEQ < '*').
    Conflict between rule 119 and token '/' resolved as shift (LEQ < '/').
    Conflict between rule 119 and token '%' resolved as shift (LEQ < '%').


State 251

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  120     | expr LNE expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, ')', ']', '}']
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '<'   posunout a přejít do stavu 176
    '>'   posunout a přejít do stavu 177
    LLE   posunout a přejít do stavu 178
    LGE   posunout a přejít do stavu 179
    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 120 (expr)

    Conflict between rule 120 and token '=' resolved as reduce ('=' < LNE).
    Conflict between rule 120 and token LPE resolved as reduce (LPE < LNE).
    Conflict between rule 120 and token LME resolved as reduce (LME < LNE).
    Conflict between rule 120 and token LMLE resolved as reduce (LMLE < LNE).
    Conflict between rule 120 and token LDVE resolved as reduce (LDVE < LNE).
    Conflict between rule 120 and token LMDE resolved as reduce (LMDE < LNE).
    Conflict between rule 120 and token LRSHE resolved as reduce (LRSHE < LNE).
    Conflict between rule 120 and token LLSHE resolved as reduce (LLSHE < LNE).
    Conflict between rule 120 and token LANDE resolved as reduce (LANDE < LNE).
    Conflict between rule 120 and token LXORE resolved as reduce (LXORE < LNE).
    Conflict between rule 120 and token LORE resolved as reduce (LORE < LNE).
    Conflict between rule 120 and token '?' resolved as reduce ('?' < LNE).
    Conflict between rule 120 and token LOROR resolved as reduce (LOROR < LNE).
    Conflict between rule 120 and token LANDAND resolved as reduce (LANDAND < LNE).
    Conflict between rule 120 and token '|' resolved as reduce ('|' < LNE).
    Conflict between rule 120 and token '^' resolved as reduce ('^' < LNE).
    Conflict between rule 120 and token '&' resolved as reduce ('&' < LNE).
    Conflict between rule 120 and token LEQ resolved as reduce (%left LEQ).
    Conflict between rule 120 and token LNE resolved as reduce (%left LNE).
    Conflict between rule 120 and token '<' resolved as shift (LNE < '<').
    Conflict between rule 120 and token '>' resolved as shift (LNE < '>').
    Conflict between rule 120 and token LLE resolved as shift (LNE < LLE).
    Conflict between rule 120 and token LGE resolved as shift (LNE < LGE).
    Conflict between rule 120 and token LLSH resolved as shift (LNE < LLSH).
    Conflict between rule 120 and token LRSH resolved as shift (LNE < LRSH).
    Conflict between rule 120 and token '+' resolved as shift (LNE < '+').
    Conflict between rule 120 and token '-' resolved as shift (LNE < '-').
    Conflict between rule 120 and token '*' resolved as shift (LNE < '*').
    Conflict between rule 120 and token '/' resolved as shift (LNE < '/').
    Conflict between rule 120 and token '%' resolved as shift (LNE < '%').


State 252

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  115     | expr '<' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, ')', ']', '}']
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 115 (expr)

    Conflict between rule 115 and token '=' resolved as reduce ('=' < '<').
    Conflict between rule 115 and token LPE resolved as reduce (LPE < '<').
    Conflict between rule 115 and token LME resolved as reduce (LME < '<').
    Conflict between rule 115 and token LMLE resolved as reduce (LMLE < '<').
    Conflict between rule 115 and token LDVE resolved as reduce (LDVE < '<').
    Conflict between rule 115 and token LMDE resolved as reduce (LMDE < '<').
    Conflict between rule 115 and token LRSHE resolved as reduce (LRSHE < '<').
    Conflict between rule 115 and token LLSHE resolved as reduce (LLSHE < '<').
    Conflict between rule 115 and token LANDE resolved as reduce (LANDE < '<').
    Conflict between rule 115 and token LXORE resolved as reduce (LXORE < '<').
    Conflict between rule 115 and token LORE resolved as reduce (LORE < '<').
    Conflict between rule 115 and token '?' resolved as reduce ('?' < '<').
    Conflict between rule 115 and token LOROR resolved as reduce (LOROR < '<').
    Conflict between rule 115 and token LANDAND resolved as reduce (LANDAND < '<').
    Conflict between rule 115 and token '|' resolved as reduce ('|' < '<').
    Conflict between rule 115 and token '^' resolved as reduce ('^' < '<').
    Conflict between rule 115 and token '&' resolved as reduce ('&' < '<').
    Conflict between rule 115 and token LEQ resolved as reduce (LEQ < '<').
    Conflict between rule 115 and token LNE resolved as reduce (LNE < '<').
    Conflict between rule 115 and token '<' resolved as reduce (%left '<').
    Conflict between rule 115 and token '>' resolved as reduce (%left '>').
    Conflict between rule 115 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 115 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 115 and token LLSH resolved as shift ('<' < LLSH).
    Conflict between rule 115 and token LRSH resolved as shift ('<' < LRSH).
    Conflict between rule 115 and token '+' resolved as shift ('<' < '+').
    Conflict between rule 115 and token '-' resolved as shift ('<' < '-').
    Conflict between rule 115 and token '*' resolved as shift ('<' < '*').
    Conflict between rule 115 and token '/' resolved as shift ('<' < '/').
    Conflict between rule 115 and token '%' resolved as shift ('<' < '%').


State 253

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  116     | expr '>' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, ')', ']', '}']
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 116 (expr)

    Conflict between rule 116 and token '=' resolved as reduce ('=' < '>').
    Conflict between rule 116 and token LPE resolved as reduce (LPE < '>').
    Conflict between rule 116 and token LME resolved as reduce (LME < '>').
    Conflict between rule 116 and token LMLE resolved as reduce (LMLE < '>').
    Conflict between rule 116 and token LDVE resolved as reduce (LDVE < '>').
    Conflict between rule 116 and token LMDE resolved as reduce (LMDE < '>').
    Conflict between rule 116 and token LRSHE resolved as reduce (LRSHE < '>').
    Conflict between rule 116 and token LLSHE resolved as reduce (LLSHE < '>').
    Conflict between rule 116 and token LANDE resolved as reduce (LANDE < '>').
    Conflict between rule 116 and token LXORE resolved as reduce (LXORE < '>').
    Conflict between rule 116 and token LORE resolved as reduce (LORE < '>').
    Conflict between rule 116 and token '?' resolved as reduce ('?' < '>').
    Conflict between rule 116 and token LOROR resolved as reduce (LOROR < '>').
    Conflict between rule 116 and token LANDAND resolved as reduce (LANDAND < '>').
    Conflict between rule 116 and token '|' resolved as reduce ('|' < '>').
    Conflict between rule 116 and token '^' resolved as reduce ('^' < '>').
    Conflict between rule 116 and token '&' resolved as reduce ('&' < '>').
    Conflict between rule 116 and token LEQ resolved as reduce (LEQ < '>').
    Conflict between rule 116 and token LNE resolved as reduce (LNE < '>').
    Conflict between rule 116 and token '<' resolved as reduce (%left '<').
    Conflict between rule 116 and token '>' resolved as reduce (%left '>').
    Conflict between rule 116 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 116 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 116 and token LLSH resolved as shift ('>' < LLSH).
    Conflict between rule 116 and token LRSH resolved as shift ('>' < LRSH).
    Conflict between rule 116 and token '+' resolved as shift ('>' < '+').
    Conflict between rule 116 and token '-' resolved as shift ('>' < '-').
    Conflict between rule 116 and token '*' resolved as shift ('>' < '*').
    Conflict between rule 116 and token '/' resolved as shift ('>' < '/').
    Conflict between rule 116 and token '%' resolved as shift ('>' < '%').


State 254

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  117     | expr LLE expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, ')', ']', '}']
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 117 (expr)

    Conflict between rule 117 and token '=' resolved as reduce ('=' < LLE).
    Conflict between rule 117 and token LPE resolved as reduce (LPE < LLE).
    Conflict between rule 117 and token LME resolved as reduce (LME < LLE).
    Conflict between rule 117 and token LMLE resolved as reduce (LMLE < LLE).
    Conflict between rule 117 and token LDVE resolved as reduce (LDVE < LLE).
    Conflict between rule 117 and token LMDE resolved as reduce (LMDE < LLE).
    Conflict between rule 117 and token LRSHE resolved as reduce (LRSHE < LLE).
    Conflict between rule 117 and token LLSHE resolved as reduce (LLSHE < LLE).
    Conflict between rule 117 and token LANDE resolved as reduce (LANDE < LLE).
    Conflict between rule 117 and token LXORE resolved as reduce (LXORE < LLE).
    Conflict between rule 117 and token LORE resolved as reduce (LORE < LLE).
    Conflict between rule 117 and token '?' resolved as reduce ('?' < LLE).
    Conflict between rule 117 and token LOROR resolved as reduce (LOROR < LLE).
    Conflict between rule 117 and token LANDAND resolved as reduce (LANDAND < LLE).
    Conflict between rule 117 and token '|' resolved as reduce ('|' < LLE).
    Conflict between rule 117 and token '^' resolved as reduce ('^' < LLE).
    Conflict between rule 117 and token '&' resolved as reduce ('&' < LLE).
    Conflict between rule 117 and token LEQ resolved as reduce (LEQ < LLE).
    Conflict between rule 117 and token LNE resolved as reduce (LNE < LLE).
    Conflict between rule 117 and token '<' resolved as reduce (%left '<').
    Conflict between rule 117 and token '>' resolved as reduce (%left '>').
    Conflict between rule 117 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 117 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 117 and token LLSH resolved as shift (LLE < LLSH).
    Conflict between rule 117 and token LRSH resolved as shift (LLE < LRSH).
    Conflict between rule 117 and token '+' resolved as shift (LLE < '+').
    Conflict between rule 117 and token '-' resolved as shift (LLE < '-').
    Conflict between rule 117 and token '*' resolved as shift (LLE < '*').
    Conflict between rule 117 and token '/' resolved as shift (LLE < '/').
    Conflict between rule 117 and token '%' resolved as shift (LLE < '%').


State 255

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  118     | expr LGE expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, ')', ']', '}']
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    LLSH  posunout a přejít do stavu 180
    LRSH  posunout a přejít do stavu 181
    '+'   posunout a přejít do stavu 182
    '-'   posunout a přejít do stavu 183
    '*'   posunout a přejít do stavu 184
    '/'   posunout a přejít do stavu 185
    '%'   posunout a přejít do stavu 186

    $výchozí  reduce using rule 118 (expr)

    Conflict between rule 118 and token '=' resolved as reduce ('=' < LGE).
    Conflict between rule 118 and token LPE resolved as reduce (LPE < LGE).
    Conflict between rule 118 and token LME resolved as reduce (LME < LGE).
    Conflict between rule 118 and token LMLE resolved as reduce (LMLE < LGE).
    Conflict between rule 118 and token LDVE resolved as reduce (LDVE < LGE).
    Conflict between rule 118 and token LMDE resolved as reduce (LMDE < LGE).
    Conflict between rule 118 and token LRSHE resolved as reduce (LRSHE < LGE).
    Conflict between rule 118 and token LLSHE resolved as reduce (LLSHE < LGE).
    Conflict between rule 118 and token LANDE resolved as reduce (LANDE < LGE).
    Conflict between rule 118 and token LXORE resolved as reduce (LXORE < LGE).
    Conflict between rule 118 and token LORE resolved as reduce (LORE < LGE).
    Conflict between rule 118 and token '?' resolved as reduce ('?' < LGE).
    Conflict between rule 118 and token LOROR resolved as reduce (LOROR < LGE).
    Conflict between rule 118 and token LANDAND resolved as reduce (LANDAND < LGE).
    Conflict between rule 118 and token '|' resolved as reduce ('|' < LGE).
    Conflict between rule 118 and token '^' resolved as reduce ('^' < LGE).
    Conflict between rule 118 and token '&' resolved as reduce ('&' < LGE).
    Conflict between rule 118 and token LEQ resolved as reduce (LEQ < LGE).
    Conflict between rule 118 and token LNE resolved as reduce (LNE < LGE).
    Conflict between rule 118 and token '<' resolved as reduce (%left '<').
    Conflict between rule 118 and token '>' resolved as reduce (%left '>').
    Conflict between rule 118 and token LLE resolved as reduce (%left LLE).
    Conflict between rule 118 and token LGE resolved as reduce (%left LGE).
    Conflict between rule 118 and token LLSH resolved as shift (LGE < LLSH).
    Conflict between rule 118 and token LRSH resolved as shift (LGE < LRSH).
    Conflict between rule 118 and token '+' resolved as shift (LGE < '+').
    Conflict between rule 118 and token '-' resolved as shift (LGE < '-').
    Conflict between rule 118 and token '*' resolved as shift (LGE < '*').
    Conflict between rule 118 and token '/' resolved as shift (LGE < '/').
    Conflict between rule 118 and token '%' resolved as shift (LGE < '%').


State 256

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  114     | expr LLSH expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, ')', ']', '}']
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '+'  posunout a přejít do stavu 182
    '-'  posunout a přejít do stavu 183
    '*'  posunout a přejít do stavu 184
    '/'  posunout a přejít do stavu 185
    '%'  posunout a přejít do stavu 186

    $výchozí  reduce using rule 114 (expr)

    Conflict between rule 114 and token '=' resolved as reduce ('=' < LLSH).
    Conflict between rule 114 and token LPE resolved as reduce (LPE < LLSH).
    Conflict between rule 114 and token LME resolved as reduce (LME < LLSH).
    Conflict between rule 114 and token LMLE resolved as reduce (LMLE < LLSH).
    Conflict between rule 114 and token LDVE resolved as reduce (LDVE < LLSH).
    Conflict between rule 114 and token LMDE resolved as reduce (LMDE < LLSH).
    Conflict between rule 114 and token LRSHE resolved as reduce (LRSHE < LLSH).
    Conflict between rule 114 and token LLSHE resolved as reduce (LLSHE < LLSH).
    Conflict between rule 114 and token LANDE resolved as reduce (LANDE < LLSH).
    Conflict between rule 114 and token LXORE resolved as reduce (LXORE < LLSH).
    Conflict between rule 114 and token LORE resolved as reduce (LORE < LLSH).
    Conflict between rule 114 and token '?' resolved as reduce ('?' < LLSH).
    Conflict between rule 114 and token LOROR resolved as reduce (LOROR < LLSH).
    Conflict between rule 114 and token LANDAND resolved as reduce (LANDAND < LLSH).
    Conflict between rule 114 and token '|' resolved as reduce ('|' < LLSH).
    Conflict between rule 114 and token '^' resolved as reduce ('^' < LLSH).
    Conflict between rule 114 and token '&' resolved as reduce ('&' < LLSH).
    Conflict between rule 114 and token LEQ resolved as reduce (LEQ < LLSH).
    Conflict between rule 114 and token LNE resolved as reduce (LNE < LLSH).
    Conflict between rule 114 and token '<' resolved as reduce ('<' < LLSH).
    Conflict between rule 114 and token '>' resolved as reduce ('>' < LLSH).
    Conflict between rule 114 and token LLE resolved as reduce (LLE < LLSH).
    Conflict between rule 114 and token LGE resolved as reduce (LGE < LLSH).
    Conflict between rule 114 and token LLSH resolved as reduce (%left LLSH).
    Conflict between rule 114 and token LRSH resolved as reduce (%left LRSH).
    Conflict between rule 114 and token '+' resolved as shift (LLSH < '+').
    Conflict between rule 114 and token '-' resolved as shift (LLSH < '-').
    Conflict between rule 114 and token '*' resolved as shift (LLSH < '*').
    Conflict between rule 114 and token '/' resolved as shift (LLSH < '/').
    Conflict between rule 114 and token '%' resolved as shift (LLSH < '%').


State 257

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  113     | expr LRSH expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, ')', ']', '}']
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '+'  posunout a přejít do stavu 182
    '-'  posunout a přejít do stavu 183
    '*'  posunout a přejít do stavu 184
    '/'  posunout a přejít do stavu 185
    '%'  posunout a přejít do stavu 186

    $výchozí  reduce using rule 113 (expr)

    Conflict between rule 113 and token '=' resolved as reduce ('=' < LRSH).
    Conflict between rule 113 and token LPE resolved as reduce (LPE < LRSH).
    Conflict between rule 113 and token LME resolved as reduce (LME < LRSH).
    Conflict between rule 113 and token LMLE resolved as reduce (LMLE < LRSH).
    Conflict between rule 113 and token LDVE resolved as reduce (LDVE < LRSH).
    Conflict between rule 113 and token LMDE resolved as reduce (LMDE < LRSH).
    Conflict between rule 113 and token LRSHE resolved as reduce (LRSHE < LRSH).
    Conflict between rule 113 and token LLSHE resolved as reduce (LLSHE < LRSH).
    Conflict between rule 113 and token LANDE resolved as reduce (LANDE < LRSH).
    Conflict between rule 113 and token LXORE resolved as reduce (LXORE < LRSH).
    Conflict between rule 113 and token LORE resolved as reduce (LORE < LRSH).
    Conflict between rule 113 and token '?' resolved as reduce ('?' < LRSH).
    Conflict between rule 113 and token LOROR resolved as reduce (LOROR < LRSH).
    Conflict between rule 113 and token LANDAND resolved as reduce (LANDAND < LRSH).
    Conflict between rule 113 and token '|' resolved as reduce ('|' < LRSH).
    Conflict between rule 113 and token '^' resolved as reduce ('^' < LRSH).
    Conflict between rule 113 and token '&' resolved as reduce ('&' < LRSH).
    Conflict between rule 113 and token LEQ resolved as reduce (LEQ < LRSH).
    Conflict between rule 113 and token LNE resolved as reduce (LNE < LRSH).
    Conflict between rule 113 and token '<' resolved as reduce ('<' < LRSH).
    Conflict between rule 113 and token '>' resolved as reduce ('>' < LRSH).
    Conflict between rule 113 and token LLE resolved as reduce (LLE < LRSH).
    Conflict between rule 113 and token LGE resolved as reduce (LGE < LRSH).
    Conflict between rule 113 and token LLSH resolved as reduce (%left LLSH).
    Conflict between rule 113 and token LRSH resolved as reduce (%left LRSH).
    Conflict between rule 113 and token '+' resolved as shift (LRSH < '+').
    Conflict between rule 113 and token '-' resolved as shift (LRSH < '-').
    Conflict between rule 113 and token '*' resolved as shift (LRSH < '*').
    Conflict between rule 113 and token '/' resolved as shift (LRSH < '/').
    Conflict between rule 113 and token '%' resolved as shift (LRSH < '%').


State 258

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  111     | expr '+' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, '+', '-', ')', ']', '}']
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '*'  posunout a přejít do stavu 184
    '/'  posunout a přejít do stavu 185
    '%'  posunout a přejít do stavu 186

    $výchozí  reduce using rule 111 (expr)

    Conflict between rule 111 and token '=' resolved as reduce ('=' < '+').
    Conflict between rule 111 and token LPE resolved as reduce (LPE < '+').
    Conflict between rule 111 and token LME resolved as reduce (LME < '+').
    Conflict between rule 111 and token LMLE resolved as reduce (LMLE < '+').
    Conflict between rule 111 and token LDVE resolved as reduce (LDVE < '+').
    Conflict between rule 111 and token LMDE resolved as reduce (LMDE < '+').
    Conflict between rule 111 and token LRSHE resolved as reduce (LRSHE < '+').
    Conflict between rule 111 and token LLSHE resolved as reduce (LLSHE < '+').
    Conflict between rule 111 and token LANDE resolved as reduce (LANDE < '+').
    Conflict between rule 111 and token LXORE resolved as reduce (LXORE < '+').
    Conflict between rule 111 and token LORE resolved as reduce (LORE < '+').
    Conflict between rule 111 and token '?' resolved as reduce ('?' < '+').
    Conflict between rule 111 and token LOROR resolved as reduce (LOROR < '+').
    Conflict between rule 111 and token LANDAND resolved as reduce (LANDAND < '+').
    Conflict between rule 111 and token '|' resolved as reduce ('|' < '+').
    Conflict between rule 111 and token '^' resolved as reduce ('^' < '+').
    Conflict between rule 111 and token '&' resolved as reduce ('&' < '+').
    Conflict between rule 111 and token LEQ resolved as reduce (LEQ < '+').
    Conflict between rule 111 and token LNE resolved as reduce (LNE < '+').
    Conflict between rule 111 and token '<' resolved as reduce ('<' < '+').
    Conflict between rule 111 and token '>' resolved as reduce ('>' < '+').
    Conflict between rule 111 and token LLE resolved as reduce (LLE < '+').
    Conflict between rule 111 and token LGE resolved as reduce (LGE < '+').
    Conflict between rule 111 and token LLSH resolved as reduce (LLSH < '+').
    Conflict between rule 111 and token LRSH resolved as reduce (LRSH < '+').
    Conflict between rule 111 and token '+' resolved as reduce (%left '+').
    Conflict between rule 111 and token '-' resolved as reduce (%left '-').
    Conflict between rule 111 and token '*' resolved as shift ('+' < '*').
    Conflict between rule 111 and token '/' resolved as shift ('+' < '/').
    Conflict between rule 111 and token '%' resolved as shift ('+' < '%').


State 259

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  112     | expr '-' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, '+', '-', ')', ']', '}']
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '*'  posunout a přejít do stavu 184
    '/'  posunout a přejít do stavu 185
    '%'  posunout a přejít do stavu 186

    $výchozí  reduce using rule 112 (expr)

    Conflict between rule 112 and token '=' resolved as reduce ('=' < '-').
    Conflict between rule 112 and token LPE resolved as reduce (LPE < '-').
    Conflict between rule 112 and token LME resolved as reduce (LME < '-').
    Conflict between rule 112 and token LMLE resolved as reduce (LMLE < '-').
    Conflict between rule 112 and token LDVE resolved as reduce (LDVE < '-').
    Conflict between rule 112 and token LMDE resolved as reduce (LMDE < '-').
    Conflict between rule 112 and token LRSHE resolved as reduce (LRSHE < '-').
    Conflict between rule 112 and token LLSHE resolved as reduce (LLSHE < '-').
    Conflict between rule 112 and token LANDE resolved as reduce (LANDE < '-').
    Conflict between rule 112 and token LXORE resolved as reduce (LXORE < '-').
    Conflict between rule 112 and token LORE resolved as reduce (LORE < '-').
    Conflict between rule 112 and token '?' resolved as reduce ('?' < '-').
    Conflict between rule 112 and token LOROR resolved as reduce (LOROR < '-').
    Conflict between rule 112 and token LANDAND resolved as reduce (LANDAND < '-').
    Conflict between rule 112 and token '|' resolved as reduce ('|' < '-').
    Conflict between rule 112 and token '^' resolved as reduce ('^' < '-').
    Conflict between rule 112 and token '&' resolved as reduce ('&' < '-').
    Conflict between rule 112 and token LEQ resolved as reduce (LEQ < '-').
    Conflict between rule 112 and token LNE resolved as reduce (LNE < '-').
    Conflict between rule 112 and token '<' resolved as reduce ('<' < '-').
    Conflict between rule 112 and token '>' resolved as reduce ('>' < '-').
    Conflict between rule 112 and token LLE resolved as reduce (LLE < '-').
    Conflict between rule 112 and token LGE resolved as reduce (LGE < '-').
    Conflict between rule 112 and token LLSH resolved as reduce (LLSH < '-').
    Conflict between rule 112 and token LRSH resolved as reduce (LRSH < '-').
    Conflict between rule 112 and token '+' resolved as reduce (%left '+').
    Conflict between rule 112 and token '-' resolved as reduce (%left '-').
    Conflict between rule 112 and token '*' resolved as shift ('-' < '*').
    Conflict between rule 112 and token '/' resolved as shift ('-' < '/').
    Conflict between rule 112 and token '%' resolved as shift ('-' < '%').


State 260

  108 expr: expr . '*' expr
  108     | expr '*' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, '+', '-', '*', '/', '%', ')', ']', '}']
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    $výchozí  reduce using rule 108 (expr)

    Conflict between rule 108 and token '=' resolved as reduce ('=' < '*').
    Conflict between rule 108 and token LPE resolved as reduce (LPE < '*').
    Conflict between rule 108 and token LME resolved as reduce (LME < '*').
    Conflict between rule 108 and token LMLE resolved as reduce (LMLE < '*').
    Conflict between rule 108 and token LDVE resolved as reduce (LDVE < '*').
    Conflict between rule 108 and token LMDE resolved as reduce (LMDE < '*').
    Conflict between rule 108 and token LRSHE resolved as reduce (LRSHE < '*').
    Conflict between rule 108 and token LLSHE resolved as reduce (LLSHE < '*').
    Conflict between rule 108 and token LANDE resolved as reduce (LANDE < '*').
    Conflict between rule 108 and token LXORE resolved as reduce (LXORE < '*').
    Conflict between rule 108 and token LORE resolved as reduce (LORE < '*').
    Conflict between rule 108 and token '?' resolved as reduce ('?' < '*').
    Conflict between rule 108 and token LOROR resolved as reduce (LOROR < '*').
    Conflict between rule 108 and token LANDAND resolved as reduce (LANDAND < '*').
    Conflict between rule 108 and token '|' resolved as reduce ('|' < '*').
    Conflict between rule 108 and token '^' resolved as reduce ('^' < '*').
    Conflict between rule 108 and token '&' resolved as reduce ('&' < '*').
    Conflict between rule 108 and token LEQ resolved as reduce (LEQ < '*').
    Conflict between rule 108 and token LNE resolved as reduce (LNE < '*').
    Conflict between rule 108 and token '<' resolved as reduce ('<' < '*').
    Conflict between rule 108 and token '>' resolved as reduce ('>' < '*').
    Conflict between rule 108 and token LLE resolved as reduce (LLE < '*').
    Conflict between rule 108 and token LGE resolved as reduce (LGE < '*').
    Conflict between rule 108 and token LLSH resolved as reduce (LLSH < '*').
    Conflict between rule 108 and token LRSH resolved as reduce (LRSH < '*').
    Conflict between rule 108 and token '+' resolved as reduce ('+' < '*').
    Conflict between rule 108 and token '-' resolved as reduce ('-' < '*').
    Conflict between rule 108 and token '*' resolved as reduce (%left '*').
    Conflict between rule 108 and token '/' resolved as reduce (%left '/').
    Conflict between rule 108 and token '%' resolved as reduce (%left '%').


State 261

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  109     | expr '/' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, '+', '-', '*', '/', '%', ')', ']', '}']
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    $výchozí  reduce using rule 109 (expr)

    Conflict between rule 109 and token '=' resolved as reduce ('=' < '/').
    Conflict between rule 109 and token LPE resolved as reduce (LPE < '/').
    Conflict between rule 109 and token LME resolved as reduce (LME < '/').
    Conflict between rule 109 and token LMLE resolved as reduce (LMLE < '/').
    Conflict between rule 109 and token LDVE resolved as reduce (LDVE < '/').
    Conflict between rule 109 and token LMDE resolved as reduce (LMDE < '/').
    Conflict between rule 109 and token LRSHE resolved as reduce (LRSHE < '/').
    Conflict between rule 109 and token LLSHE resolved as reduce (LLSHE < '/').
    Conflict between rule 109 and token LANDE resolved as reduce (LANDE < '/').
    Conflict between rule 109 and token LXORE resolved as reduce (LXORE < '/').
    Conflict between rule 109 and token LORE resolved as reduce (LORE < '/').
    Conflict between rule 109 and token '?' resolved as reduce ('?' < '/').
    Conflict between rule 109 and token LOROR resolved as reduce (LOROR < '/').
    Conflict between rule 109 and token LANDAND resolved as reduce (LANDAND < '/').
    Conflict between rule 109 and token '|' resolved as reduce ('|' < '/').
    Conflict between rule 109 and token '^' resolved as reduce ('^' < '/').
    Conflict between rule 109 and token '&' resolved as reduce ('&' < '/').
    Conflict between rule 109 and token LEQ resolved as reduce (LEQ < '/').
    Conflict between rule 109 and token LNE resolved as reduce (LNE < '/').
    Conflict between rule 109 and token '<' resolved as reduce ('<' < '/').
    Conflict between rule 109 and token '>' resolved as reduce ('>' < '/').
    Conflict between rule 109 and token LLE resolved as reduce (LLE < '/').
    Conflict between rule 109 and token LGE resolved as reduce (LGE < '/').
    Conflict between rule 109 and token LLSH resolved as reduce (LLSH < '/').
    Conflict between rule 109 and token LRSH resolved as reduce (LRSH < '/').
    Conflict between rule 109 and token '+' resolved as reduce ('+' < '/').
    Conflict between rule 109 and token '-' resolved as reduce ('-' < '/').
    Conflict between rule 109 and token '*' resolved as reduce (%left '*').
    Conflict between rule 109 and token '/' resolved as reduce (%left '/').
    Conflict between rule 109 and token '%' resolved as reduce (%left '%').


State 262

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  110     | expr '%' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', ':', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, '+', '-', '*', '/', '%', ')', ']', '}']
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    $výchozí  reduce using rule 110 (expr)

    Conflict between rule 110 and token '=' resolved as reduce ('=' < '%').
    Conflict between rule 110 and token LPE resolved as reduce (LPE < '%').
    Conflict between rule 110 and token LME resolved as reduce (LME < '%').
    Conflict between rule 110 and token LMLE resolved as reduce (LMLE < '%').
    Conflict between rule 110 and token LDVE resolved as reduce (LDVE < '%').
    Conflict between rule 110 and token LMDE resolved as reduce (LMDE < '%').
    Conflict between rule 110 and token LRSHE resolved as reduce (LRSHE < '%').
    Conflict between rule 110 and token LLSHE resolved as reduce (LLSHE < '%').
    Conflict between rule 110 and token LANDE resolved as reduce (LANDE < '%').
    Conflict between rule 110 and token LXORE resolved as reduce (LXORE < '%').
    Conflict between rule 110 and token LORE resolved as reduce (LORE < '%').
    Conflict between rule 110 and token '?' resolved as reduce ('?' < '%').
    Conflict between rule 110 and token LOROR resolved as reduce (LOROR < '%').
    Conflict between rule 110 and token LANDAND resolved as reduce (LANDAND < '%').
    Conflict between rule 110 and token '|' resolved as reduce ('|' < '%').
    Conflict between rule 110 and token '^' resolved as reduce ('^' < '%').
    Conflict between rule 110 and token '&' resolved as reduce ('&' < '%').
    Conflict between rule 110 and token LEQ resolved as reduce (LEQ < '%').
    Conflict between rule 110 and token LNE resolved as reduce (LNE < '%').
    Conflict between rule 110 and token '<' resolved as reduce ('<' < '%').
    Conflict between rule 110 and token '>' resolved as reduce ('>' < '%').
    Conflict between rule 110 and token LLE resolved as reduce (LLE < '%').
    Conflict between rule 110 and token LGE resolved as reduce (LGE < '%').
    Conflict between rule 110 and token LLSH resolved as reduce (LLSH < '%').
    Conflict between rule 110 and token LRSH resolved as reduce (LRSH < '%').
    Conflict between rule 110 and token '+' resolved as reduce ('+' < '%').
    Conflict between rule 110 and token '-' resolved as reduce ('-' < '%').
    Conflict between rule 110 and token '*' resolved as reduce (%left '*').
    Conflict between rule 110 and token '/' resolved as reduce (%left '/').
    Conflict between rule 110 and token '%' resolved as reduce (%left '%').


State 263

  157 pexpr: pexpr LMG ltag .

    $výchozí  reduce using rule 157 (pexpr)


State 264

  158 pexpr: pexpr '.' ltag .

    $výchozí  reduce using rule 158 (pexpr)


State 265

  106 cexpr: cexpr . ',' cexpr
  156 pexpr: pexpr '[' cexpr . ']'

    ','  posunout a přejít do stavu 226
    ']'  posunout a přejít do stavu 299


State 266

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr
  178 elist: expr .  [',', ')']

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 178 (elist)


State 267

  155 pexpr: pexpr '(' zelist . ')'

    ')'  posunout a přejít do stavu 300


State 268

  177 zelist: elist .  [')']
  179 elist: elist . ',' elist

    ','  posunout a přejít do stavu 301

    $výchozí  reduce using rule 177 (zelist)


State 269

   67 arglist: '.' '.' '.' .

    $výchozí  reduce using rule 67 (arglist)


State 270

   68 arglist: arglist . ',' arglist
   68        | arglist ',' arglist .  [',', ')']

    $výchozí  reduce using rule 68 (arglist)

    Conflict between rule 68 and token ',' resolved as reduce (%left ',').


State 271

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   13       | '*' zgnlist . xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   41 abdecor1: . '*' zgnlist
   41         | '*' zgnlist .  [',', ')']
   42         | . '*' zgnlist abdecor1
   42         | '*' zgnlist . abdecor1
   43         | . abdecor2
   44 abdecor2: . abdecor3
   45         | . abdecor2 '(' zarglist ')'
   46         | . abdecor2 '[' zexpr ']'
   47 abdecor3: . '(' ')'
   48         | . '[' zexpr ']'
   49         | . '(' abdecor1 ')'
  212 zgnlist: zgnlist . gname
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'        posunout a přejít do stavu 198
    '['        posunout a přejít do stavu 199
    '('        posunout a přejít do stavu 200
    LNAME      posunout a přejít do stavu 36
    LTYPE      posunout a přejít do stavu 37
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24

    $výchozí  reduce using rule 41 (abdecor1)

    xdecor    přejít do stavu 86
    xdecor2   přejít do stavu 50
    abdecor1  přejít do stavu 302
    abdecor2  přejít do stavu 204
    abdecor3  přejít do stavu 205
    gname     přejít do stavu 87
    tag       přejít do stavu 51
    ltag      přejít do stavu 52


State 272

   48 abdecor3: '[' zexpr . ']'

    ']'  posunout a přejít do stavu 303


State 273

   47 abdecor3: '(' ')' .

    $výchozí  reduce using rule 47 (abdecor3)


State 274

   49 abdecor3: '(' abdecor1 . ')'

    ')'  posunout a přejít do stavu 304


State 275

   46 abdecor2: abdecor2 '[' . zexpr ']'
  102 zexpr: . %empty  [']']
  103      | . lexpr
  104 lexpr: . expr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 102 (zexpr)

    zexpr    přejít do stavu 305
    lexpr    přejít do stavu 116
    expr     přejít do stavu 117
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 276

   45 abdecor2: abdecor2 '(' . zarglist ')'
   62 zarglist: . %empty  [')']
   63         | . arglist
   64 arglist: . name
   65        | . tlist abdecor
   66        | . tlist xdecor
   67        | . '.' '.' '.'
   68        | . arglist ',' arglist
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  192 tlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  243 name: . LNAME

    '.'        posunout a přejít do stavu 124
    LNAME      posunout a přejít do stavu 100
    LTYPE      posunout a přejít do stavu 3
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25

    $výchozí  reduce using rule 62 (zarglist)

    zarglist  přejít do stavu 306
    arglist   přejít do stavu 126
    types     přejít do stavu 79
    tlist     přejít do stavu 127
    complex   přejít do stavu 30
    gcnlist   přejít do stavu 31
    gcname    přejít do stavu 32
    tname     přejít do stavu 33
    cname     přejít do stavu 34
    gname     přejít do stavu 35
    name      přejít do stavu 128


State 277

   31 edecl: edecl tlist $@6 zedlist . ';'

    ';'  posunout a přejít do stavu 307


State 278

   38 edecor: ':' lexpr .

    $výchozí  reduce using rule 38 (edecor)


State 279

   29 edecl: tlist $@5 zedlist ';' .

    $výchozí  reduce using rule 29 (edecl)


State 280

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   34 edlist: . edecor
   35       | . edlist ',' edlist
   35       | edlist ',' . edlist
   36 edecor: . xdecor
   37       | . tag ':' lexpr
   38       | . ':' lexpr
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    ':'    posunout a přejít do stavu 207
    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    xdecor   přejít do stavu 208
    xdecor2  přejít do stavu 50
    edlist   přejít do stavu 308
    edecor   přejít do stavu 211
    tag      přejít do stavu 212
    ltag     přejít do stavu 52


State 281

   37 edecor: tag ':' . lexpr
  104 lexpr: . expr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    lexpr    přejít do stavu 309
    expr     přejít do stavu 117
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 282

  205 complex: LENUM ltag $@12 '{' $@13 enum '}' .

    $výchozí  reduce using rule 205 (complex)


State 283

   18 adecl: . ctlist ';'
   19      | . ctlist adlist ';'
   69 block: '{' slist . '}'
   71 slist: slist . adecl
   72      | slist . stmnt
   73 labels: . label
   74       | . labels label
   75 label: . LCASE expr ':'
   76      | . LDEFAULT ':'
   77      | . LNAME ':'
   78 stmnt: . error ';'
   79      | . ulstmnt
   80      | . labels ulstmnt
   83 ulstmnt: . zcexpr ';'
   84 $@7: . %empty  ['{']
   85 ulstmnt: . $@7 block
   86        | . LIF '(' cexpr ')' stmnt
   87        | . LIF '(' cexpr ')' stmnt LELSE stmnt
   88 $@8: . %empty  [LFOR]
   89 ulstmnt: . $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt
   90        | . LWHILE '(' cexpr ')' stmnt
   91        | . LDO stmnt LWHILE '(' cexpr ')' ';'
   92        | . LRETURN zcexpr ';'
   93        | . LSWITCH '(' cexpr ')' stmnt
   94        | . LBREAK ';'
   95        | . LCONTINUE ';'
   96        | . LGOTO ltag ';'
   97        | . LUSED '(' zelist ')' ';'
   98        | . LPREFETCH '(' zelist ')' ';'
   99        | . LSET '(' zelist ')' ';'
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  193 ctlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  243 name: . LNAME

    error      posunout a přejít do stavu 310
    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 311
    LTYPE      posunout a přejít do stavu 3
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LAUTO      posunout a přejít do stavu 4
    LBREAK     posunout a přejít do stavu 312
    LCASE      posunout a přejít do stavu 313
    LCHAR      posunout a přejít do stavu 5
    LCONTINUE  posunout a přejít do stavu 314
    LDEFAULT   posunout a přejít do stavu 315
    LDO        posunout a přejít do stavu 316
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LGOTO      posunout a přejít do stavu 317
    LIF        posunout a přejít do stavu 318
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LPREFETCH  posunout a přejít do stavu 319
    LREGISTER  posunout a přejít do stavu 11
    LRETURN    posunout a přejít do stavu 320
    LSHORT     posunout a přejít do stavu 12
    LSIZEOF    posunout a přejít do stavu 111
    LUSED      posunout a přejít do stavu 321
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LSWITCH    posunout a přejít do stavu 322
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LWHILE     posunout a přejít do stavu 323
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LSET       posunout a přejít do stavu 324
    LSIGNOF    posunout a přejít do stavu 112
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25
    '}'        posunout a přejít do stavu 325
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    ';'   reduce using rule 100 (zcexpr)
    LFOR  reduce using rule 88 ($@8)
    '{'   reduce using rule 84 ($@7)

    adecl    přejít do stavu 326
    labels   přejít do stavu 327
    label    přejít do stavu 328
    stmnt    přejít do stavu 329
    ulstmnt  přejít do stavu 330
    $@7      přejít do stavu 331
    $@8      přejít do stavu 332
    zcexpr   přejít do stavu 333
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    types    přejít do stavu 28
    ctlist   přejít do stavu 335
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35
    name     přejít do stavu 123


State 284

   25 pdecl: pdecl ctlist pdlist ';' .

    $výchozí  reduce using rule 25 (pdecl)


State 285

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   26 pdlist: . xdecor
   27       | . pdlist ',' pdlist
   27       | pdlist ',' . pdlist
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    xdecor   přejít do stavu 218
    xdecor2  přejít do stavu 50
    pdlist   přejít do stavu 336
    tag      přejít do stavu 51
    ltag     přejít do stavu 52


State 286

   53 qual: '.' ltag .

    $výchozí  reduce using rule 53 (qual)


State 287

   52 qual: '[' lexpr . ']'

    ']'  posunout a přejít do stavu 337


State 288

   55 qlist: init ',' .

    $výchozí  reduce using rule 55 (qlist)


State 289

   54 qual: qual '=' .

    $výchozí  reduce using rule 54 (qual)


State 290

   56 qlist: qlist init . ','
   61 ilist: qlist init .  ['}']

    ','  posunout a přejít do stavu 338

    $výchozí  reduce using rule 61 (ilist)


State 291

   54 qual: qual . '='
   58 qlist: qlist qual .  ['&', '+', '-', '*', LMM, LPP, '.', '[', '(', LNAME, LFCONST, LDCONST, LCONST, LLCONST, LUCONST, LULCONST, LVLCONST, LUVLCONST, LSTRING, LLSTRING, LSIZEOF, LSIGNOF, '{', '}', '!', '~']

    '='  posunout a přejít do stavu 289

    $výchozí  reduce using rule 58 (qlist)


State 292

   51 init: '{' ilist '}' .

    $výchozí  reduce using rule 51 (init)


State 293

  106 cexpr: cexpr . ',' cexpr
  106      | cexpr ',' cexpr .  [';', ',', ':', ')', ']']

    $výchozí  reduce using rule 106 (cexpr)

    Conflict between rule 106 and token ',' resolved as reduce (%left ',').


State 294

   41 abdecor1: . '*' zgnlist
   41         | '*' zgnlist .  [')']
   42         | . '*' zgnlist abdecor1
   42         | '*' zgnlist . abdecor1
   43         | . abdecor2
   44 abdecor2: . abdecor3
   45         | . abdecor2 '(' zarglist ')'
   46         | . abdecor2 '[' zexpr ']'
   47 abdecor3: . '(' ')'
   48         | . '[' zexpr ']'
   49         | . '(' abdecor1 ')'
  212 zgnlist: zgnlist . gname
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT

    '*'        posunout a přejít do stavu 228
    '['        posunout a přejít do stavu 199
    '('        posunout a přejít do stavu 229
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LRESTRICT  posunout a přejít do stavu 24

    $výchozí  reduce using rule 41 (abdecor1)

    abdecor1  přejít do stavu 302
    abdecor2  přejít do stavu 204
    abdecor3  přejít do stavu 205
    gname     přejít do stavu 87


State 295

  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  139       | '(' tlist abdecor ')' . xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  140       | '(' tlist abdecor ')' . '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '{'        posunout a přejít do stavu 339
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    xuexpr   přejít do stavu 340
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 296

  153 pexpr: LSIZEOF '(' tlist abdecor . ')'

    ')'  posunout a přejít do stavu 341


State 297

  154 pexpr: LSIGNOF '(' tlist abdecor . ')'

    ')'  posunout a přejít do stavu 342


State 298

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  126     | expr '?' cexpr ':' . expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 343
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 299

  156 pexpr: pexpr '[' cexpr ']' .

    $výchozí  reduce using rule 156 (pexpr)


State 300

  155 pexpr: pexpr '(' zelist ')' .

    $výchozí  reduce using rule 155 (pexpr)


State 301

  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  178 elist: . expr
  179      | . elist ',' elist
  179      | elist ',' . elist
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 266
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    elist    přejít do stavu 344
    name     přejít do stavu 123


State 302

   42 abdecor1: '*' zgnlist abdecor1 .

    $výchozí  reduce using rule 42 (abdecor1)


State 303

   48 abdecor3: '[' zexpr ']' .

    $výchozí  reduce using rule 48 (abdecor3)


State 304

   49 abdecor3: '(' abdecor1 ')' .

    $výchozí  reduce using rule 49 (abdecor3)


State 305

   46 abdecor2: abdecor2 '[' zexpr . ']'

    ']'  posunout a přejít do stavu 345


State 306

   45 abdecor2: abdecor2 '(' zarglist . ')'

    ')'  posunout a přejít do stavu 346


State 307

   31 edecl: edecl tlist $@6 zedlist ';' .

    $výchozí  reduce using rule 31 (edecl)


State 308

   35 edlist: edlist . ',' edlist
   35       | edlist ',' edlist .  [';', ',']

    $výchozí  reduce using rule 35 (edlist)

    Conflict between rule 35 and token ',' resolved as reduce (%left ',').


State 309

   37 edecor: tag ':' lexpr .

    $výchozí  reduce using rule 37 (edecor)


State 310

   78 stmnt: error . ';'

    ';'  posunout a přejít do stavu 347


State 311

   77 label: LNAME . ':'
  243 name: LNAME .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, '?', LOROR, LANDAND, '|', '^', '&', LEQ, LNE, '<', '>', LLE, LGE, LLSH, LRSH, '+', '-', '*', '/', '%', LMM, LPP, LMG, '.', '[', '(']

    ':'  posunout a přejít do stavu 348

    $výchozí  reduce using rule 243 (name)


State 312

   94 ulstmnt: LBREAK . ';'

    ';'  posunout a přejít do stavu 349


State 313

   75 label: LCASE . expr ':'
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    expr     přejít do stavu 350
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 314

   95 ulstmnt: LCONTINUE . ';'

    ';'  posunout a přejít do stavu 351


State 315

   76 label: LDEFAULT . ':'

    ':'  posunout a přejít do stavu 352


State 316

   73 labels: . label
   74       | . labels label
   75 label: . LCASE expr ':'
   76      | . LDEFAULT ':'
   77      | . LNAME ':'
   78 stmnt: . error ';'
   79      | . ulstmnt
   80      | . labels ulstmnt
   83 ulstmnt: . zcexpr ';'
   84 $@7: . %empty  ['{']
   85 ulstmnt: . $@7 block
   86        | . LIF '(' cexpr ')' stmnt
   87        | . LIF '(' cexpr ')' stmnt LELSE stmnt
   88 $@8: . %empty  [LFOR]
   89 ulstmnt: . $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt
   90        | . LWHILE '(' cexpr ')' stmnt
   91        | . LDO stmnt LWHILE '(' cexpr ')' ';'
   91        | LDO . stmnt LWHILE '(' cexpr ')' ';'
   92        | . LRETURN zcexpr ';'
   93        | . LSWITCH '(' cexpr ')' stmnt
   94        | . LBREAK ';'
   95        | . LCONTINUE ';'
   96        | . LGOTO ltag ';'
   97        | . LUSED '(' zelist ')' ';'
   98        | . LPREFETCH '(' zelist ')' ';'
   99        | . LSET '(' zelist ')' ';'
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    error      posunout a přejít do stavu 310
    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 311
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LBREAK     posunout a přejít do stavu 312
    LCASE      posunout a přejít do stavu 313
    LCONTINUE  posunout a přejít do stavu 314
    LDEFAULT   posunout a přejít do stavu 315
    LDO        posunout a přejít do stavu 316
    LGOTO      posunout a přejít do stavu 317
    LIF        posunout a přejít do stavu 318
    LPREFETCH  posunout a přejít do stavu 319
    LRETURN    posunout a přejít do stavu 320
    LSIZEOF    posunout a přejít do stavu 111
    LUSED      posunout a přejít do stavu 321
    LSWITCH    posunout a přejít do stavu 322
    LWHILE     posunout a přejít do stavu 323
    LSET       posunout a přejít do stavu 324
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    ';'   reduce using rule 100 (zcexpr)
    LFOR  reduce using rule 88 ($@8)
    '{'   reduce using rule 84 ($@7)

    labels   přejít do stavu 327
    label    přejít do stavu 328
    stmnt    přejít do stavu 353
    ulstmnt  přejít do stavu 330
    $@7      přejít do stavu 331
    $@8      přejít do stavu 332
    zcexpr   přejít do stavu 333
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 317

   96 ulstmnt: LGOTO . ltag ';'
  245 ltag: . LNAME
  246     | . LTYPE

    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    ltag  přejít do stavu 354


State 318

   86 ulstmnt: LIF . '(' cexpr ')' stmnt
   87        | LIF . '(' cexpr ')' stmnt LELSE stmnt

    '('  posunout a přejít do stavu 355


State 319

   98 ulstmnt: LPREFETCH . '(' zelist ')' ';'

    '('  posunout a přejít do stavu 356


State 320

   92 ulstmnt: LRETURN . zcexpr ';'
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 100 (zcexpr)

    zcexpr   přejít do stavu 357
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 321

   97 ulstmnt: LUSED . '(' zelist ')' ';'

    '('  posunout a přejít do stavu 358


State 322

   93 ulstmnt: LSWITCH . '(' cexpr ')' stmnt

    '('  posunout a přejít do stavu 359


State 323

   90 ulstmnt: LWHILE . '(' cexpr ')' stmnt

    '('  posunout a přejít do stavu 360


State 324

   99 ulstmnt: LSET . '(' zelist ')' ';'

    '('  posunout a přejít do stavu 361


State 325

   69 block: '{' slist '}' .

    $výchozí  reduce using rule 69 (block)


State 326

   71 slist: slist adecl .

    $výchozí  reduce using rule 71 (slist)


State 327

   74 labels: labels . label
   75 label: . LCASE expr ':'
   76      | . LDEFAULT ':'
   77      | . LNAME ':'
   80 stmnt: labels . ulstmnt
   83 ulstmnt: . zcexpr ';'
   84 $@7: . %empty  ['{']
   85 ulstmnt: . $@7 block
   86        | . LIF '(' cexpr ')' stmnt
   87        | . LIF '(' cexpr ')' stmnt LELSE stmnt
   88 $@8: . %empty  [LFOR]
   89 ulstmnt: . $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt
   90        | . LWHILE '(' cexpr ')' stmnt
   91        | . LDO stmnt LWHILE '(' cexpr ')' ';'
   92        | . LRETURN zcexpr ';'
   93        | . LSWITCH '(' cexpr ')' stmnt
   94        | . LBREAK ';'
   95        | . LCONTINUE ';'
   96        | . LGOTO ltag ';'
   97        | . LUSED '(' zelist ')' ';'
   98        | . LPREFETCH '(' zelist ')' ';'
   99        | . LSET '(' zelist ')' ';'
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 311
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LBREAK     posunout a přejít do stavu 312
    LCASE      posunout a přejít do stavu 313
    LCONTINUE  posunout a přejít do stavu 314
    LDEFAULT   posunout a přejít do stavu 315
    LDO        posunout a přejít do stavu 316
    LGOTO      posunout a přejít do stavu 317
    LIF        posunout a přejít do stavu 318
    LPREFETCH  posunout a přejít do stavu 319
    LRETURN    posunout a přejít do stavu 320
    LSIZEOF    posunout a přejít do stavu 111
    LUSED      posunout a přejít do stavu 321
    LSWITCH    posunout a přejít do stavu 322
    LWHILE     posunout a přejít do stavu 323
    LSET       posunout a přejít do stavu 324
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    ';'         reduce using rule 100 (zcexpr)
    LFOR        reduce using rule 88 ($@8)
    $výchozí  reduce using rule 84 ($@7)

    label    přejít do stavu 362
    ulstmnt  přejít do stavu 363
    $@7      přejít do stavu 331
    $@8      přejít do stavu 332
    zcexpr   přejít do stavu 333
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 328

   73 labels: label .

    $výchozí  reduce using rule 73 (labels)


State 329

   72 slist: slist stmnt .

    $výchozí  reduce using rule 72 (slist)


State 330

   79 stmnt: ulstmnt .

    $výchozí  reduce using rule 79 (stmnt)


State 331

   69 block: . '{' slist '}'
   85 ulstmnt: $@7 . block

    '{'  posunout a přejít do stavu 216

    block  přejít do stavu 364


State 332

   89 ulstmnt: $@8 . LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt

    LFOR  posunout a přejít do stavu 365


State 333

   83 ulstmnt: zcexpr . ';'

    ';'  posunout a přejít do stavu 366


State 334

  101 zcexpr: cexpr .  [';', ')']
  106 cexpr: cexpr . ',' cexpr

    ','  posunout a přejít do stavu 226

    $výchozí  reduce using rule 101 (zcexpr)


State 335

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   18 adecl: ctlist . ';'
   19      | ctlist . adlist ';'
   20 adlist: . xdecor
   22       | . xdecor $@4 '=' init
   23       | . adlist ',' adlist
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    ';'    posunout a přejít do stavu 367
    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    xdecor   přejít do stavu 368
    xdecor2  přejít do stavu 50
    adlist   přejít do stavu 369
    tag      přejít do stavu 51
    ltag     přejít do stavu 52


State 336

   27 pdlist: pdlist . ',' pdlist
   27       | pdlist ',' pdlist .  [';', ',']

    $výchozí  reduce using rule 27 (pdlist)

    Conflict between rule 27 and token ',' resolved as reduce (%left ',').


State 337

   52 qual: '[' lexpr ']' .

    $výchozí  reduce using rule 52 (qual)


State 338

   56 qlist: qlist init ',' .

    $výchozí  reduce using rule 56 (qlist)


State 339

   50 init: . expr
   51     | . '{' ilist '}'
   52 qual: . '[' lexpr ']'
   53     | . '.' ltag
   54     | . qual '='
   55 qlist: . init ','
   56      | . qlist init ','
   57      | . qual
   58      | . qlist qual
   59 ilist: . qlist
   60      | . init
   61      | . qlist init
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  140       | '(' tlist abdecor ')' '{' . ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '.'        posunout a přejít do stavu 220
    '['        posunout a přejít do stavu 221
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '{'        posunout a přejít do stavu 138
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    init     přejít do stavu 222
    qual     přejít do stavu 223
    qlist    přejít do stavu 224
    ilist    přejít do stavu 370
    expr     přejít do stavu 140
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 340

  139 xuexpr: '(' tlist abdecor ')' xuexpr .

    $výchozí  reduce using rule 139 (xuexpr)


State 341

  153 pexpr: LSIZEOF '(' tlist abdecor ')' .

    $výchozí  reduce using rule 153 (pexpr)


State 342

  154 pexpr: LSIGNOF '(' tlist abdecor ')' .

    $výchozí  reduce using rule 154 (pexpr)


State 343

  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  126     | expr '?' cexpr ':' expr .  [';', ',', '=', LPE, LME, LMLE, LDVE, LMDE, LRSHE, LLSHE, LANDE, LXORE, LORE, ':', ')', ']', '}']
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '?'      posunout a přejít do stavu 168
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186

    $výchozí  reduce using rule 126 (expr)

    Conflict between rule 126 and token '=' resolved as reduce ('=' < ':').
    Conflict between rule 126 and token LPE resolved as reduce (LPE < ':').
    Conflict between rule 126 and token LME resolved as reduce (LME < ':').
    Conflict between rule 126 and token LMLE resolved as reduce (LMLE < ':').
    Conflict between rule 126 and token LDVE resolved as reduce (LDVE < ':').
    Conflict between rule 126 and token LMDE resolved as reduce (LMDE < ':').
    Conflict between rule 126 and token LRSHE resolved as reduce (LRSHE < ':').
    Conflict between rule 126 and token LLSHE resolved as reduce (LLSHE < ':').
    Conflict between rule 126 and token LANDE resolved as reduce (LANDE < ':').
    Conflict between rule 126 and token LXORE resolved as reduce (LXORE < ':').
    Conflict between rule 126 and token LORE resolved as reduce (LORE < ':').
    Conflict between rule 126 and token '?' resolved as shift (%right '?').
    Conflict between rule 126 and token LOROR resolved as shift (':' < LOROR).
    Conflict between rule 126 and token LANDAND resolved as shift (':' < LANDAND).
    Conflict between rule 126 and token '|' resolved as shift (':' < '|').
    Conflict between rule 126 and token '^' resolved as shift (':' < '^').
    Conflict between rule 126 and token '&' resolved as shift (':' < '&').
    Conflict between rule 126 and token LEQ resolved as shift (':' < LEQ).
    Conflict between rule 126 and token LNE resolved as shift (':' < LNE).
    Conflict between rule 126 and token '<' resolved as shift (':' < '<').
    Conflict between rule 126 and token '>' resolved as shift (':' < '>').
    Conflict between rule 126 and token LLE resolved as shift (':' < LLE).
    Conflict between rule 126 and token LGE resolved as shift (':' < LGE).
    Conflict between rule 126 and token LLSH resolved as shift (':' < LLSH).
    Conflict between rule 126 and token LRSH resolved as shift (':' < LRSH).
    Conflict between rule 126 and token '+' resolved as shift (':' < '+').
    Conflict between rule 126 and token '-' resolved as shift (':' < '-').
    Conflict between rule 126 and token '*' resolved as shift (':' < '*').
    Conflict between rule 126 and token '/' resolved as shift (':' < '/').
    Conflict between rule 126 and token '%' resolved as shift (':' < '%').


State 344

  179 elist: elist . ',' elist
  179      | elist ',' elist .  [',', ')']

    $výchozí  reduce using rule 179 (elist)

    Conflict between rule 179 and token ',' resolved as reduce (%left ',').


State 345

   46 abdecor2: abdecor2 '[' zexpr ']' .

    $výchozí  reduce using rule 46 (abdecor2)


State 346

   45 abdecor2: abdecor2 '(' zarglist ')' .

    $výchozí  reduce using rule 45 (abdecor2)


State 347

   78 stmnt: error ';' .

    $výchozí  reduce using rule 78 (stmnt)


State 348

   77 label: LNAME ':' .

    $výchozí  reduce using rule 77 (label)


State 349

   94 ulstmnt: LBREAK ';' .

    $výchozí  reduce using rule 94 (ulstmnt)


State 350

   75 label: LCASE expr . ':'
  108 expr: expr . '*' expr
  109     | expr . '/' expr
  110     | expr . '%' expr
  111     | expr . '+' expr
  112     | expr . '-' expr
  113     | expr . LRSH expr
  114     | expr . LLSH expr
  115     | expr . '<' expr
  116     | expr . '>' expr
  117     | expr . LLE expr
  118     | expr . LGE expr
  119     | expr . LEQ expr
  120     | expr . LNE expr
  121     | expr . '&' expr
  122     | expr . '^' expr
  123     | expr . '|' expr
  124     | expr . LANDAND expr
  125     | expr . LOROR expr
  126     | expr . '?' cexpr ':' expr
  127     | expr . '=' expr
  128     | expr . LPE expr
  129     | expr . LME expr
  130     | expr . LMLE expr
  131     | expr . LDVE expr
  132     | expr . LMDE expr
  133     | expr . LLSHE expr
  134     | expr . LRSHE expr
  135     | expr . LANDE expr
  136     | expr . LXORE expr
  137     | expr . LORE expr

    '='      posunout a přejít do stavu 157
    LPE      posunout a přejít do stavu 158
    LME      posunout a přejít do stavu 159
    LMLE     posunout a přejít do stavu 160
    LDVE     posunout a přejít do stavu 161
    LMDE     posunout a přejít do stavu 162
    LRSHE    posunout a přejít do stavu 163
    LLSHE    posunout a přejít do stavu 164
    LANDE    posunout a přejít do stavu 165
    LXORE    posunout a přejít do stavu 166
    LORE     posunout a přejít do stavu 167
    '?'      posunout a přejít do stavu 168
    ':'      posunout a přejít do stavu 371
    LOROR    posunout a přejít do stavu 169
    LANDAND  posunout a přejít do stavu 170
    '|'      posunout a přejít do stavu 171
    '^'      posunout a přejít do stavu 172
    '&'      posunout a přejít do stavu 173
    LEQ      posunout a přejít do stavu 174
    LNE      posunout a přejít do stavu 175
    '<'      posunout a přejít do stavu 176
    '>'      posunout a přejít do stavu 177
    LLE      posunout a přejít do stavu 178
    LGE      posunout a přejít do stavu 179
    LLSH     posunout a přejít do stavu 180
    LRSH     posunout a přejít do stavu 181
    '+'      posunout a přejít do stavu 182
    '-'      posunout a přejít do stavu 183
    '*'      posunout a přejít do stavu 184
    '/'      posunout a přejít do stavu 185
    '%'      posunout a přejít do stavu 186


State 351

   95 ulstmnt: LCONTINUE ';' .

    $výchozí  reduce using rule 95 (ulstmnt)


State 352

   76 label: LDEFAULT ':' .

    $výchozí  reduce using rule 76 (label)


State 353

   91 ulstmnt: LDO stmnt . LWHILE '(' cexpr ')' ';'

    LWHILE  posunout a přejít do stavu 372


State 354

   96 ulstmnt: LGOTO ltag . ';'

    ';'  posunout a přejít do stavu 373


State 355

   86 ulstmnt: LIF '(' . cexpr ')' stmnt
   87        | LIF '(' . cexpr ')' stmnt LELSE stmnt
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 374
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 356

   98 ulstmnt: LPREFETCH '(' . zelist ')' ';'
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  176 zelist: . %empty  [')']
  177       | . elist
  178 elist: . expr
  179      | . elist ',' elist
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 176 (zelist)

    expr     přejít do stavu 266
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    zelist   přejít do stavu 375
    elist    přejít do stavu 268
    name     přejít do stavu 123


State 357

   92 ulstmnt: LRETURN zcexpr . ';'

    ';'  posunout a přejít do stavu 376


State 358

   97 ulstmnt: LUSED '(' . zelist ')' ';'
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  176 zelist: . %empty  [')']
  177       | . elist
  178 elist: . expr
  179      | . elist ',' elist
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 176 (zelist)

    expr     přejít do stavu 266
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    zelist   přejít do stavu 377
    elist    přejít do stavu 268
    name     přejít do stavu 123


State 359

   93 ulstmnt: LSWITCH '(' . cexpr ')' stmnt
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 378
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 360

   90 ulstmnt: LWHILE '(' . cexpr ')' stmnt
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 379
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 361

   99 ulstmnt: LSET '(' . zelist ')' ';'
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  176 zelist: . %empty  [')']
  177       | . elist
  178 elist: . expr
  179      | . elist ',' elist
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 176 (zelist)

    expr     přejít do stavu 266
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    zelist   přejít do stavu 380
    elist    přejít do stavu 268
    name     přejít do stavu 123


State 362

   74 labels: labels label .

    $výchozí  reduce using rule 74 (labels)


State 363

   80 stmnt: labels ulstmnt .

    $výchozí  reduce using rule 80 (stmnt)


State 364

   85 ulstmnt: $@7 block .

    $výchozí  reduce using rule 85 (ulstmnt)


State 365

   89 ulstmnt: $@8 LFOR . '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt

    '('  posunout a přejít do stavu 381


State 366

   83 ulstmnt: zcexpr ';' .

    $výchozí  reduce using rule 83 (ulstmnt)


State 367

   18 adecl: ctlist ';' .

    $výchozí  reduce using rule 18 (adecl)


State 368

   20 adlist: xdecor .  [';', ',']
   21 $@4: . %empty  ['=']
   22 adlist: xdecor . $@4 '=' init

    '='         reduce using rule 21 ($@4)
    $výchozí  reduce using rule 20 (adlist)

    $@4  přejít do stavu 382


State 369

   19 adecl: ctlist adlist . ';'
   23 adlist: adlist . ',' adlist

    ';'  posunout a přejít do stavu 383
    ','  posunout a přejít do stavu 384


State 370

  140 xuexpr: '(' tlist abdecor ')' '{' ilist . '}'

    '}'  posunout a přejít do stavu 385


State 371

   75 label: LCASE expr ':' .

    $výchozí  reduce using rule 75 (label)


State 372

   91 ulstmnt: LDO stmnt LWHILE . '(' cexpr ')' ';'

    '('  posunout a přejít do stavu 386


State 373

   96 ulstmnt: LGOTO ltag ';' .

    $výchozí  reduce using rule 96 (ulstmnt)


State 374

   86 ulstmnt: LIF '(' cexpr . ')' stmnt
   87        | LIF '(' cexpr . ')' stmnt LELSE stmnt
  106 cexpr: cexpr . ',' cexpr

    ','  posunout a přejít do stavu 226
    ')'  posunout a přejít do stavu 387


State 375

   98 ulstmnt: LPREFETCH '(' zelist . ')' ';'

    ')'  posunout a přejít do stavu 388


State 376

   92 ulstmnt: LRETURN zcexpr ';' .

    $výchozí  reduce using rule 92 (ulstmnt)


State 377

   97 ulstmnt: LUSED '(' zelist . ')' ';'

    ')'  posunout a přejít do stavu 389


State 378

   93 ulstmnt: LSWITCH '(' cexpr . ')' stmnt
  106 cexpr: cexpr . ',' cexpr

    ','  posunout a přejít do stavu 226
    ')'  posunout a přejít do stavu 390


State 379

   90 ulstmnt: LWHILE '(' cexpr . ')' stmnt
  106 cexpr: cexpr . ',' cexpr

    ','  posunout a přejít do stavu 226
    ')'  posunout a přejít do stavu 391


State 380

   99 ulstmnt: LSET '(' zelist . ')' ';'

    ')'  posunout a přejít do stavu 392


State 381

   81 forexpr: . zcexpr
   82        | . ctlist adlist
   89 ulstmnt: $@8 LFOR '(' . forexpr ';' zcexpr ';' zcexpr ')' stmnt
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  184 types: . complex
  185      | . tname
  186      | . gcnlist
  187      | . complex gctnlist
  188      | . tname gctnlist
  189      | . gcnlist complex zgnlist
  190      | . gcnlist tname
  191      | . gcnlist tname gctnlist
  193 ctlist: . types
  194 complex: . LSTRUCT ltag
  196        | . LSTRUCT ltag $@10 sbody
  197        | . LSTRUCT sbody
  198        | . LUNION ltag
  200        | . LUNION ltag $@11 sbody
  201        | . LUNION sbody
  202        | . LENUM ltag
  205        | . LENUM ltag $@12 '{' $@13 enum '}'
  207        | . LENUM '{' $@14 enum '}'
  208        | . LTYPE
  216 gcnlist: . gcname
  217        | . gcnlist gcname
  218 gcname: . gname
  219       | . cname
  224 tname: . LCHAR
  225      | . LSHORT
  226      | . LINT
  227      | . LLONG
  228      | . LSIGNED
  229      | . LUNSIGNED
  230      | . LFLOAT
  231      | . LDOUBLE
  232      | . LVOID
  233 cname: . LAUTO
  234      | . LSTATIC
  235      | . LEXTERN
  236      | . LTYPEDEF
  237      | . LTYPESTR
  238      | . LREGISTER
  239      | . LINLINE
  240 gname: . LCONSTNT
  241      | . LVOLATILE
  242      | . LRESTRICT
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LTYPE      posunout a přejít do stavu 3
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LAUTO      posunout a přejít do stavu 4
    LCHAR      posunout a přejít do stavu 5
    LDOUBLE    posunout a přejít do stavu 6
    LEXTERN    posunout a přejít do stavu 7
    LFLOAT     posunout a přejít do stavu 8
    LINT       posunout a přejít do stavu 9
    LLONG      posunout a přejít do stavu 10
    LREGISTER  posunout a přejít do stavu 11
    LSHORT     posunout a přejít do stavu 12
    LSIZEOF    posunout a přejít do stavu 111
    LSTATIC    posunout a přejít do stavu 13
    LSTRUCT    posunout a přejít do stavu 14
    LTYPEDEF   posunout a přejít do stavu 15
    LTYPESTR   posunout a přejít do stavu 16
    LUNION     posunout a přejít do stavu 17
    LUNSIGNED  posunout a přejít do stavu 18
    LVOID      posunout a přejít do stavu 19
    LENUM      posunout a přejít do stavu 20
    LSIGNED    posunout a přejít do stavu 21
    LCONSTNT   posunout a přejít do stavu 22
    LVOLATILE  posunout a přejít do stavu 23
    LSIGNOF    posunout a přejít do stavu 112
    LRESTRICT  posunout a přejít do stavu 24
    LINLINE    posunout a přejít do stavu 25
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 100 (zcexpr)

    forexpr  přejít do stavu 393
    zcexpr   přejít do stavu 394
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    types    přejít do stavu 28
    ctlist   přejít do stavu 395
    complex  přejít do stavu 30
    gcnlist  přejít do stavu 31
    gcname   přejít do stavu 32
    tname    přejít do stavu 33
    cname    přejít do stavu 34
    gname    přejít do stavu 35
    name     přejít do stavu 123


State 382

   22 adlist: xdecor $@4 . '=' init

    '='  posunout a přejít do stavu 396


State 383

   19 adecl: ctlist adlist ';' .

    $výchozí  reduce using rule 19 (adecl)


State 384

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   20 adlist: . xdecor
   22       | . xdecor $@4 '=' init
   23       | . adlist ',' adlist
   23       | adlist ',' . adlist
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    xdecor   přejít do stavu 368
    xdecor2  přejít do stavu 50
    adlist   přejít do stavu 397
    tag      přejít do stavu 51
    ltag     přejít do stavu 52


State 385

  140 xuexpr: '(' tlist abdecor ')' '{' ilist '}' .

    $výchozí  reduce using rule 140 (xuexpr)


State 386

   91 ulstmnt: LDO stmnt LWHILE '(' . cexpr ')' ';'
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    cexpr    přejít do stavu 398
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 387

   73 labels: . label
   74       | . labels label
   75 label: . LCASE expr ':'
   76      | . LDEFAULT ':'
   77      | . LNAME ':'
   78 stmnt: . error ';'
   79      | . ulstmnt
   80      | . labels ulstmnt
   83 ulstmnt: . zcexpr ';'
   84 $@7: . %empty  ['{']
   85 ulstmnt: . $@7 block
   86        | . LIF '(' cexpr ')' stmnt
   86        | LIF '(' cexpr ')' . stmnt
   87        | . LIF '(' cexpr ')' stmnt LELSE stmnt
   87        | LIF '(' cexpr ')' . stmnt LELSE stmnt
   88 $@8: . %empty  [LFOR]
   89 ulstmnt: . $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt
   90        | . LWHILE '(' cexpr ')' stmnt
   91        | . LDO stmnt LWHILE '(' cexpr ')' ';'
   92        | . LRETURN zcexpr ';'
   93        | . LSWITCH '(' cexpr ')' stmnt
   94        | . LBREAK ';'
   95        | . LCONTINUE ';'
   96        | . LGOTO ltag ';'
   97        | . LUSED '(' zelist ')' ';'
   98        | . LPREFETCH '(' zelist ')' ';'
   99        | . LSET '(' zelist ')' ';'
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    error      posunout a přejít do stavu 310
    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 311
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LBREAK     posunout a přejít do stavu 312
    LCASE      posunout a přejít do stavu 313
    LCONTINUE  posunout a přejít do stavu 314
    LDEFAULT   posunout a přejít do stavu 315
    LDO        posunout a přejít do stavu 316
    LGOTO      posunout a přejít do stavu 317
    LIF        posunout a přejít do stavu 318
    LPREFETCH  posunout a přejít do stavu 319
    LRETURN    posunout a přejít do stavu 320
    LSIZEOF    posunout a přejít do stavu 111
    LUSED      posunout a přejít do stavu 321
    LSWITCH    posunout a přejít do stavu 322
    LWHILE     posunout a přejít do stavu 323
    LSET       posunout a přejít do stavu 324
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    ';'   reduce using rule 100 (zcexpr)
    LFOR  reduce using rule 88 ($@8)
    '{'   reduce using rule 84 ($@7)

    labels   přejít do stavu 327
    label    přejít do stavu 328
    stmnt    přejít do stavu 399
    ulstmnt  přejít do stavu 330
    $@7      přejít do stavu 331
    $@8      přejít do stavu 332
    zcexpr   přejít do stavu 333
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 388

   98 ulstmnt: LPREFETCH '(' zelist ')' . ';'

    ';'  posunout a přejít do stavu 400


State 389

   97 ulstmnt: LUSED '(' zelist ')' . ';'

    ';'  posunout a přejít do stavu 401


State 390

   73 labels: . label
   74       | . labels label
   75 label: . LCASE expr ':'
   76      | . LDEFAULT ':'
   77      | . LNAME ':'
   78 stmnt: . error ';'
   79      | . ulstmnt
   80      | . labels ulstmnt
   83 ulstmnt: . zcexpr ';'
   84 $@7: . %empty  ['{']
   85 ulstmnt: . $@7 block
   86        | . LIF '(' cexpr ')' stmnt
   87        | . LIF '(' cexpr ')' stmnt LELSE stmnt
   88 $@8: . %empty  [LFOR]
   89 ulstmnt: . $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt
   90        | . LWHILE '(' cexpr ')' stmnt
   91        | . LDO stmnt LWHILE '(' cexpr ')' ';'
   92        | . LRETURN zcexpr ';'
   93        | . LSWITCH '(' cexpr ')' stmnt
   93        | LSWITCH '(' cexpr ')' . stmnt
   94        | . LBREAK ';'
   95        | . LCONTINUE ';'
   96        | . LGOTO ltag ';'
   97        | . LUSED '(' zelist ')' ';'
   98        | . LPREFETCH '(' zelist ')' ';'
   99        | . LSET '(' zelist ')' ';'
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    error      posunout a přejít do stavu 310
    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 311
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LBREAK     posunout a přejít do stavu 312
    LCASE      posunout a přejít do stavu 313
    LCONTINUE  posunout a přejít do stavu 314
    LDEFAULT   posunout a přejít do stavu 315
    LDO        posunout a přejít do stavu 316
    LGOTO      posunout a přejít do stavu 317
    LIF        posunout a přejít do stavu 318
    LPREFETCH  posunout a přejít do stavu 319
    LRETURN    posunout a přejít do stavu 320
    LSIZEOF    posunout a přejít do stavu 111
    LUSED      posunout a přejít do stavu 321
    LSWITCH    posunout a přejít do stavu 322
    LWHILE     posunout a přejít do stavu 323
    LSET       posunout a přejít do stavu 324
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    ';'   reduce using rule 100 (zcexpr)
    LFOR  reduce using rule 88 ($@8)
    '{'   reduce using rule 84 ($@7)

    labels   přejít do stavu 327
    label    přejít do stavu 328
    stmnt    přejít do stavu 402
    ulstmnt  přejít do stavu 330
    $@7      přejít do stavu 331
    $@8      přejít do stavu 332
    zcexpr   přejít do stavu 333
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 391

   73 labels: . label
   74       | . labels label
   75 label: . LCASE expr ':'
   76      | . LDEFAULT ':'
   77      | . LNAME ':'
   78 stmnt: . error ';'
   79      | . ulstmnt
   80      | . labels ulstmnt
   83 ulstmnt: . zcexpr ';'
   84 $@7: . %empty  ['{']
   85 ulstmnt: . $@7 block
   86        | . LIF '(' cexpr ')' stmnt
   87        | . LIF '(' cexpr ')' stmnt LELSE stmnt
   88 $@8: . %empty  [LFOR]
   89 ulstmnt: . $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt
   90        | . LWHILE '(' cexpr ')' stmnt
   90        | LWHILE '(' cexpr ')' . stmnt
   91        | . LDO stmnt LWHILE '(' cexpr ')' ';'
   92        | . LRETURN zcexpr ';'
   93        | . LSWITCH '(' cexpr ')' stmnt
   94        | . LBREAK ';'
   95        | . LCONTINUE ';'
   96        | . LGOTO ltag ';'
   97        | . LUSED '(' zelist ')' ';'
   98        | . LPREFETCH '(' zelist ')' ';'
   99        | . LSET '(' zelist ')' ';'
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    error      posunout a přejít do stavu 310
    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 311
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LBREAK     posunout a přejít do stavu 312
    LCASE      posunout a přejít do stavu 313
    LCONTINUE  posunout a přejít do stavu 314
    LDEFAULT   posunout a přejít do stavu 315
    LDO        posunout a přejít do stavu 316
    LGOTO      posunout a přejít do stavu 317
    LIF        posunout a přejít do stavu 318
    LPREFETCH  posunout a přejít do stavu 319
    LRETURN    posunout a přejít do stavu 320
    LSIZEOF    posunout a přejít do stavu 111
    LUSED      posunout a přejít do stavu 321
    LSWITCH    posunout a přejít do stavu 322
    LWHILE     posunout a přejít do stavu 323
    LSET       posunout a přejít do stavu 324
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    ';'   reduce using rule 100 (zcexpr)
    LFOR  reduce using rule 88 ($@8)
    '{'   reduce using rule 84 ($@7)

    labels   přejít do stavu 327
    label    přejít do stavu 328
    stmnt    přejít do stavu 403
    ulstmnt  přejít do stavu 330
    $@7      přejít do stavu 331
    $@8      přejít do stavu 332
    zcexpr   přejít do stavu 333
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 392

   99 ulstmnt: LSET '(' zelist ')' . ';'

    ';'  posunout a přejít do stavu 404


State 393

   89 ulstmnt: $@8 LFOR '(' forexpr . ';' zcexpr ';' zcexpr ')' stmnt

    ';'  posunout a přejít do stavu 405


State 394

   81 forexpr: zcexpr .

    $výchozí  reduce using rule 81 (forexpr)


State 395

   12 xdecor: . xdecor2
   13       | . '*' zgnlist xdecor
   14 xdecor2: . tag
   15        | . '(' xdecor ')'
   16        | . xdecor2 '(' zarglist ')'
   17        | . xdecor2 '[' zexpr ']'
   20 adlist: . xdecor
   22       | . xdecor $@4 '=' init
   23       | . adlist ',' adlist
   82 forexpr: ctlist . adlist
  244 tag: . ltag
  245 ltag: . LNAME
  246     | . LTYPE

    '*'    posunout a přejít do stavu 46
    '('    posunout a přejít do stavu 47
    LNAME  posunout a přejít do stavu 36
    LTYPE  posunout a přejít do stavu 37

    xdecor   přejít do stavu 368
    xdecor2  přejít do stavu 50
    adlist   přejít do stavu 406
    tag      přejít do stavu 51
    ltag     přejít do stavu 52


State 396

   22 adlist: xdecor $@4 '=' . init
   50 init: . expr
   51     | . '{' ilist '}'
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '{'        posunout a přejít do stavu 138
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    init     přejít do stavu 407
    expr     přejít do stavu 140
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 397

   23 adlist: adlist . ',' adlist
   23       | adlist ',' adlist .  [';', ',']

    $výchozí  reduce using rule 23 (adlist)

    Conflict between rule 23 and token ',' resolved as reduce (%left ',').


State 398

   91 ulstmnt: LDO stmnt LWHILE '(' cexpr . ')' ';'
  106 cexpr: cexpr . ',' cexpr

    ','  posunout a přejít do stavu 226
    ')'  posunout a přejít do stavu 408


State 399

   86 ulstmnt: LIF '(' cexpr ')' stmnt .  [error, ';', '&', '+', '-', '*', LMM, LPP, '(', LNAME, LTYPE, LFCONST, LDCONST, LCONST, LLCONST, LUCONST, LULCONST, LVLCONST, LUVLCONST, LSTRING, LLSTRING, LAUTO, LBREAK, LCASE, LCHAR, LCONTINUE, LDEFAULT, LDO, LDOUBLE, LELSE, LEXTERN, LFLOAT, LFOR, LGOTO, LIF, LINT, LLONG, LPREFETCH, LREGISTER, LRETURN, LSHORT, LSIZEOF, LUSED, LSTATIC, LSTRUCT, LSWITCH, LTYPEDEF, LTYPESTR, LUNION, LUNSIGNED, LWHILE, LVOID, LENUM, LSIGNED, LCONSTNT, LVOLATILE, LSET, LSIGNOF, LRESTRICT, LINLINE, '{', '}', '!', '~']
   87        | LIF '(' cexpr ')' stmnt . LELSE stmnt

    LELSE  posunout a přejít do stavu 409

    LELSE       [reduce using rule 86 (ulstmnt)]
    $výchozí  reduce using rule 86 (ulstmnt)


State 400

   98 ulstmnt: LPREFETCH '(' zelist ')' ';' .

    $výchozí  reduce using rule 98 (ulstmnt)


State 401

   97 ulstmnt: LUSED '(' zelist ')' ';' .

    $výchozí  reduce using rule 97 (ulstmnt)


State 402

   93 ulstmnt: LSWITCH '(' cexpr ')' stmnt .

    $výchozí  reduce using rule 93 (ulstmnt)


State 403

   90 ulstmnt: LWHILE '(' cexpr ')' stmnt .

    $výchozí  reduce using rule 90 (ulstmnt)


State 404

   99 ulstmnt: LSET '(' zelist ')' ';' .

    $výchozí  reduce using rule 99 (ulstmnt)


State 405

   89 ulstmnt: $@8 LFOR '(' forexpr ';' . zcexpr ';' zcexpr ')' stmnt
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 100 (zcexpr)

    zcexpr   přejít do stavu 410
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 406

   23 adlist: adlist . ',' adlist
   82 forexpr: ctlist adlist .  [';']

    ','  posunout a přejít do stavu 384

    $výchozí  reduce using rule 82 (forexpr)


State 407

   22 adlist: xdecor $@4 '=' init .

    $výchozí  reduce using rule 22 (adlist)


State 408

   91 ulstmnt: LDO stmnt LWHILE '(' cexpr ')' . ';'

    ';'  posunout a přejít do stavu 411


State 409

   73 labels: . label
   74       | . labels label
   75 label: . LCASE expr ':'
   76      | . LDEFAULT ':'
   77      | . LNAME ':'
   78 stmnt: . error ';'
   79      | . ulstmnt
   80      | . labels ulstmnt
   83 ulstmnt: . zcexpr ';'
   84 $@7: . %empty  ['{']
   85 ulstmnt: . $@7 block
   86        | . LIF '(' cexpr ')' stmnt
   87        | . LIF '(' cexpr ')' stmnt LELSE stmnt
   87        | LIF '(' cexpr ')' stmnt LELSE . stmnt
   88 $@8: . %empty  [LFOR]
   89 ulstmnt: . $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt
   90        | . LWHILE '(' cexpr ')' stmnt
   91        | . LDO stmnt LWHILE '(' cexpr ')' ';'
   92        | . LRETURN zcexpr ';'
   93        | . LSWITCH '(' cexpr ')' stmnt
   94        | . LBREAK ';'
   95        | . LCONTINUE ';'
   96        | . LGOTO ltag ';'
   97        | . LUSED '(' zelist ')' ';'
   98        | . LPREFETCH '(' zelist ')' ';'
   99        | . LSET '(' zelist ')' ';'
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    error      posunout a přejít do stavu 310
    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 311
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LBREAK     posunout a přejít do stavu 312
    LCASE      posunout a přejít do stavu 313
    LCONTINUE  posunout a přejít do stavu 314
    LDEFAULT   posunout a přejít do stavu 315
    LDO        posunout a přejít do stavu 316
    LGOTO      posunout a přejít do stavu 317
    LIF        posunout a přejít do stavu 318
    LPREFETCH  posunout a přejít do stavu 319
    LRETURN    posunout a přejít do stavu 320
    LSIZEOF    posunout a přejít do stavu 111
    LUSED      posunout a přejít do stavu 321
    LSWITCH    posunout a přejít do stavu 322
    LWHILE     posunout a přejít do stavu 323
    LSET       posunout a přejít do stavu 324
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    ';'   reduce using rule 100 (zcexpr)
    LFOR  reduce using rule 88 ($@8)
    '{'   reduce using rule 84 ($@7)

    labels   přejít do stavu 327
    label    přejít do stavu 328
    stmnt    přejít do stavu 412
    ulstmnt  přejít do stavu 330
    $@7      přejít do stavu 331
    $@8      přejít do stavu 332
    zcexpr   přejít do stavu 333
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 410

   89 ulstmnt: $@8 LFOR '(' forexpr ';' zcexpr . ';' zcexpr ')' stmnt

    ';'  posunout a přejít do stavu 413


State 411

   91 ulstmnt: LDO stmnt LWHILE '(' cexpr ')' ';' .

    $výchozí  reduce using rule 91 (ulstmnt)


State 412

   87 ulstmnt: LIF '(' cexpr ')' stmnt LELSE stmnt .

    $výchozí  reduce using rule 87 (ulstmnt)


State 413

   89 ulstmnt: $@8 LFOR '(' forexpr ';' zcexpr ';' . zcexpr ')' stmnt
  100 zcexpr: . %empty  [')']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 100
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LSIZEOF    posunout a přejít do stavu 111
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    $výchozí  reduce using rule 100 (zcexpr)

    zcexpr   přejít do stavu 414
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 414

   89 ulstmnt: $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr . ')' stmnt

    ')'  posunout a přejít do stavu 415


State 415

   73 labels: . label
   74       | . labels label
   75 label: . LCASE expr ':'
   76      | . LDEFAULT ':'
   77      | . LNAME ':'
   78 stmnt: . error ';'
   79      | . ulstmnt
   80      | . labels ulstmnt
   83 ulstmnt: . zcexpr ';'
   84 $@7: . %empty  ['{']
   85 ulstmnt: . $@7 block
   86        | . LIF '(' cexpr ')' stmnt
   87        | . LIF '(' cexpr ')' stmnt LELSE stmnt
   88 $@8: . %empty  [LFOR]
   89 ulstmnt: . $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt
   89        | $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' . stmnt
   90        | . LWHILE '(' cexpr ')' stmnt
   91        | . LDO stmnt LWHILE '(' cexpr ')' ';'
   92        | . LRETURN zcexpr ';'
   93        | . LSWITCH '(' cexpr ')' stmnt
   94        | . LBREAK ';'
   95        | . LCONTINUE ';'
   96        | . LGOTO ltag ';'
   97        | . LUSED '(' zelist ')' ';'
   98        | . LPREFETCH '(' zelist ')' ';'
   99        | . LSET '(' zelist ')' ';'
  100 zcexpr: . %empty  [';']
  101       | . cexpr
  105 cexpr: . expr
  106      | . cexpr ',' cexpr
  107 expr: . xuexpr
  108     | . expr '*' expr
  109     | . expr '/' expr
  110     | . expr '%' expr
  111     | . expr '+' expr
  112     | . expr '-' expr
  113     | . expr LRSH expr
  114     | . expr LLSH expr
  115     | . expr '<' expr
  116     | . expr '>' expr
  117     | . expr LLE expr
  118     | . expr LGE expr
  119     | . expr LEQ expr
  120     | . expr LNE expr
  121     | . expr '&' expr
  122     | . expr '^' expr
  123     | . expr '|' expr
  124     | . expr LANDAND expr
  125     | . expr LOROR expr
  126     | . expr '?' cexpr ':' expr
  127     | . expr '=' expr
  128     | . expr LPE expr
  129     | . expr LME expr
  130     | . expr LMLE expr
  131     | . expr LDVE expr
  132     | . expr LMDE expr
  133     | . expr LLSHE expr
  134     | . expr LRSHE expr
  135     | . expr LANDE expr
  136     | . expr LXORE expr
  137     | . expr LORE expr
  138 xuexpr: . uexpr
  139       | . '(' tlist abdecor ')' xuexpr
  140       | . '(' tlist abdecor ')' '{' ilist '}'
  141 uexpr: . pexpr
  142      | . '*' xuexpr
  143      | . '&' xuexpr
  144      | . '+' xuexpr
  145      | . '-' xuexpr
  146      | . '!' xuexpr
  147      | . '~' xuexpr
  148      | . LPP xuexpr
  149      | . LMM xuexpr
  150      | . LSIZEOF uexpr
  151      | . LSIGNOF uexpr
  152 pexpr: . '(' cexpr ')'
  153      | . LSIZEOF '(' tlist abdecor ')'
  154      | . LSIGNOF '(' tlist abdecor ')'
  155      | . pexpr '(' zelist ')'
  156      | . pexpr '[' cexpr ']'
  157      | . pexpr LMG ltag
  158      | . pexpr '.' ltag
  159      | . pexpr LPP
  160      | . pexpr LMM
  161      | . name
  162      | . LCONST
  163      | . LLCONST
  164      | . LUCONST
  165      | . LULCONST
  166      | . LDCONST
  167      | . LFCONST
  168      | . LVLCONST
  169      | . LUVLCONST
  170      | . string
  171      | . lstring
  172 string: . LSTRING
  173       | . string LSTRING
  174 lstring: . LLSTRING
  175        | . lstring LLSTRING
  243 name: . LNAME

    error      posunout a přejít do stavu 310
    '&'        posunout a přejít do stavu 93
    '+'        posunout a přejít do stavu 94
    '-'        posunout a přejít do stavu 95
    '*'        posunout a přejít do stavu 96
    LMM        posunout a přejít do stavu 97
    LPP        posunout a přejít do stavu 98
    '('        posunout a přejít do stavu 99
    LNAME      posunout a přejít do stavu 311
    LFCONST    posunout a přejít do stavu 101
    LDCONST    posunout a přejít do stavu 102
    LCONST     posunout a přejít do stavu 103
    LLCONST    posunout a přejít do stavu 104
    LUCONST    posunout a přejít do stavu 105
    LULCONST   posunout a přejít do stavu 106
    LVLCONST   posunout a přejít do stavu 107
    LUVLCONST  posunout a přejít do stavu 108
    LSTRING    posunout a přejít do stavu 109
    LLSTRING   posunout a přejít do stavu 110
    LBREAK     posunout a přejít do stavu 312
    LCASE      posunout a přejít do stavu 313
    LCONTINUE  posunout a přejít do stavu 314
    LDEFAULT   posunout a přejít do stavu 315
    LDO        posunout a přejít do stavu 316
    LGOTO      posunout a přejít do stavu 317
    LIF        posunout a přejít do stavu 318
    LPREFETCH  posunout a přejít do stavu 319
    LRETURN    posunout a přejít do stavu 320
    LSIZEOF    posunout a přejít do stavu 111
    LUSED      posunout a přejít do stavu 321
    LSWITCH    posunout a přejít do stavu 322
    LWHILE     posunout a přejít do stavu 323
    LSET       posunout a přejít do stavu 324
    LSIGNOF    posunout a přejít do stavu 112
    '!'        posunout a přejít do stavu 113
    '~'        posunout a přejít do stavu 114

    ';'   reduce using rule 100 (zcexpr)
    LFOR  reduce using rule 88 ($@8)
    '{'   reduce using rule 84 ($@7)

    labels   přejít do stavu 327
    label    přejít do stavu 328
    stmnt    přejít do stavu 416
    ulstmnt  přejít do stavu 330
    $@7      přejít do stavu 331
    $@8      přejít do stavu 332
    zcexpr   přejít do stavu 333
    cexpr    přejít do stavu 334
    expr     přejít do stavu 148
    xuexpr   přejít do stavu 118
    uexpr    přejít do stavu 119
    pexpr    přejít do stavu 120
    string   přejít do stavu 121
    lstring  přejít do stavu 122
    name     přejít do stavu 123


State 416

   89 ulstmnt: $@8 LFOR '(' forexpr ';' zcexpr ';' zcexpr ')' stmnt .

    $výchozí  reduce using rule 89 (ulstmnt)
