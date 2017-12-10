Terminals unused in grammar

   LTYPEG
   LTYPEX


Gramatika

    0 $accept: prog $end

    1 prog: %empty

    2 $@1: %empty

    3 prog: prog $@1 line

    4 $@2: %empty

    5 line: LLAB ':' $@2 line

    6 $@3: %empty

    7 line: LNAME ':' $@3 line
    8     | LNAME '=' expr ';'
    9     | LVAR '=' expr ';'
   10     | ';'
   11     | inst ';'
   12     | error ';'

   13 inst: LTYPE1 cond imsr ',' spreg ',' reg
   14     | LTYPE1 cond imsr ',' spreg ','
   15     | LTYPE1 cond imsr ',' reg
   16     | LTYPE2 cond imsr ',' reg
   17     | LTYPE3 cond gen ',' gen
   18     | LTYPE4 cond comma rel
   19     | LTYPE4 cond comma nireg
   20     | LTYPEBX comma ireg
   21     | LTYPE5 comma rel
   22     | LTYPE6 cond comma gen
   23     | LTYPE7 cond imsr ',' spreg comma
   24     | LTYPE8 cond ioreg ',' '[' reglist ']'
   25     | LTYPE8 cond '[' reglist ']' ',' ioreg
   26     | LTYPE9 cond reg ',' ireg ',' reg
   27     | LTYPE9 cond reg ',' ireg comma
   28     | LTYPE9 cond comma ireg ',' reg
   29     | LTYPEA cond comma
   30     | LTYPEB name ',' imm
   31     | LTYPEB name ',' con ',' imm
   32     | LTYPEB name ',' con ',' imm '-' con
   33     | LTYPEC name '/' con ',' ximm
   34     | LTYPED cond reg comma
   35     | LTYPEH comma ximm
   36     | LTYPEI cond freg ',' freg
   37     | LTYPEK cond frcon ',' freg
   38     | LTYPEK cond frcon ',' LFREG ',' freg
   39     | LTYPEL cond freg ',' freg comma
   40     | LTYPEJ cond con ',' expr ',' spreg ',' creg ',' creg oexpr
   41     | LTYPEM cond reg ',' reg ',' regreg
   42     | LTYPEN cond reg ',' reg ',' reg ',' spreg
   43     | LTYPEPLD oreg
   44     | LTYPEPC gen ',' gen
   45     | LTYPEF gen ',' gen
   46     | LTYPEE comma

   47 cond: %empty
   48     | cond LCOND
   49     | cond LS

   50 comma: %empty
   51      | ',' comma

   52 rel: con '(' LPC ')'
   53    | LNAME offset
   54    | LLAB offset

   55 ximm: '$' con
   56     | '$' oreg
   57     | '$' '*' '$' oreg
   58     | '$' LSCONST
   59     | fcon

   60 fcon: '$' LFCONST
   61     | '$' '-' LFCONST

   62 reglist: spreg
   63        | spreg '-' spreg
   64        | spreg comma reglist

   65 gen: reg
   66    | ximm
   67    | shift
   68    | shift '(' spreg ')'
   69    | LPSR
   70    | LFCR
   71    | con
   72    | oreg
   73    | freg

   74 nireg: ireg
   75      | name

   76 ireg: '(' spreg ')'

   77 ioreg: ireg
   78      | con '(' sreg ')'

   79 oreg: name
   80     | name '(' sreg ')'
   81     | ioreg

   82 imsr: reg
   83     | imm
   84     | shift

   85 imm: '$' con

   86 reg: spreg

   87 regreg: '(' spreg ',' spreg ')'

   88 shift: spreg '<' '<' rcon
   89      | spreg '>' '>' rcon
   90      | spreg '-' '>' rcon
   91      | spreg LAT '>' rcon

   92 rcon: spreg
   93     | con

   94 sreg: LREG
   95     | LPC
   96     | LR '(' expr ')'

   97 spreg: sreg
   98      | LSP

   99 creg: LCREG
  100     | LC '(' expr ')'

  101 frcon: freg
  102      | fcon

  103 freg: LFREG
  104     | LF '(' con ')'

  105 name: con '(' pointer ')'
  106     | LNAME offset '(' pointer ')'
  107     | LNAME '<' '>' offset '(' LSB ')'

  108 offset: %empty
  109       | '+' con
  110       | '-' con

  111 pointer: LSB
  112        | LSP
  113        | LFP

  114 con: LCONST
  115    | LVAR
  116    | '-' con
  117    | '+' con
  118    | '~' con
  119    | '(' expr ')'

  120 oexpr: %empty
  121      | ',' expr

  122 expr: con
  123     | expr '+' expr
  124     | expr '-' expr
  125     | expr '*' expr
  126     | expr '/' expr
  127     | expr '%' expr
  128     | expr '<' '<' expr
  129     | expr '>' '>' expr
  130     | expr '&' expr
  131     | expr '^' expr
  132     | expr '|' expr


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'$' (36) 55 56 57 58 60 61 85
'%' (37) 127
'&' (38) 130
'(' (40) 52 68 76 78 80 87 96 100 104 105 106 107 119
')' (41) 52 68 76 78 80 87 96 100 104 105 106 107 119
'*' (42) 57 125
'+' (43) 109 117 123
',' (44) 13 14 15 16 17 23 24 25 26 27 28 30 31 32 33 36 37 38 39 40
    41 42 44 45 51 87 121
'-' (45) 32 61 63 90 110 116 124
'/' (47) 33 126
':' (58) 5 7
';' (59) 8 9 10 11 12
'<' (60) 88 107 128
'=' (61) 8 9
'>' (62) 89 90 91 107 129
'[' (91) 24 25
']' (93) 24 25
'^' (94) 131
'|' (124) 132
'~' (126) 118
error (256) 12
LTYPE1 (258) 13 14 15
LTYPE2 (259) 16
LTYPE3 (260) 17
LTYPE4 (261) 18 19
LTYPE5 (262) 21
LTYPE6 (263) 22
LTYPE7 (264) 23
LTYPE8 (265) 24 25
LTYPE9 (266) 26 27 28
LTYPEA (267) 29
LTYPEB (268) 30 31 32
LTYPEC (269) 33
LTYPED (270) 34
LTYPEE (271) 46
LTYPEG (272)
LTYPEH (273) 35
LTYPEI (274) 36
LTYPEJ (275) 40
LTYPEK (276) 37 38
LTYPEL (277) 39
LTYPEM (278) 41
LTYPEN (279) 42
LTYPEBX (280) 20
LTYPEPLD (281) 43
LCONST (282) 114
LSP (283) 98 112
LSB (284) 107 111
LFP (285) 113
LPC (286) 52 95
LTYPEX (287)
LTYPEPC (288) 44
LTYPEF (289) 45
LR (290) 96
LREG (291) 94
LF (292) 104
LFREG (293) 38 103
LC (294) 100
LCREG (295) 99
LPSR (296) 69
LFCR (297) 70
LCOND (298) 48
LS (299) 49
LAT (300) 91
LFCONST (301) 60 61
LSCONST (302) 58
LNAME (303) 7 8 53 106 107
LLAB (304) 5 54
LVAR (305) 9 115


Neterminály s pravidly, ve kterých se objevují

$accept (71)
    vlevo: 0
prog (72)
    vlevo: 1 3, vpravo: 0 3
$@1 (73)
    vlevo: 2, vpravo: 3
line (74)
    vlevo: 5 7 8 9 10 11 12, vpravo: 3 5 7
$@2 (75)
    vlevo: 4, vpravo: 5
$@3 (76)
    vlevo: 6, vpravo: 7
inst (77)
    vlevo: 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31
    32 33 34 35 36 37 38 39 40 41 42 43 44 45 46, vpravo: 11
cond (78)
    vlevo: 47 48 49, vpravo: 13 14 15 16 17 18 19 22 23 24 25 26 27
    28 29 34 36 37 38 39 40 41 42 48 49
comma (79)
    vlevo: 50 51, vpravo: 18 19 20 21 22 23 27 28 29 34 35 39 46 51
    64
rel (80)
    vlevo: 52 53 54, vpravo: 18 21
ximm (81)
    vlevo: 55 56 57 58 59, vpravo: 33 35 66
fcon (82)
    vlevo: 60 61, vpravo: 59 102
reglist (83)
    vlevo: 62 63 64, vpravo: 24 25 64
gen (84)
    vlevo: 65 66 67 68 69 70 71 72 73, vpravo: 17 22 44 45
nireg (85)
    vlevo: 74 75, vpravo: 19
ireg (86)
    vlevo: 76, vpravo: 20 26 27 28 74 77
ioreg (87)
    vlevo: 77 78, vpravo: 24 25 81
oreg (88)
    vlevo: 79 80 81, vpravo: 43 56 57 72
imsr (89)
    vlevo: 82 83 84, vpravo: 13 14 15 16 23
imm (90)
    vlevo: 85, vpravo: 30 31 32 83
reg (91)
    vlevo: 86, vpravo: 13 15 16 26 27 28 34 41 42 65 82
regreg (92)
    vlevo: 87, vpravo: 41
shift (93)
    vlevo: 88 89 90 91, vpravo: 67 68 84
rcon (94)
    vlevo: 92 93, vpravo: 88 89 90 91
sreg (95)
    vlevo: 94 95 96, vpravo: 78 80 97
spreg (96)
    vlevo: 97 98, vpravo: 13 14 23 40 42 62 63 64 68 76 86 87 88 89
    90 91 92
creg (97)
    vlevo: 99 100, vpravo: 40
frcon (98)
    vlevo: 101 102, vpravo: 37 38
freg (99)
    vlevo: 103 104, vpravo: 36 37 38 39 73 101
name (100)
    vlevo: 105 106 107, vpravo: 30 31 32 33 75 79 80
offset (101)
    vlevo: 108 109 110, vpravo: 53 54 106 107
pointer (102)
    vlevo: 111 112 113, vpravo: 105 106
con (103)
    vlevo: 114 115 116 117 118 119, vpravo: 31 32 33 40 52 55 71 78
    85 93 104 105 109 110 116 117 118 122
oexpr (104)
    vlevo: 120 121, vpravo: 40
expr (105)
    vlevo: 122 123 124 125 126 127 128 129 130 131 132, vpravo: 8 9
    40 96 100 119 121 123 124 125 126 127 128 129 130 131 132


State 0

    0 $accept: . prog $end
    1 prog: . %empty
    3     | . prog $@1 line

    $výchozí  reduce using rule 1 (prog)

    prog  přejít do stavu 1


State 1

    0 $accept: prog . $end
    2 $@1: . %empty  [error, LTYPE1, LTYPE2, LTYPE3, LTYPE4, LTYPE5, LTYPE6, LTYPE7, LTYPE8, LTYPE9, LTYPEA, LTYPEB, LTYPEC, LTYPED, LTYPEE, LTYPEH, LTYPEI, LTYPEJ, LTYPEK, LTYPEL, LTYPEM, LTYPEN, LTYPEBX, LTYPEPLD, LTYPEPC, LTYPEF, LNAME, LLAB, LVAR, ';']
    3 prog: prog . $@1 line

    $end  posunout a přejít do stavu 2

    $výchozí  reduce using rule 2 ($@1)

    $@1  přejít do stavu 3


State 2

    0 $accept: prog $end .

    $výchozí  přijmout


State 3

    3 prog: prog $@1 . line
    5 line: . LLAB ':' $@2 line
    7     | . LNAME ':' $@3 line
    8     | . LNAME '=' expr ';'
    9     | . LVAR '=' expr ';'
   10     | . ';'
   11     | . inst ';'
   12     | . error ';'
   13 inst: . LTYPE1 cond imsr ',' spreg ',' reg
   14     | . LTYPE1 cond imsr ',' spreg ','
   15     | . LTYPE1 cond imsr ',' reg
   16     | . LTYPE2 cond imsr ',' reg
   17     | . LTYPE3 cond gen ',' gen
   18     | . LTYPE4 cond comma rel
   19     | . LTYPE4 cond comma nireg
   20     | . LTYPEBX comma ireg
   21     | . LTYPE5 comma rel
   22     | . LTYPE6 cond comma gen
   23     | . LTYPE7 cond imsr ',' spreg comma
   24     | . LTYPE8 cond ioreg ',' '[' reglist ']'
   25     | . LTYPE8 cond '[' reglist ']' ',' ioreg
   26     | . LTYPE9 cond reg ',' ireg ',' reg
   27     | . LTYPE9 cond reg ',' ireg comma
   28     | . LTYPE9 cond comma ireg ',' reg
   29     | . LTYPEA cond comma
   30     | . LTYPEB name ',' imm
   31     | . LTYPEB name ',' con ',' imm
   32     | . LTYPEB name ',' con ',' imm '-' con
   33     | . LTYPEC name '/' con ',' ximm
   34     | . LTYPED cond reg comma
   35     | . LTYPEH comma ximm
   36     | . LTYPEI cond freg ',' freg
   37     | . LTYPEK cond frcon ',' freg
   38     | . LTYPEK cond frcon ',' LFREG ',' freg
   39     | . LTYPEL cond freg ',' freg comma
   40     | . LTYPEJ cond con ',' expr ',' spreg ',' creg ',' creg oexpr
   41     | . LTYPEM cond reg ',' reg ',' regreg
   42     | . LTYPEN cond reg ',' reg ',' reg ',' spreg
   43     | . LTYPEPLD oreg
   44     | . LTYPEPC gen ',' gen
   45     | . LTYPEF gen ',' gen
   46     | . LTYPEE comma

    error     posunout a přejít do stavu 4
    LTYPE1    posunout a přejít do stavu 5
    LTYPE2    posunout a přejít do stavu 6
    LTYPE3    posunout a přejít do stavu 7
    LTYPE4    posunout a přejít do stavu 8
    LTYPE5    posunout a přejít do stavu 9
    LTYPE6    posunout a přejít do stavu 10
    LTYPE7    posunout a přejít do stavu 11
    LTYPE8    posunout a přejít do stavu 12
    LTYPE9    posunout a přejít do stavu 13
    LTYPEA    posunout a přejít do stavu 14
    LTYPEB    posunout a přejít do stavu 15
    LTYPEC    posunout a přejít do stavu 16
    LTYPED    posunout a přejít do stavu 17
    LTYPEE    posunout a přejít do stavu 18
    LTYPEH    posunout a přejít do stavu 19
    LTYPEI    posunout a přejít do stavu 20
    LTYPEJ    posunout a přejít do stavu 21
    LTYPEK    posunout a přejít do stavu 22
    LTYPEL    posunout a přejít do stavu 23
    LTYPEM    posunout a přejít do stavu 24
    LTYPEN    posunout a přejít do stavu 25
    LTYPEBX   posunout a přejít do stavu 26
    LTYPEPLD  posunout a přejít do stavu 27
    LTYPEPC   posunout a přejít do stavu 28
    LTYPEF    posunout a přejít do stavu 29
    LNAME     posunout a přejít do stavu 30
    LLAB      posunout a přejít do stavu 31
    LVAR      posunout a přejít do stavu 32
    ';'       posunout a přejít do stavu 33

    line  přejít do stavu 34
    inst  přejít do stavu 35


State 4

   12 line: error . ';'

    ';'  posunout a přejít do stavu 36


State 5

   13 inst: LTYPE1 . cond imsr ',' spreg ',' reg
   14     | LTYPE1 . cond imsr ',' spreg ','
   15     | LTYPE1 . cond imsr ',' reg
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 37


State 6

   16 inst: LTYPE2 . cond imsr ',' reg
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 38


State 7

   17 inst: LTYPE3 . cond gen ',' gen
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 39


State 8

   18 inst: LTYPE4 . cond comma rel
   19     | LTYPE4 . cond comma nireg
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 40


State 9

   21 inst: LTYPE5 . comma rel
   50 comma: . %empty  ['+', '-', LCONST, LNAME, LLAB, LVAR, '(', '~']
   51      | . ',' comma

    ','  posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 42


State 10

   22 inst: LTYPE6 . cond comma gen
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 43


State 11

   23 inst: LTYPE7 . cond imsr ',' spreg comma
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 44


State 12

   24 inst: LTYPE8 . cond ioreg ',' '[' reglist ']'
   25     | LTYPE8 . cond '[' reglist ']' ',' ioreg
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 45


State 13

   26 inst: LTYPE9 . cond reg ',' ireg ',' reg
   27     | LTYPE9 . cond reg ',' ireg comma
   28     | LTYPE9 . cond comma ireg ',' reg
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 46


State 14

   29 inst: LTYPEA . cond comma
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 47


State 15

   30 inst: LTYPEB . name ',' imm
   31     | LTYPEB . name ',' con ',' imm
   32     | LTYPEB . name ',' con ',' imm '-' con
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    name  přejít do stavu 55
    con   přejít do stavu 56


State 16

   33 inst: LTYPEC . name '/' con ',' ximm
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    name  přejít do stavu 57
    con   přejít do stavu 56


State 17

   34 inst: LTYPED . cond reg comma
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 58


State 18

   46 inst: LTYPEE . comma
   50 comma: . %empty  [';']
   51      | . ',' comma

    ','  posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 59


State 19

   35 inst: LTYPEH . comma ximm
   50 comma: . %empty  ['$']
   51      | . ',' comma

    ','  posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 60


State 20

   36 inst: LTYPEI . cond freg ',' freg
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 61


State 21

   40 inst: LTYPEJ . cond con ',' expr ',' spreg ',' creg ',' creg oexpr
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 62


State 22

   37 inst: LTYPEK . cond frcon ',' freg
   38     | LTYPEK . cond frcon ',' LFREG ',' freg
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 63


State 23

   39 inst: LTYPEL . cond freg ',' freg comma
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 64


State 24

   41 inst: LTYPEM . cond reg ',' reg ',' regreg
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 65


State 25

   42 inst: LTYPEN . cond reg ',' reg ',' reg ',' spreg
   47 cond: . %empty
   48     | . cond LCOND
   49     | . cond LS

    $výchozí  reduce using rule 47 (cond)

    cond  přejít do stavu 66


State 26

   20 inst: LTYPEBX . comma ireg
   50 comma: . %empty  ['(']
   51      | . ',' comma

    ','  posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 67


State 27

   43 inst: LTYPEPLD . oreg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '~'     posunout a přejít do stavu 54

    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 71
    name   přejít do stavu 72
    con    přejít do stavu 73


State 28

   44 inst: LTYPEPC . gen ',' gen
   55 ximm: . '$' con
   56     | . '$' oreg
   57     | . '$' '*' '$' oreg
   58     | . '$' LSCONST
   59     | . fcon
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST
   65 gen: . reg
   66    | . ximm
   67    | . shift
   68    | . shift '(' spreg ')'
   69    | . LPSR
   70    | . LFCR
   71    | . con
   72    | . oreg
   73    | . freg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  103 freg: . LFREG
  104     | . LF '(' con ')'
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LF      posunout a přejít do stavu 78
    LFREG   posunout a přejít do stavu 79
    LPSR    posunout a přejít do stavu 80
    LFCR    posunout a přejít do stavu 81
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '$'     posunout a přejít do stavu 82
    '~'     posunout a přejít do stavu 54

    ximm   přejít do stavu 83
    fcon   přejít do stavu 84
    gen    přejít do stavu 85
    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 86
    reg    přejít do stavu 87
    shift  přejít do stavu 88
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90
    freg   přejít do stavu 91
    name   přejít do stavu 72
    con    přejít do stavu 92


State 29

   45 inst: LTYPEF . gen ',' gen
   55 ximm: . '$' con
   56     | . '$' oreg
   57     | . '$' '*' '$' oreg
   58     | . '$' LSCONST
   59     | . fcon
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST
   65 gen: . reg
   66    | . ximm
   67    | . shift
   68    | . shift '(' spreg ')'
   69    | . LPSR
   70    | . LFCR
   71    | . con
   72    | . oreg
   73    | . freg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  103 freg: . LFREG
  104     | . LF '(' con ')'
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LF      posunout a přejít do stavu 78
    LFREG   posunout a přejít do stavu 79
    LPSR    posunout a přejít do stavu 80
    LFCR    posunout a přejít do stavu 81
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '$'     posunout a přejít do stavu 82
    '~'     posunout a přejít do stavu 54

    ximm   přejít do stavu 83
    fcon   přejít do stavu 84
    gen    přejít do stavu 93
    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 86
    reg    přejít do stavu 87
    shift  přejít do stavu 88
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90
    freg   přejít do stavu 91
    name   přejít do stavu 72
    con    přejít do stavu 92


State 30

    7 line: LNAME . ':' $@3 line
    8     | LNAME . '=' expr ';'

    ':'  posunout a přejít do stavu 94
    '='  posunout a přejít do stavu 95


State 31

    5 line: LLAB . ':' $@2 line

    ':'  posunout a přejít do stavu 96


State 32

    9 line: LVAR . '=' expr ';'

    '='  posunout a přejít do stavu 97


State 33

   10 line: ';' .

    $výchozí  reduce using rule 10 (line)


State 34

    3 prog: prog $@1 line .

    $výchozí  reduce using rule 3 (prog)


State 35

   11 line: inst . ';'

    ';'  posunout a přejít do stavu 98


State 36

   12 line: error ';' .

    $výchozí  reduce using rule 12 (line)


State 37

   13 inst: LTYPE1 cond . imsr ',' spreg ',' reg
   14     | LTYPE1 cond . imsr ',' spreg ','
   15     | LTYPE1 cond . imsr ',' reg
   48 cond: cond . LCOND
   49     | cond . LS
   82 imsr: . reg
   83     | . imm
   84     | . shift
   85 imm: . '$' con
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP    posunout a přejít do stavu 74
    LPC    posunout a přejít do stavu 75
    LR     posunout a přejít do stavu 76
    LREG   posunout a přejít do stavu 77
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100
    '$'    posunout a přejít do stavu 101

    imsr   přejít do stavu 102
    imm    přejít do stavu 103
    reg    přejít do stavu 104
    shift  přejít do stavu 105
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90


State 38

   16 inst: LTYPE2 cond . imsr ',' reg
   48 cond: cond . LCOND
   49     | cond . LS
   82 imsr: . reg
   83     | . imm
   84     | . shift
   85 imm: . '$' con
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP    posunout a přejít do stavu 74
    LPC    posunout a přejít do stavu 75
    LR     posunout a přejít do stavu 76
    LREG   posunout a přejít do stavu 77
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100
    '$'    posunout a přejít do stavu 101

    imsr   přejít do stavu 106
    imm    přejít do stavu 103
    reg    přejít do stavu 104
    shift  přejít do stavu 105
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90


State 39

   17 inst: LTYPE3 cond . gen ',' gen
   48 cond: cond . LCOND
   49     | cond . LS
   55 ximm: . '$' con
   56     | . '$' oreg
   57     | . '$' '*' '$' oreg
   58     | . '$' LSCONST
   59     | . fcon
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST
   65 gen: . reg
   66    | . ximm
   67    | . shift
   68    | . shift '(' spreg ')'
   69    | . LPSR
   70    | . LFCR
   71    | . con
   72    | . oreg
   73    | . freg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  103 freg: . LFREG
  104     | . LF '(' con ')'
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LF      posunout a přejít do stavu 78
    LFREG   posunout a přejít do stavu 79
    LPSR    posunout a přejít do stavu 80
    LFCR    posunout a přejít do stavu 81
    LCOND   posunout a přejít do stavu 99
    LS      posunout a přejít do stavu 100
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '$'     posunout a přejít do stavu 82
    '~'     posunout a přejít do stavu 54

    ximm   přejít do stavu 83
    fcon   přejít do stavu 84
    gen    přejít do stavu 107
    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 86
    reg    přejít do stavu 87
    shift  přejít do stavu 88
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90
    freg   přejít do stavu 91
    name   přejít do stavu 72
    con    přejít do stavu 92


State 40

   18 inst: LTYPE4 cond . comma rel
   19     | LTYPE4 cond . comma nireg
   48 cond: cond . LCOND
   49     | cond . LS
   50 comma: . %empty  ['+', '-', LCONST, LNAME, LLAB, LVAR, '(', '~']
   51      | . ',' comma

    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100
    ','    posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 108


State 41

   50 comma: . %empty  ['+', '-', LCONST, LSP, LPC, LR, LREG, LF, LFREG, LPSR, LFCR, LNAME, LLAB, LVAR, ';', '(', '$', '~']
   51      | . ',' comma
   51      | ',' . comma

    ','  posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 109


State 42

   21 inst: LTYPE5 comma . rel
   52 rel: . con '(' LPC ')'
   53    | . LNAME offset
   54    | . LLAB offset
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LNAME   posunout a přejít do stavu 110
    LLAB    posunout a přejít do stavu 111
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    rel  přejít do stavu 112
    con  přejít do stavu 113


State 43

   22 inst: LTYPE6 cond . comma gen
   48 cond: cond . LCOND
   49     | cond . LS
   50 comma: . %empty  ['+', '-', LCONST, LSP, LPC, LR, LREG, LF, LFREG, LPSR, LFCR, LNAME, LVAR, '(', '$', '~']
   51      | . ',' comma

    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100
    ','    posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 114


State 44

   23 inst: LTYPE7 cond . imsr ',' spreg comma
   48 cond: cond . LCOND
   49     | cond . LS
   82 imsr: . reg
   83     | . imm
   84     | . shift
   85 imm: . '$' con
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP    posunout a přejít do stavu 74
    LPC    posunout a přejít do stavu 75
    LR     posunout a přejít do stavu 76
    LREG   posunout a přejít do stavu 77
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100
    '$'    posunout a přejít do stavu 101

    imsr   přejít do stavu 115
    imm    přejít do stavu 103
    reg    přejít do stavu 104
    shift  přejít do stavu 105
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90


State 45

   24 inst: LTYPE8 cond . ioreg ',' '[' reglist ']'
   25     | LTYPE8 cond . '[' reglist ']' ',' ioreg
   48 cond: cond . LCOND
   49     | cond . LS
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LCOND   posunout a přejít do stavu 99
    LS      posunout a přejít do stavu 100
    LVAR    posunout a přejít do stavu 52
    '['     posunout a přejít do stavu 116
    '('     posunout a přejít do stavu 68
    '~'     posunout a přejít do stavu 54

    ireg   přejít do stavu 69
    ioreg  přejít do stavu 117
    con    přejít do stavu 118


State 46

   26 inst: LTYPE9 cond . reg ',' ireg ',' reg
   27     | LTYPE9 cond . reg ',' ireg comma
   28     | LTYPE9 cond . comma ireg ',' reg
   48 cond: cond . LCOND
   49     | cond . LS
   50 comma: . %empty  ['(']
   51      | . ',' comma
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP    posunout a přejít do stavu 74
    LPC    posunout a přejít do stavu 75
    LR     posunout a přejít do stavu 76
    LREG   posunout a přejít do stavu 77
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100
    ','    posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 119
    reg    přejít do stavu 120
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 47

   29 inst: LTYPEA cond . comma
   48 cond: cond . LCOND
   49     | cond . LS
   50 comma: . %empty  [';']
   51      | . ',' comma

    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100
    ','    posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 122


State 48

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  117    | '+' . con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 123


State 49

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  116    | '-' . con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 124


State 50

  114 con: LCONST .

    $výchozí  reduce using rule 114 (con)


State 51

  106 name: LNAME . offset '(' pointer ')'
  107     | LNAME . '<' '>' offset '(' LSB ')'
  108 offset: . %empty  ['(']
  109       | . '+' con
  110       | . '-' con

    '<'  posunout a přejít do stavu 125
    '+'  posunout a přejít do stavu 126
    '-'  posunout a přejít do stavu 127

    $výchozí  reduce using rule 108 (offset)

    offset  přejít do stavu 128


State 52

  115 con: LVAR .

    $výchozí  reduce using rule 115 (con)


State 53

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  119    | '(' . expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 130


State 54

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  118    | '~' . con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 131


State 55

   30 inst: LTYPEB name . ',' imm
   31     | LTYPEB name . ',' con ',' imm
   32     | LTYPEB name . ',' con ',' imm '-' con

    ','  posunout a přejít do stavu 132


State 56

  105 name: con . '(' pointer ')'

    '('  posunout a přejít do stavu 133


State 57

   33 inst: LTYPEC name . '/' con ',' ximm

    '/'  posunout a přejít do stavu 134


State 58

   34 inst: LTYPED cond . reg comma
   48 cond: cond . LCOND
   49     | cond . LS
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP    posunout a přejít do stavu 74
    LPC    posunout a přejít do stavu 75
    LR     posunout a přejít do stavu 76
    LREG   posunout a přejít do stavu 77
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100

    reg    přejít do stavu 135
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 59

   46 inst: LTYPEE comma .

    $výchozí  reduce using rule 46 (inst)


State 60

   35 inst: LTYPEH comma . ximm
   55 ximm: . '$' con
   56     | . '$' oreg
   57     | . '$' '*' '$' oreg
   58     | . '$' LSCONST
   59     | . fcon
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST

    '$'  posunout a přejít do stavu 82

    ximm  přejít do stavu 136
    fcon  přejít do stavu 84


State 61

   36 inst: LTYPEI cond . freg ',' freg
   48 cond: cond . LCOND
   49     | cond . LS
  103 freg: . LFREG
  104     | . LF '(' con ')'

    LF     posunout a přejít do stavu 78
    LFREG  posunout a přejít do stavu 79
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100

    freg  přejít do stavu 137


State 62

   40 inst: LTYPEJ cond . con ',' expr ',' spreg ',' creg ',' creg oexpr
   48 cond: cond . LCOND
   49     | cond . LS
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LCOND   posunout a přejít do stavu 99
    LS      posunout a přejít do stavu 100
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 138


State 63

   37 inst: LTYPEK cond . frcon ',' freg
   38     | LTYPEK cond . frcon ',' LFREG ',' freg
   48 cond: cond . LCOND
   49     | cond . LS
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST
  101 frcon: . freg
  102      | . fcon
  103 freg: . LFREG
  104     | . LF '(' con ')'

    LF     posunout a přejít do stavu 78
    LFREG  posunout a přejít do stavu 79
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100
    '$'    posunout a přejít do stavu 139

    fcon   přejít do stavu 140
    frcon  přejít do stavu 141
    freg   přejít do stavu 142


State 64

   39 inst: LTYPEL cond . freg ',' freg comma
   48 cond: cond . LCOND
   49     | cond . LS
  103 freg: . LFREG
  104     | . LF '(' con ')'

    LF     posunout a přejít do stavu 78
    LFREG  posunout a přejít do stavu 79
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100

    freg  přejít do stavu 143


State 65

   41 inst: LTYPEM cond . reg ',' reg ',' regreg
   48 cond: cond . LCOND
   49     | cond . LS
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP    posunout a přejít do stavu 74
    LPC    posunout a přejít do stavu 75
    LR     posunout a přejít do stavu 76
    LREG   posunout a přejít do stavu 77
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100

    reg    přejít do stavu 144
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 66

   42 inst: LTYPEN cond . reg ',' reg ',' reg ',' spreg
   48 cond: cond . LCOND
   49     | cond . LS
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP    posunout a přejít do stavu 74
    LPC    posunout a přejít do stavu 75
    LR     posunout a přejít do stavu 76
    LREG   posunout a přejít do stavu 77
    LCOND  posunout a přejít do stavu 99
    LS     posunout a přejít do stavu 100

    reg    přejít do stavu 145
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 67

   20 inst: LTYPEBX comma . ireg
   76 ireg: . '(' spreg ')'

    '('  posunout a přejít do stavu 146

    ireg  přejít do stavu 147


State 68

   76 ireg: '(' . spreg ')'
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  119    | '(' . expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    sreg   přejít do stavu 89
    spreg  přejít do stavu 148
    con    přejít do stavu 129
    expr   přejít do stavu 130


State 69

   77 ioreg: ireg .

    $výchozí  reduce using rule 77 (ioreg)


State 70

   81 oreg: ioreg .

    $výchozí  reduce using rule 81 (oreg)


State 71

   43 inst: LTYPEPLD oreg .

    $výchozí  reduce using rule 43 (inst)


State 72

   79 oreg: name .  [';', ',']
   80     | name . '(' sreg ')'

    '('  posunout a přejít do stavu 149

    $výchozí  reduce using rule 79 (oreg)


State 73

   78 ioreg: con . '(' sreg ')'
  105 name: con . '(' pointer ')'

    '('  posunout a přejít do stavu 150


State 74

   98 spreg: LSP .

    $výchozí  reduce using rule 98 (spreg)


State 75

   95 sreg: LPC .

    $výchozí  reduce using rule 95 (sreg)


State 76

   96 sreg: LR . '(' expr ')'

    '('  posunout a přejít do stavu 151


State 77

   94 sreg: LREG .

    $výchozí  reduce using rule 94 (sreg)


State 78

  104 freg: LF . '(' con ')'

    '('  posunout a přejít do stavu 152


State 79

  103 freg: LFREG .

    $výchozí  reduce using rule 103 (freg)


State 80

   69 gen: LPSR .

    $výchozí  reduce using rule 69 (gen)


State 81

   70 gen: LFCR .

    $výchozí  reduce using rule 70 (gen)


State 82

   55 ximm: '$' . con
   56     | '$' . oreg
   57     | '$' . '*' '$' oreg
   58     | '$' . LSCONST
   60 fcon: '$' . LFCONST
   61     | '$' . '-' LFCONST
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'      posunout a přejít do stavu 48
    '-'      posunout a přejít do stavu 153
    '*'      posunout a přejít do stavu 154
    LCONST   posunout a přejít do stavu 50
    LFCONST  posunout a přejít do stavu 155
    LSCONST  posunout a přejít do stavu 156
    LNAME    posunout a přejít do stavu 51
    LVAR     posunout a přejít do stavu 52
    '('      posunout a přejít do stavu 68
    '~'      posunout a přejít do stavu 54

    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 157
    name   přejít do stavu 72
    con    přejít do stavu 158


State 83

   66 gen: ximm .

    $výchozí  reduce using rule 66 (gen)


State 84

   59 ximm: fcon .

    $výchozí  reduce using rule 59 (ximm)


State 85

   44 inst: LTYPEPC gen . ',' gen

    ','  posunout a přejít do stavu 159


State 86

   72 gen: oreg .

    $výchozí  reduce using rule 72 (gen)


State 87

   65 gen: reg .

    $výchozí  reduce using rule 65 (gen)


State 88

   67 gen: shift .  [';', ',']
   68    | shift . '(' spreg ')'

    '('  posunout a přejít do stavu 160

    $výchozí  reduce using rule 67 (gen)


State 89

   97 spreg: sreg .

    $výchozí  reduce using rule 97 (spreg)


State 90

   86 reg: spreg .  [';', ',']
   88 shift: spreg . '<' '<' rcon
   89      | spreg . '>' '>' rcon
   90      | spreg . '-' '>' rcon
   91      | spreg . LAT '>' rcon

    '<'  posunout a přejít do stavu 161
    '>'  posunout a přejít do stavu 162
    '-'  posunout a přejít do stavu 163
    LAT  posunout a přejít do stavu 164

    $výchozí  reduce using rule 86 (reg)


State 91

   73 gen: freg .

    $výchozí  reduce using rule 73 (gen)


State 92

   71 gen: con .  [';', ',']
   78 ioreg: con . '(' sreg ')'
  105 name: con . '(' pointer ')'

    '('  posunout a přejít do stavu 150

    $výchozí  reduce using rule 71 (gen)


State 93

   45 inst: LTYPEF gen . ',' gen

    ','  posunout a přejít do stavu 165


State 94

    6 $@3: . %empty
    7 line: LNAME ':' . $@3 line

    $výchozí  reduce using rule 6 ($@3)

    $@3  přejít do stavu 166


State 95

    8 line: LNAME '=' . expr ';'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 167


State 96

    4 $@2: . %empty
    5 line: LLAB ':' . $@2 line

    $výchozí  reduce using rule 4 ($@2)

    $@2  přejít do stavu 168


State 97

    9 line: LVAR '=' . expr ';'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 169


State 98

   11 line: inst ';' .

    $výchozí  reduce using rule 11 (line)


State 99

   48 cond: cond LCOND .

    $výchozí  reduce using rule 48 (cond)


State 100

   49 cond: cond LS .

    $výchozí  reduce using rule 49 (cond)


State 101

   85 imm: '$' . con
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 170


State 102

   13 inst: LTYPE1 cond imsr . ',' spreg ',' reg
   14     | LTYPE1 cond imsr . ',' spreg ','
   15     | LTYPE1 cond imsr . ',' reg

    ','  posunout a přejít do stavu 171


State 103

   83 imsr: imm .

    $výchozí  reduce using rule 83 (imsr)


State 104

   82 imsr: reg .

    $výchozí  reduce using rule 82 (imsr)


State 105

   84 imsr: shift .

    $výchozí  reduce using rule 84 (imsr)


State 106

   16 inst: LTYPE2 cond imsr . ',' reg

    ','  posunout a přejít do stavu 172


State 107

   17 inst: LTYPE3 cond gen . ',' gen

    ','  posunout a přejít do stavu 173


State 108

   18 inst: LTYPE4 cond comma . rel
   19     | LTYPE4 cond comma . nireg
   52 rel: . con '(' LPC ')'
   53    | . LNAME offset
   54    | . LLAB offset
   74 nireg: . ireg
   75      | . name
   76 ireg: . '(' spreg ')'
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LNAME   posunout a přejít do stavu 174
    LLAB    posunout a přejít do stavu 111
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '~'     posunout a přejít do stavu 54

    rel    přejít do stavu 175
    nireg  přejít do stavu 176
    ireg   přejít do stavu 177
    name   přejít do stavu 178
    con    přejít do stavu 179


State 109

   51 comma: ',' comma .

    $výchozí  reduce using rule 51 (comma)


State 110

   53 rel: LNAME . offset
  108 offset: . %empty  [';']
  109       | . '+' con
  110       | . '-' con

    '+'  posunout a přejít do stavu 126
    '-'  posunout a přejít do stavu 127

    $výchozí  reduce using rule 108 (offset)

    offset  přejít do stavu 180


State 111

   54 rel: LLAB . offset
  108 offset: . %empty  [';']
  109       | . '+' con
  110       | . '-' con

    '+'  posunout a přejít do stavu 126
    '-'  posunout a přejít do stavu 127

    $výchozí  reduce using rule 108 (offset)

    offset  přejít do stavu 181


State 112

   21 inst: LTYPE5 comma rel .

    $výchozí  reduce using rule 21 (inst)


State 113

   52 rel: con . '(' LPC ')'

    '('  posunout a přejít do stavu 182


State 114

   22 inst: LTYPE6 cond comma . gen
   55 ximm: . '$' con
   56     | . '$' oreg
   57     | . '$' '*' '$' oreg
   58     | . '$' LSCONST
   59     | . fcon
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST
   65 gen: . reg
   66    | . ximm
   67    | . shift
   68    | . shift '(' spreg ')'
   69    | . LPSR
   70    | . LFCR
   71    | . con
   72    | . oreg
   73    | . freg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  103 freg: . LFREG
  104     | . LF '(' con ')'
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LF      posunout a přejít do stavu 78
    LFREG   posunout a přejít do stavu 79
    LPSR    posunout a přejít do stavu 80
    LFCR    posunout a přejít do stavu 81
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '$'     posunout a přejít do stavu 82
    '~'     posunout a přejít do stavu 54

    ximm   přejít do stavu 83
    fcon   přejít do stavu 84
    gen    přejít do stavu 183
    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 86
    reg    přejít do stavu 87
    shift  přejít do stavu 88
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90
    freg   přejít do stavu 91
    name   přejít do stavu 72
    con    přejít do stavu 92


State 115

   23 inst: LTYPE7 cond imsr . ',' spreg comma

    ','  posunout a přejít do stavu 184


State 116

   25 inst: LTYPE8 cond '[' . reglist ']' ',' ioreg
   62 reglist: . spreg
   63        | . spreg '-' spreg
   64        | . spreg comma reglist
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    reglist  přejít do stavu 185
    sreg     přejít do stavu 89
    spreg    přejít do stavu 186


State 117

   24 inst: LTYPE8 cond ioreg . ',' '[' reglist ']'

    ','  posunout a přejít do stavu 187


State 118

   78 ioreg: con . '(' sreg ')'

    '('  posunout a přejít do stavu 188


State 119

   28 inst: LTYPE9 cond comma . ireg ',' reg
   76 ireg: . '(' spreg ')'

    '('  posunout a přejít do stavu 146

    ireg  přejít do stavu 189


State 120

   26 inst: LTYPE9 cond reg . ',' ireg ',' reg
   27     | LTYPE9 cond reg . ',' ireg comma

    ','  posunout a přejít do stavu 190


State 121

   86 reg: spreg .

    $výchozí  reduce using rule 86 (reg)


State 122

   29 inst: LTYPEA cond comma .

    $výchozí  reduce using rule 29 (inst)


State 123

  117 con: '+' con .

    $výchozí  reduce using rule 117 (con)


State 124

  116 con: '-' con .

    $výchozí  reduce using rule 116 (con)


State 125

  107 name: LNAME '<' . '>' offset '(' LSB ')'

    '>'  posunout a přejít do stavu 191


State 126

  109 offset: '+' . con
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 192


State 127

  110 offset: '-' . con
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 193


State 128

  106 name: LNAME offset . '(' pointer ')'

    '('  posunout a přejít do stavu 194


State 129

  122 expr: con .

    $výchozí  reduce using rule 122 (expr)


State 130

  119 con: '(' expr . ')'
  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '|'  posunout a přejít do stavu 195
    '^'  posunout a přejít do stavu 196
    '&'  posunout a přejít do stavu 197
    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204
    ')'  posunout a přejít do stavu 205


State 131

  118 con: '~' con .

    $výchozí  reduce using rule 118 (con)


State 132

   30 inst: LTYPEB name ',' . imm
   31     | LTYPEB name ',' . con ',' imm
   32     | LTYPEB name ',' . con ',' imm '-' con
   85 imm: . '$' con
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '$'     posunout a přejít do stavu 101
    '~'     posunout a přejít do stavu 54

    imm  přejít do stavu 206
    con  přejít do stavu 207


State 133

  105 name: con '(' . pointer ')'
  111 pointer: . LSB
  112        | . LSP
  113        | . LFP

    LSP  posunout a přejít do stavu 208
    LSB  posunout a přejít do stavu 209
    LFP  posunout a přejít do stavu 210

    pointer  přejít do stavu 211


State 134

   33 inst: LTYPEC name '/' . con ',' ximm
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 212


State 135

   34 inst: LTYPED cond reg . comma
   50 comma: . %empty  [';']
   51      | . ',' comma

    ','  posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 213


State 136

   35 inst: LTYPEH comma ximm .

    $výchozí  reduce using rule 35 (inst)


State 137

   36 inst: LTYPEI cond freg . ',' freg

    ','  posunout a přejít do stavu 214


State 138

   40 inst: LTYPEJ cond con . ',' expr ',' spreg ',' creg ',' creg oexpr

    ','  posunout a přejít do stavu 215


State 139

   60 fcon: '$' . LFCONST
   61     | '$' . '-' LFCONST

    '-'      posunout a přejít do stavu 216
    LFCONST  posunout a přejít do stavu 155


State 140

  102 frcon: fcon .

    $výchozí  reduce using rule 102 (frcon)


State 141

   37 inst: LTYPEK cond frcon . ',' freg
   38     | LTYPEK cond frcon . ',' LFREG ',' freg

    ','  posunout a přejít do stavu 217


State 142

  101 frcon: freg .

    $výchozí  reduce using rule 101 (frcon)


State 143

   39 inst: LTYPEL cond freg . ',' freg comma

    ','  posunout a přejít do stavu 218


State 144

   41 inst: LTYPEM cond reg . ',' reg ',' regreg

    ','  posunout a přejít do stavu 219


State 145

   42 inst: LTYPEN cond reg . ',' reg ',' reg ',' spreg

    ','  posunout a přejít do stavu 220


State 146

   76 ireg: '(' . spreg ')'
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg   přejít do stavu 89
    spreg  přejít do stavu 148


State 147

   20 inst: LTYPEBX comma ireg .

    $výchozí  reduce using rule 20 (inst)


State 148

   76 ireg: '(' spreg . ')'

    ')'  posunout a přejít do stavu 221


State 149

   80 oreg: name '(' . sreg ')'
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'

    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg  přejít do stavu 222


State 150

   78 ioreg: con '(' . sreg ')'
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
  105 name: con '(' . pointer ')'
  111 pointer: . LSB
  112        | . LSP
  113        | . LFP

    LSP   posunout a přejít do stavu 208
    LSB   posunout a přejít do stavu 209
    LFP   posunout a přejít do stavu 210
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg     přejít do stavu 223
    pointer  přejít do stavu 211


State 151

   96 sreg: LR '(' . expr ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 224


State 152

  104 freg: LF '(' . con ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 225


State 153

   61 fcon: '$' '-' . LFCONST
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  116    | '-' . con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'      posunout a přejít do stavu 48
    '-'      posunout a přejít do stavu 49
    LCONST   posunout a přejít do stavu 50
    LFCONST  posunout a přejít do stavu 226
    LVAR     posunout a přejít do stavu 52
    '('      posunout a přejít do stavu 53
    '~'      posunout a přejít do stavu 54

    con  přejít do stavu 124


State 154

   57 ximm: '$' '*' . '$' oreg

    '$'  posunout a přejít do stavu 227


State 155

   60 fcon: '$' LFCONST .

    $výchozí  reduce using rule 60 (fcon)


State 156

   58 ximm: '$' LSCONST .

    $výchozí  reduce using rule 58 (ximm)


State 157

   56 ximm: '$' oreg .

    $výchozí  reduce using rule 56 (ximm)


State 158

   55 ximm: '$' con .  [';', ',']
   78 ioreg: con . '(' sreg ')'
  105 name: con . '(' pointer ')'

    '('  posunout a přejít do stavu 150

    $výchozí  reduce using rule 55 (ximm)


State 159

   44 inst: LTYPEPC gen ',' . gen
   55 ximm: . '$' con
   56     | . '$' oreg
   57     | . '$' '*' '$' oreg
   58     | . '$' LSCONST
   59     | . fcon
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST
   65 gen: . reg
   66    | . ximm
   67    | . shift
   68    | . shift '(' spreg ')'
   69    | . LPSR
   70    | . LFCR
   71    | . con
   72    | . oreg
   73    | . freg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  103 freg: . LFREG
  104     | . LF '(' con ')'
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LF      posunout a přejít do stavu 78
    LFREG   posunout a přejít do stavu 79
    LPSR    posunout a přejít do stavu 80
    LFCR    posunout a přejít do stavu 81
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '$'     posunout a přejít do stavu 82
    '~'     posunout a přejít do stavu 54

    ximm   přejít do stavu 83
    fcon   přejít do stavu 84
    gen    přejít do stavu 228
    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 86
    reg    přejít do stavu 87
    shift  přejít do stavu 88
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90
    freg   přejít do stavu 91
    name   přejít do stavu 72
    con    přejít do stavu 92


State 160

   68 gen: shift '(' . spreg ')'
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg   přejít do stavu 89
    spreg  přejít do stavu 229


State 161

   88 shift: spreg '<' . '<' rcon

    '<'  posunout a přejít do stavu 230


State 162

   89 shift: spreg '>' . '>' rcon

    '>'  posunout a přejít do stavu 231


State 163

   90 shift: spreg '-' . '>' rcon

    '>'  posunout a přejít do stavu 232


State 164

   91 shift: spreg LAT . '>' rcon

    '>'  posunout a přejít do stavu 233


State 165

   45 inst: LTYPEF gen ',' . gen
   55 ximm: . '$' con
   56     | . '$' oreg
   57     | . '$' '*' '$' oreg
   58     | . '$' LSCONST
   59     | . fcon
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST
   65 gen: . reg
   66    | . ximm
   67    | . shift
   68    | . shift '(' spreg ')'
   69    | . LPSR
   70    | . LFCR
   71    | . con
   72    | . oreg
   73    | . freg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  103 freg: . LFREG
  104     | . LF '(' con ')'
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LF      posunout a přejít do stavu 78
    LFREG   posunout a přejít do stavu 79
    LPSR    posunout a přejít do stavu 80
    LFCR    posunout a přejít do stavu 81
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '$'     posunout a přejít do stavu 82
    '~'     posunout a přejít do stavu 54

    ximm   přejít do stavu 83
    fcon   přejít do stavu 84
    gen    přejít do stavu 234
    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 86
    reg    přejít do stavu 87
    shift  přejít do stavu 88
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90
    freg   přejít do stavu 91
    name   přejít do stavu 72
    con    přejít do stavu 92


State 166

    5 line: . LLAB ':' $@2 line
    7     | . LNAME ':' $@3 line
    7     | LNAME ':' $@3 . line
    8     | . LNAME '=' expr ';'
    9     | . LVAR '=' expr ';'
   10     | . ';'
   11     | . inst ';'
   12     | . error ';'
   13 inst: . LTYPE1 cond imsr ',' spreg ',' reg
   14     | . LTYPE1 cond imsr ',' spreg ','
   15     | . LTYPE1 cond imsr ',' reg
   16     | . LTYPE2 cond imsr ',' reg
   17     | . LTYPE3 cond gen ',' gen
   18     | . LTYPE4 cond comma rel
   19     | . LTYPE4 cond comma nireg
   20     | . LTYPEBX comma ireg
   21     | . LTYPE5 comma rel
   22     | . LTYPE6 cond comma gen
   23     | . LTYPE7 cond imsr ',' spreg comma
   24     | . LTYPE8 cond ioreg ',' '[' reglist ']'
   25     | . LTYPE8 cond '[' reglist ']' ',' ioreg
   26     | . LTYPE9 cond reg ',' ireg ',' reg
   27     | . LTYPE9 cond reg ',' ireg comma
   28     | . LTYPE9 cond comma ireg ',' reg
   29     | . LTYPEA cond comma
   30     | . LTYPEB name ',' imm
   31     | . LTYPEB name ',' con ',' imm
   32     | . LTYPEB name ',' con ',' imm '-' con
   33     | . LTYPEC name '/' con ',' ximm
   34     | . LTYPED cond reg comma
   35     | . LTYPEH comma ximm
   36     | . LTYPEI cond freg ',' freg
   37     | . LTYPEK cond frcon ',' freg
   38     | . LTYPEK cond frcon ',' LFREG ',' freg
   39     | . LTYPEL cond freg ',' freg comma
   40     | . LTYPEJ cond con ',' expr ',' spreg ',' creg ',' creg oexpr
   41     | . LTYPEM cond reg ',' reg ',' regreg
   42     | . LTYPEN cond reg ',' reg ',' reg ',' spreg
   43     | . LTYPEPLD oreg
   44     | . LTYPEPC gen ',' gen
   45     | . LTYPEF gen ',' gen
   46     | . LTYPEE comma

    error     posunout a přejít do stavu 4
    LTYPE1    posunout a přejít do stavu 5
    LTYPE2    posunout a přejít do stavu 6
    LTYPE3    posunout a přejít do stavu 7
    LTYPE4    posunout a přejít do stavu 8
    LTYPE5    posunout a přejít do stavu 9
    LTYPE6    posunout a přejít do stavu 10
    LTYPE7    posunout a přejít do stavu 11
    LTYPE8    posunout a přejít do stavu 12
    LTYPE9    posunout a přejít do stavu 13
    LTYPEA    posunout a přejít do stavu 14
    LTYPEB    posunout a přejít do stavu 15
    LTYPEC    posunout a přejít do stavu 16
    LTYPED    posunout a přejít do stavu 17
    LTYPEE    posunout a přejít do stavu 18
    LTYPEH    posunout a přejít do stavu 19
    LTYPEI    posunout a přejít do stavu 20
    LTYPEJ    posunout a přejít do stavu 21
    LTYPEK    posunout a přejít do stavu 22
    LTYPEL    posunout a přejít do stavu 23
    LTYPEM    posunout a přejít do stavu 24
    LTYPEN    posunout a přejít do stavu 25
    LTYPEBX   posunout a přejít do stavu 26
    LTYPEPLD  posunout a přejít do stavu 27
    LTYPEPC   posunout a přejít do stavu 28
    LTYPEF    posunout a přejít do stavu 29
    LNAME     posunout a přejít do stavu 30
    LLAB      posunout a přejít do stavu 31
    LVAR      posunout a přejít do stavu 32
    ';'       posunout a přejít do stavu 33

    line  přejít do stavu 235
    inst  přejít do stavu 35


State 167

    8 line: LNAME '=' expr . ';'
  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '|'  posunout a přejít do stavu 195
    '^'  posunout a přejít do stavu 196
    '&'  posunout a přejít do stavu 197
    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204
    ';'  posunout a přejít do stavu 236


State 168

    5 line: . LLAB ':' $@2 line
    5     | LLAB ':' $@2 . line
    7     | . LNAME ':' $@3 line
    8     | . LNAME '=' expr ';'
    9     | . LVAR '=' expr ';'
   10     | . ';'
   11     | . inst ';'
   12     | . error ';'
   13 inst: . LTYPE1 cond imsr ',' spreg ',' reg
   14     | . LTYPE1 cond imsr ',' spreg ','
   15     | . LTYPE1 cond imsr ',' reg
   16     | . LTYPE2 cond imsr ',' reg
   17     | . LTYPE3 cond gen ',' gen
   18     | . LTYPE4 cond comma rel
   19     | . LTYPE4 cond comma nireg
   20     | . LTYPEBX comma ireg
   21     | . LTYPE5 comma rel
   22     | . LTYPE6 cond comma gen
   23     | . LTYPE7 cond imsr ',' spreg comma
   24     | . LTYPE8 cond ioreg ',' '[' reglist ']'
   25     | . LTYPE8 cond '[' reglist ']' ',' ioreg
   26     | . LTYPE9 cond reg ',' ireg ',' reg
   27     | . LTYPE9 cond reg ',' ireg comma
   28     | . LTYPE9 cond comma ireg ',' reg
   29     | . LTYPEA cond comma
   30     | . LTYPEB name ',' imm
   31     | . LTYPEB name ',' con ',' imm
   32     | . LTYPEB name ',' con ',' imm '-' con
   33     | . LTYPEC name '/' con ',' ximm
   34     | . LTYPED cond reg comma
   35     | . LTYPEH comma ximm
   36     | . LTYPEI cond freg ',' freg
   37     | . LTYPEK cond frcon ',' freg
   38     | . LTYPEK cond frcon ',' LFREG ',' freg
   39     | . LTYPEL cond freg ',' freg comma
   40     | . LTYPEJ cond con ',' expr ',' spreg ',' creg ',' creg oexpr
   41     | . LTYPEM cond reg ',' reg ',' regreg
   42     | . LTYPEN cond reg ',' reg ',' reg ',' spreg
   43     | . LTYPEPLD oreg
   44     | . LTYPEPC gen ',' gen
   45     | . LTYPEF gen ',' gen
   46     | . LTYPEE comma

    error     posunout a přejít do stavu 4
    LTYPE1    posunout a přejít do stavu 5
    LTYPE2    posunout a přejít do stavu 6
    LTYPE3    posunout a přejít do stavu 7
    LTYPE4    posunout a přejít do stavu 8
    LTYPE5    posunout a přejít do stavu 9
    LTYPE6    posunout a přejít do stavu 10
    LTYPE7    posunout a přejít do stavu 11
    LTYPE8    posunout a přejít do stavu 12
    LTYPE9    posunout a přejít do stavu 13
    LTYPEA    posunout a přejít do stavu 14
    LTYPEB    posunout a přejít do stavu 15
    LTYPEC    posunout a přejít do stavu 16
    LTYPED    posunout a přejít do stavu 17
    LTYPEE    posunout a přejít do stavu 18
    LTYPEH    posunout a přejít do stavu 19
    LTYPEI    posunout a přejít do stavu 20
    LTYPEJ    posunout a přejít do stavu 21
    LTYPEK    posunout a přejít do stavu 22
    LTYPEL    posunout a přejít do stavu 23
    LTYPEM    posunout a přejít do stavu 24
    LTYPEN    posunout a přejít do stavu 25
    LTYPEBX   posunout a přejít do stavu 26
    LTYPEPLD  posunout a přejít do stavu 27
    LTYPEPC   posunout a přejít do stavu 28
    LTYPEF    posunout a přejít do stavu 29
    LNAME     posunout a přejít do stavu 30
    LLAB      posunout a přejít do stavu 31
    LVAR      posunout a přejít do stavu 32
    ';'       posunout a přejít do stavu 33

    line  přejít do stavu 237
    inst  přejít do stavu 35


State 169

    9 line: LVAR '=' expr . ';'
  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '|'  posunout a přejít do stavu 195
    '^'  posunout a přejít do stavu 196
    '&'  posunout a přejít do stavu 197
    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204
    ';'  posunout a přejít do stavu 238


State 170

   85 imm: '$' con .

    $výchozí  reduce using rule 85 (imm)


State 171

   13 inst: LTYPE1 cond imsr ',' . spreg ',' reg
   14     | LTYPE1 cond imsr ',' . spreg ','
   15     | LTYPE1 cond imsr ',' . reg
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    reg    přejít do stavu 239
    sreg   přejít do stavu 89
    spreg  přejít do stavu 240


State 172

   16 inst: LTYPE2 cond imsr ',' . reg
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    reg    přejít do stavu 241
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 173

   17 inst: LTYPE3 cond gen ',' . gen
   55 ximm: . '$' con
   56     | . '$' oreg
   57     | . '$' '*' '$' oreg
   58     | . '$' LSCONST
   59     | . fcon
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST
   65 gen: . reg
   66    | . ximm
   67    | . shift
   68    | . shift '(' spreg ')'
   69    | . LPSR
   70    | . LFCR
   71    | . con
   72    | . oreg
   73    | . freg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
   86 reg: . spreg
   88 shift: . spreg '<' '<' rcon
   89      | . spreg '>' '>' rcon
   90      | . spreg '-' '>' rcon
   91      | . spreg LAT '>' rcon
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  103 freg: . LFREG
  104     | . LF '(' con ')'
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LF      posunout a přejít do stavu 78
    LFREG   posunout a přejít do stavu 79
    LPSR    posunout a přejít do stavu 80
    LFCR    posunout a přejít do stavu 81
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '$'     posunout a přejít do stavu 82
    '~'     posunout a přejít do stavu 54

    ximm   přejít do stavu 83
    fcon   přejít do stavu 84
    gen    přejít do stavu 242
    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 86
    reg    přejít do stavu 87
    shift  přejít do stavu 88
    sreg   přejít do stavu 89
    spreg  přejít do stavu 90
    freg   přejít do stavu 91
    name   přejít do stavu 72
    con    přejít do stavu 92


State 174

   53 rel: LNAME . offset
  106 name: LNAME . offset '(' pointer ')'
  107     | LNAME . '<' '>' offset '(' LSB ')'
  108 offset: . %empty  [';', '(']
  109       | . '+' con
  110       | . '-' con

    '<'  posunout a přejít do stavu 125
    '+'  posunout a přejít do stavu 126
    '-'  posunout a přejít do stavu 127

    $výchozí  reduce using rule 108 (offset)

    offset  přejít do stavu 243


State 175

   18 inst: LTYPE4 cond comma rel .

    $výchozí  reduce using rule 18 (inst)


State 176

   19 inst: LTYPE4 cond comma nireg .

    $výchozí  reduce using rule 19 (inst)


State 177

   74 nireg: ireg .

    $výchozí  reduce using rule 74 (nireg)


State 178

   75 nireg: name .

    $výchozí  reduce using rule 75 (nireg)


State 179

   52 rel: con . '(' LPC ')'
  105 name: con . '(' pointer ')'

    '('  posunout a přejít do stavu 244


State 180

   53 rel: LNAME offset .

    $výchozí  reduce using rule 53 (rel)


State 181

   54 rel: LLAB offset .

    $výchozí  reduce using rule 54 (rel)


State 182

   52 rel: con '(' . LPC ')'

    LPC  posunout a přejít do stavu 245


State 183

   22 inst: LTYPE6 cond comma gen .

    $výchozí  reduce using rule 22 (inst)


State 184

   23 inst: LTYPE7 cond imsr ',' . spreg comma
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg   přejít do stavu 89
    spreg  přejít do stavu 246


State 185

   25 inst: LTYPE8 cond '[' reglist . ']' ',' ioreg

    ']'  posunout a přejít do stavu 247


State 186

   50 comma: . %empty  [LSP, LPC, LR, LREG]
   51      | . ',' comma
   62 reglist: spreg .  [']']
   63        | spreg . '-' spreg
   64        | spreg . comma reglist

    '-'  posunout a přejít do stavu 248
    ','  posunout a přejít do stavu 41

    ']'         reduce using rule 62 (reglist)
    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 249


State 187

   24 inst: LTYPE8 cond ioreg ',' . '[' reglist ']'

    '['  posunout a přejít do stavu 250


State 188

   78 ioreg: con '(' . sreg ')'
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'

    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg  přejít do stavu 223


State 189

   28 inst: LTYPE9 cond comma ireg . ',' reg

    ','  posunout a přejít do stavu 251


State 190

   26 inst: LTYPE9 cond reg ',' . ireg ',' reg
   27     | LTYPE9 cond reg ',' . ireg comma
   76 ireg: . '(' spreg ')'

    '('  posunout a přejít do stavu 146

    ireg  přejít do stavu 252


State 191

  107 name: LNAME '<' '>' . offset '(' LSB ')'
  108 offset: . %empty  ['(']
  109       | . '+' con
  110       | . '-' con

    '+'  posunout a přejít do stavu 126
    '-'  posunout a přejít do stavu 127

    $výchozí  reduce using rule 108 (offset)

    offset  přejít do stavu 253


State 192

  109 offset: '+' con .

    $výchozí  reduce using rule 109 (offset)


State 193

  110 offset: '-' con .

    $výchozí  reduce using rule 110 (offset)


State 194

  106 name: LNAME offset '(' . pointer ')'
  111 pointer: . LSB
  112        | . LSP
  113        | . LFP

    LSP  posunout a přejít do stavu 208
    LSB  posunout a přejít do stavu 209
    LFP  posunout a přejít do stavu 210

    pointer  přejít do stavu 254


State 195

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr
  132     | expr '|' . expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 255


State 196

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  131     | expr '^' . expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 256


State 197

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  130     | expr '&' . expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 257


State 198

  128 expr: expr '<' . '<' expr

    '<'  posunout a přejít do stavu 258


State 199

  129 expr: expr '>' . '>' expr

    '>'  posunout a přejít do stavu 259


State 200

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  123     | expr '+' . expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 260


State 201

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  124     | expr '-' . expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 261


State 202

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  125     | expr '*' . expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 262


State 203

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  126     | expr '/' . expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 263


State 204

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  127     | expr '%' . expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 264


State 205

  119 con: '(' expr ')' .

    $výchozí  reduce using rule 119 (con)


State 206

   30 inst: LTYPEB name ',' imm .

    $výchozí  reduce using rule 30 (inst)


State 207

   31 inst: LTYPEB name ',' con . ',' imm
   32     | LTYPEB name ',' con . ',' imm '-' con

    ','  posunout a přejít do stavu 265


State 208

  112 pointer: LSP .

    $výchozí  reduce using rule 112 (pointer)


State 209

  111 pointer: LSB .

    $výchozí  reduce using rule 111 (pointer)


State 210

  113 pointer: LFP .

    $výchozí  reduce using rule 113 (pointer)


State 211

  105 name: con '(' pointer . ')'

    ')'  posunout a přejít do stavu 266


State 212

   33 inst: LTYPEC name '/' con . ',' ximm

    ','  posunout a přejít do stavu 267


State 213

   34 inst: LTYPED cond reg comma .

    $výchozí  reduce using rule 34 (inst)


State 214

   36 inst: LTYPEI cond freg ',' . freg
  103 freg: . LFREG
  104     | . LF '(' con ')'

    LF     posunout a přejít do stavu 78
    LFREG  posunout a přejít do stavu 79

    freg  přejít do stavu 268


State 215

   40 inst: LTYPEJ cond con ',' . expr ',' spreg ',' creg ',' creg oexpr
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 269


State 216

   61 fcon: '$' '-' . LFCONST

    LFCONST  posunout a přejít do stavu 226


State 217

   37 inst: LTYPEK cond frcon ',' . freg
   38     | LTYPEK cond frcon ',' . LFREG ',' freg
  103 freg: . LFREG
  104     | . LF '(' con ')'

    LF     posunout a přejít do stavu 78
    LFREG  posunout a přejít do stavu 270

    freg  přejít do stavu 271


State 218

   39 inst: LTYPEL cond freg ',' . freg comma
  103 freg: . LFREG
  104     | . LF '(' con ')'

    LF     posunout a přejít do stavu 78
    LFREG  posunout a přejít do stavu 79

    freg  přejít do stavu 272


State 219

   41 inst: LTYPEM cond reg ',' . reg ',' regreg
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    reg    přejít do stavu 273
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 220

   42 inst: LTYPEN cond reg ',' . reg ',' reg ',' spreg
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    reg    přejít do stavu 274
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 221

   76 ireg: '(' spreg ')' .

    $výchozí  reduce using rule 76 (ireg)


State 222

   80 oreg: name '(' sreg . ')'

    ')'  posunout a přejít do stavu 275


State 223

   78 ioreg: con '(' sreg . ')'

    ')'  posunout a přejít do stavu 276


State 224

   96 sreg: LR '(' expr . ')'
  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '|'  posunout a přejít do stavu 195
    '^'  posunout a přejít do stavu 196
    '&'  posunout a přejít do stavu 197
    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204
    ')'  posunout a přejít do stavu 277


State 225

  104 freg: LF '(' con . ')'

    ')'  posunout a přejít do stavu 278


State 226

   61 fcon: '$' '-' LFCONST .

    $výchozí  reduce using rule 61 (fcon)


State 227

   57 ximm: '$' '*' '$' . oreg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
   79 oreg: . name
   80     | . name '(' sreg ')'
   81     | . ioreg
  105 name: . con '(' pointer ')'
  106     | . LNAME offset '(' pointer ')'
  107     | . LNAME '<' '>' offset '(' LSB ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LNAME   posunout a přejít do stavu 51
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '~'     posunout a přejít do stavu 54

    ireg   přejít do stavu 69
    ioreg  přejít do stavu 70
    oreg   přejít do stavu 279
    name   přejít do stavu 72
    con    přejít do stavu 73


State 228

   44 inst: LTYPEPC gen ',' gen .

    $výchozí  reduce using rule 44 (inst)


State 229

   68 gen: shift '(' spreg . ')'

    ')'  posunout a přejít do stavu 280


State 230

   88 shift: spreg '<' '<' . rcon
   92 rcon: . spreg
   93     | . con
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    rcon   přejít do stavu 281
    sreg   přejít do stavu 89
    spreg  přejít do stavu 282
    con    přejít do stavu 283


State 231

   89 shift: spreg '>' '>' . rcon
   92 rcon: . spreg
   93     | . con
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    rcon   přejít do stavu 284
    sreg   přejít do stavu 89
    spreg  přejít do stavu 282
    con    přejít do stavu 283


State 232

   90 shift: spreg '-' '>' . rcon
   92 rcon: . spreg
   93     | . con
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    rcon   přejít do stavu 285
    sreg   přejít do stavu 89
    spreg  přejít do stavu 282
    con    přejít do stavu 283


State 233

   91 shift: spreg LAT '>' . rcon
   92 rcon: . spreg
   93     | . con
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LSP     posunout a přejít do stavu 74
    LPC     posunout a přejít do stavu 75
    LR      posunout a přejít do stavu 76
    LREG    posunout a přejít do stavu 77
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    rcon   přejít do stavu 286
    sreg   přejít do stavu 89
    spreg  přejít do stavu 282
    con    přejít do stavu 283


State 234

   45 inst: LTYPEF gen ',' gen .

    $výchozí  reduce using rule 45 (inst)


State 235

    7 line: LNAME ':' $@3 line .

    $výchozí  reduce using rule 7 (line)


State 236

    8 line: LNAME '=' expr ';' .

    $výchozí  reduce using rule 8 (line)


State 237

    5 line: LLAB ':' $@2 line .

    $výchozí  reduce using rule 5 (line)


State 238

    9 line: LVAR '=' expr ';' .

    $výchozí  reduce using rule 9 (line)


State 239

   15 inst: LTYPE1 cond imsr ',' reg .

    $výchozí  reduce using rule 15 (inst)


State 240

   13 inst: LTYPE1 cond imsr ',' spreg . ',' reg
   14     | LTYPE1 cond imsr ',' spreg . ','
   86 reg: spreg .  [';']

    ','  posunout a přejít do stavu 287

    $výchozí  reduce using rule 86 (reg)


State 241

   16 inst: LTYPE2 cond imsr ',' reg .

    $výchozí  reduce using rule 16 (inst)


State 242

   17 inst: LTYPE3 cond gen ',' gen .

    $výchozí  reduce using rule 17 (inst)


State 243

   53 rel: LNAME offset .  [';']
  106 name: LNAME offset . '(' pointer ')'

    '('  posunout a přejít do stavu 194

    $výchozí  reduce using rule 53 (rel)


State 244

   52 rel: con '(' . LPC ')'
  105 name: con '(' . pointer ')'
  111 pointer: . LSB
  112        | . LSP
  113        | . LFP

    LSP  posunout a přejít do stavu 208
    LSB  posunout a přejít do stavu 209
    LFP  posunout a přejít do stavu 210
    LPC  posunout a přejít do stavu 245

    pointer  přejít do stavu 211


State 245

   52 rel: con '(' LPC . ')'

    ')'  posunout a přejít do stavu 288


State 246

   23 inst: LTYPE7 cond imsr ',' spreg . comma
   50 comma: . %empty  [';']
   51      | . ',' comma

    ','  posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 289


State 247

   25 inst: LTYPE8 cond '[' reglist ']' . ',' ioreg

    ','  posunout a přejít do stavu 290


State 248

   63 reglist: spreg '-' . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg   přejít do stavu 89
    spreg  přejít do stavu 291


State 249

   62 reglist: . spreg
   63        | . spreg '-' spreg
   64        | . spreg comma reglist
   64        | spreg comma . reglist
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    reglist  přejít do stavu 292
    sreg     přejít do stavu 89
    spreg    přejít do stavu 186


State 250

   24 inst: LTYPE8 cond ioreg ',' '[' . reglist ']'
   62 reglist: . spreg
   63        | . spreg '-' spreg
   64        | . spreg comma reglist
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    reglist  přejít do stavu 293
    sreg     přejít do stavu 89
    spreg    přejít do stavu 186


State 251

   28 inst: LTYPE9 cond comma ireg ',' . reg
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    reg    přejít do stavu 294
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 252

   26 inst: LTYPE9 cond reg ',' ireg . ',' reg
   27     | LTYPE9 cond reg ',' ireg . comma
   50 comma: . %empty  [';']
   51      | . ',' comma

    ','  posunout a přejít do stavu 295

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 296


State 253

  107 name: LNAME '<' '>' offset . '(' LSB ')'

    '('  posunout a přejít do stavu 297


State 254

  106 name: LNAME offset '(' pointer . ')'

    ')'  posunout a přejít do stavu 298


State 255

  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr
  132     | expr '|' expr .  ['|', ';', ',', ')']

    '^'  posunout a přejít do stavu 196
    '&'  posunout a přejít do stavu 197
    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204

    $výchozí  reduce using rule 132 (expr)

    Conflict between rule 132 and token '|' resolved as reduce (%left '|').
    Conflict between rule 132 and token '^' resolved as shift ('|' < '^').
    Conflict between rule 132 and token '&' resolved as shift ('|' < '&').
    Conflict between rule 132 and token '<' resolved as shift ('|' < '<').
    Conflict between rule 132 and token '>' resolved as shift ('|' < '>').
    Conflict between rule 132 and token '+' resolved as shift ('|' < '+').
    Conflict between rule 132 and token '-' resolved as shift ('|' < '-').
    Conflict between rule 132 and token '*' resolved as shift ('|' < '*').
    Conflict between rule 132 and token '/' resolved as shift ('|' < '/').
    Conflict between rule 132 and token '%' resolved as shift ('|' < '%').


State 256

  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  131     | expr '^' expr .  ['|', '^', ';', ',', ')']
  132     | expr . '|' expr

    '&'  posunout a přejít do stavu 197
    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204

    $výchozí  reduce using rule 131 (expr)

    Conflict between rule 131 and token '|' resolved as reduce ('|' < '^').
    Conflict between rule 131 and token '^' resolved as reduce (%left '^').
    Conflict between rule 131 and token '&' resolved as shift ('^' < '&').
    Conflict between rule 131 and token '<' resolved as shift ('^' < '<').
    Conflict between rule 131 and token '>' resolved as shift ('^' < '>').
    Conflict between rule 131 and token '+' resolved as shift ('^' < '+').
    Conflict between rule 131 and token '-' resolved as shift ('^' < '-').
    Conflict between rule 131 and token '*' resolved as shift ('^' < '*').
    Conflict between rule 131 and token '/' resolved as shift ('^' < '/').
    Conflict between rule 131 and token '%' resolved as shift ('^' < '%').


State 257

  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  130     | expr '&' expr .  ['|', '^', '&', ';', ',', ')']
  131     | expr . '^' expr
  132     | expr . '|' expr

    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204

    $výchozí  reduce using rule 130 (expr)

    Conflict between rule 130 and token '|' resolved as reduce ('|' < '&').
    Conflict between rule 130 and token '^' resolved as reduce ('^' < '&').
    Conflict between rule 130 and token '&' resolved as reduce (%left '&').
    Conflict between rule 130 and token '<' resolved as shift ('&' < '<').
    Conflict between rule 130 and token '>' resolved as shift ('&' < '>').
    Conflict between rule 130 and token '+' resolved as shift ('&' < '+').
    Conflict between rule 130 and token '-' resolved as shift ('&' < '-').
    Conflict between rule 130 and token '*' resolved as shift ('&' < '*').
    Conflict between rule 130 and token '/' resolved as shift ('&' < '/').
    Conflict between rule 130 and token '%' resolved as shift ('&' < '%').


State 258

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  128     | expr '<' '<' . expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 299


State 259

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  129     | expr '>' '>' . expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 300


State 260

  123 expr: expr . '+' expr
  123     | expr '+' expr .  ['|', '^', '&', '<', '>', '+', '-', ';', ',', ')']
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204

    $výchozí  reduce using rule 123 (expr)

    Conflict between rule 123 and token '|' resolved as reduce ('|' < '+').
    Conflict between rule 123 and token '^' resolved as reduce ('^' < '+').
    Conflict between rule 123 and token '&' resolved as reduce ('&' < '+').
    Conflict between rule 123 and token '<' resolved as reduce ('<' < '+').
    Conflict between rule 123 and token '>' resolved as reduce ('>' < '+').
    Conflict between rule 123 and token '+' resolved as reduce (%left '+').
    Conflict between rule 123 and token '-' resolved as reduce (%left '-').
    Conflict between rule 123 and token '*' resolved as shift ('+' < '*').
    Conflict between rule 123 and token '/' resolved as shift ('+' < '/').
    Conflict between rule 123 and token '%' resolved as shift ('+' < '%').


State 261

  123 expr: expr . '+' expr
  124     | expr . '-' expr
  124     | expr '-' expr .  ['|', '^', '&', '<', '>', '+', '-', ';', ',', ')']
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204

    $výchozí  reduce using rule 124 (expr)

    Conflict between rule 124 and token '|' resolved as reduce ('|' < '-').
    Conflict between rule 124 and token '^' resolved as reduce ('^' < '-').
    Conflict between rule 124 and token '&' resolved as reduce ('&' < '-').
    Conflict between rule 124 and token '<' resolved as reduce ('<' < '-').
    Conflict between rule 124 and token '>' resolved as reduce ('>' < '-').
    Conflict between rule 124 and token '+' resolved as reduce (%left '+').
    Conflict between rule 124 and token '-' resolved as reduce (%left '-').
    Conflict between rule 124 and token '*' resolved as shift ('-' < '*').
    Conflict between rule 124 and token '/' resolved as shift ('-' < '/').
    Conflict between rule 124 and token '%' resolved as shift ('-' < '%').


State 262

  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  125     | expr '*' expr .  ['|', '^', '&', '<', '>', '+', '-', '*', '/', '%', ';', ',', ')']
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    $výchozí  reduce using rule 125 (expr)

    Conflict between rule 125 and token '|' resolved as reduce ('|' < '*').
    Conflict between rule 125 and token '^' resolved as reduce ('^' < '*').
    Conflict between rule 125 and token '&' resolved as reduce ('&' < '*').
    Conflict between rule 125 and token '<' resolved as reduce ('<' < '*').
    Conflict between rule 125 and token '>' resolved as reduce ('>' < '*').
    Conflict between rule 125 and token '+' resolved as reduce ('+' < '*').
    Conflict between rule 125 and token '-' resolved as reduce ('-' < '*').
    Conflict between rule 125 and token '*' resolved as reduce (%left '*').
    Conflict between rule 125 and token '/' resolved as reduce (%left '/').
    Conflict between rule 125 and token '%' resolved as reduce (%left '%').


State 263

  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  126     | expr '/' expr .  ['|', '^', '&', '<', '>', '+', '-', '*', '/', '%', ';', ',', ')']
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    $výchozí  reduce using rule 126 (expr)

    Conflict between rule 126 and token '|' resolved as reduce ('|' < '/').
    Conflict between rule 126 and token '^' resolved as reduce ('^' < '/').
    Conflict between rule 126 and token '&' resolved as reduce ('&' < '/').
    Conflict between rule 126 and token '<' resolved as reduce ('<' < '/').
    Conflict between rule 126 and token '>' resolved as reduce ('>' < '/').
    Conflict between rule 126 and token '+' resolved as reduce ('+' < '/').
    Conflict between rule 126 and token '-' resolved as reduce ('-' < '/').
    Conflict between rule 126 and token '*' resolved as reduce (%left '*').
    Conflict between rule 126 and token '/' resolved as reduce (%left '/').
    Conflict between rule 126 and token '%' resolved as reduce (%left '%').


State 264

  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  127     | expr '%' expr .  ['|', '^', '&', '<', '>', '+', '-', '*', '/', '%', ';', ',', ')']
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    $výchozí  reduce using rule 127 (expr)

    Conflict between rule 127 and token '|' resolved as reduce ('|' < '%').
    Conflict between rule 127 and token '^' resolved as reduce ('^' < '%').
    Conflict between rule 127 and token '&' resolved as reduce ('&' < '%').
    Conflict between rule 127 and token '<' resolved as reduce ('<' < '%').
    Conflict between rule 127 and token '>' resolved as reduce ('>' < '%').
    Conflict between rule 127 and token '+' resolved as reduce ('+' < '%').
    Conflict between rule 127 and token '-' resolved as reduce ('-' < '%').
    Conflict between rule 127 and token '*' resolved as reduce (%left '*').
    Conflict between rule 127 and token '/' resolved as reduce (%left '/').
    Conflict between rule 127 and token '%' resolved as reduce (%left '%').


State 265

   31 inst: LTYPEB name ',' con ',' . imm
   32     | LTYPEB name ',' con ',' . imm '-' con
   85 imm: . '$' con

    '$'  posunout a přejít do stavu 101

    imm  přejít do stavu 301


State 266

  105 name: con '(' pointer ')' .

    $výchozí  reduce using rule 105 (name)


State 267

   33 inst: LTYPEC name '/' con ',' . ximm
   55 ximm: . '$' con
   56     | . '$' oreg
   57     | . '$' '*' '$' oreg
   58     | . '$' LSCONST
   59     | . fcon
   60 fcon: . '$' LFCONST
   61     | . '$' '-' LFCONST

    '$'  posunout a přejít do stavu 82

    ximm  přejít do stavu 302
    fcon  přejít do stavu 84


State 268

   36 inst: LTYPEI cond freg ',' freg .

    $výchozí  reduce using rule 36 (inst)


State 269

   40 inst: LTYPEJ cond con ',' expr . ',' spreg ',' creg ',' creg oexpr
  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '|'  posunout a přejít do stavu 195
    '^'  posunout a přejít do stavu 196
    '&'  posunout a přejít do stavu 197
    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204
    ','  posunout a přejít do stavu 303


State 270

   38 inst: LTYPEK cond frcon ',' LFREG . ',' freg
  103 freg: LFREG .  [';']

    ','  posunout a přejít do stavu 304

    $výchozí  reduce using rule 103 (freg)


State 271

   37 inst: LTYPEK cond frcon ',' freg .

    $výchozí  reduce using rule 37 (inst)


State 272

   39 inst: LTYPEL cond freg ',' freg . comma
   50 comma: . %empty  [';']
   51      | . ',' comma

    ','  posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 305


State 273

   41 inst: LTYPEM cond reg ',' reg . ',' regreg

    ','  posunout a přejít do stavu 306


State 274

   42 inst: LTYPEN cond reg ',' reg . ',' reg ',' spreg

    ','  posunout a přejít do stavu 307


State 275

   80 oreg: name '(' sreg ')' .

    $výchozí  reduce using rule 80 (oreg)


State 276

   78 ioreg: con '(' sreg ')' .

    $výchozí  reduce using rule 78 (ioreg)


State 277

   96 sreg: LR '(' expr ')' .

    $výchozí  reduce using rule 96 (sreg)


State 278

  104 freg: LF '(' con ')' .

    $výchozí  reduce using rule 104 (freg)


State 279

   57 ximm: '$' '*' '$' oreg .

    $výchozí  reduce using rule 57 (ximm)


State 280

   68 gen: shift '(' spreg ')' .

    $výchozí  reduce using rule 68 (gen)


State 281

   88 shift: spreg '<' '<' rcon .

    $výchozí  reduce using rule 88 (shift)


State 282

   92 rcon: spreg .

    $výchozí  reduce using rule 92 (rcon)


State 283

   93 rcon: con .

    $výchozí  reduce using rule 93 (rcon)


State 284

   89 shift: spreg '>' '>' rcon .

    $výchozí  reduce using rule 89 (shift)


State 285

   90 shift: spreg '-' '>' rcon .

    $výchozí  reduce using rule 90 (shift)


State 286

   91 shift: spreg LAT '>' rcon .

    $výchozí  reduce using rule 91 (shift)


State 287

   13 inst: LTYPE1 cond imsr ',' spreg ',' . reg
   14     | LTYPE1 cond imsr ',' spreg ',' .  [';']
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    $výchozí  reduce using rule 14 (inst)

    reg    přejít do stavu 308
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 288

   52 rel: con '(' LPC ')' .

    $výchozí  reduce using rule 52 (rel)


State 289

   23 inst: LTYPE7 cond imsr ',' spreg comma .

    $výchozí  reduce using rule 23 (inst)


State 290

   25 inst: LTYPE8 cond '[' reglist ']' ',' . ioreg
   76 ireg: . '(' spreg ')'
   77 ioreg: . ireg
   78      | . con '(' sreg ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 68
    '~'     posunout a přejít do stavu 54

    ireg   přejít do stavu 69
    ioreg  přejít do stavu 309
    con    přejít do stavu 118


State 291

   63 reglist: spreg '-' spreg .

    $výchozí  reduce using rule 63 (reglist)


State 292

   64 reglist: spreg comma reglist .

    $výchozí  reduce using rule 64 (reglist)


State 293

   24 inst: LTYPE8 cond ioreg ',' '[' reglist . ']'

    ']'  posunout a přejít do stavu 310


State 294

   28 inst: LTYPE9 cond comma ireg ',' reg .

    $výchozí  reduce using rule 28 (inst)


State 295

   26 inst: LTYPE9 cond reg ',' ireg ',' . reg
   50 comma: . %empty  [';']
   51      | . ',' comma
   51      | ',' . comma
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77
    ','   posunout a přejít do stavu 41

    $výchozí  reduce using rule 50 (comma)

    comma  přejít do stavu 109
    reg    přejít do stavu 311
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 296

   27 inst: LTYPE9 cond reg ',' ireg comma .

    $výchozí  reduce using rule 27 (inst)


State 297

  107 name: LNAME '<' '>' offset '(' . LSB ')'

    LSB  posunout a přejít do stavu 312


State 298

  106 name: LNAME offset '(' pointer ')' .

    $výchozí  reduce using rule 106 (name)


State 299

  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  128     | expr '<' '<' expr .  ['|', '^', '&', '<', '>', ';', ',', ')']
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204

    $výchozí  reduce using rule 128 (expr)

    Conflict between rule 128 and token '|' resolved as reduce ('|' < '<').
    Conflict between rule 128 and token '^' resolved as reduce ('^' < '<').
    Conflict between rule 128 and token '&' resolved as reduce ('&' < '<').
    Conflict between rule 128 and token '<' resolved as reduce (%left '<').
    Conflict between rule 128 and token '>' resolved as reduce (%left '>').
    Conflict between rule 128 and token '+' resolved as shift ('<' < '+').
    Conflict between rule 128 and token '-' resolved as shift ('<' < '-').
    Conflict between rule 128 and token '*' resolved as shift ('<' < '*').
    Conflict between rule 128 and token '/' resolved as shift ('<' < '/').
    Conflict between rule 128 and token '%' resolved as shift ('<' < '%').


State 300

  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  129     | expr '>' '>' expr .  ['|', '^', '&', '<', '>', ';', ',', ')']
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204

    $výchozí  reduce using rule 129 (expr)

    Conflict between rule 129 and token '|' resolved as reduce ('|' < '>').
    Conflict between rule 129 and token '^' resolved as reduce ('^' < '>').
    Conflict between rule 129 and token '&' resolved as reduce ('&' < '>').
    Conflict between rule 129 and token '<' resolved as reduce (%left '<').
    Conflict between rule 129 and token '>' resolved as reduce (%left '>').
    Conflict between rule 129 and token '+' resolved as shift ('>' < '+').
    Conflict between rule 129 and token '-' resolved as shift ('>' < '-').
    Conflict between rule 129 and token '*' resolved as shift ('>' < '*').
    Conflict between rule 129 and token '/' resolved as shift ('>' < '/').
    Conflict between rule 129 and token '%' resolved as shift ('>' < '%').


State 301

   31 inst: LTYPEB name ',' con ',' imm .  [';']
   32     | LTYPEB name ',' con ',' imm . '-' con

    '-'  posunout a přejít do stavu 313

    $výchozí  reduce using rule 31 (inst)


State 302

   33 inst: LTYPEC name '/' con ',' ximm .

    $výchozí  reduce using rule 33 (inst)


State 303

   40 inst: LTYPEJ cond con ',' expr ',' . spreg ',' creg ',' creg oexpr
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg   přejít do stavu 89
    spreg  přejít do stavu 314


State 304

   38 inst: LTYPEK cond frcon ',' LFREG ',' . freg
  103 freg: . LFREG
  104     | . LF '(' con ')'

    LF     posunout a přejít do stavu 78
    LFREG  posunout a přejít do stavu 79

    freg  přejít do stavu 315


State 305

   39 inst: LTYPEL cond freg ',' freg comma .

    $výchozí  reduce using rule 39 (inst)


State 306

   41 inst: LTYPEM cond reg ',' reg ',' . regreg
   87 regreg: . '(' spreg ',' spreg ')'

    '('  posunout a přejít do stavu 316

    regreg  přejít do stavu 317


State 307

   42 inst: LTYPEN cond reg ',' reg ',' . reg ',' spreg
   86 reg: . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    reg    přejít do stavu 318
    sreg   přejít do stavu 89
    spreg  přejít do stavu 121


State 308

   13 inst: LTYPE1 cond imsr ',' spreg ',' reg .

    $výchozí  reduce using rule 13 (inst)


State 309

   25 inst: LTYPE8 cond '[' reglist ']' ',' ioreg .

    $výchozí  reduce using rule 25 (inst)


State 310

   24 inst: LTYPE8 cond ioreg ',' '[' reglist ']' .

    $výchozí  reduce using rule 24 (inst)


State 311

   26 inst: LTYPE9 cond reg ',' ireg ',' reg .

    $výchozí  reduce using rule 26 (inst)


State 312

  107 name: LNAME '<' '>' offset '(' LSB . ')'

    ')'  posunout a přejít do stavu 319


State 313

   32 inst: LTYPEB name ',' con ',' imm '-' . con
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con  přejít do stavu 320


State 314

   40 inst: LTYPEJ cond con ',' expr ',' spreg . ',' creg ',' creg oexpr

    ','  posunout a přejít do stavu 321


State 315

   38 inst: LTYPEK cond frcon ',' LFREG ',' freg .

    $výchozí  reduce using rule 38 (inst)


State 316

   87 regreg: '(' . spreg ',' spreg ')'
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg   přejít do stavu 89
    spreg  přejít do stavu 322


State 317

   41 inst: LTYPEM cond reg ',' reg ',' regreg .

    $výchozí  reduce using rule 41 (inst)


State 318

   42 inst: LTYPEN cond reg ',' reg ',' reg . ',' spreg

    ','  posunout a přejít do stavu 323


State 319

  107 name: LNAME '<' '>' offset '(' LSB ')' .

    $výchozí  reduce using rule 107 (name)


State 320

   32 inst: LTYPEB name ',' con ',' imm '-' con .

    $výchozí  reduce using rule 32 (inst)


State 321

   40 inst: LTYPEJ cond con ',' expr ',' spreg ',' . creg ',' creg oexpr
   99 creg: . LCREG
  100     | . LC '(' expr ')'

    LC     posunout a přejít do stavu 324
    LCREG  posunout a přejít do stavu 325

    creg  přejít do stavu 326


State 322

   87 regreg: '(' spreg . ',' spreg ')'

    ','  posunout a přejít do stavu 327


State 323

   42 inst: LTYPEN cond reg ',' reg ',' reg ',' . spreg
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg   přejít do stavu 89
    spreg  přejít do stavu 328


State 324

  100 creg: LC . '(' expr ')'

    '('  posunout a přejít do stavu 329


State 325

   99 creg: LCREG .

    $výchozí  reduce using rule 99 (creg)


State 326

   40 inst: LTYPEJ cond con ',' expr ',' spreg ',' creg . ',' creg oexpr

    ','  posunout a přejít do stavu 330


State 327

   87 regreg: '(' spreg ',' . spreg ')'
   94 sreg: . LREG
   95     | . LPC
   96     | . LR '(' expr ')'
   97 spreg: . sreg
   98      | . LSP

    LSP   posunout a přejít do stavu 74
    LPC   posunout a přejít do stavu 75
    LR    posunout a přejít do stavu 76
    LREG  posunout a přejít do stavu 77

    sreg   přejít do stavu 89
    spreg  přejít do stavu 331


State 328

   42 inst: LTYPEN cond reg ',' reg ',' reg ',' spreg .

    $výchozí  reduce using rule 42 (inst)


State 329

  100 creg: LC '(' . expr ')'
  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 332


State 330

   40 inst: LTYPEJ cond con ',' expr ',' spreg ',' creg ',' . creg oexpr
   99 creg: . LCREG
  100     | . LC '(' expr ')'

    LC     posunout a přejít do stavu 324
    LCREG  posunout a přejít do stavu 325

    creg  přejít do stavu 333


State 331

   87 regreg: '(' spreg ',' spreg . ')'

    ')'  posunout a přejít do stavu 334


State 332

  100 creg: LC '(' expr . ')'
  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '|'  posunout a přejít do stavu 195
    '^'  posunout a přejít do stavu 196
    '&'  posunout a přejít do stavu 197
    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204
    ')'  posunout a přejít do stavu 335


State 333

   40 inst: LTYPEJ cond con ',' expr ',' spreg ',' creg ',' creg . oexpr
  120 oexpr: . %empty  [';']
  121      | . ',' expr

    ','  posunout a přejít do stavu 336

    $výchozí  reduce using rule 120 (oexpr)

    oexpr  přejít do stavu 337


State 334

   87 regreg: '(' spreg ',' spreg ')' .

    $výchozí  reduce using rule 87 (regreg)


State 335

  100 creg: LC '(' expr ')' .

    $výchozí  reduce using rule 100 (creg)


State 336

  114 con: . LCONST
  115    | . LVAR
  116    | . '-' con
  117    | . '+' con
  118    | . '~' con
  119    | . '(' expr ')'
  121 oexpr: ',' . expr
  122 expr: . con
  123     | . expr '+' expr
  124     | . expr '-' expr
  125     | . expr '*' expr
  126     | . expr '/' expr
  127     | . expr '%' expr
  128     | . expr '<' '<' expr
  129     | . expr '>' '>' expr
  130     | . expr '&' expr
  131     | . expr '^' expr
  132     | . expr '|' expr

    '+'     posunout a přejít do stavu 48
    '-'     posunout a přejít do stavu 49
    LCONST  posunout a přejít do stavu 50
    LVAR    posunout a přejít do stavu 52
    '('     posunout a přejít do stavu 53
    '~'     posunout a přejít do stavu 54

    con   přejít do stavu 129
    expr  přejít do stavu 338


State 337

   40 inst: LTYPEJ cond con ',' expr ',' spreg ',' creg ',' creg oexpr .

    $výchozí  reduce using rule 40 (inst)


State 338

  121 oexpr: ',' expr .  [';']
  123 expr: expr . '+' expr
  124     | expr . '-' expr
  125     | expr . '*' expr
  126     | expr . '/' expr
  127     | expr . '%' expr
  128     | expr . '<' '<' expr
  129     | expr . '>' '>' expr
  130     | expr . '&' expr
  131     | expr . '^' expr
  132     | expr . '|' expr

    '|'  posunout a přejít do stavu 195
    '^'  posunout a přejít do stavu 196
    '&'  posunout a přejít do stavu 197
    '<'  posunout a přejít do stavu 198
    '>'  posunout a přejít do stavu 199
    '+'  posunout a přejít do stavu 200
    '-'  posunout a přejít do stavu 201
    '*'  posunout a přejít do stavu 202
    '/'  posunout a přejít do stavu 203
    '%'  posunout a přejít do stavu 204

    $výchozí  reduce using rule 121 (oexpr)
