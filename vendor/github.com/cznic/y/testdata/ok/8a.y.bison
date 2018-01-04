Gramatika

    0 $accept: prog $end

    1 prog: %empty

    2 $@1: %empty

    3 prog: prog $@1 line

    4 $@2: %empty

    5 line: LLAB ':' $@2 line

    6 $@3: %empty

    7 line: LNAME ':' $@3 line
    8     | ';'
    9     | inst ';'
   10     | error ';'

   11 inst: LNAME '=' expr
   12     | LVAR '=' expr
   13     | LTYPE0 nonnon
   14     | LTYPE1 nonrem
   15     | LTYPE2 rimnon
   16     | LTYPE3 rimrem
   17     | LTYPE4 remrim
   18     | LTYPER nonrel
   19     | LTYPED spec1
   20     | LTYPET spec2
   21     | LTYPEC spec3
   22     | LTYPEN spec4
   23     | LTYPES spec5
   24     | LTYPEM spec6
   25     | LTYPEI spec7
   26     | LTYPEG spec8
   27     | LTYPEXC spec9
   28     | LTYPEX spec10
   29     | LTYPEPC spec11
   30     | LTYPEF spec12

   31 nonnon: %empty
   32       | ','

   33 rimrem: rim ',' rem

   34 remrim: rem ',' rim

   35 rimnon: rim ','
   36       | rim

   37 nonrem: ',' rem
   38       | rem

   39 nonrel: ',' rel
   40       | rel
   41       | imm ',' rel

   42 spec1: nam '/' con ',' imm

   43 spec2: mem ',' imm2
   44      | mem ',' con ',' imm2

   45 spec3: ',' rom
   46      | rom
   47      | '*' nam

   48 spec4: nonnon
   49      | nonrem

   50 spec5: rim ',' rem
   51      | rim ',' rem ':' LLREG

   52 spec6: rim ',' rem
   53      | rim ',' rem ':' LSREG

   54 spec7: rim ','
   55      | rim
   56      | rim ',' rem

   57 spec8: mem ',' imm
   58      | mem ',' con ',' imm

   59 spec9: reg ',' rem ',' con

   60 spec10: imm ',' rem ',' reg

   61 spec11: rim ',' rim

   62 spec12: rim ',' rim

   63 rem: reg
   64    | mem

   65 rom: rel
   66    | nmem
   67    | '*' reg
   68    | '*' omem
   69    | reg
   70    | omem
   71    | imm

   72 rim: rem
   73    | imm

   74 rel: con '(' LPC ')'
   75    | LNAME offset
   76    | LLAB offset

   77 reg: LBREG
   78    | LFREG
   79    | LLREG
   80    | LXREG
   81    | LSP
   82    | LSREG

   83 imm: '$' con
   84    | '$' nam
   85    | '$' LSCONST
   86    | '$' LFCONST
   87    | '$' '(' LFCONST ')'
   88    | '$' '(' '-' LFCONST ')'
   89    | '$' '-' LFCONST

   90 imm2: '$' con2

   91 con2: LCONST
   92     | '-' LCONST
   93     | LCONST '-' LCONST
   94     | '-' LCONST '-' LCONST

   95 mem: omem
   96    | nmem

   97 omem: con
   98     | con '(' LLREG ')'
   99     | con '(' LSP ')'
  100     | con '(' LLREG '*' con ')'
  101     | con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | '(' LLREG ')'
  104     | '(' LSP ')'
  105     | con '(' LSREG ')'
  106     | '(' LLREG '*' con ')'
  107     | '(' LLREG ')' '(' LLREG '*' con ')'

  108 nmem: nam
  109     | nam '(' LLREG '*' con ')'

  110 nam: LNAME offset '(' pointer ')'
  111    | LNAME '<' '>' offset '(' LSB ')'

  112 offset: %empty
  113       | '+' con
  114       | '-' con

  115 pointer: LSB
  116        | LSP
  117        | LFP

  118 con: LCONST
  119    | LVAR
  120    | '-' con
  121    | '+' con
  122    | '~' con
  123    | '(' expr ')'

  124 expr: con
  125     | expr '+' expr
  126     | expr '-' expr
  127     | expr '*' expr
  128     | expr '/' expr
  129     | expr '%' expr
  130     | expr '<' '<' expr
  131     | expr '>' '>' expr
  132     | expr '&' expr
  133     | expr '^' expr
  134     | expr '|' expr


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'$' (36) 83 84 85 86 87 88 89 90
'%' (37) 129
'&' (38) 132
'(' (40) 74 87 88 98 99 100 101 102 103 104 105 106 107 109 110 111
    123
')' (41) 74 87 88 98 99 100 101 102 103 104 105 106 107 109 110 111
    123
'*' (42) 47 67 68 100 101 102 106 107 109 127
'+' (43) 113 121 125
',' (44) 32 33 34 35 37 39 41 42 43 44 45 50 51 52 53 54 56 57 58 59
    60 61 62
'-' (45) 88 89 92 93 94 114 120 126
'/' (47) 42 128
':' (58) 5 7 51 53
';' (59) 8 9 10
'<' (60) 111 130
'=' (61) 11 12
'>' (62) 111 131
'^' (94) 133
'|' (124) 134
'~' (126) 122
error (256) 10
LTYPE0 (258) 13
LTYPE1 (259) 14
LTYPE2 (260) 15
LTYPE3 (261) 16
LTYPE4 (262) 17
LTYPEC (263) 21
LTYPED (264) 19
LTYPEN (265) 22
LTYPER (266) 18
LTYPET (267) 20
LTYPES (268) 23
LTYPEM (269) 24
LTYPEI (270) 25
LTYPEG (271) 26
LTYPEXC (272) 27
LTYPEX (273) 28
LTYPEPC (274) 29
LTYPEF (275) 30
LCONST (276) 91 92 93 94 118
LFP (277) 117
LPC (278) 74
LSB (279) 111 115
LBREG (280) 77
LLREG (281) 51 79 98 100 101 102 103 106 107 109
LSREG (282) 53 82 102 105
LFREG (283) 78
LXREG (284) 80
LFCONST (285) 86 87 88 89
LSCONST (286) 85
LSP (287) 81 99 104 116
LNAME (288) 7 11 75 110 111
LLAB (289) 5 76
LVAR (290) 12 119


Neterminály s pravidly, ve kterých se objevují

$accept (54)
    vlevo: 0
prog (55)
    vlevo: 1 3, vpravo: 0 3
$@1 (56)
    vlevo: 2, vpravo: 3
line (57)
    vlevo: 5 7 8 9 10, vpravo: 3 5 7
$@2 (58)
    vlevo: 4, vpravo: 5
$@3 (59)
    vlevo: 6, vpravo: 7
inst (60)
    vlevo: 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29
    30, vpravo: 9
nonnon (61)
    vlevo: 31 32, vpravo: 13 48
rimrem (62)
    vlevo: 33, vpravo: 16
remrim (63)
    vlevo: 34, vpravo: 17
rimnon (64)
    vlevo: 35 36, vpravo: 15
nonrem (65)
    vlevo: 37 38, vpravo: 14 49
nonrel (66)
    vlevo: 39 40 41, vpravo: 18
spec1 (67)
    vlevo: 42, vpravo: 19
spec2 (68)
    vlevo: 43 44, vpravo: 20
spec3 (69)
    vlevo: 45 46 47, vpravo: 21
spec4 (70)
    vlevo: 48 49, vpravo: 22
spec5 (71)
    vlevo: 50 51, vpravo: 23
spec6 (72)
    vlevo: 52 53, vpravo: 24
spec7 (73)
    vlevo: 54 55 56, vpravo: 25
spec8 (74)
    vlevo: 57 58, vpravo: 26
spec9 (75)
    vlevo: 59, vpravo: 27
spec10 (76)
    vlevo: 60, vpravo: 28
spec11 (77)
    vlevo: 61, vpravo: 29
spec12 (78)
    vlevo: 62, vpravo: 30
rem (79)
    vlevo: 63 64, vpravo: 33 34 37 38 50 51 52 53 56 59 60 72
rom (80)
    vlevo: 65 66 67 68 69 70 71, vpravo: 45 46
rim (81)
    vlevo: 72 73, vpravo: 33 34 35 36 50 51 52 53 54 55 56 61 62
rel (82)
    vlevo: 74 75 76, vpravo: 39 40 41 65
reg (83)
    vlevo: 77 78 79 80 81 82, vpravo: 59 60 63 67 69
imm (84)
    vlevo: 83 84 85 86 87 88 89, vpravo: 41 42 57 58 60 71 73
imm2 (85)
    vlevo: 90, vpravo: 43 44
con2 (86)
    vlevo: 91 92 93 94, vpravo: 90
mem (87)
    vlevo: 95 96, vpravo: 43 44 57 58 64
omem (88)
    vlevo: 97 98 99 100 101 102 103 104 105 106 107, vpravo: 68 70
    95
nmem (89)
    vlevo: 108 109, vpravo: 66 96
nam (90)
    vlevo: 110 111, vpravo: 42 47 84 108 109
offset (91)
    vlevo: 112 113 114, vpravo: 75 76 110 111
pointer (92)
    vlevo: 115 116 117, vpravo: 110
con (93)
    vlevo: 118 119 120 121 122 123, vpravo: 42 44 58 59 74 83 97 98
    99 100 101 102 105 106 107 109 113 114 120 121 122 124
expr (94)
    vlevo: 124 125 126 127 128 129 130 131 132 133 134, vpravo: 11
    12 123 125 126 127 128 129 130 131 132 133 134


State 0

    0 $accept: . prog $end
    1 prog: . %empty
    3     | . prog $@1 line

    $výchozí  reduce using rule 1 (prog)

    prog  přejít do stavu 1


State 1

    0 $accept: prog . $end
    2 $@1: . %empty  [error, LTYPE0, LTYPE1, LTYPE2, LTYPE3, LTYPE4, LTYPEC, LTYPED, LTYPEN, LTYPER, LTYPET, LTYPES, LTYPEM, LTYPEI, LTYPEG, LTYPEXC, LTYPEX, LTYPEPC, LTYPEF, LNAME, LLAB, LVAR, ';']
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
    8     | . ';'
    9     | . inst ';'
   10     | . error ';'
   11 inst: . LNAME '=' expr
   12     | . LVAR '=' expr
   13     | . LTYPE0 nonnon
   14     | . LTYPE1 nonrem
   15     | . LTYPE2 rimnon
   16     | . LTYPE3 rimrem
   17     | . LTYPE4 remrim
   18     | . LTYPER nonrel
   19     | . LTYPED spec1
   20     | . LTYPET spec2
   21     | . LTYPEC spec3
   22     | . LTYPEN spec4
   23     | . LTYPES spec5
   24     | . LTYPEM spec6
   25     | . LTYPEI spec7
   26     | . LTYPEG spec8
   27     | . LTYPEXC spec9
   28     | . LTYPEX spec10
   29     | . LTYPEPC spec11
   30     | . LTYPEF spec12

    error    posunout a přejít do stavu 4
    LTYPE0   posunout a přejít do stavu 5
    LTYPE1   posunout a přejít do stavu 6
    LTYPE2   posunout a přejít do stavu 7
    LTYPE3   posunout a přejít do stavu 8
    LTYPE4   posunout a přejít do stavu 9
    LTYPEC   posunout a přejít do stavu 10
    LTYPED   posunout a přejít do stavu 11
    LTYPEN   posunout a přejít do stavu 12
    LTYPER   posunout a přejít do stavu 13
    LTYPET   posunout a přejít do stavu 14
    LTYPES   posunout a přejít do stavu 15
    LTYPEM   posunout a přejít do stavu 16
    LTYPEI   posunout a přejít do stavu 17
    LTYPEG   posunout a přejít do stavu 18
    LTYPEXC  posunout a přejít do stavu 19
    LTYPEX   posunout a přejít do stavu 20
    LTYPEPC  posunout a přejít do stavu 21
    LTYPEF   posunout a přejít do stavu 22
    LNAME    posunout a přejít do stavu 23
    LLAB     posunout a přejít do stavu 24
    LVAR     posunout a přejít do stavu 25
    ';'      posunout a přejít do stavu 26

    line  přejít do stavu 27
    inst  přejít do stavu 28


State 4

   10 line: error . ';'

    ';'  posunout a přejít do stavu 29


State 5

   13 inst: LTYPE0 . nonnon
   31 nonnon: . %empty  [';']
   32       | . ','

    ','  posunout a přejít do stavu 30

    $výchozí  reduce using rule 31 (nonnon)

    nonnon  přejít do stavu 31


State 6

   14 inst: LTYPE1 . nonrem
   37 nonrem: . ',' rem
   38       | . rem
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    ','     posunout a přejít do stavu 43
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    nonrem  přejít do stavu 46
    rem     přejít do stavu 47
    reg     přejít do stavu 48
    mem     přejít do stavu 49
    omem    přejít do stavu 50
    nmem    přejít do stavu 51
    nam     přejít do stavu 52
    con     přejít do stavu 53


State 7

   15 inst: LTYPE2 . rimnon
   35 rimnon: . rim ','
   36       | . rim
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    rimnon  přejít do stavu 55
    rem     přejít do stavu 56
    rim     přejít do stavu 57
    reg     přejít do stavu 48
    imm     přejít do stavu 58
    mem     přejít do stavu 49
    omem    přejít do stavu 50
    nmem    přejít do stavu 51
    nam     přejít do stavu 52
    con     přejít do stavu 53


State 8

   16 inst: LTYPE3 . rimrem
   33 rimrem: . rim ',' rem
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    rimrem  přejít do stavu 59
    rem     přejít do stavu 56
    rim     přejít do stavu 60
    reg     přejít do stavu 48
    imm     přejít do stavu 58
    mem     přejít do stavu 49
    omem    přejít do stavu 50
    nmem    přejít do stavu 51
    nam     přejít do stavu 52
    con     přejít do stavu 53


State 9

   17 inst: LTYPE4 . remrim
   34 remrim: . rem ',' rim
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    remrim  přejít do stavu 61
    rem     přejít do stavu 62
    reg     přejít do stavu 48
    mem     přejít do stavu 49
    omem    přejít do stavu 50
    nmem    přejít do stavu 51
    nam     přejít do stavu 52
    con     přejít do stavu 53


State 10

   21 inst: LTYPEC . spec3
   45 spec3: . ',' rom
   46      | . rom
   47      | . '*' nam
   65 rom: . rel
   66    | . nmem
   67    | . '*' reg
   68    | . '*' omem
   69    | . reg
   70    | . omem
   71    | . imm
   74 rel: . con '(' LPC ')'
   75    | . LNAME offset
   76    | . LLAB offset
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    '*'     posunout a přejít do stavu 63
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 64
    LLAB    posunout a přejít do stavu 65
    LVAR    posunout a přejít do stavu 42
    ','     posunout a přejít do stavu 66
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    spec3  přejít do stavu 67
    rom    přejít do stavu 68
    rel    přejít do stavu 69
    reg    přejít do stavu 70
    imm    přejít do stavu 71
    omem   přejít do stavu 72
    nmem   přejít do stavu 73
    nam    přejít do stavu 52
    con    přejít do stavu 74


State 11

   19 inst: LTYPED . spec1
   42 spec1: . nam '/' con ',' imm
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'

    LNAME  posunout a přejít do stavu 41

    spec1  přejít do stavu 75
    nam    přejít do stavu 76


State 12

   22 inst: LTYPEN . spec4
   31 nonnon: . %empty  [';']
   32       | . ','
   37 nonrem: . ',' rem
   38       | . rem
   48 spec4: . nonnon
   49      | . nonrem
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    ','     posunout a přejít do stavu 77
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    $výchozí  reduce using rule 31 (nonnon)

    nonnon  přejít do stavu 78
    nonrem  přejít do stavu 79
    spec4   přejít do stavu 80
    rem     přejít do stavu 47
    reg     přejít do stavu 48
    mem     přejít do stavu 49
    omem    přejít do stavu 50
    nmem    přejít do stavu 51
    nam     přejít do stavu 52
    con     přejít do stavu 53


State 13

   18 inst: LTYPER . nonrel
   39 nonrel: . ',' rel
   40       | . rel
   41       | . imm ',' rel
   74 rel: . con '(' LPC ')'
   75    | . LNAME offset
   76    | . LLAB offset
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LNAME   posunout a přejít do stavu 81
    LLAB    posunout a přejít do stavu 65
    LVAR    posunout a přejít do stavu 42
    ','     posunout a přejít do stavu 82
    '('     posunout a přejít do stavu 83
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    nonrel  přejít do stavu 84
    rel     přejít do stavu 85
    imm     přejít do stavu 86
    con     přejít do stavu 87


State 14

   20 inst: LTYPET . spec2
   43 spec2: . mem ',' imm2
   44      | . mem ',' con ',' imm2
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    spec2  přejít do stavu 88
    mem    přejít do stavu 89
    omem   přejít do stavu 50
    nmem   přejít do stavu 51
    nam    přejít do stavu 52
    con    přejít do stavu 53


State 15

   23 inst: LTYPES . spec5
   50 spec5: . rim ',' rem
   51      | . rim ',' rem ':' LLREG
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    spec5  přejít do stavu 90
    rem    přejít do stavu 56
    rim    přejít do stavu 91
    reg    přejít do stavu 48
    imm    přejít do stavu 58
    mem    přejít do stavu 49
    omem   přejít do stavu 50
    nmem   přejít do stavu 51
    nam    přejít do stavu 52
    con    přejít do stavu 53


State 16

   24 inst: LTYPEM . spec6
   52 spec6: . rim ',' rem
   53      | . rim ',' rem ':' LSREG
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    spec6  přejít do stavu 92
    rem    přejít do stavu 56
    rim    přejít do stavu 93
    reg    přejít do stavu 48
    imm    přejít do stavu 58
    mem    přejít do stavu 49
    omem   přejít do stavu 50
    nmem   přejít do stavu 51
    nam    přejít do stavu 52
    con    přejít do stavu 53


State 17

   25 inst: LTYPEI . spec7
   54 spec7: . rim ','
   55      | . rim
   56      | . rim ',' rem
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    spec7  přejít do stavu 94
    rem    přejít do stavu 56
    rim    přejít do stavu 95
    reg    přejít do stavu 48
    imm    přejít do stavu 58
    mem    přejít do stavu 49
    omem   přejít do stavu 50
    nmem   přejít do stavu 51
    nam    přejít do stavu 52
    con    přejít do stavu 53


State 18

   26 inst: LTYPEG . spec8
   57 spec8: . mem ',' imm
   58      | . mem ',' con ',' imm
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    spec8  přejít do stavu 96
    mem    přejít do stavu 97
    omem   přejít do stavu 50
    nmem   přejít do stavu 51
    nam    přejít do stavu 52
    con    přejít do stavu 53


State 19

   27 inst: LTYPEXC . spec9
   59 spec9: . reg ',' rem ',' con
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG

    LBREG  posunout a přejít do stavu 35
    LLREG  posunout a přejít do stavu 36
    LSREG  posunout a přejít do stavu 37
    LFREG  posunout a přejít do stavu 38
    LXREG  posunout a přejít do stavu 39
    LSP    posunout a přejít do stavu 40

    spec9  přejít do stavu 98
    reg    přejít do stavu 99


State 20

   28 inst: LTYPEX . spec10
   60 spec10: . imm ',' rem ',' reg
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST

    '$'  posunout a přejít do stavu 54

    spec10  přejít do stavu 100
    imm     přejít do stavu 101


State 21

   29 inst: LTYPEPC . spec11
   61 spec11: . rim ',' rim
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    spec11  přejít do stavu 102
    rem     přejít do stavu 56
    rim     přejít do stavu 103
    reg     přejít do stavu 48
    imm     přejít do stavu 58
    mem     přejít do stavu 49
    omem    přejít do stavu 50
    nmem    přejít do stavu 51
    nam     přejít do stavu 52
    con     přejít do stavu 53


State 22

   30 inst: LTYPEF . spec12
   62 spec12: . rim ',' rim
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    spec12  přejít do stavu 104
    rem     přejít do stavu 56
    rim     přejít do stavu 105
    reg     přejít do stavu 48
    imm     přejít do stavu 58
    mem     přejít do stavu 49
    omem    přejít do stavu 50
    nmem    přejít do stavu 51
    nam     přejít do stavu 52
    con     přejít do stavu 53


State 23

    7 line: LNAME . ':' $@3 line
   11 inst: LNAME . '=' expr

    ':'  posunout a přejít do stavu 106
    '='  posunout a přejít do stavu 107


State 24

    5 line: LLAB . ':' $@2 line

    ':'  posunout a přejít do stavu 108


State 25

   12 inst: LVAR . '=' expr

    '='  posunout a přejít do stavu 109


State 26

    8 line: ';' .

    $výchozí  reduce using rule 8 (line)


State 27

    3 prog: prog $@1 line .

    $výchozí  reduce using rule 3 (prog)


State 28

    9 line: inst . ';'

    ';'  posunout a přejít do stavu 110


State 29

   10 line: error ';' .

    $výchozí  reduce using rule 10 (line)


State 30

   32 nonnon: ',' .

    $výchozí  reduce using rule 32 (nonnon)


State 31

   13 inst: LTYPE0 nonnon .

    $výchozí  reduce using rule 13 (inst)


State 32

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  121    | '+' . con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 111


State 33

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  120    | '-' . con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 112


State 34

  118 con: LCONST .

    $výchozí  reduce using rule 118 (con)


State 35

   77 reg: LBREG .

    $výchozí  reduce using rule 77 (reg)


State 36

   79 reg: LLREG .

    $výchozí  reduce using rule 79 (reg)


State 37

   82 reg: LSREG .

    $výchozí  reduce using rule 82 (reg)


State 38

   78 reg: LFREG .

    $výchozí  reduce using rule 78 (reg)


State 39

   80 reg: LXREG .

    $výchozí  reduce using rule 80 (reg)


State 40

   81 reg: LSP .

    $výchozí  reduce using rule 81 (reg)


State 41

  110 nam: LNAME . offset '(' pointer ')'
  111    | LNAME . '<' '>' offset '(' LSB ')'
  112 offset: . %empty  ['(']
  113       | . '+' con
  114       | . '-' con

    '<'  posunout a přejít do stavu 113
    '+'  posunout a přejít do stavu 114
    '-'  posunout a přejít do stavu 115

    $výchozí  reduce using rule 112 (offset)

    offset  přejít do stavu 116


State 42

  119 con: LVAR .

    $výchozí  reduce using rule 119 (con)


State 43

   37 nonrem: ',' . rem
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    rem   přejít do stavu 117
    reg   přejít do stavu 48
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 44

  103 omem: '(' . LLREG ')'
  104     | '(' . LSP ')'
  106     | '(' . LLREG '*' con ')'
  107     | '(' . LLREG ')' '(' LLREG '*' con ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  123    | '(' . expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LLREG   posunout a přejít do stavu 118
    LSP     posunout a přejít do stavu 119
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 121


State 45

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  122    | '~' . con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 122


State 46

   14 inst: LTYPE1 nonrem .

    $výchozí  reduce using rule 14 (inst)


State 47

   38 nonrem: rem .

    $výchozí  reduce using rule 38 (nonrem)


State 48

   63 rem: reg .

    $výchozí  reduce using rule 63 (rem)


State 49

   64 rem: mem .

    $výchozí  reduce using rule 64 (rem)


State 50

   95 mem: omem .

    $výchozí  reduce using rule 95 (mem)


State 51

   96 mem: nmem .

    $výchozí  reduce using rule 96 (mem)


State 52

  108 nmem: nam .  [':', ';', ',']
  109     | nam . '(' LLREG '*' con ')'

    '('  posunout a přejít do stavu 123

    $výchozí  reduce using rule 108 (nmem)


State 53

   97 omem: con .  [':', ';', ',']
   98     | con . '(' LLREG ')'
   99     | con . '(' LSP ')'
  100     | con . '(' LLREG '*' con ')'
  101     | con . '(' LLREG ')' '(' LLREG '*' con ')'
  102     | con . '(' LLREG ')' '(' LSREG '*' con ')'
  105     | con . '(' LSREG ')'

    '('  posunout a přejít do stavu 124

    $výchozí  reduce using rule 97 (omem)


State 54

   83 imm: '$' . con
   84    | '$' . nam
   85    | '$' . LSCONST
   86    | '$' . LFCONST
   87    | '$' . '(' LFCONST ')'
   88    | '$' . '(' '-' LFCONST ')'
   89    | '$' . '-' LFCONST
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'      posunout a přejít do stavu 32
    '-'      posunout a přejít do stavu 125
    LCONST   posunout a přejít do stavu 34
    LFCONST  posunout a přejít do stavu 126
    LSCONST  posunout a přejít do stavu 127
    LNAME    posunout a přejít do stavu 41
    LVAR     posunout a přejít do stavu 42
    '('      posunout a přejít do stavu 128
    '~'      posunout a přejít do stavu 45

    nam  přejít do stavu 129
    con  přejít do stavu 130


State 55

   15 inst: LTYPE2 rimnon .

    $výchozí  reduce using rule 15 (inst)


State 56

   72 rim: rem .

    $výchozí  reduce using rule 72 (rim)


State 57

   35 rimnon: rim . ','
   36       | rim .  [';']

    ','  posunout a přejít do stavu 131

    $výchozí  reduce using rule 36 (rimnon)


State 58

   73 rim: imm .

    $výchozí  reduce using rule 73 (rim)


State 59

   16 inst: LTYPE3 rimrem .

    $výchozí  reduce using rule 16 (inst)


State 60

   33 rimrem: rim . ',' rem

    ','  posunout a přejít do stavu 132


State 61

   17 inst: LTYPE4 remrim .

    $výchozí  reduce using rule 17 (inst)


State 62

   34 remrim: rem . ',' rim

    ','  posunout a přejít do stavu 133


State 63

   47 spec3: '*' . nam
   67 rom: '*' . reg
   68    | '*' . omem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    reg   přejít do stavu 134
    omem  přejít do stavu 135
    nam   přejít do stavu 136
    con   přejít do stavu 53


State 64

   75 rel: LNAME . offset
  110 nam: LNAME . offset '(' pointer ')'
  111    | LNAME . '<' '>' offset '(' LSB ')'
  112 offset: . %empty  [';', '(']
  113       | . '+' con
  114       | . '-' con

    '<'  posunout a přejít do stavu 113
    '+'  posunout a přejít do stavu 114
    '-'  posunout a přejít do stavu 115

    $výchozí  reduce using rule 112 (offset)

    offset  přejít do stavu 137


State 65

   76 rel: LLAB . offset
  112 offset: . %empty  [';']
  113       | . '+' con
  114       | . '-' con

    '+'  posunout a přejít do stavu 114
    '-'  posunout a přejít do stavu 115

    $výchozí  reduce using rule 112 (offset)

    offset  přejít do stavu 138


State 66

   45 spec3: ',' . rom
   65 rom: . rel
   66    | . nmem
   67    | . '*' reg
   68    | . '*' omem
   69    | . reg
   70    | . omem
   71    | . imm
   74 rel: . con '(' LPC ')'
   75    | . LNAME offset
   76    | . LLAB offset
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    '*'     posunout a přejít do stavu 139
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 64
    LLAB    posunout a přejít do stavu 65
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    rom   přejít do stavu 140
    rel   přejít do stavu 69
    reg   přejít do stavu 70
    imm   přejít do stavu 71
    omem  přejít do stavu 72
    nmem  přejít do stavu 73
    nam   přejít do stavu 52
    con   přejít do stavu 74


State 67

   21 inst: LTYPEC spec3 .

    $výchozí  reduce using rule 21 (inst)


State 68

   46 spec3: rom .

    $výchozí  reduce using rule 46 (spec3)


State 69

   65 rom: rel .

    $výchozí  reduce using rule 65 (rom)


State 70

   69 rom: reg .

    $výchozí  reduce using rule 69 (rom)


State 71

   71 rom: imm .

    $výchozí  reduce using rule 71 (rom)


State 72

   70 rom: omem .

    $výchozí  reduce using rule 70 (rom)


State 73

   66 rom: nmem .

    $výchozí  reduce using rule 66 (rom)


State 74

   74 rel: con . '(' LPC ')'
   97 omem: con .  [';']
   98     | con . '(' LLREG ')'
   99     | con . '(' LSP ')'
  100     | con . '(' LLREG '*' con ')'
  101     | con . '(' LLREG ')' '(' LLREG '*' con ')'
  102     | con . '(' LLREG ')' '(' LSREG '*' con ')'
  105     | con . '(' LSREG ')'

    '('  posunout a přejít do stavu 141

    $výchozí  reduce using rule 97 (omem)


State 75

   19 inst: LTYPED spec1 .

    $výchozí  reduce using rule 19 (inst)


State 76

   42 spec1: nam . '/' con ',' imm

    '/'  posunout a přejít do stavu 142


State 77

   32 nonnon: ',' .  [';']
   37 nonrem: ',' . rem
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    $výchozí  reduce using rule 32 (nonnon)

    rem   přejít do stavu 117
    reg   přejít do stavu 48
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 78

   48 spec4: nonnon .

    $výchozí  reduce using rule 48 (spec4)


State 79

   49 spec4: nonrem .

    $výchozí  reduce using rule 49 (spec4)


State 80

   22 inst: LTYPEN spec4 .

    $výchozí  reduce using rule 22 (inst)


State 81

   75 rel: LNAME . offset
  112 offset: . %empty  [';']
  113       | . '+' con
  114       | . '-' con

    '+'  posunout a přejít do stavu 114
    '-'  posunout a přejít do stavu 115

    $výchozí  reduce using rule 112 (offset)

    offset  přejít do stavu 143


State 82

   39 nonrel: ',' . rel
   74 rel: . con '(' LPC ')'
   75    | . LNAME offset
   76    | . LLAB offset
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LNAME   posunout a přejít do stavu 81
    LLAB    posunout a přejít do stavu 65
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    rel  přejít do stavu 144
    con  přejít do stavu 87


State 83

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  123    | '(' . expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 121


State 84

   18 inst: LTYPER nonrel .

    $výchozí  reduce using rule 18 (inst)


State 85

   40 nonrel: rel .

    $výchozí  reduce using rule 40 (nonrel)


State 86

   41 nonrel: imm . ',' rel

    ','  posunout a přejít do stavu 145


State 87

   74 rel: con . '(' LPC ')'

    '('  posunout a přejít do stavu 146


State 88

   20 inst: LTYPET spec2 .

    $výchozí  reduce using rule 20 (inst)


State 89

   43 spec2: mem . ',' imm2
   44      | mem . ',' con ',' imm2

    ','  posunout a přejít do stavu 147


State 90

   23 inst: LTYPES spec5 .

    $výchozí  reduce using rule 23 (inst)


State 91

   50 spec5: rim . ',' rem
   51      | rim . ',' rem ':' LLREG

    ','  posunout a přejít do stavu 148


State 92

   24 inst: LTYPEM spec6 .

    $výchozí  reduce using rule 24 (inst)


State 93

   52 spec6: rim . ',' rem
   53      | rim . ',' rem ':' LSREG

    ','  posunout a přejít do stavu 149


State 94

   25 inst: LTYPEI spec7 .

    $výchozí  reduce using rule 25 (inst)


State 95

   54 spec7: rim . ','
   55      | rim .  [';']
   56      | rim . ',' rem

    ','  posunout a přejít do stavu 150

    $výchozí  reduce using rule 55 (spec7)


State 96

   26 inst: LTYPEG spec8 .

    $výchozí  reduce using rule 26 (inst)


State 97

   57 spec8: mem . ',' imm
   58      | mem . ',' con ',' imm

    ','  posunout a přejít do stavu 151


State 98

   27 inst: LTYPEXC spec9 .

    $výchozí  reduce using rule 27 (inst)


State 99

   59 spec9: reg . ',' rem ',' con

    ','  posunout a přejít do stavu 152


State 100

   28 inst: LTYPEX spec10 .

    $výchozí  reduce using rule 28 (inst)


State 101

   60 spec10: imm . ',' rem ',' reg

    ','  posunout a přejít do stavu 153


State 102

   29 inst: LTYPEPC spec11 .

    $výchozí  reduce using rule 29 (inst)


State 103

   61 spec11: rim . ',' rim

    ','  posunout a přejít do stavu 154


State 104

   30 inst: LTYPEF spec12 .

    $výchozí  reduce using rule 30 (inst)


State 105

   62 spec12: rim . ',' rim

    ','  posunout a přejít do stavu 155


State 106

    6 $@3: . %empty
    7 line: LNAME ':' . $@3 line

    $výchozí  reduce using rule 6 ($@3)

    $@3  přejít do stavu 156


State 107

   11 inst: LNAME '=' . expr
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 157


State 108

    4 $@2: . %empty
    5 line: LLAB ':' . $@2 line

    $výchozí  reduce using rule 4 ($@2)

    $@2  přejít do stavu 158


State 109

   12 inst: LVAR '=' . expr
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 159


State 110

    9 line: inst ';' .

    $výchozí  reduce using rule 9 (line)


State 111

  121 con: '+' con .

    $výchozí  reduce using rule 121 (con)


State 112

  120 con: '-' con .

    $výchozí  reduce using rule 120 (con)


State 113

  111 nam: LNAME '<' . '>' offset '(' LSB ')'

    '>'  posunout a přejít do stavu 160


State 114

  113 offset: '+' . con
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 161


State 115

  114 offset: '-' . con
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 162


State 116

  110 nam: LNAME offset . '(' pointer ')'

    '('  posunout a přejít do stavu 163


State 117

   37 nonrem: ',' rem .

    $výchozí  reduce using rule 37 (nonrem)


State 118

  103 omem: '(' LLREG . ')'
  106     | '(' LLREG . '*' con ')'
  107     | '(' LLREG . ')' '(' LLREG '*' con ')'

    '*'  posunout a přejít do stavu 164
    ')'  posunout a přejít do stavu 165


State 119

  104 omem: '(' LSP . ')'

    ')'  posunout a přejít do stavu 166


State 120

  124 expr: con .

    $výchozí  reduce using rule 124 (expr)


State 121

  123 con: '(' expr . ')'
  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    '|'  posunout a přejít do stavu 167
    '^'  posunout a přejít do stavu 168
    '&'  posunout a přejít do stavu 169
    '<'  posunout a přejít do stavu 170
    '>'  posunout a přejít do stavu 171
    '+'  posunout a přejít do stavu 172
    '-'  posunout a přejít do stavu 173
    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176
    ')'  posunout a přejít do stavu 177


State 122

  122 con: '~' con .

    $výchozí  reduce using rule 122 (con)


State 123

  109 nmem: nam '(' . LLREG '*' con ')'

    LLREG  posunout a přejít do stavu 178


State 124

   98 omem: con '(' . LLREG ')'
   99     | con '(' . LSP ')'
  100     | con '(' . LLREG '*' con ')'
  101     | con '(' . LLREG ')' '(' LLREG '*' con ')'
  102     | con '(' . LLREG ')' '(' LSREG '*' con ')'
  105     | con '(' . LSREG ')'

    LLREG  posunout a přejít do stavu 179
    LSREG  posunout a přejít do stavu 180
    LSP    posunout a přejít do stavu 181


State 125

   89 imm: '$' '-' . LFCONST
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  120    | '-' . con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'      posunout a přejít do stavu 32
    '-'      posunout a přejít do stavu 33
    LCONST   posunout a přejít do stavu 34
    LFCONST  posunout a přejít do stavu 182
    LVAR     posunout a přejít do stavu 42
    '('      posunout a přejít do stavu 83
    '~'      posunout a přejít do stavu 45

    con  přejít do stavu 112


State 126

   86 imm: '$' LFCONST .

    $výchozí  reduce using rule 86 (imm)


State 127

   85 imm: '$' LSCONST .

    $výchozí  reduce using rule 85 (imm)


State 128

   87 imm: '$' '(' . LFCONST ')'
   88    | '$' '(' . '-' LFCONST ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  123    | '(' . expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'      posunout a přejít do stavu 32
    '-'      posunout a přejít do stavu 183
    LCONST   posunout a přejít do stavu 34
    LFCONST  posunout a přejít do stavu 184
    LVAR     posunout a přejít do stavu 42
    '('      posunout a přejít do stavu 83
    '~'      posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 121


State 129

   84 imm: '$' nam .

    $výchozí  reduce using rule 84 (imm)


State 130

   83 imm: '$' con .

    $výchozí  reduce using rule 83 (imm)


State 131

   35 rimnon: rim ',' .

    $výchozí  reduce using rule 35 (rimnon)


State 132

   33 rimrem: rim ',' . rem
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    rem   přejít do stavu 185
    reg   přejít do stavu 48
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 133

   34 remrim: rem ',' . rim
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    rem   přejít do stavu 56
    rim   přejít do stavu 186
    reg   přejít do stavu 48
    imm   přejít do stavu 58
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 134

   67 rom: '*' reg .

    $výchozí  reduce using rule 67 (rom)


State 135

   68 rom: '*' omem .

    $výchozí  reduce using rule 68 (rom)


State 136

   47 spec3: '*' nam .

    $výchozí  reduce using rule 47 (spec3)


State 137

   75 rel: LNAME offset .  [';']
  110 nam: LNAME offset . '(' pointer ')'

    '('  posunout a přejít do stavu 163

    $výchozí  reduce using rule 75 (rel)


State 138

   76 rel: LLAB offset .

    $výchozí  reduce using rule 76 (rel)


State 139

   67 rom: '*' . reg
   68    | '*' . omem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    reg   přejít do stavu 134
    omem  přejít do stavu 135
    con   přejít do stavu 53


State 140

   45 spec3: ',' rom .

    $výchozí  reduce using rule 45 (spec3)


State 141

   74 rel: con '(' . LPC ')'
   98 omem: con '(' . LLREG ')'
   99     | con '(' . LSP ')'
  100     | con '(' . LLREG '*' con ')'
  101     | con '(' . LLREG ')' '(' LLREG '*' con ')'
  102     | con '(' . LLREG ')' '(' LSREG '*' con ')'
  105     | con '(' . LSREG ')'

    LPC    posunout a přejít do stavu 187
    LLREG  posunout a přejít do stavu 179
    LSREG  posunout a přejít do stavu 180
    LSP    posunout a přejít do stavu 181


State 142

   42 spec1: nam '/' . con ',' imm
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 188


State 143

   75 rel: LNAME offset .

    $výchozí  reduce using rule 75 (rel)


State 144

   39 nonrel: ',' rel .

    $výchozí  reduce using rule 39 (nonrel)


State 145

   41 nonrel: imm ',' . rel
   74 rel: . con '(' LPC ')'
   75    | . LNAME offset
   76    | . LLAB offset
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LNAME   posunout a přejít do stavu 81
    LLAB    posunout a přejít do stavu 65
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    rel  přejít do stavu 189
    con  přejít do stavu 87


State 146

   74 rel: con '(' . LPC ')'

    LPC  posunout a přejít do stavu 187


State 147

   43 spec2: mem ',' . imm2
   44      | mem ',' . con ',' imm2
   90 imm2: . '$' con2
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '$'     posunout a přejít do stavu 190
    '~'     posunout a přejít do stavu 45

    imm2  přejít do stavu 191
    con   přejít do stavu 192


State 148

   50 spec5: rim ',' . rem
   51      | rim ',' . rem ':' LLREG
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    rem   přejít do stavu 193
    reg   přejít do stavu 48
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 149

   52 spec6: rim ',' . rem
   53      | rim ',' . rem ':' LSREG
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    rem   přejít do stavu 194
    reg   přejít do stavu 48
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 150

   54 spec7: rim ',' .  [';']
   56      | rim ',' . rem
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    $výchozí  reduce using rule 54 (spec7)

    rem   přejít do stavu 195
    reg   přejít do stavu 48
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 151

   57 spec8: mem ',' . imm
   58      | mem ',' . con ',' imm
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    imm  přejít do stavu 196
    con  přejít do stavu 197


State 152

   59 spec9: reg ',' . rem ',' con
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    rem   přejít do stavu 198
    reg   přejít do stavu 48
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 153

   60 spec10: imm ',' . rem ',' reg
   63 rem: . reg
   64    | . mem
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '~'     posunout a přejít do stavu 45

    rem   přejít do stavu 199
    reg   přejít do stavu 48
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 154

   61 spec11: rim ',' . rim
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    rem   přejít do stavu 56
    rim   přejít do stavu 200
    reg   přejít do stavu 48
    imm   přejít do stavu 58
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 155

   62 spec12: rim ',' . rim
   63 rem: . reg
   64    | . mem
   72 rim: . rem
   73    | . imm
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST
   95 mem: . omem
   96    | . nmem
   97 omem: . con
   98     | . con '(' LLREG ')'
   99     | . con '(' LSP ')'
  100     | . con '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  102     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  103     | . '(' LLREG ')'
  104     | . '(' LSP ')'
  105     | . con '(' LSREG ')'
  106     | . '(' LLREG '*' con ')'
  107     | . '(' LLREG ')' '(' LLREG '*' con ')'
  108 nmem: . nam
  109     | . nam '(' LLREG '*' con ')'
  110 nam: . LNAME offset '(' pointer ')'
  111    | . LNAME '<' '>' offset '(' LSB ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LBREG   posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 36
    LSREG   posunout a přejít do stavu 37
    LFREG   posunout a přejít do stavu 38
    LXREG   posunout a přejít do stavu 39
    LSP     posunout a přejít do stavu 40
    LNAME   posunout a přejít do stavu 41
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 44
    '$'     posunout a přejít do stavu 54
    '~'     posunout a přejít do stavu 45

    rem   přejít do stavu 56
    rim   přejít do stavu 201
    reg   přejít do stavu 48
    imm   přejít do stavu 58
    mem   přejít do stavu 49
    omem  přejít do stavu 50
    nmem  přejít do stavu 51
    nam   přejít do stavu 52
    con   přejít do stavu 53


State 156

    5 line: . LLAB ':' $@2 line
    7     | . LNAME ':' $@3 line
    7     | LNAME ':' $@3 . line
    8     | . ';'
    9     | . inst ';'
   10     | . error ';'
   11 inst: . LNAME '=' expr
   12     | . LVAR '=' expr
   13     | . LTYPE0 nonnon
   14     | . LTYPE1 nonrem
   15     | . LTYPE2 rimnon
   16     | . LTYPE3 rimrem
   17     | . LTYPE4 remrim
   18     | . LTYPER nonrel
   19     | . LTYPED spec1
   20     | . LTYPET spec2
   21     | . LTYPEC spec3
   22     | . LTYPEN spec4
   23     | . LTYPES spec5
   24     | . LTYPEM spec6
   25     | . LTYPEI spec7
   26     | . LTYPEG spec8
   27     | . LTYPEXC spec9
   28     | . LTYPEX spec10
   29     | . LTYPEPC spec11
   30     | . LTYPEF spec12

    error    posunout a přejít do stavu 4
    LTYPE0   posunout a přejít do stavu 5
    LTYPE1   posunout a přejít do stavu 6
    LTYPE2   posunout a přejít do stavu 7
    LTYPE3   posunout a přejít do stavu 8
    LTYPE4   posunout a přejít do stavu 9
    LTYPEC   posunout a přejít do stavu 10
    LTYPED   posunout a přejít do stavu 11
    LTYPEN   posunout a přejít do stavu 12
    LTYPER   posunout a přejít do stavu 13
    LTYPET   posunout a přejít do stavu 14
    LTYPES   posunout a přejít do stavu 15
    LTYPEM   posunout a přejít do stavu 16
    LTYPEI   posunout a přejít do stavu 17
    LTYPEG   posunout a přejít do stavu 18
    LTYPEXC  posunout a přejít do stavu 19
    LTYPEX   posunout a přejít do stavu 20
    LTYPEPC  posunout a přejít do stavu 21
    LTYPEF   posunout a přejít do stavu 22
    LNAME    posunout a přejít do stavu 23
    LLAB     posunout a přejít do stavu 24
    LVAR     posunout a přejít do stavu 25
    ';'      posunout a přejít do stavu 26

    line  přejít do stavu 202
    inst  přejít do stavu 28


State 157

   11 inst: LNAME '=' expr .  [';']
  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    '|'  posunout a přejít do stavu 167
    '^'  posunout a přejít do stavu 168
    '&'  posunout a přejít do stavu 169
    '<'  posunout a přejít do stavu 170
    '>'  posunout a přejít do stavu 171
    '+'  posunout a přejít do stavu 172
    '-'  posunout a přejít do stavu 173
    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176

    $výchozí  reduce using rule 11 (inst)


State 158

    5 line: . LLAB ':' $@2 line
    5     | LLAB ':' $@2 . line
    7     | . LNAME ':' $@3 line
    8     | . ';'
    9     | . inst ';'
   10     | . error ';'
   11 inst: . LNAME '=' expr
   12     | . LVAR '=' expr
   13     | . LTYPE0 nonnon
   14     | . LTYPE1 nonrem
   15     | . LTYPE2 rimnon
   16     | . LTYPE3 rimrem
   17     | . LTYPE4 remrim
   18     | . LTYPER nonrel
   19     | . LTYPED spec1
   20     | . LTYPET spec2
   21     | . LTYPEC spec3
   22     | . LTYPEN spec4
   23     | . LTYPES spec5
   24     | . LTYPEM spec6
   25     | . LTYPEI spec7
   26     | . LTYPEG spec8
   27     | . LTYPEXC spec9
   28     | . LTYPEX spec10
   29     | . LTYPEPC spec11
   30     | . LTYPEF spec12

    error    posunout a přejít do stavu 4
    LTYPE0   posunout a přejít do stavu 5
    LTYPE1   posunout a přejít do stavu 6
    LTYPE2   posunout a přejít do stavu 7
    LTYPE3   posunout a přejít do stavu 8
    LTYPE4   posunout a přejít do stavu 9
    LTYPEC   posunout a přejít do stavu 10
    LTYPED   posunout a přejít do stavu 11
    LTYPEN   posunout a přejít do stavu 12
    LTYPER   posunout a přejít do stavu 13
    LTYPET   posunout a přejít do stavu 14
    LTYPES   posunout a přejít do stavu 15
    LTYPEM   posunout a přejít do stavu 16
    LTYPEI   posunout a přejít do stavu 17
    LTYPEG   posunout a přejít do stavu 18
    LTYPEXC  posunout a přejít do stavu 19
    LTYPEX   posunout a přejít do stavu 20
    LTYPEPC  posunout a přejít do stavu 21
    LTYPEF   posunout a přejít do stavu 22
    LNAME    posunout a přejít do stavu 23
    LLAB     posunout a přejít do stavu 24
    LVAR     posunout a přejít do stavu 25
    ';'      posunout a přejít do stavu 26

    line  přejít do stavu 203
    inst  přejít do stavu 28


State 159

   12 inst: LVAR '=' expr .  [';']
  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    '|'  posunout a přejít do stavu 167
    '^'  posunout a přejít do stavu 168
    '&'  posunout a přejít do stavu 169
    '<'  posunout a přejít do stavu 170
    '>'  posunout a přejít do stavu 171
    '+'  posunout a přejít do stavu 172
    '-'  posunout a přejít do stavu 173
    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176

    $výchozí  reduce using rule 12 (inst)


State 160

  111 nam: LNAME '<' '>' . offset '(' LSB ')'
  112 offset: . %empty  ['(']
  113       | . '+' con
  114       | . '-' con

    '+'  posunout a přejít do stavu 114
    '-'  posunout a přejít do stavu 115

    $výchozí  reduce using rule 112 (offset)

    offset  přejít do stavu 204


State 161

  113 offset: '+' con .

    $výchozí  reduce using rule 113 (offset)


State 162

  114 offset: '-' con .

    $výchozí  reduce using rule 114 (offset)


State 163

  110 nam: LNAME offset '(' . pointer ')'
  115 pointer: . LSB
  116        | . LSP
  117        | . LFP

    LFP  posunout a přejít do stavu 205
    LSB  posunout a přejít do stavu 206
    LSP  posunout a přejít do stavu 207

    pointer  přejít do stavu 208


State 164

  106 omem: '(' LLREG '*' . con ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 209


State 165

  103 omem: '(' LLREG ')' .  [':', ';', ',']
  107     | '(' LLREG ')' . '(' LLREG '*' con ')'

    '('  posunout a přejít do stavu 210

    $výchozí  reduce using rule 103 (omem)


State 166

  104 omem: '(' LSP ')' .

    $výchozí  reduce using rule 104 (omem)


State 167

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr
  134     | expr '|' . expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 211


State 168

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  133     | expr '^' . expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 212


State 169

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  132     | expr '&' . expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 213


State 170

  130 expr: expr '<' . '<' expr

    '<'  posunout a přejít do stavu 214


State 171

  131 expr: expr '>' . '>' expr

    '>'  posunout a přejít do stavu 215


State 172

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  125     | expr '+' . expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 216


State 173

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  126     | expr '-' . expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 217


State 174

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  127     | expr '*' . expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 218


State 175

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  128     | expr '/' . expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 219


State 176

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  129     | expr '%' . expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 220


State 177

  123 con: '(' expr ')' .

    $výchozí  reduce using rule 123 (con)


State 178

  109 nmem: nam '(' LLREG . '*' con ')'

    '*'  posunout a přejít do stavu 221


State 179

   98 omem: con '(' LLREG . ')'
  100     | con '(' LLREG . '*' con ')'
  101     | con '(' LLREG . ')' '(' LLREG '*' con ')'
  102     | con '(' LLREG . ')' '(' LSREG '*' con ')'

    '*'  posunout a přejít do stavu 222
    ')'  posunout a přejít do stavu 223


State 180

  105 omem: con '(' LSREG . ')'

    ')'  posunout a přejít do stavu 224


State 181

   99 omem: con '(' LSP . ')'

    ')'  posunout a přejít do stavu 225


State 182

   89 imm: '$' '-' LFCONST .

    $výchozí  reduce using rule 89 (imm)


State 183

   88 imm: '$' '(' '-' . LFCONST ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  120    | '-' . con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'      posunout a přejít do stavu 32
    '-'      posunout a přejít do stavu 33
    LCONST   posunout a přejít do stavu 34
    LFCONST  posunout a přejít do stavu 226
    LVAR     posunout a přejít do stavu 42
    '('      posunout a přejít do stavu 83
    '~'      posunout a přejít do stavu 45

    con  přejít do stavu 112


State 184

   87 imm: '$' '(' LFCONST . ')'

    ')'  posunout a přejít do stavu 227


State 185

   33 rimrem: rim ',' rem .

    $výchozí  reduce using rule 33 (rimrem)


State 186

   34 remrim: rem ',' rim .

    $výchozí  reduce using rule 34 (remrim)


State 187

   74 rel: con '(' LPC . ')'

    ')'  posunout a přejít do stavu 228


State 188

   42 spec1: nam '/' con . ',' imm

    ','  posunout a přejít do stavu 229


State 189

   41 nonrel: imm ',' rel .

    $výchozí  reduce using rule 41 (nonrel)


State 190

   90 imm2: '$' . con2
   91 con2: . LCONST
   92     | . '-' LCONST
   93     | . LCONST '-' LCONST
   94     | . '-' LCONST '-' LCONST

    '-'     posunout a přejít do stavu 230
    LCONST  posunout a přejít do stavu 231

    con2  přejít do stavu 232


State 191

   43 spec2: mem ',' imm2 .

    $výchozí  reduce using rule 43 (spec2)


State 192

   44 spec2: mem ',' con . ',' imm2

    ','  posunout a přejít do stavu 233


State 193

   50 spec5: rim ',' rem .  [';']
   51      | rim ',' rem . ':' LLREG

    ':'  posunout a přejít do stavu 234

    $výchozí  reduce using rule 50 (spec5)


State 194

   52 spec6: rim ',' rem .  [';']
   53      | rim ',' rem . ':' LSREG

    ':'  posunout a přejít do stavu 235

    $výchozí  reduce using rule 52 (spec6)


State 195

   56 spec7: rim ',' rem .

    $výchozí  reduce using rule 56 (spec7)


State 196

   57 spec8: mem ',' imm .

    $výchozí  reduce using rule 57 (spec8)


State 197

   58 spec8: mem ',' con . ',' imm

    ','  posunout a přejít do stavu 236


State 198

   59 spec9: reg ',' rem . ',' con

    ','  posunout a přejít do stavu 237


State 199

   60 spec10: imm ',' rem . ',' reg

    ','  posunout a přejít do stavu 238


State 200

   61 spec11: rim ',' rim .

    $výchozí  reduce using rule 61 (spec11)


State 201

   62 spec12: rim ',' rim .

    $výchozí  reduce using rule 62 (spec12)


State 202

    7 line: LNAME ':' $@3 line .

    $výchozí  reduce using rule 7 (line)


State 203

    5 line: LLAB ':' $@2 line .

    $výchozí  reduce using rule 5 (line)


State 204

  111 nam: LNAME '<' '>' offset . '(' LSB ')'

    '('  posunout a přejít do stavu 239


State 205

  117 pointer: LFP .

    $výchozí  reduce using rule 117 (pointer)


State 206

  115 pointer: LSB .

    $výchozí  reduce using rule 115 (pointer)


State 207

  116 pointer: LSP .

    $výchozí  reduce using rule 116 (pointer)


State 208

  110 nam: LNAME offset '(' pointer . ')'

    ')'  posunout a přejít do stavu 240


State 209

  106 omem: '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 241


State 210

  107 omem: '(' LLREG ')' '(' . LLREG '*' con ')'

    LLREG  posunout a přejít do stavu 242


State 211

  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr
  134     | expr '|' expr .  ['|', ';', ')']

    '^'  posunout a přejít do stavu 168
    '&'  posunout a přejít do stavu 169
    '<'  posunout a přejít do stavu 170
    '>'  posunout a přejít do stavu 171
    '+'  posunout a přejít do stavu 172
    '-'  posunout a přejít do stavu 173
    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176

    $výchozí  reduce using rule 134 (expr)

    Conflict between rule 134 and token '|' resolved as reduce (%left '|').
    Conflict between rule 134 and token '^' resolved as shift ('|' < '^').
    Conflict between rule 134 and token '&' resolved as shift ('|' < '&').
    Conflict between rule 134 and token '<' resolved as shift ('|' < '<').
    Conflict between rule 134 and token '>' resolved as shift ('|' < '>').
    Conflict between rule 134 and token '+' resolved as shift ('|' < '+').
    Conflict between rule 134 and token '-' resolved as shift ('|' < '-').
    Conflict between rule 134 and token '*' resolved as shift ('|' < '*').
    Conflict between rule 134 and token '/' resolved as shift ('|' < '/').
    Conflict between rule 134 and token '%' resolved as shift ('|' < '%').


State 212

  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  133     | expr '^' expr .  ['|', '^', ';', ')']
  134     | expr . '|' expr

    '&'  posunout a přejít do stavu 169
    '<'  posunout a přejít do stavu 170
    '>'  posunout a přejít do stavu 171
    '+'  posunout a přejít do stavu 172
    '-'  posunout a přejít do stavu 173
    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176

    $výchozí  reduce using rule 133 (expr)

    Conflict between rule 133 and token '|' resolved as reduce ('|' < '^').
    Conflict between rule 133 and token '^' resolved as reduce (%left '^').
    Conflict between rule 133 and token '&' resolved as shift ('^' < '&').
    Conflict between rule 133 and token '<' resolved as shift ('^' < '<').
    Conflict between rule 133 and token '>' resolved as shift ('^' < '>').
    Conflict between rule 133 and token '+' resolved as shift ('^' < '+').
    Conflict between rule 133 and token '-' resolved as shift ('^' < '-').
    Conflict between rule 133 and token '*' resolved as shift ('^' < '*').
    Conflict between rule 133 and token '/' resolved as shift ('^' < '/').
    Conflict between rule 133 and token '%' resolved as shift ('^' < '%').


State 213

  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  132     | expr '&' expr .  ['|', '^', '&', ';', ')']
  133     | expr . '^' expr
  134     | expr . '|' expr

    '<'  posunout a přejít do stavu 170
    '>'  posunout a přejít do stavu 171
    '+'  posunout a přejít do stavu 172
    '-'  posunout a přejít do stavu 173
    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176

    $výchozí  reduce using rule 132 (expr)

    Conflict between rule 132 and token '|' resolved as reduce ('|' < '&').
    Conflict between rule 132 and token '^' resolved as reduce ('^' < '&').
    Conflict between rule 132 and token '&' resolved as reduce (%left '&').
    Conflict between rule 132 and token '<' resolved as shift ('&' < '<').
    Conflict between rule 132 and token '>' resolved as shift ('&' < '>').
    Conflict between rule 132 and token '+' resolved as shift ('&' < '+').
    Conflict between rule 132 and token '-' resolved as shift ('&' < '-').
    Conflict between rule 132 and token '*' resolved as shift ('&' < '*').
    Conflict between rule 132 and token '/' resolved as shift ('&' < '/').
    Conflict between rule 132 and token '%' resolved as shift ('&' < '%').


State 214

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  130     | expr '<' '<' . expr
  131     | . expr '>' '>' expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 243


State 215

  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'
  124 expr: . con
  125     | . expr '+' expr
  126     | . expr '-' expr
  127     | . expr '*' expr
  128     | . expr '/' expr
  129     | . expr '%' expr
  130     | . expr '<' '<' expr
  131     | . expr '>' '>' expr
  131     | expr '>' '>' . expr
  132     | . expr '&' expr
  133     | . expr '^' expr
  134     | . expr '|' expr

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con   přejít do stavu 120
    expr  přejít do stavu 244


State 216

  125 expr: expr . '+' expr
  125     | expr '+' expr .  ['|', '^', '&', '<', '>', '+', '-', ';', ')']
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176

    $výchozí  reduce using rule 125 (expr)

    Conflict between rule 125 and token '|' resolved as reduce ('|' < '+').
    Conflict between rule 125 and token '^' resolved as reduce ('^' < '+').
    Conflict between rule 125 and token '&' resolved as reduce ('&' < '+').
    Conflict between rule 125 and token '<' resolved as reduce ('<' < '+').
    Conflict between rule 125 and token '>' resolved as reduce ('>' < '+').
    Conflict between rule 125 and token '+' resolved as reduce (%left '+').
    Conflict between rule 125 and token '-' resolved as reduce (%left '-').
    Conflict between rule 125 and token '*' resolved as shift ('+' < '*').
    Conflict between rule 125 and token '/' resolved as shift ('+' < '/').
    Conflict between rule 125 and token '%' resolved as shift ('+' < '%').


State 217

  125 expr: expr . '+' expr
  126     | expr . '-' expr
  126     | expr '-' expr .  ['|', '^', '&', '<', '>', '+', '-', ';', ')']
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176

    $výchozí  reduce using rule 126 (expr)

    Conflict between rule 126 and token '|' resolved as reduce ('|' < '-').
    Conflict between rule 126 and token '^' resolved as reduce ('^' < '-').
    Conflict between rule 126 and token '&' resolved as reduce ('&' < '-').
    Conflict between rule 126 and token '<' resolved as reduce ('<' < '-').
    Conflict between rule 126 and token '>' resolved as reduce ('>' < '-').
    Conflict between rule 126 and token '+' resolved as reduce (%left '+').
    Conflict between rule 126 and token '-' resolved as reduce (%left '-').
    Conflict between rule 126 and token '*' resolved as shift ('-' < '*').
    Conflict between rule 126 and token '/' resolved as shift ('-' < '/').
    Conflict between rule 126 and token '%' resolved as shift ('-' < '%').


State 218

  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  127     | expr '*' expr .  ['|', '^', '&', '<', '>', '+', '-', '*', '/', '%', ';', ')']
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    $výchozí  reduce using rule 127 (expr)

    Conflict between rule 127 and token '|' resolved as reduce ('|' < '*').
    Conflict between rule 127 and token '^' resolved as reduce ('^' < '*').
    Conflict between rule 127 and token '&' resolved as reduce ('&' < '*').
    Conflict between rule 127 and token '<' resolved as reduce ('<' < '*').
    Conflict between rule 127 and token '>' resolved as reduce ('>' < '*').
    Conflict between rule 127 and token '+' resolved as reduce ('+' < '*').
    Conflict between rule 127 and token '-' resolved as reduce ('-' < '*').
    Conflict between rule 127 and token '*' resolved as reduce (%left '*').
    Conflict between rule 127 and token '/' resolved as reduce (%left '/').
    Conflict between rule 127 and token '%' resolved as reduce (%left '%').


State 219

  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  128     | expr '/' expr .  ['|', '^', '&', '<', '>', '+', '-', '*', '/', '%', ';', ')']
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    $výchozí  reduce using rule 128 (expr)

    Conflict between rule 128 and token '|' resolved as reduce ('|' < '/').
    Conflict between rule 128 and token '^' resolved as reduce ('^' < '/').
    Conflict between rule 128 and token '&' resolved as reduce ('&' < '/').
    Conflict between rule 128 and token '<' resolved as reduce ('<' < '/').
    Conflict between rule 128 and token '>' resolved as reduce ('>' < '/').
    Conflict between rule 128 and token '+' resolved as reduce ('+' < '/').
    Conflict between rule 128 and token '-' resolved as reduce ('-' < '/').
    Conflict between rule 128 and token '*' resolved as reduce (%left '*').
    Conflict between rule 128 and token '/' resolved as reduce (%left '/').
    Conflict between rule 128 and token '%' resolved as reduce (%left '%').


State 220

  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  129     | expr '%' expr .  ['|', '^', '&', '<', '>', '+', '-', '*', '/', '%', ';', ')']
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    $výchozí  reduce using rule 129 (expr)

    Conflict between rule 129 and token '|' resolved as reduce ('|' < '%').
    Conflict between rule 129 and token '^' resolved as reduce ('^' < '%').
    Conflict between rule 129 and token '&' resolved as reduce ('&' < '%').
    Conflict between rule 129 and token '<' resolved as reduce ('<' < '%').
    Conflict between rule 129 and token '>' resolved as reduce ('>' < '%').
    Conflict between rule 129 and token '+' resolved as reduce ('+' < '%').
    Conflict between rule 129 and token '-' resolved as reduce ('-' < '%').
    Conflict between rule 129 and token '*' resolved as reduce (%left '*').
    Conflict between rule 129 and token '/' resolved as reduce (%left '/').
    Conflict between rule 129 and token '%' resolved as reduce (%left '%').


State 221

  109 nmem: nam '(' LLREG '*' . con ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 245


State 222

  100 omem: con '(' LLREG '*' . con ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 246


State 223

   98 omem: con '(' LLREG ')' .  [':', ';', ',']
  101     | con '(' LLREG ')' . '(' LLREG '*' con ')'
  102     | con '(' LLREG ')' . '(' LSREG '*' con ')'

    '('  posunout a přejít do stavu 247

    $výchozí  reduce using rule 98 (omem)


State 224

  105 omem: con '(' LSREG ')' .

    $výchozí  reduce using rule 105 (omem)


State 225

   99 omem: con '(' LSP ')' .

    $výchozí  reduce using rule 99 (omem)


State 226

   88 imm: '$' '(' '-' LFCONST . ')'

    ')'  posunout a přejít do stavu 248


State 227

   87 imm: '$' '(' LFCONST ')' .

    $výchozí  reduce using rule 87 (imm)


State 228

   74 rel: con '(' LPC ')' .

    $výchozí  reduce using rule 74 (rel)


State 229

   42 spec1: nam '/' con ',' . imm
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST

    '$'  posunout a přejít do stavu 54

    imm  přejít do stavu 249


State 230

   92 con2: '-' . LCONST
   94     | '-' . LCONST '-' LCONST

    LCONST  posunout a přejít do stavu 250


State 231

   91 con2: LCONST .  [';']
   93     | LCONST . '-' LCONST

    '-'  posunout a přejít do stavu 251

    $výchozí  reduce using rule 91 (con2)


State 232

   90 imm2: '$' con2 .

    $výchozí  reduce using rule 90 (imm2)


State 233

   44 spec2: mem ',' con ',' . imm2
   90 imm2: . '$' con2

    '$'  posunout a přejít do stavu 190

    imm2  přejít do stavu 252


State 234

   51 spec5: rim ',' rem ':' . LLREG

    LLREG  posunout a přejít do stavu 253


State 235

   53 spec6: rim ',' rem ':' . LSREG

    LSREG  posunout a přejít do stavu 254


State 236

   58 spec8: mem ',' con ',' . imm
   83 imm: . '$' con
   84    | . '$' nam
   85    | . '$' LSCONST
   86    | . '$' LFCONST
   87    | . '$' '(' LFCONST ')'
   88    | . '$' '(' '-' LFCONST ')'
   89    | . '$' '-' LFCONST

    '$'  posunout a přejít do stavu 54

    imm  přejít do stavu 255


State 237

   59 spec9: reg ',' rem ',' . con
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 256


State 238

   60 spec10: imm ',' rem ',' . reg
   77 reg: . LBREG
   78    | . LFREG
   79    | . LLREG
   80    | . LXREG
   81    | . LSP
   82    | . LSREG

    LBREG  posunout a přejít do stavu 35
    LLREG  posunout a přejít do stavu 36
    LSREG  posunout a přejít do stavu 37
    LFREG  posunout a přejít do stavu 38
    LXREG  posunout a přejít do stavu 39
    LSP    posunout a přejít do stavu 40

    reg  přejít do stavu 257


State 239

  111 nam: LNAME '<' '>' offset '(' . LSB ')'

    LSB  posunout a přejít do stavu 258


State 240

  110 nam: LNAME offset '(' pointer ')' .

    $výchozí  reduce using rule 110 (nam)


State 241

  106 omem: '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 106 (omem)


State 242

  107 omem: '(' LLREG ')' '(' LLREG . '*' con ')'

    '*'  posunout a přejít do stavu 259


State 243

  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  130     | expr '<' '<' expr .  ['|', '^', '&', '<', '>', ';', ')']
  131     | expr . '>' '>' expr
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    '+'  posunout a přejít do stavu 172
    '-'  posunout a přejít do stavu 173
    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176

    $výchozí  reduce using rule 130 (expr)

    Conflict between rule 130 and token '|' resolved as reduce ('|' < '<').
    Conflict between rule 130 and token '^' resolved as reduce ('^' < '<').
    Conflict between rule 130 and token '&' resolved as reduce ('&' < '<').
    Conflict between rule 130 and token '<' resolved as reduce (%left '<').
    Conflict between rule 130 and token '>' resolved as reduce (%left '>').
    Conflict between rule 130 and token '+' resolved as shift ('<' < '+').
    Conflict between rule 130 and token '-' resolved as shift ('<' < '-').
    Conflict between rule 130 and token '*' resolved as shift ('<' < '*').
    Conflict between rule 130 and token '/' resolved as shift ('<' < '/').
    Conflict between rule 130 and token '%' resolved as shift ('<' < '%').


State 244

  125 expr: expr . '+' expr
  126     | expr . '-' expr
  127     | expr . '*' expr
  128     | expr . '/' expr
  129     | expr . '%' expr
  130     | expr . '<' '<' expr
  131     | expr . '>' '>' expr
  131     | expr '>' '>' expr .  ['|', '^', '&', '<', '>', ';', ')']
  132     | expr . '&' expr
  133     | expr . '^' expr
  134     | expr . '|' expr

    '+'  posunout a přejít do stavu 172
    '-'  posunout a přejít do stavu 173
    '*'  posunout a přejít do stavu 174
    '/'  posunout a přejít do stavu 175
    '%'  posunout a přejít do stavu 176

    $výchozí  reduce using rule 131 (expr)

    Conflict between rule 131 and token '|' resolved as reduce ('|' < '>').
    Conflict between rule 131 and token '^' resolved as reduce ('^' < '>').
    Conflict between rule 131 and token '&' resolved as reduce ('&' < '>').
    Conflict between rule 131 and token '<' resolved as reduce (%left '<').
    Conflict between rule 131 and token '>' resolved as reduce (%left '>').
    Conflict between rule 131 and token '+' resolved as shift ('>' < '+').
    Conflict between rule 131 and token '-' resolved as shift ('>' < '-').
    Conflict between rule 131 and token '*' resolved as shift ('>' < '*').
    Conflict between rule 131 and token '/' resolved as shift ('>' < '/').
    Conflict between rule 131 and token '%' resolved as shift ('>' < '%').


State 245

  109 nmem: nam '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 260


State 246

  100 omem: con '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 261


State 247

  101 omem: con '(' LLREG ')' '(' . LLREG '*' con ')'
  102     | con '(' LLREG ')' '(' . LSREG '*' con ')'

    LLREG  posunout a přejít do stavu 262
    LSREG  posunout a přejít do stavu 263


State 248

   88 imm: '$' '(' '-' LFCONST ')' .

    $výchozí  reduce using rule 88 (imm)


State 249

   42 spec1: nam '/' con ',' imm .

    $výchozí  reduce using rule 42 (spec1)


State 250

   92 con2: '-' LCONST .  [';']
   94     | '-' LCONST . '-' LCONST

    '-'  posunout a přejít do stavu 264

    $výchozí  reduce using rule 92 (con2)


State 251

   93 con2: LCONST '-' . LCONST

    LCONST  posunout a přejít do stavu 265


State 252

   44 spec2: mem ',' con ',' imm2 .

    $výchozí  reduce using rule 44 (spec2)


State 253

   51 spec5: rim ',' rem ':' LLREG .

    $výchozí  reduce using rule 51 (spec5)


State 254

   53 spec6: rim ',' rem ':' LSREG .

    $výchozí  reduce using rule 53 (spec6)


State 255

   58 spec8: mem ',' con ',' imm .

    $výchozí  reduce using rule 58 (spec8)


State 256

   59 spec9: reg ',' rem ',' con .

    $výchozí  reduce using rule 59 (spec9)


State 257

   60 spec10: imm ',' rem ',' reg .

    $výchozí  reduce using rule 60 (spec10)


State 258

  111 nam: LNAME '<' '>' offset '(' LSB . ')'

    ')'  posunout a přejít do stavu 266


State 259

  107 omem: '(' LLREG ')' '(' LLREG '*' . con ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 267


State 260

  109 nmem: nam '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 109 (nmem)


State 261

  100 omem: con '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 100 (omem)


State 262

  101 omem: con '(' LLREG ')' '(' LLREG . '*' con ')'

    '*'  posunout a přejít do stavu 268


State 263

  102 omem: con '(' LLREG ')' '(' LSREG . '*' con ')'

    '*'  posunout a přejít do stavu 269


State 264

   94 con2: '-' LCONST '-' . LCONST

    LCONST  posunout a přejít do stavu 270


State 265

   93 con2: LCONST '-' LCONST .

    $výchozí  reduce using rule 93 (con2)


State 266

  111 nam: LNAME '<' '>' offset '(' LSB ')' .

    $výchozí  reduce using rule 111 (nam)


State 267

  107 omem: '(' LLREG ')' '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 271


State 268

  101 omem: con '(' LLREG ')' '(' LLREG '*' . con ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 272


State 269

  102 omem: con '(' LLREG ')' '(' LSREG '*' . con ')'
  118 con: . LCONST
  119    | . LVAR
  120    | . '-' con
  121    | . '+' con
  122    | . '~' con
  123    | . '(' expr ')'

    '+'     posunout a přejít do stavu 32
    '-'     posunout a přejít do stavu 33
    LCONST  posunout a přejít do stavu 34
    LVAR    posunout a přejít do stavu 42
    '('     posunout a přejít do stavu 83
    '~'     posunout a přejít do stavu 45

    con  přejít do stavu 273


State 270

   94 con2: '-' LCONST '-' LCONST .

    $výchozí  reduce using rule 94 (con2)


State 271

  107 omem: '(' LLREG ')' '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 107 (omem)


State 272

  101 omem: con '(' LLREG ')' '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 274


State 273

  102 omem: con '(' LLREG ')' '(' LSREG '*' con . ')'

    ')'  posunout a přejít do stavu 275


State 274

  101 omem: con '(' LLREG ')' '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 101 (omem)


State 275

  102 omem: con '(' LLREG ')' '(' LSREG '*' con ')' .

    $výchozí  reduce using rule 102 (omem)
