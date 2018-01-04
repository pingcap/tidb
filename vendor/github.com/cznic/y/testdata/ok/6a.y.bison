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
   26     | LTYPEXC spec8
   27     | LTYPEX spec9
   28     | LTYPERT spec10
   29     | LTYPEG spec11
   30     | LTYPEPC spec12
   31     | LTYPEF spec13

   32 nonnon: %empty
   33       | ','

   34 rimrem: rim ',' rem

   35 remrim: rem ',' rim

   36 rimnon: rim ','
   37       | rim

   38 nonrem: ',' rem
   39       | rem

   40 nonrel: ',' rel
   41       | rel
   42       | imm ',' rel

   43 spec1: nam '/' con ',' imm

   44 spec2: mem ',' imm2
   45      | mem ',' con ',' imm2

   46 spec3: ',' rom
   47      | rom

   48 spec4: nonnon
   49      | nonrem

   50 spec5: rim ',' rem
   51      | rim ',' rem ':' LLREG

   52 spec6: rim ',' rem
   53      | rim ',' rem ':' LSREG

   54 spec7: rim ','
   55      | rim
   56      | rim ',' rem

   57 spec8: reg ',' rem ',' con

   58 spec9: imm ',' rem ',' reg

   59 spec10: %empty
   60       | imm

   61 spec11: mem ',' imm
   62       | mem ',' con ',' imm

   63 spec12: rim ',' rim

   64 spec13: rim ',' rim

   65 rem: reg
   66    | mem

   67 rom: rel
   68    | nmem
   69    | '*' reg
   70    | '*' omem
   71    | reg
   72    | omem

   73 rim: rem
   74    | imm

   75 rel: con '(' LPC ')'
   76    | LNAME offset
   77    | LLAB offset

   78 reg: LBREG
   79    | LFREG
   80    | LLREG
   81    | LMREG
   82    | LSP
   83    | LSREG
   84    | LXREG

   85 imm2: '$' con2

   86 imm: '$' con
   87    | '$' nam
   88    | '$' LSCONST
   89    | '$' LFCONST
   90    | '$' '(' LFCONST ')'
   91    | '$' '(' '-' LFCONST ')'
   92    | '$' '-' LFCONST

   93 mem: omem
   94    | nmem

   95 omem: con
   96     | con '(' LLREG ')'
   97     | con '(' LSP ')'
   98     | con '(' LSREG ')'
   99     | con '(' LLREG '*' con ')'
  100     | con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | '(' LLREG ')'
  103     | '(' LSP ')'
  104     | '(' LLREG '*' con ')'
  105     | '(' LLREG ')' '(' LLREG '*' con ')'

  106 nmem: nam
  107     | nam '(' LLREG '*' con ')'

  108 nam: LNAME offset '(' pointer ')'
  109    | LNAME '<' '>' offset '(' LSB ')'

  110 offset: %empty
  111       | '+' con
  112       | '-' con

  113 pointer: LSB
  114        | LSP
  115        | LFP

  116 con: LCONST
  117    | LVAR
  118    | '-' con
  119    | '+' con
  120    | '~' con
  121    | '(' expr ')'

  122 con2: LCONST
  123     | '-' LCONST
  124     | LCONST '-' LCONST
  125     | '-' LCONST '-' LCONST

  126 expr: con
  127     | expr '+' expr
  128     | expr '-' expr
  129     | expr '*' expr
  130     | expr '/' expr
  131     | expr '%' expr
  132     | expr '<' '<' expr
  133     | expr '>' '>' expr
  134     | expr '&' expr
  135     | expr '^' expr
  136     | expr '|' expr


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'$' (36) 85 86 87 88 89 90 91 92
'%' (37) 131
'&' (38) 134
'(' (40) 75 90 91 96 97 98 99 100 101 102 103 104 105 107 108 109 121
')' (41) 75 90 91 96 97 98 99 100 101 102 103 104 105 107 108 109 121
'*' (42) 69 70 99 100 101 104 105 107 129
'+' (43) 111 119 127
',' (44) 33 34 35 36 38 40 42 43 44 45 46 50 51 52 53 54 56 57 58 61
    62 63 64
'-' (45) 91 92 112 118 123 124 125 128
'/' (47) 43 130
':' (58) 5 7 51 53
';' (59) 8 9 10
'<' (60) 109 132
'=' (61) 11 12
'>' (62) 109 133
'^' (94) 135
'|' (124) 136
'~' (126) 120
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
LTYPEG (268) 29
LTYPEPC (269) 30
LTYPES (270) 23
LTYPEM (271) 24
LTYPEI (272) 25
LTYPEXC (273) 26
LTYPEX (274) 27
LTYPERT (275) 28
LTYPEF (276) 31
LCONST (277) 116 122 123 124 125
LFP (278) 115
LPC (279) 75
LSB (280) 109 113
LBREG (281) 78
LLREG (282) 51 80 96 99 100 101 102 104 105 107
LSREG (283) 53 83 98 101
LFREG (284) 79
LMREG (285) 81
LXREG (286) 84
LFCONST (287) 89 90 91 92
LSCONST (288) 88
LSP (289) 82 97 103 114
LNAME (290) 7 11 76 108 109
LLAB (291) 5 77
LVAR (292) 12 117


Neterminály s pravidly, ve kterých se objevují

$accept (56)
    vlevo: 0
prog (57)
    vlevo: 1 3, vpravo: 0 3
$@1 (58)
    vlevo: 2, vpravo: 3
line (59)
    vlevo: 5 7 8 9 10, vpravo: 3 5 7
$@2 (60)
    vlevo: 4, vpravo: 5
$@3 (61)
    vlevo: 6, vpravo: 7
inst (62)
    vlevo: 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29
    30 31, vpravo: 9
nonnon (63)
    vlevo: 32 33, vpravo: 13 48
rimrem (64)
    vlevo: 34, vpravo: 16
remrim (65)
    vlevo: 35, vpravo: 17
rimnon (66)
    vlevo: 36 37, vpravo: 15
nonrem (67)
    vlevo: 38 39, vpravo: 14 49
nonrel (68)
    vlevo: 40 41 42, vpravo: 18
spec1 (69)
    vlevo: 43, vpravo: 19
spec2 (70)
    vlevo: 44 45, vpravo: 20
spec3 (71)
    vlevo: 46 47, vpravo: 21
spec4 (72)
    vlevo: 48 49, vpravo: 22
spec5 (73)
    vlevo: 50 51, vpravo: 23
spec6 (74)
    vlevo: 52 53, vpravo: 24
spec7 (75)
    vlevo: 54 55 56, vpravo: 25
spec8 (76)
    vlevo: 57, vpravo: 26
spec9 (77)
    vlevo: 58, vpravo: 27
spec10 (78)
    vlevo: 59 60, vpravo: 28
spec11 (79)
    vlevo: 61 62, vpravo: 29
spec12 (80)
    vlevo: 63, vpravo: 30
spec13 (81)
    vlevo: 64, vpravo: 31
rem (82)
    vlevo: 65 66, vpravo: 34 35 38 39 50 51 52 53 56 57 58 73
rom (83)
    vlevo: 67 68 69 70 71 72, vpravo: 46 47
rim (84)
    vlevo: 73 74, vpravo: 34 35 36 37 50 51 52 53 54 55 56 63 64
rel (85)
    vlevo: 75 76 77, vpravo: 40 41 42 67
reg (86)
    vlevo: 78 79 80 81 82 83 84, vpravo: 57 58 65 69 71
imm2 (87)
    vlevo: 85, vpravo: 44 45
imm (88)
    vlevo: 86 87 88 89 90 91 92, vpravo: 42 43 58 60 61 62 74
mem (89)
    vlevo: 93 94, vpravo: 44 45 61 62 66
omem (90)
    vlevo: 95 96 97 98 99 100 101 102 103 104 105, vpravo: 70 72 93
nmem (91)
    vlevo: 106 107, vpravo: 68 94
nam (92)
    vlevo: 108 109, vpravo: 43 87 106 107
offset (93)
    vlevo: 110 111 112, vpravo: 76 77 108 109
pointer (94)
    vlevo: 113 114 115, vpravo: 108
con (95)
    vlevo: 116 117 118 119 120 121, vpravo: 43 45 57 62 75 86 95 96
    97 98 99 100 101 104 105 107 111 112 118 119 120 126
con2 (96)
    vlevo: 122 123 124 125, vpravo: 85
expr (97)
    vlevo: 126 127 128 129 130 131 132 133 134 135 136, vpravo: 11
    12 121 127 128 129 130 131 132 133 134 135 136


State 0

    0 $accept: . prog $end
    1 prog: . %empty
    3     | . prog $@1 line

    $výchozí  reduce using rule 1 (prog)

    prog  přejít do stavu 1


State 1

    0 $accept: prog . $end
    2 $@1: . %empty  [error, LTYPE0, LTYPE1, LTYPE2, LTYPE3, LTYPE4, LTYPEC, LTYPED, LTYPEN, LTYPER, LTYPET, LTYPEG, LTYPEPC, LTYPES, LTYPEM, LTYPEI, LTYPEXC, LTYPEX, LTYPERT, LTYPEF, LNAME, LLAB, LVAR, ';']
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
   26     | . LTYPEXC spec8
   27     | . LTYPEX spec9
   28     | . LTYPERT spec10
   29     | . LTYPEG spec11
   30     | . LTYPEPC spec12
   31     | . LTYPEF spec13

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
    LTYPEG   posunout a přejít do stavu 15
    LTYPEPC  posunout a přejít do stavu 16
    LTYPES   posunout a přejít do stavu 17
    LTYPEM   posunout a přejít do stavu 18
    LTYPEI   posunout a přejít do stavu 19
    LTYPEXC  posunout a přejít do stavu 20
    LTYPEX   posunout a přejít do stavu 21
    LTYPERT  posunout a přejít do stavu 22
    LTYPEF   posunout a přejít do stavu 23
    LNAME    posunout a přejít do stavu 24
    LLAB     posunout a přejít do stavu 25
    LVAR     posunout a přejít do stavu 26
    ';'      posunout a přejít do stavu 27

    line  přejít do stavu 28
    inst  přejít do stavu 29


State 4

   10 line: error . ';'

    ';'  posunout a přejít do stavu 30


State 5

   13 inst: LTYPE0 . nonnon
   32 nonnon: . %empty  [';']
   33       | . ','

    ','  posunout a přejít do stavu 31

    $výchozí  reduce using rule 32 (nonnon)

    nonnon  přejít do stavu 32


State 6

   14 inst: LTYPE1 . nonrem
   38 nonrem: . ',' rem
   39       | . rem
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    ','     posunout a přejít do stavu 45
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    nonrem  přejít do stavu 48
    rem     přejít do stavu 49
    reg     přejít do stavu 50
    mem     přejít do stavu 51
    omem    přejít do stavu 52
    nmem    přejít do stavu 53
    nam     přejít do stavu 54
    con     přejít do stavu 55


State 7

   15 inst: LTYPE2 . rimnon
   36 rimnon: . rim ','
   37       | . rim
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    rimnon  přejít do stavu 57
    rem     přejít do stavu 58
    rim     přejít do stavu 59
    reg     přejít do stavu 50
    imm     přejít do stavu 60
    mem     přejít do stavu 51
    omem    přejít do stavu 52
    nmem    přejít do stavu 53
    nam     přejít do stavu 54
    con     přejít do stavu 55


State 8

   16 inst: LTYPE3 . rimrem
   34 rimrem: . rim ',' rem
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    rimrem  přejít do stavu 61
    rem     přejít do stavu 58
    rim     přejít do stavu 62
    reg     přejít do stavu 50
    imm     přejít do stavu 60
    mem     přejít do stavu 51
    omem    přejít do stavu 52
    nmem    přejít do stavu 53
    nam     přejít do stavu 54
    con     přejít do stavu 55


State 9

   17 inst: LTYPE4 . remrim
   35 remrim: . rem ',' rim
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    remrim  přejít do stavu 63
    rem     přejít do stavu 64
    reg     přejít do stavu 50
    mem     přejít do stavu 51
    omem    přejít do stavu 52
    nmem    přejít do stavu 53
    nam     přejít do stavu 54
    con     přejít do stavu 55


State 10

   21 inst: LTYPEC . spec3
   46 spec3: . ',' rom
   47      | . rom
   67 rom: . rel
   68    | . nmem
   69    | . '*' reg
   70    | . '*' omem
   71    | . reg
   72    | . omem
   75 rel: . con '(' LPC ')'
   76    | . LNAME offset
   77    | . LLAB offset
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    '*'     posunout a přejít do stavu 65
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 66
    LLAB    posunout a přejít do stavu 67
    LVAR    posunout a přejít do stavu 44
    ','     posunout a přejít do stavu 68
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    spec3  přejít do stavu 69
    rom    přejít do stavu 70
    rel    přejít do stavu 71
    reg    přejít do stavu 72
    omem   přejít do stavu 73
    nmem   přejít do stavu 74
    nam    přejít do stavu 54
    con    přejít do stavu 75


State 11

   19 inst: LTYPED . spec1
   43 spec1: . nam '/' con ',' imm
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'

    LNAME  posunout a přejít do stavu 43

    spec1  přejít do stavu 76
    nam    přejít do stavu 77


State 12

   22 inst: LTYPEN . spec4
   32 nonnon: . %empty  [';']
   33       | . ','
   38 nonrem: . ',' rem
   39       | . rem
   48 spec4: . nonnon
   49      | . nonrem
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    ','     posunout a přejít do stavu 78
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    $výchozí  reduce using rule 32 (nonnon)

    nonnon  přejít do stavu 79
    nonrem  přejít do stavu 80
    spec4   přejít do stavu 81
    rem     přejít do stavu 49
    reg     přejít do stavu 50
    mem     přejít do stavu 51
    omem    přejít do stavu 52
    nmem    přejít do stavu 53
    nam     přejít do stavu 54
    con     přejít do stavu 55


State 13

   18 inst: LTYPER . nonrel
   40 nonrel: . ',' rel
   41       | . rel
   42       | . imm ',' rel
   75 rel: . con '(' LPC ')'
   76    | . LNAME offset
   77    | . LLAB offset
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LNAME   posunout a přejít do stavu 82
    LLAB    posunout a přejít do stavu 67
    LVAR    posunout a přejít do stavu 44
    ','     posunout a přejít do stavu 83
    '('     posunout a přejít do stavu 84
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    nonrel  přejít do stavu 85
    rel     přejít do stavu 86
    imm     přejít do stavu 87
    con     přejít do stavu 88


State 14

   20 inst: LTYPET . spec2
   44 spec2: . mem ',' imm2
   45      | . mem ',' con ',' imm2
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    spec2  přejít do stavu 89
    mem    přejít do stavu 90
    omem   přejít do stavu 52
    nmem   přejít do stavu 53
    nam    přejít do stavu 54
    con    přejít do stavu 55


State 15

   29 inst: LTYPEG . spec11
   61 spec11: . mem ',' imm
   62       | . mem ',' con ',' imm
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    spec11  přejít do stavu 91
    mem     přejít do stavu 92
    omem    přejít do stavu 52
    nmem    přejít do stavu 53
    nam     přejít do stavu 54
    con     přejít do stavu 55


State 16

   30 inst: LTYPEPC . spec12
   63 spec12: . rim ',' rim
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    spec12  přejít do stavu 93
    rem     přejít do stavu 58
    rim     přejít do stavu 94
    reg     přejít do stavu 50
    imm     přejít do stavu 60
    mem     přejít do stavu 51
    omem    přejít do stavu 52
    nmem    přejít do stavu 53
    nam     přejít do stavu 54
    con     přejít do stavu 55


State 17

   23 inst: LTYPES . spec5
   50 spec5: . rim ',' rem
   51      | . rim ',' rem ':' LLREG
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    spec5  přejít do stavu 95
    rem    přejít do stavu 58
    rim    přejít do stavu 96
    reg    přejít do stavu 50
    imm    přejít do stavu 60
    mem    přejít do stavu 51
    omem   přejít do stavu 52
    nmem   přejít do stavu 53
    nam    přejít do stavu 54
    con    přejít do stavu 55


State 18

   24 inst: LTYPEM . spec6
   52 spec6: . rim ',' rem
   53      | . rim ',' rem ':' LSREG
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    spec6  přejít do stavu 97
    rem    přejít do stavu 58
    rim    přejít do stavu 98
    reg    přejít do stavu 50
    imm    přejít do stavu 60
    mem    přejít do stavu 51
    omem   přejít do stavu 52
    nmem   přejít do stavu 53
    nam    přejít do stavu 54
    con    přejít do stavu 55


State 19

   25 inst: LTYPEI . spec7
   54 spec7: . rim ','
   55      | . rim
   56      | . rim ',' rem
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    spec7  přejít do stavu 99
    rem    přejít do stavu 58
    rim    přejít do stavu 100
    reg    přejít do stavu 50
    imm    přejít do stavu 60
    mem    přejít do stavu 51
    omem   přejít do stavu 52
    nmem   přejít do stavu 53
    nam    přejít do stavu 54
    con    přejít do stavu 55


State 20

   26 inst: LTYPEXC . spec8
   57 spec8: . reg ',' rem ',' con
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG

    LBREG  posunout a přejít do stavu 36
    LLREG  posunout a přejít do stavu 37
    LSREG  posunout a přejít do stavu 38
    LFREG  posunout a přejít do stavu 39
    LMREG  posunout a přejít do stavu 40
    LXREG  posunout a přejít do stavu 41
    LSP    posunout a přejít do stavu 42

    spec8  přejít do stavu 101
    reg    přejít do stavu 102


State 21

   27 inst: LTYPEX . spec9
   58 spec9: . imm ',' rem ',' reg
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST

    '$'  posunout a přejít do stavu 56

    spec9  přejít do stavu 103
    imm    přejít do stavu 104


State 22

   28 inst: LTYPERT . spec10
   59 spec10: . %empty  [';']
   60       | . imm
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST

    '$'  posunout a přejít do stavu 56

    $výchozí  reduce using rule 59 (spec10)

    spec10  přejít do stavu 105
    imm     přejít do stavu 106


State 23

   31 inst: LTYPEF . spec13
   64 spec13: . rim ',' rim
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    spec13  přejít do stavu 107
    rem     přejít do stavu 58
    rim     přejít do stavu 108
    reg     přejít do stavu 50
    imm     přejít do stavu 60
    mem     přejít do stavu 51
    omem    přejít do stavu 52
    nmem    přejít do stavu 53
    nam     přejít do stavu 54
    con     přejít do stavu 55


State 24

    7 line: LNAME . ':' $@3 line
   11 inst: LNAME . '=' expr

    ':'  posunout a přejít do stavu 109
    '='  posunout a přejít do stavu 110


State 25

    5 line: LLAB . ':' $@2 line

    ':'  posunout a přejít do stavu 111


State 26

   12 inst: LVAR . '=' expr

    '='  posunout a přejít do stavu 112


State 27

    8 line: ';' .

    $výchozí  reduce using rule 8 (line)


State 28

    3 prog: prog $@1 line .

    $výchozí  reduce using rule 3 (prog)


State 29

    9 line: inst . ';'

    ';'  posunout a přejít do stavu 113


State 30

   10 line: error ';' .

    $výchozí  reduce using rule 10 (line)


State 31

   33 nonnon: ',' .

    $výchozí  reduce using rule 33 (nonnon)


State 32

   13 inst: LTYPE0 nonnon .

    $výchozí  reduce using rule 13 (inst)


State 33

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  119    | '+' . con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 114


State 34

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  118    | '-' . con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 115


State 35

  116 con: LCONST .

    $výchozí  reduce using rule 116 (con)


State 36

   78 reg: LBREG .

    $výchozí  reduce using rule 78 (reg)


State 37

   80 reg: LLREG .

    $výchozí  reduce using rule 80 (reg)


State 38

   83 reg: LSREG .

    $výchozí  reduce using rule 83 (reg)


State 39

   79 reg: LFREG .

    $výchozí  reduce using rule 79 (reg)


State 40

   81 reg: LMREG .

    $výchozí  reduce using rule 81 (reg)


State 41

   84 reg: LXREG .

    $výchozí  reduce using rule 84 (reg)


State 42

   82 reg: LSP .

    $výchozí  reduce using rule 82 (reg)


State 43

  108 nam: LNAME . offset '(' pointer ')'
  109    | LNAME . '<' '>' offset '(' LSB ')'
  110 offset: . %empty  ['(']
  111       | . '+' con
  112       | . '-' con

    '<'  posunout a přejít do stavu 116
    '+'  posunout a přejít do stavu 117
    '-'  posunout a přejít do stavu 118

    $výchozí  reduce using rule 110 (offset)

    offset  přejít do stavu 119


State 44

  117 con: LVAR .

    $výchozí  reduce using rule 117 (con)


State 45

   38 nonrem: ',' . rem
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    rem   přejít do stavu 120
    reg   přejít do stavu 50
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 46

  102 omem: '(' . LLREG ')'
  103     | '(' . LSP ')'
  104     | '(' . LLREG '*' con ')'
  105     | '(' . LLREG ')' '(' LLREG '*' con ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  121    | '(' . expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LLREG   posunout a přejít do stavu 121
    LSP     posunout a přejít do stavu 122
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 124


State 47

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  120    | '~' . con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 125


State 48

   14 inst: LTYPE1 nonrem .

    $výchozí  reduce using rule 14 (inst)


State 49

   39 nonrem: rem .

    $výchozí  reduce using rule 39 (nonrem)


State 50

   65 rem: reg .

    $výchozí  reduce using rule 65 (rem)


State 51

   66 rem: mem .

    $výchozí  reduce using rule 66 (rem)


State 52

   93 mem: omem .

    $výchozí  reduce using rule 93 (mem)


State 53

   94 mem: nmem .

    $výchozí  reduce using rule 94 (mem)


State 54

  106 nmem: nam .  [':', ';', ',']
  107     | nam . '(' LLREG '*' con ')'

    '('  posunout a přejít do stavu 126

    $výchozí  reduce using rule 106 (nmem)


State 55

   95 omem: con .  [':', ';', ',']
   96     | con . '(' LLREG ')'
   97     | con . '(' LSP ')'
   98     | con . '(' LSREG ')'
   99     | con . '(' LLREG '*' con ')'
  100     | con . '(' LLREG ')' '(' LLREG '*' con ')'
  101     | con . '(' LLREG ')' '(' LSREG '*' con ')'

    '('  posunout a přejít do stavu 127

    $výchozí  reduce using rule 95 (omem)


State 56

   86 imm: '$' . con
   87    | '$' . nam
   88    | '$' . LSCONST
   89    | '$' . LFCONST
   90    | '$' . '(' LFCONST ')'
   91    | '$' . '(' '-' LFCONST ')'
   92    | '$' . '-' LFCONST
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'      posunout a přejít do stavu 33
    '-'      posunout a přejít do stavu 128
    LCONST   posunout a přejít do stavu 35
    LFCONST  posunout a přejít do stavu 129
    LSCONST  posunout a přejít do stavu 130
    LNAME    posunout a přejít do stavu 43
    LVAR     posunout a přejít do stavu 44
    '('      posunout a přejít do stavu 131
    '~'      posunout a přejít do stavu 47

    nam  přejít do stavu 132
    con  přejít do stavu 133


State 57

   15 inst: LTYPE2 rimnon .

    $výchozí  reduce using rule 15 (inst)


State 58

   73 rim: rem .

    $výchozí  reduce using rule 73 (rim)


State 59

   36 rimnon: rim . ','
   37       | rim .  [';']

    ','  posunout a přejít do stavu 134

    $výchozí  reduce using rule 37 (rimnon)


State 60

   74 rim: imm .

    $výchozí  reduce using rule 74 (rim)


State 61

   16 inst: LTYPE3 rimrem .

    $výchozí  reduce using rule 16 (inst)


State 62

   34 rimrem: rim . ',' rem

    ','  posunout a přejít do stavu 135


State 63

   17 inst: LTYPE4 remrim .

    $výchozí  reduce using rule 17 (inst)


State 64

   35 remrim: rem . ',' rim

    ','  posunout a přejít do stavu 136


State 65

   69 rom: '*' . reg
   70    | '*' . omem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    reg   přejít do stavu 137
    omem  přejít do stavu 138
    con   přejít do stavu 55


State 66

   76 rel: LNAME . offset
  108 nam: LNAME . offset '(' pointer ')'
  109    | LNAME . '<' '>' offset '(' LSB ')'
  110 offset: . %empty  [';', '(']
  111       | . '+' con
  112       | . '-' con

    '<'  posunout a přejít do stavu 116
    '+'  posunout a přejít do stavu 117
    '-'  posunout a přejít do stavu 118

    $výchozí  reduce using rule 110 (offset)

    offset  přejít do stavu 139


State 67

   77 rel: LLAB . offset
  110 offset: . %empty  [';']
  111       | . '+' con
  112       | . '-' con

    '+'  posunout a přejít do stavu 117
    '-'  posunout a přejít do stavu 118

    $výchozí  reduce using rule 110 (offset)

    offset  přejít do stavu 140


State 68

   46 spec3: ',' . rom
   67 rom: . rel
   68    | . nmem
   69    | . '*' reg
   70    | . '*' omem
   71    | . reg
   72    | . omem
   75 rel: . con '(' LPC ')'
   76    | . LNAME offset
   77    | . LLAB offset
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    '*'     posunout a přejít do stavu 65
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 66
    LLAB    posunout a přejít do stavu 67
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    rom   přejít do stavu 141
    rel   přejít do stavu 71
    reg   přejít do stavu 72
    omem  přejít do stavu 73
    nmem  přejít do stavu 74
    nam   přejít do stavu 54
    con   přejít do stavu 75


State 69

   21 inst: LTYPEC spec3 .

    $výchozí  reduce using rule 21 (inst)


State 70

   47 spec3: rom .

    $výchozí  reduce using rule 47 (spec3)


State 71

   67 rom: rel .

    $výchozí  reduce using rule 67 (rom)


State 72

   71 rom: reg .

    $výchozí  reduce using rule 71 (rom)


State 73

   72 rom: omem .

    $výchozí  reduce using rule 72 (rom)


State 74

   68 rom: nmem .

    $výchozí  reduce using rule 68 (rom)


State 75

   75 rel: con . '(' LPC ')'
   95 omem: con .  [';']
   96     | con . '(' LLREG ')'
   97     | con . '(' LSP ')'
   98     | con . '(' LSREG ')'
   99     | con . '(' LLREG '*' con ')'
  100     | con . '(' LLREG ')' '(' LLREG '*' con ')'
  101     | con . '(' LLREG ')' '(' LSREG '*' con ')'

    '('  posunout a přejít do stavu 142

    $výchozí  reduce using rule 95 (omem)


State 76

   19 inst: LTYPED spec1 .

    $výchozí  reduce using rule 19 (inst)


State 77

   43 spec1: nam . '/' con ',' imm

    '/'  posunout a přejít do stavu 143


State 78

   33 nonnon: ',' .  [';']
   38 nonrem: ',' . rem
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    $výchozí  reduce using rule 33 (nonnon)

    rem   přejít do stavu 120
    reg   přejít do stavu 50
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 79

   48 spec4: nonnon .

    $výchozí  reduce using rule 48 (spec4)


State 80

   49 spec4: nonrem .

    $výchozí  reduce using rule 49 (spec4)


State 81

   22 inst: LTYPEN spec4 .

    $výchozí  reduce using rule 22 (inst)


State 82

   76 rel: LNAME . offset
  110 offset: . %empty  [';']
  111       | . '+' con
  112       | . '-' con

    '+'  posunout a přejít do stavu 117
    '-'  posunout a přejít do stavu 118

    $výchozí  reduce using rule 110 (offset)

    offset  přejít do stavu 144


State 83

   40 nonrel: ',' . rel
   75 rel: . con '(' LPC ')'
   76    | . LNAME offset
   77    | . LLAB offset
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LNAME   posunout a přejít do stavu 82
    LLAB    posunout a přejít do stavu 67
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    rel  přejít do stavu 145
    con  přejít do stavu 88


State 84

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  121    | '(' . expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 124


State 85

   18 inst: LTYPER nonrel .

    $výchozí  reduce using rule 18 (inst)


State 86

   41 nonrel: rel .

    $výchozí  reduce using rule 41 (nonrel)


State 87

   42 nonrel: imm . ',' rel

    ','  posunout a přejít do stavu 146


State 88

   75 rel: con . '(' LPC ')'

    '('  posunout a přejít do stavu 147


State 89

   20 inst: LTYPET spec2 .

    $výchozí  reduce using rule 20 (inst)


State 90

   44 spec2: mem . ',' imm2
   45      | mem . ',' con ',' imm2

    ','  posunout a přejít do stavu 148


State 91

   29 inst: LTYPEG spec11 .

    $výchozí  reduce using rule 29 (inst)


State 92

   61 spec11: mem . ',' imm
   62       | mem . ',' con ',' imm

    ','  posunout a přejít do stavu 149


State 93

   30 inst: LTYPEPC spec12 .

    $výchozí  reduce using rule 30 (inst)


State 94

   63 spec12: rim . ',' rim

    ','  posunout a přejít do stavu 150


State 95

   23 inst: LTYPES spec5 .

    $výchozí  reduce using rule 23 (inst)


State 96

   50 spec5: rim . ',' rem
   51      | rim . ',' rem ':' LLREG

    ','  posunout a přejít do stavu 151


State 97

   24 inst: LTYPEM spec6 .

    $výchozí  reduce using rule 24 (inst)


State 98

   52 spec6: rim . ',' rem
   53      | rim . ',' rem ':' LSREG

    ','  posunout a přejít do stavu 152


State 99

   25 inst: LTYPEI spec7 .

    $výchozí  reduce using rule 25 (inst)


State 100

   54 spec7: rim . ','
   55      | rim .  [';']
   56      | rim . ',' rem

    ','  posunout a přejít do stavu 153

    $výchozí  reduce using rule 55 (spec7)


State 101

   26 inst: LTYPEXC spec8 .

    $výchozí  reduce using rule 26 (inst)


State 102

   57 spec8: reg . ',' rem ',' con

    ','  posunout a přejít do stavu 154


State 103

   27 inst: LTYPEX spec9 .

    $výchozí  reduce using rule 27 (inst)


State 104

   58 spec9: imm . ',' rem ',' reg

    ','  posunout a přejít do stavu 155


State 105

   28 inst: LTYPERT spec10 .

    $výchozí  reduce using rule 28 (inst)


State 106

   60 spec10: imm .

    $výchozí  reduce using rule 60 (spec10)


State 107

   31 inst: LTYPEF spec13 .

    $výchozí  reduce using rule 31 (inst)


State 108

   64 spec13: rim . ',' rim

    ','  posunout a přejít do stavu 156


State 109

    6 $@3: . %empty
    7 line: LNAME ':' . $@3 line

    $výchozí  reduce using rule 6 ($@3)

    $@3  přejít do stavu 157


State 110

   11 inst: LNAME '=' . expr
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 158


State 111

    4 $@2: . %empty
    5 line: LLAB ':' . $@2 line

    $výchozí  reduce using rule 4 ($@2)

    $@2  přejít do stavu 159


State 112

   12 inst: LVAR '=' . expr
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 160


State 113

    9 line: inst ';' .

    $výchozí  reduce using rule 9 (line)


State 114

  119 con: '+' con .

    $výchozí  reduce using rule 119 (con)


State 115

  118 con: '-' con .

    $výchozí  reduce using rule 118 (con)


State 116

  109 nam: LNAME '<' . '>' offset '(' LSB ')'

    '>'  posunout a přejít do stavu 161


State 117

  111 offset: '+' . con
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 162


State 118

  112 offset: '-' . con
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 163


State 119

  108 nam: LNAME offset . '(' pointer ')'

    '('  posunout a přejít do stavu 164


State 120

   38 nonrem: ',' rem .

    $výchozí  reduce using rule 38 (nonrem)


State 121

  102 omem: '(' LLREG . ')'
  104     | '(' LLREG . '*' con ')'
  105     | '(' LLREG . ')' '(' LLREG '*' con ')'

    '*'  posunout a přejít do stavu 165
    ')'  posunout a přejít do stavu 166


State 122

  103 omem: '(' LSP . ')'

    ')'  posunout a přejít do stavu 167


State 123

  126 expr: con .

    $výchozí  reduce using rule 126 (expr)


State 124

  121 con: '(' expr . ')'
  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    '|'  posunout a přejít do stavu 168
    '^'  posunout a přejít do stavu 169
    '&'  posunout a přejít do stavu 170
    '<'  posunout a přejít do stavu 171
    '>'  posunout a přejít do stavu 172
    '+'  posunout a přejít do stavu 173
    '-'  posunout a přejít do stavu 174
    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177
    ')'  posunout a přejít do stavu 178


State 125

  120 con: '~' con .

    $výchozí  reduce using rule 120 (con)


State 126

  107 nmem: nam '(' . LLREG '*' con ')'

    LLREG  posunout a přejít do stavu 179


State 127

   96 omem: con '(' . LLREG ')'
   97     | con '(' . LSP ')'
   98     | con '(' . LSREG ')'
   99     | con '(' . LLREG '*' con ')'
  100     | con '(' . LLREG ')' '(' LLREG '*' con ')'
  101     | con '(' . LLREG ')' '(' LSREG '*' con ')'

    LLREG  posunout a přejít do stavu 180
    LSREG  posunout a přejít do stavu 181
    LSP    posunout a přejít do stavu 182


State 128

   92 imm: '$' '-' . LFCONST
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  118    | '-' . con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'      posunout a přejít do stavu 33
    '-'      posunout a přejít do stavu 34
    LCONST   posunout a přejít do stavu 35
    LFCONST  posunout a přejít do stavu 183
    LVAR     posunout a přejít do stavu 44
    '('      posunout a přejít do stavu 84
    '~'      posunout a přejít do stavu 47

    con  přejít do stavu 115


State 129

   89 imm: '$' LFCONST .

    $výchozí  reduce using rule 89 (imm)


State 130

   88 imm: '$' LSCONST .

    $výchozí  reduce using rule 88 (imm)


State 131

   90 imm: '$' '(' . LFCONST ')'
   91    | '$' '(' . '-' LFCONST ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  121    | '(' . expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'      posunout a přejít do stavu 33
    '-'      posunout a přejít do stavu 184
    LCONST   posunout a přejít do stavu 35
    LFCONST  posunout a přejít do stavu 185
    LVAR     posunout a přejít do stavu 44
    '('      posunout a přejít do stavu 84
    '~'      posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 124


State 132

   87 imm: '$' nam .

    $výchozí  reduce using rule 87 (imm)


State 133

   86 imm: '$' con .

    $výchozí  reduce using rule 86 (imm)


State 134

   36 rimnon: rim ',' .

    $výchozí  reduce using rule 36 (rimnon)


State 135

   34 rimrem: rim ',' . rem
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    rem   přejít do stavu 186
    reg   přejít do stavu 50
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 136

   35 remrim: rem ',' . rim
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    rem   přejít do stavu 58
    rim   přejít do stavu 187
    reg   přejít do stavu 50
    imm   přejít do stavu 60
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 137

   69 rom: '*' reg .

    $výchozí  reduce using rule 69 (rom)


State 138

   70 rom: '*' omem .

    $výchozí  reduce using rule 70 (rom)


State 139

   76 rel: LNAME offset .  [';']
  108 nam: LNAME offset . '(' pointer ')'

    '('  posunout a přejít do stavu 164

    $výchozí  reduce using rule 76 (rel)


State 140

   77 rel: LLAB offset .

    $výchozí  reduce using rule 77 (rel)


State 141

   46 spec3: ',' rom .

    $výchozí  reduce using rule 46 (spec3)


State 142

   75 rel: con '(' . LPC ')'
   96 omem: con '(' . LLREG ')'
   97     | con '(' . LSP ')'
   98     | con '(' . LSREG ')'
   99     | con '(' . LLREG '*' con ')'
  100     | con '(' . LLREG ')' '(' LLREG '*' con ')'
  101     | con '(' . LLREG ')' '(' LSREG '*' con ')'

    LPC    posunout a přejít do stavu 188
    LLREG  posunout a přejít do stavu 180
    LSREG  posunout a přejít do stavu 181
    LSP    posunout a přejít do stavu 182


State 143

   43 spec1: nam '/' . con ',' imm
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 189


State 144

   76 rel: LNAME offset .

    $výchozí  reduce using rule 76 (rel)


State 145

   40 nonrel: ',' rel .

    $výchozí  reduce using rule 40 (nonrel)


State 146

   42 nonrel: imm ',' . rel
   75 rel: . con '(' LPC ')'
   76    | . LNAME offset
   77    | . LLAB offset
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LNAME   posunout a přejít do stavu 82
    LLAB    posunout a přejít do stavu 67
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    rel  přejít do stavu 190
    con  přejít do stavu 88


State 147

   75 rel: con '(' . LPC ')'

    LPC  posunout a přejít do stavu 188


State 148

   44 spec2: mem ',' . imm2
   45      | mem ',' . con ',' imm2
   85 imm2: . '$' con2
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '$'     posunout a přejít do stavu 191
    '~'     posunout a přejít do stavu 47

    imm2  přejít do stavu 192
    con   přejít do stavu 193


State 149

   61 spec11: mem ',' . imm
   62       | mem ',' . con ',' imm
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    imm  přejít do stavu 194
    con  přejít do stavu 195


State 150

   63 spec12: rim ',' . rim
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    rem   přejít do stavu 58
    rim   přejít do stavu 196
    reg   přejít do stavu 50
    imm   přejít do stavu 60
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 151

   50 spec5: rim ',' . rem
   51      | rim ',' . rem ':' LLREG
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    rem   přejít do stavu 197
    reg   přejít do stavu 50
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 152

   52 spec6: rim ',' . rem
   53      | rim ',' . rem ':' LSREG
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    rem   přejít do stavu 198
    reg   přejít do stavu 50
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 153

   54 spec7: rim ',' .  [';']
   56      | rim ',' . rem
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    $výchozí  reduce using rule 54 (spec7)

    rem   přejít do stavu 199
    reg   přejít do stavu 50
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 154

   57 spec8: reg ',' . rem ',' con
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    rem   přejít do stavu 200
    reg   přejít do stavu 50
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 155

   58 spec9: imm ',' . rem ',' reg
   65 rem: . reg
   66    | . mem
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '~'     posunout a přejít do stavu 47

    rem   přejít do stavu 201
    reg   přejít do stavu 50
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 156

   64 spec13: rim ',' . rim
   65 rem: . reg
   66    | . mem
   73 rim: . rem
   74    | . imm
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST
   93 mem: . omem
   94    | . nmem
   95 omem: . con
   96     | . con '(' LLREG ')'
   97     | . con '(' LSP ')'
   98     | . con '(' LSREG ')'
   99     | . con '(' LLREG '*' con ')'
  100     | . con '(' LLREG ')' '(' LLREG '*' con ')'
  101     | . con '(' LLREG ')' '(' LSREG '*' con ')'
  102     | . '(' LLREG ')'
  103     | . '(' LSP ')'
  104     | . '(' LLREG '*' con ')'
  105     | . '(' LLREG ')' '(' LLREG '*' con ')'
  106 nmem: . nam
  107     | . nam '(' LLREG '*' con ')'
  108 nam: . LNAME offset '(' pointer ')'
  109    | . LNAME '<' '>' offset '(' LSB ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LBREG   posunout a přejít do stavu 36
    LLREG   posunout a přejít do stavu 37
    LSREG   posunout a přejít do stavu 38
    LFREG   posunout a přejít do stavu 39
    LMREG   posunout a přejít do stavu 40
    LXREG   posunout a přejít do stavu 41
    LSP     posunout a přejít do stavu 42
    LNAME   posunout a přejít do stavu 43
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 46
    '$'     posunout a přejít do stavu 56
    '~'     posunout a přejít do stavu 47

    rem   přejít do stavu 58
    rim   přejít do stavu 202
    reg   přejít do stavu 50
    imm   přejít do stavu 60
    mem   přejít do stavu 51
    omem  přejít do stavu 52
    nmem  přejít do stavu 53
    nam   přejít do stavu 54
    con   přejít do stavu 55


State 157

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
   26     | . LTYPEXC spec8
   27     | . LTYPEX spec9
   28     | . LTYPERT spec10
   29     | . LTYPEG spec11
   30     | . LTYPEPC spec12
   31     | . LTYPEF spec13

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
    LTYPEG   posunout a přejít do stavu 15
    LTYPEPC  posunout a přejít do stavu 16
    LTYPES   posunout a přejít do stavu 17
    LTYPEM   posunout a přejít do stavu 18
    LTYPEI   posunout a přejít do stavu 19
    LTYPEXC  posunout a přejít do stavu 20
    LTYPEX   posunout a přejít do stavu 21
    LTYPERT  posunout a přejít do stavu 22
    LTYPEF   posunout a přejít do stavu 23
    LNAME    posunout a přejít do stavu 24
    LLAB     posunout a přejít do stavu 25
    LVAR     posunout a přejít do stavu 26
    ';'      posunout a přejít do stavu 27

    line  přejít do stavu 203
    inst  přejít do stavu 29


State 158

   11 inst: LNAME '=' expr .  [';']
  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    '|'  posunout a přejít do stavu 168
    '^'  posunout a přejít do stavu 169
    '&'  posunout a přejít do stavu 170
    '<'  posunout a přejít do stavu 171
    '>'  posunout a přejít do stavu 172
    '+'  posunout a přejít do stavu 173
    '-'  posunout a přejít do stavu 174
    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177

    $výchozí  reduce using rule 11 (inst)


State 159

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
   26     | . LTYPEXC spec8
   27     | . LTYPEX spec9
   28     | . LTYPERT spec10
   29     | . LTYPEG spec11
   30     | . LTYPEPC spec12
   31     | . LTYPEF spec13

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
    LTYPEG   posunout a přejít do stavu 15
    LTYPEPC  posunout a přejít do stavu 16
    LTYPES   posunout a přejít do stavu 17
    LTYPEM   posunout a přejít do stavu 18
    LTYPEI   posunout a přejít do stavu 19
    LTYPEXC  posunout a přejít do stavu 20
    LTYPEX   posunout a přejít do stavu 21
    LTYPERT  posunout a přejít do stavu 22
    LTYPEF   posunout a přejít do stavu 23
    LNAME    posunout a přejít do stavu 24
    LLAB     posunout a přejít do stavu 25
    LVAR     posunout a přejít do stavu 26
    ';'      posunout a přejít do stavu 27

    line  přejít do stavu 204
    inst  přejít do stavu 29


State 160

   12 inst: LVAR '=' expr .  [';']
  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    '|'  posunout a přejít do stavu 168
    '^'  posunout a přejít do stavu 169
    '&'  posunout a přejít do stavu 170
    '<'  posunout a přejít do stavu 171
    '>'  posunout a přejít do stavu 172
    '+'  posunout a přejít do stavu 173
    '-'  posunout a přejít do stavu 174
    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177

    $výchozí  reduce using rule 12 (inst)


State 161

  109 nam: LNAME '<' '>' . offset '(' LSB ')'
  110 offset: . %empty  ['(']
  111       | . '+' con
  112       | . '-' con

    '+'  posunout a přejít do stavu 117
    '-'  posunout a přejít do stavu 118

    $výchozí  reduce using rule 110 (offset)

    offset  přejít do stavu 205


State 162

  111 offset: '+' con .

    $výchozí  reduce using rule 111 (offset)


State 163

  112 offset: '-' con .

    $výchozí  reduce using rule 112 (offset)


State 164

  108 nam: LNAME offset '(' . pointer ')'
  113 pointer: . LSB
  114        | . LSP
  115        | . LFP

    LFP  posunout a přejít do stavu 206
    LSB  posunout a přejít do stavu 207
    LSP  posunout a přejít do stavu 208

    pointer  přejít do stavu 209


State 165

  104 omem: '(' LLREG '*' . con ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 210


State 166

  102 omem: '(' LLREG ')' .  [':', ';', ',']
  105     | '(' LLREG ')' . '(' LLREG '*' con ')'

    '('  posunout a přejít do stavu 211

    $výchozí  reduce using rule 102 (omem)


State 167

  103 omem: '(' LSP ')' .

    $výchozí  reduce using rule 103 (omem)


State 168

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr
  136     | expr '|' . expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 212


State 169

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  135     | expr '^' . expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 213


State 170

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  134     | expr '&' . expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 214


State 171

  132 expr: expr '<' . '<' expr

    '<'  posunout a přejít do stavu 215


State 172

  133 expr: expr '>' . '>' expr

    '>'  posunout a přejít do stavu 216


State 173

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  127     | expr '+' . expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 217


State 174

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  128     | expr '-' . expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 218


State 175

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  129     | expr '*' . expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 219


State 176

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  130     | expr '/' . expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 220


State 177

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  131     | expr '%' . expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 221


State 178

  121 con: '(' expr ')' .

    $výchozí  reduce using rule 121 (con)


State 179

  107 nmem: nam '(' LLREG . '*' con ')'

    '*'  posunout a přejít do stavu 222


State 180

   96 omem: con '(' LLREG . ')'
   99     | con '(' LLREG . '*' con ')'
  100     | con '(' LLREG . ')' '(' LLREG '*' con ')'
  101     | con '(' LLREG . ')' '(' LSREG '*' con ')'

    '*'  posunout a přejít do stavu 223
    ')'  posunout a přejít do stavu 224


State 181

   98 omem: con '(' LSREG . ')'

    ')'  posunout a přejít do stavu 225


State 182

   97 omem: con '(' LSP . ')'

    ')'  posunout a přejít do stavu 226


State 183

   92 imm: '$' '-' LFCONST .

    $výchozí  reduce using rule 92 (imm)


State 184

   91 imm: '$' '(' '-' . LFCONST ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  118    | '-' . con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'      posunout a přejít do stavu 33
    '-'      posunout a přejít do stavu 34
    LCONST   posunout a přejít do stavu 35
    LFCONST  posunout a přejít do stavu 227
    LVAR     posunout a přejít do stavu 44
    '('      posunout a přejít do stavu 84
    '~'      posunout a přejít do stavu 47

    con  přejít do stavu 115


State 185

   90 imm: '$' '(' LFCONST . ')'

    ')'  posunout a přejít do stavu 228


State 186

   34 rimrem: rim ',' rem .

    $výchozí  reduce using rule 34 (rimrem)


State 187

   35 remrim: rem ',' rim .

    $výchozí  reduce using rule 35 (remrim)


State 188

   75 rel: con '(' LPC . ')'

    ')'  posunout a přejít do stavu 229


State 189

   43 spec1: nam '/' con . ',' imm

    ','  posunout a přejít do stavu 230


State 190

   42 nonrel: imm ',' rel .

    $výchozí  reduce using rule 42 (nonrel)


State 191

   85 imm2: '$' . con2
  122 con2: . LCONST
  123     | . '-' LCONST
  124     | . LCONST '-' LCONST
  125     | . '-' LCONST '-' LCONST

    '-'     posunout a přejít do stavu 231
    LCONST  posunout a přejít do stavu 232

    con2  přejít do stavu 233


State 192

   44 spec2: mem ',' imm2 .

    $výchozí  reduce using rule 44 (spec2)


State 193

   45 spec2: mem ',' con . ',' imm2

    ','  posunout a přejít do stavu 234


State 194

   61 spec11: mem ',' imm .

    $výchozí  reduce using rule 61 (spec11)


State 195

   62 spec11: mem ',' con . ',' imm

    ','  posunout a přejít do stavu 235


State 196

   63 spec12: rim ',' rim .

    $výchozí  reduce using rule 63 (spec12)


State 197

   50 spec5: rim ',' rem .  [';']
   51      | rim ',' rem . ':' LLREG

    ':'  posunout a přejít do stavu 236

    $výchozí  reduce using rule 50 (spec5)


State 198

   52 spec6: rim ',' rem .  [';']
   53      | rim ',' rem . ':' LSREG

    ':'  posunout a přejít do stavu 237

    $výchozí  reduce using rule 52 (spec6)


State 199

   56 spec7: rim ',' rem .

    $výchozí  reduce using rule 56 (spec7)


State 200

   57 spec8: reg ',' rem . ',' con

    ','  posunout a přejít do stavu 238


State 201

   58 spec9: imm ',' rem . ',' reg

    ','  posunout a přejít do stavu 239


State 202

   64 spec13: rim ',' rim .

    $výchozí  reduce using rule 64 (spec13)


State 203

    7 line: LNAME ':' $@3 line .

    $výchozí  reduce using rule 7 (line)


State 204

    5 line: LLAB ':' $@2 line .

    $výchozí  reduce using rule 5 (line)


State 205

  109 nam: LNAME '<' '>' offset . '(' LSB ')'

    '('  posunout a přejít do stavu 240


State 206

  115 pointer: LFP .

    $výchozí  reduce using rule 115 (pointer)


State 207

  113 pointer: LSB .

    $výchozí  reduce using rule 113 (pointer)


State 208

  114 pointer: LSP .

    $výchozí  reduce using rule 114 (pointer)


State 209

  108 nam: LNAME offset '(' pointer . ')'

    ')'  posunout a přejít do stavu 241


State 210

  104 omem: '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 242


State 211

  105 omem: '(' LLREG ')' '(' . LLREG '*' con ')'

    LLREG  posunout a přejít do stavu 243


State 212

  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr
  136     | expr '|' expr .  ['|', ';', ')']

    '^'  posunout a přejít do stavu 169
    '&'  posunout a přejít do stavu 170
    '<'  posunout a přejít do stavu 171
    '>'  posunout a přejít do stavu 172
    '+'  posunout a přejít do stavu 173
    '-'  posunout a přejít do stavu 174
    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177

    $výchozí  reduce using rule 136 (expr)

    Conflict between rule 136 and token '|' resolved as reduce (%left '|').
    Conflict between rule 136 and token '^' resolved as shift ('|' < '^').
    Conflict between rule 136 and token '&' resolved as shift ('|' < '&').
    Conflict between rule 136 and token '<' resolved as shift ('|' < '<').
    Conflict between rule 136 and token '>' resolved as shift ('|' < '>').
    Conflict between rule 136 and token '+' resolved as shift ('|' < '+').
    Conflict between rule 136 and token '-' resolved as shift ('|' < '-').
    Conflict between rule 136 and token '*' resolved as shift ('|' < '*').
    Conflict between rule 136 and token '/' resolved as shift ('|' < '/').
    Conflict between rule 136 and token '%' resolved as shift ('|' < '%').


State 213

  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  135     | expr '^' expr .  ['|', '^', ';', ')']
  136     | expr . '|' expr

    '&'  posunout a přejít do stavu 170
    '<'  posunout a přejít do stavu 171
    '>'  posunout a přejít do stavu 172
    '+'  posunout a přejít do stavu 173
    '-'  posunout a přejít do stavu 174
    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177

    $výchozí  reduce using rule 135 (expr)

    Conflict between rule 135 and token '|' resolved as reduce ('|' < '^').
    Conflict between rule 135 and token '^' resolved as reduce (%left '^').
    Conflict between rule 135 and token '&' resolved as shift ('^' < '&').
    Conflict between rule 135 and token '<' resolved as shift ('^' < '<').
    Conflict between rule 135 and token '>' resolved as shift ('^' < '>').
    Conflict between rule 135 and token '+' resolved as shift ('^' < '+').
    Conflict between rule 135 and token '-' resolved as shift ('^' < '-').
    Conflict between rule 135 and token '*' resolved as shift ('^' < '*').
    Conflict between rule 135 and token '/' resolved as shift ('^' < '/').
    Conflict between rule 135 and token '%' resolved as shift ('^' < '%').


State 214

  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  134     | expr '&' expr .  ['|', '^', '&', ';', ')']
  135     | expr . '^' expr
  136     | expr . '|' expr

    '<'  posunout a přejít do stavu 171
    '>'  posunout a přejít do stavu 172
    '+'  posunout a přejít do stavu 173
    '-'  posunout a přejít do stavu 174
    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177

    $výchozí  reduce using rule 134 (expr)

    Conflict between rule 134 and token '|' resolved as reduce ('|' < '&').
    Conflict between rule 134 and token '^' resolved as reduce ('^' < '&').
    Conflict between rule 134 and token '&' resolved as reduce (%left '&').
    Conflict between rule 134 and token '<' resolved as shift ('&' < '<').
    Conflict between rule 134 and token '>' resolved as shift ('&' < '>').
    Conflict between rule 134 and token '+' resolved as shift ('&' < '+').
    Conflict between rule 134 and token '-' resolved as shift ('&' < '-').
    Conflict between rule 134 and token '*' resolved as shift ('&' < '*').
    Conflict between rule 134 and token '/' resolved as shift ('&' < '/').
    Conflict between rule 134 and token '%' resolved as shift ('&' < '%').


State 215

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  132     | expr '<' '<' . expr
  133     | . expr '>' '>' expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 244


State 216

  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'
  126 expr: . con
  127     | . expr '+' expr
  128     | . expr '-' expr
  129     | . expr '*' expr
  130     | . expr '/' expr
  131     | . expr '%' expr
  132     | . expr '<' '<' expr
  133     | . expr '>' '>' expr
  133     | expr '>' '>' . expr
  134     | . expr '&' expr
  135     | . expr '^' expr
  136     | . expr '|' expr

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con   přejít do stavu 123
    expr  přejít do stavu 245


State 217

  127 expr: expr . '+' expr
  127     | expr '+' expr .  ['|', '^', '&', '<', '>', '+', '-', ';', ')']
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177

    $výchozí  reduce using rule 127 (expr)

    Conflict between rule 127 and token '|' resolved as reduce ('|' < '+').
    Conflict between rule 127 and token '^' resolved as reduce ('^' < '+').
    Conflict between rule 127 and token '&' resolved as reduce ('&' < '+').
    Conflict between rule 127 and token '<' resolved as reduce ('<' < '+').
    Conflict between rule 127 and token '>' resolved as reduce ('>' < '+').
    Conflict between rule 127 and token '+' resolved as reduce (%left '+').
    Conflict between rule 127 and token '-' resolved as reduce (%left '-').
    Conflict between rule 127 and token '*' resolved as shift ('+' < '*').
    Conflict between rule 127 and token '/' resolved as shift ('+' < '/').
    Conflict between rule 127 and token '%' resolved as shift ('+' < '%').


State 218

  127 expr: expr . '+' expr
  128     | expr . '-' expr
  128     | expr '-' expr .  ['|', '^', '&', '<', '>', '+', '-', ';', ')']
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177

    $výchozí  reduce using rule 128 (expr)

    Conflict between rule 128 and token '|' resolved as reduce ('|' < '-').
    Conflict between rule 128 and token '^' resolved as reduce ('^' < '-').
    Conflict between rule 128 and token '&' resolved as reduce ('&' < '-').
    Conflict between rule 128 and token '<' resolved as reduce ('<' < '-').
    Conflict between rule 128 and token '>' resolved as reduce ('>' < '-').
    Conflict between rule 128 and token '+' resolved as reduce (%left '+').
    Conflict between rule 128 and token '-' resolved as reduce (%left '-').
    Conflict between rule 128 and token '*' resolved as shift ('-' < '*').
    Conflict between rule 128 and token '/' resolved as shift ('-' < '/').
    Conflict between rule 128 and token '%' resolved as shift ('-' < '%').


State 219

  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  129     | expr '*' expr .  ['|', '^', '&', '<', '>', '+', '-', '*', '/', '%', ';', ')']
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    $výchozí  reduce using rule 129 (expr)

    Conflict between rule 129 and token '|' resolved as reduce ('|' < '*').
    Conflict between rule 129 and token '^' resolved as reduce ('^' < '*').
    Conflict between rule 129 and token '&' resolved as reduce ('&' < '*').
    Conflict between rule 129 and token '<' resolved as reduce ('<' < '*').
    Conflict between rule 129 and token '>' resolved as reduce ('>' < '*').
    Conflict between rule 129 and token '+' resolved as reduce ('+' < '*').
    Conflict between rule 129 and token '-' resolved as reduce ('-' < '*').
    Conflict between rule 129 and token '*' resolved as reduce (%left '*').
    Conflict between rule 129 and token '/' resolved as reduce (%left '/').
    Conflict between rule 129 and token '%' resolved as reduce (%left '%').


State 220

  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  130     | expr '/' expr .  ['|', '^', '&', '<', '>', '+', '-', '*', '/', '%', ';', ')']
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    $výchozí  reduce using rule 130 (expr)

    Conflict between rule 130 and token '|' resolved as reduce ('|' < '/').
    Conflict between rule 130 and token '^' resolved as reduce ('^' < '/').
    Conflict between rule 130 and token '&' resolved as reduce ('&' < '/').
    Conflict between rule 130 and token '<' resolved as reduce ('<' < '/').
    Conflict between rule 130 and token '>' resolved as reduce ('>' < '/').
    Conflict between rule 130 and token '+' resolved as reduce ('+' < '/').
    Conflict between rule 130 and token '-' resolved as reduce ('-' < '/').
    Conflict between rule 130 and token '*' resolved as reduce (%left '*').
    Conflict between rule 130 and token '/' resolved as reduce (%left '/').
    Conflict between rule 130 and token '%' resolved as reduce (%left '%').


State 221

  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  131     | expr '%' expr .  ['|', '^', '&', '<', '>', '+', '-', '*', '/', '%', ';', ')']
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    $výchozí  reduce using rule 131 (expr)

    Conflict between rule 131 and token '|' resolved as reduce ('|' < '%').
    Conflict between rule 131 and token '^' resolved as reduce ('^' < '%').
    Conflict between rule 131 and token '&' resolved as reduce ('&' < '%').
    Conflict between rule 131 and token '<' resolved as reduce ('<' < '%').
    Conflict between rule 131 and token '>' resolved as reduce ('>' < '%').
    Conflict between rule 131 and token '+' resolved as reduce ('+' < '%').
    Conflict between rule 131 and token '-' resolved as reduce ('-' < '%').
    Conflict between rule 131 and token '*' resolved as reduce (%left '*').
    Conflict between rule 131 and token '/' resolved as reduce (%left '/').
    Conflict between rule 131 and token '%' resolved as reduce (%left '%').


State 222

  107 nmem: nam '(' LLREG '*' . con ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 246


State 223

   99 omem: con '(' LLREG '*' . con ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 247


State 224

   96 omem: con '(' LLREG ')' .  [':', ';', ',']
  100     | con '(' LLREG ')' . '(' LLREG '*' con ')'
  101     | con '(' LLREG ')' . '(' LSREG '*' con ')'

    '('  posunout a přejít do stavu 248

    $výchozí  reduce using rule 96 (omem)


State 225

   98 omem: con '(' LSREG ')' .

    $výchozí  reduce using rule 98 (omem)


State 226

   97 omem: con '(' LSP ')' .

    $výchozí  reduce using rule 97 (omem)


State 227

   91 imm: '$' '(' '-' LFCONST . ')'

    ')'  posunout a přejít do stavu 249


State 228

   90 imm: '$' '(' LFCONST ')' .

    $výchozí  reduce using rule 90 (imm)


State 229

   75 rel: con '(' LPC ')' .

    $výchozí  reduce using rule 75 (rel)


State 230

   43 spec1: nam '/' con ',' . imm
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST

    '$'  posunout a přejít do stavu 56

    imm  přejít do stavu 250


State 231

  123 con2: '-' . LCONST
  125     | '-' . LCONST '-' LCONST

    LCONST  posunout a přejít do stavu 251


State 232

  122 con2: LCONST .  [';']
  124     | LCONST . '-' LCONST

    '-'  posunout a přejít do stavu 252

    $výchozí  reduce using rule 122 (con2)


State 233

   85 imm2: '$' con2 .

    $výchozí  reduce using rule 85 (imm2)


State 234

   45 spec2: mem ',' con ',' . imm2
   85 imm2: . '$' con2

    '$'  posunout a přejít do stavu 191

    imm2  přejít do stavu 253


State 235

   62 spec11: mem ',' con ',' . imm
   86 imm: . '$' con
   87    | . '$' nam
   88    | . '$' LSCONST
   89    | . '$' LFCONST
   90    | . '$' '(' LFCONST ')'
   91    | . '$' '(' '-' LFCONST ')'
   92    | . '$' '-' LFCONST

    '$'  posunout a přejít do stavu 56

    imm  přejít do stavu 254


State 236

   51 spec5: rim ',' rem ':' . LLREG

    LLREG  posunout a přejít do stavu 255


State 237

   53 spec6: rim ',' rem ':' . LSREG

    LSREG  posunout a přejít do stavu 256


State 238

   57 spec8: reg ',' rem ',' . con
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 257


State 239

   58 spec9: imm ',' rem ',' . reg
   78 reg: . LBREG
   79    | . LFREG
   80    | . LLREG
   81    | . LMREG
   82    | . LSP
   83    | . LSREG
   84    | . LXREG

    LBREG  posunout a přejít do stavu 36
    LLREG  posunout a přejít do stavu 37
    LSREG  posunout a přejít do stavu 38
    LFREG  posunout a přejít do stavu 39
    LMREG  posunout a přejít do stavu 40
    LXREG  posunout a přejít do stavu 41
    LSP    posunout a přejít do stavu 42

    reg  přejít do stavu 258


State 240

  109 nam: LNAME '<' '>' offset '(' . LSB ')'

    LSB  posunout a přejít do stavu 259


State 241

  108 nam: LNAME offset '(' pointer ')' .

    $výchozí  reduce using rule 108 (nam)


State 242

  104 omem: '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 104 (omem)


State 243

  105 omem: '(' LLREG ')' '(' LLREG . '*' con ')'

    '*'  posunout a přejít do stavu 260


State 244

  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  132     | expr '<' '<' expr .  ['|', '^', '&', '<', '>', ';', ')']
  133     | expr . '>' '>' expr
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    '+'  posunout a přejít do stavu 173
    '-'  posunout a přejít do stavu 174
    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177

    $výchozí  reduce using rule 132 (expr)

    Conflict between rule 132 and token '|' resolved as reduce ('|' < '<').
    Conflict between rule 132 and token '^' resolved as reduce ('^' < '<').
    Conflict between rule 132 and token '&' resolved as reduce ('&' < '<').
    Conflict between rule 132 and token '<' resolved as reduce (%left '<').
    Conflict between rule 132 and token '>' resolved as reduce (%left '>').
    Conflict between rule 132 and token '+' resolved as shift ('<' < '+').
    Conflict between rule 132 and token '-' resolved as shift ('<' < '-').
    Conflict between rule 132 and token '*' resolved as shift ('<' < '*').
    Conflict between rule 132 and token '/' resolved as shift ('<' < '/').
    Conflict between rule 132 and token '%' resolved as shift ('<' < '%').


State 245

  127 expr: expr . '+' expr
  128     | expr . '-' expr
  129     | expr . '*' expr
  130     | expr . '/' expr
  131     | expr . '%' expr
  132     | expr . '<' '<' expr
  133     | expr . '>' '>' expr
  133     | expr '>' '>' expr .  ['|', '^', '&', '<', '>', ';', ')']
  134     | expr . '&' expr
  135     | expr . '^' expr
  136     | expr . '|' expr

    '+'  posunout a přejít do stavu 173
    '-'  posunout a přejít do stavu 174
    '*'  posunout a přejít do stavu 175
    '/'  posunout a přejít do stavu 176
    '%'  posunout a přejít do stavu 177

    $výchozí  reduce using rule 133 (expr)

    Conflict between rule 133 and token '|' resolved as reduce ('|' < '>').
    Conflict between rule 133 and token '^' resolved as reduce ('^' < '>').
    Conflict between rule 133 and token '&' resolved as reduce ('&' < '>').
    Conflict between rule 133 and token '<' resolved as reduce (%left '<').
    Conflict between rule 133 and token '>' resolved as reduce (%left '>').
    Conflict between rule 133 and token '+' resolved as shift ('>' < '+').
    Conflict between rule 133 and token '-' resolved as shift ('>' < '-').
    Conflict between rule 133 and token '*' resolved as shift ('>' < '*').
    Conflict between rule 133 and token '/' resolved as shift ('>' < '/').
    Conflict between rule 133 and token '%' resolved as shift ('>' < '%').


State 246

  107 nmem: nam '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 261


State 247

   99 omem: con '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 262


State 248

  100 omem: con '(' LLREG ')' '(' . LLREG '*' con ')'
  101     | con '(' LLREG ')' '(' . LSREG '*' con ')'

    LLREG  posunout a přejít do stavu 263
    LSREG  posunout a přejít do stavu 264


State 249

   91 imm: '$' '(' '-' LFCONST ')' .

    $výchozí  reduce using rule 91 (imm)


State 250

   43 spec1: nam '/' con ',' imm .

    $výchozí  reduce using rule 43 (spec1)


State 251

  123 con2: '-' LCONST .  [';']
  125     | '-' LCONST . '-' LCONST

    '-'  posunout a přejít do stavu 265

    $výchozí  reduce using rule 123 (con2)


State 252

  124 con2: LCONST '-' . LCONST

    LCONST  posunout a přejít do stavu 266


State 253

   45 spec2: mem ',' con ',' imm2 .

    $výchozí  reduce using rule 45 (spec2)


State 254

   62 spec11: mem ',' con ',' imm .

    $výchozí  reduce using rule 62 (spec11)


State 255

   51 spec5: rim ',' rem ':' LLREG .

    $výchozí  reduce using rule 51 (spec5)


State 256

   53 spec6: rim ',' rem ':' LSREG .

    $výchozí  reduce using rule 53 (spec6)


State 257

   57 spec8: reg ',' rem ',' con .

    $výchozí  reduce using rule 57 (spec8)


State 258

   58 spec9: imm ',' rem ',' reg .

    $výchozí  reduce using rule 58 (spec9)


State 259

  109 nam: LNAME '<' '>' offset '(' LSB . ')'

    ')'  posunout a přejít do stavu 267


State 260

  105 omem: '(' LLREG ')' '(' LLREG '*' . con ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 268


State 261

  107 nmem: nam '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 107 (nmem)


State 262

   99 omem: con '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 99 (omem)


State 263

  100 omem: con '(' LLREG ')' '(' LLREG . '*' con ')'

    '*'  posunout a přejít do stavu 269


State 264

  101 omem: con '(' LLREG ')' '(' LSREG . '*' con ')'

    '*'  posunout a přejít do stavu 270


State 265

  125 con2: '-' LCONST '-' . LCONST

    LCONST  posunout a přejít do stavu 271


State 266

  124 con2: LCONST '-' LCONST .

    $výchozí  reduce using rule 124 (con2)


State 267

  109 nam: LNAME '<' '>' offset '(' LSB ')' .

    $výchozí  reduce using rule 109 (nam)


State 268

  105 omem: '(' LLREG ')' '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 272


State 269

  100 omem: con '(' LLREG ')' '(' LLREG '*' . con ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 273


State 270

  101 omem: con '(' LLREG ')' '(' LSREG '*' . con ')'
  116 con: . LCONST
  117    | . LVAR
  118    | . '-' con
  119    | . '+' con
  120    | . '~' con
  121    | . '(' expr ')'

    '+'     posunout a přejít do stavu 33
    '-'     posunout a přejít do stavu 34
    LCONST  posunout a přejít do stavu 35
    LVAR    posunout a přejít do stavu 44
    '('     posunout a přejít do stavu 84
    '~'     posunout a přejít do stavu 47

    con  přejít do stavu 274


State 271

  125 con2: '-' LCONST '-' LCONST .

    $výchozí  reduce using rule 125 (con2)


State 272

  105 omem: '(' LLREG ')' '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 105 (omem)


State 273

  100 omem: con '(' LLREG ')' '(' LLREG '*' con . ')'

    ')'  posunout a přejít do stavu 275


State 274

  101 omem: con '(' LLREG ')' '(' LSREG '*' con . ')'

    ')'  posunout a přejít do stavu 276


State 275

  100 omem: con '(' LLREG ')' '(' LLREG '*' con ')' .

    $výchozí  reduce using rule 100 (omem)


State 276

  101 omem: con '(' LLREG ')' '(' LSREG '*' con ')' .

    $výchozí  reduce using rule 101 (omem)
