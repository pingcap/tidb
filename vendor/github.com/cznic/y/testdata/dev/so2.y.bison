Rules useless in parser due to conflicts

   20 type: IDENTIFIER


Stav 18 conflicts: 1 reduce/reduce


Gramatika

    0 $accept: file $end

    1 file: %empty
    2     | statement file

    3 statement: expression ';'

    4 expression: expression '=' expression
    5           | '(' type ')' expression
    6           | '(' expression ')'
    7           | IDENTIFIER

    8 type: VOID
    9     | AUTO
   10     | BOOL
   11     | BYTE
   12     | SHORT
   13     | INT
   14     | LONG
   15     | FLOAT
   16     | DOUBLE
   17     | SIGNED
   18     | UNSIGNED
   19     | STRING
   20     | IDENTIFIER


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'(' (40) 5 6
')' (41) 5 6
';' (59) 3
'=' (61) 4
error (256)
CAST (258)
AUTO (259) 9
BOOL (260) 10
BYTE (261) 11
DOUBLE (262) 16
FLOAT (263) 15
INT (264) 13
LONG (265) 14
SHORT (266) 12
SIGNED (267) 17
STRING (268) 19
UNSIGNED (269) 18
VOID (270) 8
IDENTIFIER (271) 7 20


Neterminály s pravidly, ve kterých se objevují

$accept (21)
    vlevo: 0
file (22)
    vlevo: 1 2, vpravo: 0 2
statement (23)
    vlevo: 3, vpravo: 2
expression (24)
    vlevo: 4 5 6 7, vpravo: 3 4 5 6
type (25)
    vlevo: 8 9 10 11 12 13 14 15 16 17 18 19 20, vpravo: 5


State 0

    0 $accept: . file $end
    1 file: . %empty  [$end]
    2     | . statement file
    3 statement: . expression ';'
    4 expression: . expression '=' expression
    5           | . '(' type ')' expression
    6           | . '(' expression ')'
    7           | . IDENTIFIER

    '('         posunout a přejít do stavu 1
    IDENTIFIER  posunout a přejít do stavu 2

    $výchozí  reduce using rule 1 (file)

    file        přejít do stavu 3
    statement   přejít do stavu 4
    expression  přejít do stavu 5


State 1

    4 expression: . expression '=' expression
    5           | . '(' type ')' expression
    5           | '(' . type ')' expression
    6           | . '(' expression ')'
    6           | '(' . expression ')'
    7           | . IDENTIFIER
    8 type: . VOID
    9     | . AUTO
   10     | . BOOL
   11     | . BYTE
   12     | . SHORT
   13     | . INT
   14     | . LONG
   15     | . FLOAT
   16     | . DOUBLE
   17     | . SIGNED
   18     | . UNSIGNED
   19     | . STRING
   20     | . IDENTIFIER

    '('         posunout a přejít do stavu 1
    AUTO        posunout a přejít do stavu 6
    BOOL        posunout a přejít do stavu 7
    BYTE        posunout a přejít do stavu 8
    DOUBLE      posunout a přejít do stavu 9
    FLOAT       posunout a přejít do stavu 10
    INT         posunout a přejít do stavu 11
    LONG        posunout a přejít do stavu 12
    SHORT       posunout a přejít do stavu 13
    SIGNED      posunout a přejít do stavu 14
    STRING      posunout a přejít do stavu 15
    UNSIGNED    posunout a přejít do stavu 16
    VOID        posunout a přejít do stavu 17
    IDENTIFIER  posunout a přejít do stavu 18

    expression  přejít do stavu 19
    type        přejít do stavu 20


State 2

    7 expression: IDENTIFIER .

    $výchozí  reduce using rule 7 (expression)


State 3

    0 $accept: file . $end

    $end  posunout a přejít do stavu 21


State 4

    1 file: . %empty  [$end]
    2     | . statement file
    2     | statement . file
    3 statement: . expression ';'
    4 expression: . expression '=' expression
    5           | . '(' type ')' expression
    6           | . '(' expression ')'
    7           | . IDENTIFIER

    '('         posunout a přejít do stavu 1
    IDENTIFIER  posunout a přejít do stavu 2

    $výchozí  reduce using rule 1 (file)

    file        přejít do stavu 22
    statement   přejít do stavu 4
    expression  přejít do stavu 5


State 5

    3 statement: expression . ';'
    4 expression: expression . '=' expression

    '='  posunout a přejít do stavu 23
    ';'  posunout a přejít do stavu 24


State 6

    9 type: AUTO .

    $výchozí  reduce using rule 9 (type)


State 7

   10 type: BOOL .

    $výchozí  reduce using rule 10 (type)


State 8

   11 type: BYTE .

    $výchozí  reduce using rule 11 (type)


State 9

   16 type: DOUBLE .

    $výchozí  reduce using rule 16 (type)


State 10

   15 type: FLOAT .

    $výchozí  reduce using rule 15 (type)


State 11

   13 type: INT .

    $výchozí  reduce using rule 13 (type)


State 12

   14 type: LONG .

    $výchozí  reduce using rule 14 (type)


State 13

   12 type: SHORT .

    $výchozí  reduce using rule 12 (type)


State 14

   17 type: SIGNED .

    $výchozí  reduce using rule 17 (type)


State 15

   19 type: STRING .

    $výchozí  reduce using rule 19 (type)


State 16

   18 type: UNSIGNED .

    $výchozí  reduce using rule 18 (type)


State 17

    8 type: VOID .

    $výchozí  reduce using rule 8 (type)


State 18

    7 expression: IDENTIFIER .  ['=', ')']
   20 type: IDENTIFIER .  [')']

    ')'         reduce using rule 7 (expression)
    ')'         [reduce using rule 20 (type)]
    $výchozí  reduce using rule 7 (expression)


State 19

    4 expression: expression . '=' expression
    6           | '(' expression . ')'

    '='  posunout a přejít do stavu 23
    ')'  posunout a přejít do stavu 25


State 20

    5 expression: '(' type . ')' expression

    ')'  posunout a přejít do stavu 26


State 21

    0 $accept: file $end .

    $výchozí  přijmout


State 22

    2 file: statement file .

    $výchozí  reduce using rule 2 (file)


State 23

    4 expression: . expression '=' expression
    4           | expression '=' . expression
    5           | . '(' type ')' expression
    6           | . '(' expression ')'
    7           | . IDENTIFIER

    '('         posunout a přejít do stavu 1
    IDENTIFIER  posunout a přejít do stavu 2

    expression  přejít do stavu 27


State 24

    3 statement: expression ';' .

    $výchozí  reduce using rule 3 (statement)


State 25

    6 expression: '(' expression ')' .

    $výchozí  reduce using rule 6 (expression)


State 26

    4 expression: . expression '=' expression
    5           | . '(' type ')' expression
    5           | '(' type ')' . expression
    6           | . '(' expression ')'
    7           | . IDENTIFIER

    '('         posunout a přejít do stavu 1
    IDENTIFIER  posunout a přejít do stavu 2

    expression  přejít do stavu 28


State 27

    4 expression: expression . '=' expression
    4           | expression '=' expression .  [';', ')']

    '='  posunout a přejít do stavu 23

    $výchozí  reduce using rule 4 (expression)

    Conflict between rule 4 and token '=' resolved as shift (%right '=').


State 28

    4 expression: expression . '=' expression
    5           | '(' type ')' expression .  ['=', ';', ')']

    $výchozí  reduce using rule 5 (expression)

    Conflict between rule 5 and token '=' resolved as reduce ('=' < CAST).
