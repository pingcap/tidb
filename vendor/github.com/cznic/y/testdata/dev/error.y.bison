Gramatika

    0 $accept: program $end

    1 program: program statement '\n'
    2        | program error '\n'
    3        | %empty

    4 statement: expression
    5          | VARIABLE '=' expression

    6 expression: INTEGER
    7           | VARIABLE
    8           | expression '+' expression
    9           | expression '-' expression
   10           | expression '*' expression
   11           | expression '/' expression
   12           | '(' expression ')'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'\n' (10) 1 2
'(' (40) 12
')' (41) 12
'*' (42) 10
'+' (43) 8
'-' (45) 9
'/' (47) 11
'=' (61) 5
error (256) 2
INTEGER (258) 6
VARIABLE (259) 5 7


Neterminály s pravidly, ve kterých se objevují

$accept (13)
    vlevo: 0
program (14)
    vlevo: 1 2 3, vpravo: 0 1 2
statement (15)
    vlevo: 4 5, vpravo: 1
expression (16)
    vlevo: 6 7 8 9 10 11 12, vpravo: 4 5 8 9 10 11 12


State 0

    0 $accept: . program $end
    1 program: . program statement '\n'
    2        | . program error '\n'
    3        | . %empty

    $výchozí  reduce using rule 3 (program)

    program  přejít do stavu 1


State 1

    0 $accept: program . $end
    1 program: program . statement '\n'
    2        | program . error '\n'
    4 statement: . expression
    5          | . VARIABLE '=' expression
    6 expression: . INTEGER
    7           | . VARIABLE
    8           | . expression '+' expression
    9           | . expression '-' expression
   10           | . expression '*' expression
   11           | . expression '/' expression
   12           | . '(' expression ')'

    $end      posunout a přejít do stavu 2
    error     posunout a přejít do stavu 3
    INTEGER   posunout a přejít do stavu 4
    VARIABLE  posunout a přejít do stavu 5
    '('       posunout a přejít do stavu 6

    statement   přejít do stavu 7
    expression  přejít do stavu 8


State 2

    0 $accept: program $end .

    $výchozí  přijmout


State 3

    2 program: program error . '\n'

    '\n'  posunout a přejít do stavu 9


State 4

    6 expression: INTEGER .

    $výchozí  reduce using rule 6 (expression)


State 5

    5 statement: VARIABLE . '=' expression
    7 expression: VARIABLE .  ['+', '-', '*', '/', '\n']

    '='  posunout a přejít do stavu 10

    $výchozí  reduce using rule 7 (expression)


State 6

    6 expression: . INTEGER
    7           | . VARIABLE
    8           | . expression '+' expression
    9           | . expression '-' expression
   10           | . expression '*' expression
   11           | . expression '/' expression
   12           | . '(' expression ')'
   12           | '(' . expression ')'

    INTEGER   posunout a přejít do stavu 4
    VARIABLE  posunout a přejít do stavu 11
    '('       posunout a přejít do stavu 6

    expression  přejít do stavu 12


State 7

    1 program: program statement . '\n'

    '\n'  posunout a přejít do stavu 13


State 8

    4 statement: expression .  ['\n']
    8 expression: expression . '+' expression
    9           | expression . '-' expression
   10           | expression . '*' expression
   11           | expression . '/' expression

    '+'  posunout a přejít do stavu 14
    '-'  posunout a přejít do stavu 15
    '*'  posunout a přejít do stavu 16
    '/'  posunout a přejít do stavu 17

    $výchozí  reduce using rule 4 (statement)


State 9

    2 program: program error '\n' .

    $výchozí  reduce using rule 2 (program)


State 10

    5 statement: VARIABLE '=' . expression
    6 expression: . INTEGER
    7           | . VARIABLE
    8           | . expression '+' expression
    9           | . expression '-' expression
   10           | . expression '*' expression
   11           | . expression '/' expression
   12           | . '(' expression ')'

    INTEGER   posunout a přejít do stavu 4
    VARIABLE  posunout a přejít do stavu 11
    '('       posunout a přejít do stavu 6

    expression  přejít do stavu 18


State 11

    7 expression: VARIABLE .

    $výchozí  reduce using rule 7 (expression)


State 12

    8 expression: expression . '+' expression
    9           | expression . '-' expression
   10           | expression . '*' expression
   11           | expression . '/' expression
   12           | '(' expression . ')'

    '+'  posunout a přejít do stavu 14
    '-'  posunout a přejít do stavu 15
    '*'  posunout a přejít do stavu 16
    '/'  posunout a přejít do stavu 17
    ')'  posunout a přejít do stavu 19


State 13

    1 program: program statement '\n' .

    $výchozí  reduce using rule 1 (program)


State 14

    6 expression: . INTEGER
    7           | . VARIABLE
    8           | . expression '+' expression
    8           | expression '+' . expression
    9           | . expression '-' expression
   10           | . expression '*' expression
   11           | . expression '/' expression
   12           | . '(' expression ')'

    INTEGER   posunout a přejít do stavu 4
    VARIABLE  posunout a přejít do stavu 11
    '('       posunout a přejít do stavu 6

    expression  přejít do stavu 20


State 15

    6 expression: . INTEGER
    7           | . VARIABLE
    8           | . expression '+' expression
    9           | . expression '-' expression
    9           | expression '-' . expression
   10           | . expression '*' expression
   11           | . expression '/' expression
   12           | . '(' expression ')'

    INTEGER   posunout a přejít do stavu 4
    VARIABLE  posunout a přejít do stavu 11
    '('       posunout a přejít do stavu 6

    expression  přejít do stavu 21


State 16

    6 expression: . INTEGER
    7           | . VARIABLE
    8           | . expression '+' expression
    9           | . expression '-' expression
   10           | . expression '*' expression
   10           | expression '*' . expression
   11           | . expression '/' expression
   12           | . '(' expression ')'

    INTEGER   posunout a přejít do stavu 4
    VARIABLE  posunout a přejít do stavu 11
    '('       posunout a přejít do stavu 6

    expression  přejít do stavu 22


State 17

    6 expression: . INTEGER
    7           | . VARIABLE
    8           | . expression '+' expression
    9           | . expression '-' expression
   10           | . expression '*' expression
   11           | . expression '/' expression
   11           | expression '/' . expression
   12           | . '(' expression ')'

    INTEGER   posunout a přejít do stavu 4
    VARIABLE  posunout a přejít do stavu 11
    '('       posunout a přejít do stavu 6

    expression  přejít do stavu 23


State 18

    5 statement: VARIABLE '=' expression .  ['\n']
    8 expression: expression . '+' expression
    9           | expression . '-' expression
   10           | expression . '*' expression
   11           | expression . '/' expression

    '+'  posunout a přejít do stavu 14
    '-'  posunout a přejít do stavu 15
    '*'  posunout a přejít do stavu 16
    '/'  posunout a přejít do stavu 17

    $výchozí  reduce using rule 5 (statement)


State 19

   12 expression: '(' expression ')' .

    $výchozí  reduce using rule 12 (expression)


State 20

    8 expression: expression . '+' expression
    8           | expression '+' expression .  ['+', '-', '\n', ')']
    9           | expression . '-' expression
   10           | expression . '*' expression
   11           | expression . '/' expression

    '*'  posunout a přejít do stavu 16
    '/'  posunout a přejít do stavu 17

    $výchozí  reduce using rule 8 (expression)

    Conflict between rule 8 and token '+' resolved as reduce (%left '+').
    Conflict between rule 8 and token '-' resolved as reduce (%left '-').
    Conflict between rule 8 and token '*' resolved as shift ('+' < '*').
    Conflict between rule 8 and token '/' resolved as shift ('+' < '/').


State 21

    8 expression: expression . '+' expression
    9           | expression . '-' expression
    9           | expression '-' expression .  ['+', '-', '\n', ')']
   10           | expression . '*' expression
   11           | expression . '/' expression

    '*'  posunout a přejít do stavu 16
    '/'  posunout a přejít do stavu 17

    $výchozí  reduce using rule 9 (expression)

    Conflict between rule 9 and token '+' resolved as reduce (%left '+').
    Conflict between rule 9 and token '-' resolved as reduce (%left '-').
    Conflict between rule 9 and token '*' resolved as shift ('-' < '*').
    Conflict between rule 9 and token '/' resolved as shift ('-' < '/').


State 22

    8 expression: expression . '+' expression
    9           | expression . '-' expression
   10           | expression . '*' expression
   10           | expression '*' expression .  ['+', '-', '*', '/', '\n', ')']
   11           | expression . '/' expression

    $výchozí  reduce using rule 10 (expression)

    Conflict between rule 10 and token '+' resolved as reduce ('+' < '*').
    Conflict between rule 10 and token '-' resolved as reduce ('-' < '*').
    Conflict between rule 10 and token '*' resolved as reduce (%left '*').
    Conflict between rule 10 and token '/' resolved as reduce (%left '/').


State 23

    8 expression: expression . '+' expression
    9           | expression . '-' expression
   10           | expression . '*' expression
   11           | expression . '/' expression
   11           | expression '/' expression .  ['+', '-', '*', '/', '\n', ')']

    $výchozí  reduce using rule 11 (expression)

    Conflict between rule 11 and token '+' resolved as reduce ('+' < '/').
    Conflict between rule 11 and token '-' resolved as reduce ('-' < '/').
    Conflict between rule 11 and token '*' resolved as reduce (%left '*').
    Conflict between rule 11 and token '/' resolved as reduce (%left '/').
