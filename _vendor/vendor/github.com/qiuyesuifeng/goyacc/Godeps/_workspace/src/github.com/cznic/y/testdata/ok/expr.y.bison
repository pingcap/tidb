Gramatika

    0 $accept: top $end

    1 top: expr

    2 expr: expr1
    3     | '+' expr
    4     | '-' expr

    5 expr1: expr2
    6      | expr1 '+' expr2
    7      | expr1 '-' expr2

    8 expr2: expr3
    9      | expr2 '*' expr3
   10      | expr2 '/' expr3

   11 expr3: NUM
   12      | '(' expr ')'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'(' (40) 12
')' (41) 12
'*' (42) 9
'+' (43) 3 6
'-' (45) 4 7
'/' (47) 10
error (256)
NUM (258) 11


Neterminály s pravidly, ve kterých se objevují

$accept (10)
    vlevo: 0
top (11)
    vlevo: 1, vpravo: 0
expr (12)
    vlevo: 2 3 4, vpravo: 1 3 4 12
expr1 (13)
    vlevo: 5 6 7, vpravo: 2 6 7
expr2 (14)
    vlevo: 8 9 10, vpravo: 5 6 7 9 10
expr3 (15)
    vlevo: 11 12, vpravo: 8 9 10


State 0

    0 $accept: . top $end
    1 top: . expr
    2 expr: . expr1
    3     | . '+' expr
    4     | . '-' expr
    5 expr1: . expr2
    6      | . expr1 '+' expr2
    7      | . expr1 '-' expr2
    8 expr2: . expr3
    9      | . expr2 '*' expr3
   10      | . expr2 '/' expr3
   11 expr3: . NUM
   12      | . '(' expr ')'

    NUM  posunout a přejít do stavu 1
    '+'  posunout a přejít do stavu 2
    '-'  posunout a přejít do stavu 3
    '('  posunout a přejít do stavu 4

    top    přejít do stavu 5
    expr   přejít do stavu 6
    expr1  přejít do stavu 7
    expr2  přejít do stavu 8
    expr3  přejít do stavu 9


State 1

   11 expr3: NUM .

    $výchozí  reduce using rule 11 (expr3)


State 2

    2 expr: . expr1
    3     | . '+' expr
    3     | '+' . expr
    4     | . '-' expr
    5 expr1: . expr2
    6      | . expr1 '+' expr2
    7      | . expr1 '-' expr2
    8 expr2: . expr3
    9      | . expr2 '*' expr3
   10      | . expr2 '/' expr3
   11 expr3: . NUM
   12      | . '(' expr ')'

    NUM  posunout a přejít do stavu 1
    '+'  posunout a přejít do stavu 2
    '-'  posunout a přejít do stavu 3
    '('  posunout a přejít do stavu 4

    expr   přejít do stavu 10
    expr1  přejít do stavu 7
    expr2  přejít do stavu 8
    expr3  přejít do stavu 9


State 3

    2 expr: . expr1
    3     | . '+' expr
    4     | . '-' expr
    4     | '-' . expr
    5 expr1: . expr2
    6      | . expr1 '+' expr2
    7      | . expr1 '-' expr2
    8 expr2: . expr3
    9      | . expr2 '*' expr3
   10      | . expr2 '/' expr3
   11 expr3: . NUM
   12      | . '(' expr ')'

    NUM  posunout a přejít do stavu 1
    '+'  posunout a přejít do stavu 2
    '-'  posunout a přejít do stavu 3
    '('  posunout a přejít do stavu 4

    expr   přejít do stavu 11
    expr1  přejít do stavu 7
    expr2  přejít do stavu 8
    expr3  přejít do stavu 9


State 4

    2 expr: . expr1
    3     | . '+' expr
    4     | . '-' expr
    5 expr1: . expr2
    6      | . expr1 '+' expr2
    7      | . expr1 '-' expr2
    8 expr2: . expr3
    9      | . expr2 '*' expr3
   10      | . expr2 '/' expr3
   11 expr3: . NUM
   12      | . '(' expr ')'
   12      | '(' . expr ')'

    NUM  posunout a přejít do stavu 1
    '+'  posunout a přejít do stavu 2
    '-'  posunout a přejít do stavu 3
    '('  posunout a přejít do stavu 4

    expr   přejít do stavu 12
    expr1  přejít do stavu 7
    expr2  přejít do stavu 8
    expr3  přejít do stavu 9


State 5

    0 $accept: top . $end

    $end  posunout a přejít do stavu 13


State 6

    1 top: expr .

    $výchozí  reduce using rule 1 (top)


State 7

    2 expr: expr1 .  [$end, ')']
    6 expr1: expr1 . '+' expr2
    7      | expr1 . '-' expr2

    '+'  posunout a přejít do stavu 14
    '-'  posunout a přejít do stavu 15

    $výchozí  reduce using rule 2 (expr)


State 8

    5 expr1: expr2 .  [$end, '+', '-', ')']
    9 expr2: expr2 . '*' expr3
   10      | expr2 . '/' expr3

    '*'  posunout a přejít do stavu 16
    '/'  posunout a přejít do stavu 17

    $výchozí  reduce using rule 5 (expr1)


State 9

    8 expr2: expr3 .

    $výchozí  reduce using rule 8 (expr2)


State 10

    3 expr: '+' expr .

    $výchozí  reduce using rule 3 (expr)


State 11

    4 expr: '-' expr .

    $výchozí  reduce using rule 4 (expr)


State 12

   12 expr3: '(' expr . ')'

    ')'  posunout a přejít do stavu 18


State 13

    0 $accept: top $end .

    $výchozí  přijmout


State 14

    6 expr1: expr1 '+' . expr2
    8 expr2: . expr3
    9      | . expr2 '*' expr3
   10      | . expr2 '/' expr3
   11 expr3: . NUM
   12      | . '(' expr ')'

    NUM  posunout a přejít do stavu 1
    '('  posunout a přejít do stavu 4

    expr2  přejít do stavu 19
    expr3  přejít do stavu 9


State 15

    7 expr1: expr1 '-' . expr2
    8 expr2: . expr3
    9      | . expr2 '*' expr3
   10      | . expr2 '/' expr3
   11 expr3: . NUM
   12      | . '(' expr ')'

    NUM  posunout a přejít do stavu 1
    '('  posunout a přejít do stavu 4

    expr2  přejít do stavu 20
    expr3  přejít do stavu 9


State 16

    9 expr2: expr2 '*' . expr3
   11 expr3: . NUM
   12      | . '(' expr ')'

    NUM  posunout a přejít do stavu 1
    '('  posunout a přejít do stavu 4

    expr3  přejít do stavu 21


State 17

   10 expr2: expr2 '/' . expr3
   11 expr3: . NUM
   12      | . '(' expr ')'

    NUM  posunout a přejít do stavu 1
    '('  posunout a přejít do stavu 4

    expr3  přejít do stavu 22


State 18

   12 expr3: '(' expr ')' .

    $výchozí  reduce using rule 12 (expr3)


State 19

    6 expr1: expr1 '+' expr2 .  [$end, '+', '-', ')']
    9 expr2: expr2 . '*' expr3
   10      | expr2 . '/' expr3

    '*'  posunout a přejít do stavu 16
    '/'  posunout a přejít do stavu 17

    $výchozí  reduce using rule 6 (expr1)


State 20

    7 expr1: expr1 '-' expr2 .  [$end, '+', '-', ')']
    9 expr2: expr2 . '*' expr3
   10      | expr2 . '/' expr3

    '*'  posunout a přejít do stavu 16
    '/'  posunout a přejít do stavu 17

    $výchozí  reduce using rule 7 (expr1)


State 21

    9 expr2: expr2 '*' expr3 .

    $výchozí  reduce using rule 9 (expr2)


State 22

   10 expr2: expr2 '/' expr3 .

    $výchozí  reduce using rule 10 (expr2)
