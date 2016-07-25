Gramatika

    0 $accept: E $end

    1 E: T E2

    2 E2: '+' T E2
    3   | %empty

    4 T: F T2

    5 T2: '*' F T2
    6   | %empty

    7 F: '(' E ')'
    8  | id


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'(' (40) 7
')' (41) 7
'*' (42) 5
'+' (43) 2
error (256)
id (258) 8


Neterminály s pravidly, ve kterých se objevují

$accept (8)
    vlevo: 0
E (9)
    vlevo: 1, vpravo: 0 7
E2 (10)
    vlevo: 2 3, vpravo: 1 2
T (11)
    vlevo: 4, vpravo: 1 2
T2 (12)
    vlevo: 5 6, vpravo: 4 5
F (13)
    vlevo: 7 8, vpravo: 4 5


State 0

    0 $accept: . E $end
    1 E: . T E2
    4 T: . F T2
    7 F: . '(' E ')'
    8  | . id

    id   posunout a přejít do stavu 1
    '('  posunout a přejít do stavu 2

    E  přejít do stavu 3
    T  přejít do stavu 4
    F  přejít do stavu 5


State 1

    8 F: id .

    $výchozí  reduce using rule 8 (F)


State 2

    1 E: . T E2
    4 T: . F T2
    7 F: . '(' E ')'
    7  | '(' . E ')'
    8  | . id

    id   posunout a přejít do stavu 1
    '('  posunout a přejít do stavu 2

    E  přejít do stavu 6
    T  přejít do stavu 4
    F  přejít do stavu 5


State 3

    0 $accept: E . $end

    $end  posunout a přejít do stavu 7


State 4

    1 E: T . E2
    2 E2: . '+' T E2
    3   | . %empty  [$end, ')']

    '+'  posunout a přejít do stavu 8

    $výchozí  reduce using rule 3 (E2)

    E2  přejít do stavu 9


State 5

    4 T: F . T2
    5 T2: . '*' F T2
    6   | . %empty  [$end, '+', ')']

    '*'  posunout a přejít do stavu 10

    $výchozí  reduce using rule 6 (T2)

    T2  přejít do stavu 11


State 6

    7 F: '(' E . ')'

    ')'  posunout a přejít do stavu 12


State 7

    0 $accept: E $end .

    $výchozí  přijmout


State 8

    2 E2: '+' . T E2
    4 T: . F T2
    7 F: . '(' E ')'
    8  | . id

    id   posunout a přejít do stavu 1
    '('  posunout a přejít do stavu 2

    T  přejít do stavu 13
    F  přejít do stavu 5


State 9

    1 E: T E2 .

    $výchozí  reduce using rule 1 (E)


State 10

    5 T2: '*' . F T2
    7 F: . '(' E ')'
    8  | . id

    id   posunout a přejít do stavu 1
    '('  posunout a přejít do stavu 2

    F  přejít do stavu 14


State 11

    4 T: F T2 .

    $výchozí  reduce using rule 4 (T)


State 12

    7 F: '(' E ')' .

    $výchozí  reduce using rule 7 (F)


State 13

    2 E2: . '+' T E2
    2   | '+' T . E2
    3   | . %empty  [$end, ')']

    '+'  posunout a přejít do stavu 8

    $výchozí  reduce using rule 3 (E2)

    E2  přejít do stavu 15


State 14

    5 T2: . '*' F T2
    5   | '*' F . T2
    6   | . %empty  [$end, '+', ')']

    '*'  posunout a přejít do stavu 10

    $výchozí  reduce using rule 6 (T2)

    T2  přejít do stavu 16


State 15

    2 E2: '+' T E2 .

    $výchozí  reduce using rule 2 (E2)


State 16

    5 T2: '*' F T2 .

    $výchozí  reduce using rule 5 (T2)
