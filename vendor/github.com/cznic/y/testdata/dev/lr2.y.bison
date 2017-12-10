Stav 8 conflicts: 2 shift/reduce
Stav 9 conflicts: 2 shift/reduce


Gramatika

    0 $accept: E $end

    1 E: E '*' E
    2  | E '+' E
    3  | B

    4 B: '0'
    5  | '1'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'*' (42) 1
'+' (43) 2
'0' (48) 4
'1' (49) 5
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (7)
    vlevo: 0
E (8)
    vlevo: 1 2 3, vpravo: 0 1 2
B (9)
    vlevo: 4 5, vpravo: 3


State 0

    0 $accept: . E $end
    1 E: . E '*' E
    2  | . E '+' E
    3  | . B
    4 B: . '0'
    5  | . '1'

    '0'  posunout a přejít do stavu 1
    '1'  posunout a přejít do stavu 2

    E  přejít do stavu 3
    B  přejít do stavu 4


State 1

    4 B: '0' .

    $výchozí  reduce using rule 4 (B)


State 2

    5 B: '1' .

    $výchozí  reduce using rule 5 (B)


State 3

    0 $accept: E . $end
    1 E: E . '*' E
    2  | E . '+' E

    $end  posunout a přejít do stavu 5
    '*'   posunout a přejít do stavu 6
    '+'   posunout a přejít do stavu 7


State 4

    3 E: B .

    $výchozí  reduce using rule 3 (E)


State 5

    0 $accept: E $end .

    $výchozí  přijmout


State 6

    1 E: . E '*' E
    1  | E '*' . E
    2  | . E '+' E
    3  | . B
    4 B: . '0'
    5  | . '1'

    '0'  posunout a přejít do stavu 1
    '1'  posunout a přejít do stavu 2

    E  přejít do stavu 8
    B  přejít do stavu 4


State 7

    1 E: . E '*' E
    2  | . E '+' E
    2  | E '+' . E
    3  | . B
    4 B: . '0'
    5  | . '1'

    '0'  posunout a přejít do stavu 1
    '1'  posunout a přejít do stavu 2

    E  přejít do stavu 9
    B  přejít do stavu 4


State 8

    1 E: E . '*' E
    1  | E '*' E .  [$end, '*', '+']
    2  | E . '+' E

    '*'  posunout a přejít do stavu 6
    '+'  posunout a přejít do stavu 7

    '*'         [reduce using rule 1 (E)]
    '+'         [reduce using rule 1 (E)]
    $výchozí  reduce using rule 1 (E)


State 9

    1 E: E . '*' E
    2  | E . '+' E
    2  | E '+' E .  [$end, '*', '+']

    '*'  posunout a přejít do stavu 6
    '+'  posunout a přejít do stavu 7

    '*'         [reduce using rule 2 (E)]
    '+'         [reduce using rule 2 (E)]
    $výchozí  reduce using rule 2 (E)
