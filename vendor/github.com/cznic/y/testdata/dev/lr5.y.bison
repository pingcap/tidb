Rules useless in parser due to conflicts

    6 F: 'e'


Stav 4 conflicts: 2 reduce/reduce


Gramatika

    0 $accept: S $end

    1 S: 'a' E 'c'
    2  | 'a' F 'd'
    3  | 'b' F 'c'
    4  | 'b' E 'd'

    5 E: 'e'

    6 F: 'e'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'a' (97) 1 2
'b' (98) 3 4
'c' (99) 1 3
'd' (100) 2 4
'e' (101) 5 6
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (8)
    vlevo: 0
S (9)
    vlevo: 1 2 3 4, vpravo: 0
E (10)
    vlevo: 5, vpravo: 1 4
F (11)
    vlevo: 6, vpravo: 2 3


State 0

    0 $accept: . S $end
    1 S: . 'a' E 'c'
    2  | . 'a' F 'd'
    3  | . 'b' F 'c'
    4  | . 'b' E 'd'

    'a'  posunout a přejít do stavu 1
    'b'  posunout a přejít do stavu 2

    S  přejít do stavu 3


State 1

    1 S: 'a' . E 'c'
    2  | 'a' . F 'd'
    5 E: . 'e'
    6 F: . 'e'

    'e'  posunout a přejít do stavu 4

    E  přejít do stavu 5
    F  přejít do stavu 6


State 2

    3 S: 'b' . F 'c'
    4  | 'b' . E 'd'
    5 E: . 'e'
    6 F: . 'e'

    'e'  posunout a přejít do stavu 4

    E  přejít do stavu 7
    F  přejít do stavu 8


State 3

    0 $accept: S . $end

    $end  posunout a přejít do stavu 9


State 4

    5 E: 'e' .  ['c', 'd']
    6 F: 'e' .  ['c', 'd']

    'c'         reduce using rule 5 (E)
    'c'         [reduce using rule 6 (F)]
    'd'         reduce using rule 5 (E)
    'd'         [reduce using rule 6 (F)]
    $výchozí  reduce using rule 5 (E)


State 5

    1 S: 'a' E . 'c'

    'c'  posunout a přejít do stavu 10


State 6

    2 S: 'a' F . 'd'

    'd'  posunout a přejít do stavu 11


State 7

    4 S: 'b' E . 'd'

    'd'  posunout a přejít do stavu 12


State 8

    3 S: 'b' F . 'c'

    'c'  posunout a přejít do stavu 13


State 9

    0 $accept: S $end .

    $výchozí  přijmout


State 10

    1 S: 'a' E 'c' .

    $výchozí  reduce using rule 1 (S)


State 11

    2 S: 'a' F 'd' .

    $výchozí  reduce using rule 2 (S)


State 12

    4 S: 'b' E 'd' .

    $výchozí  reduce using rule 4 (S)


State 13

    3 S: 'b' F 'c' .

    $výchozí  reduce using rule 3 (S)
