Gramatika

    0 $accept: E $end

    1 E: A '1'
    2  | B '2'

    3 A: '1'

    4 B: '1'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'1' (49) 1 3 4
'2' (50) 2
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (5)
    vlevo: 0
E (6)
    vlevo: 1 2, vpravo: 0
A (7)
    vlevo: 3, vpravo: 1
B (8)
    vlevo: 4, vpravo: 2


State 0

    0 $accept: . E $end
    1 E: . A '1'
    2  | . B '2'
    3 A: . '1'
    4 B: . '1'

    '1'  posunout a přejít do stavu 1

    E  přejít do stavu 2
    A  přejít do stavu 3
    B  přejít do stavu 4


State 1

    3 A: '1' .  ['1']
    4 B: '1' .  ['2']

    '2'         reduce using rule 4 (B)
    $výchozí  reduce using rule 3 (A)


State 2

    0 $accept: E . $end

    $end  posunout a přejít do stavu 5


State 3

    1 E: A . '1'

    '1'  posunout a přejít do stavu 6


State 4

    2 E: B . '2'

    '2'  posunout a přejít do stavu 7


State 5

    0 $accept: E $end .

    $výchozí  přijmout


State 6

    1 E: A '1' .

    $výchozí  reduce using rule 1 (E)


State 7

    2 E: B '2' .

    $výchozí  reduce using rule 2 (E)
