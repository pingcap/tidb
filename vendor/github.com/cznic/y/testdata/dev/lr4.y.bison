Gramatika

    0 $accept: S $end

    1 S: T

    2 T: A B

    3 A: 'A'

    4 B: 'B'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'A' (65) 3
'B' (66) 4
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (5)
    vlevo: 0
S (6)
    vlevo: 1, vpravo: 0
T (7)
    vlevo: 2, vpravo: 1
A (8)
    vlevo: 3, vpravo: 2
B (9)
    vlevo: 4, vpravo: 2


State 0

    0 $accept: . S $end
    1 S: . T
    2 T: . A B
    3 A: . 'A'

    'A'  posunout a přejít do stavu 1

    S  přejít do stavu 2
    T  přejít do stavu 3
    A  přejít do stavu 4


State 1

    3 A: 'A' .

    $výchozí  reduce using rule 3 (A)


State 2

    0 $accept: S . $end

    $end  posunout a přejít do stavu 5


State 3

    1 S: T .

    $výchozí  reduce using rule 1 (S)


State 4

    2 T: A . B
    4 B: . 'B'

    'B'  posunout a přejít do stavu 6

    B  přejít do stavu 7


State 5

    0 $accept: S $end .

    $výchozí  přijmout


State 6

    4 B: 'B' .

    $výchozí  reduce using rule 4 (B)


State 7

    2 T: A B .

    $výchozí  reduce using rule 2 (T)
