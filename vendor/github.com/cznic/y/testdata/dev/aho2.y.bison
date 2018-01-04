Gramatika

    0 $accept: S $end

    1 S: L '=' R
    2  | R

    3 L: '*' R
    4  | id

    5 R: L


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'*' (42) 3
'=' (61) 1
error (256)
id (258) 4


Neterminály s pravidly, ve kterých se objevují

$accept (6)
    vlevo: 0
S (7)
    vlevo: 1 2, vpravo: 0
L (8)
    vlevo: 3 4, vpravo: 1 5
R (9)
    vlevo: 5, vpravo: 1 2 3


State 0

    0 $accept: . S $end
    1 S: . L '=' R
    2  | . R
    3 L: . '*' R
    4  | . id
    5 R: . L

    id   posunout a přejít do stavu 1
    '*'  posunout a přejít do stavu 2

    S  přejít do stavu 3
    L  přejít do stavu 4
    R  přejít do stavu 5


State 1

    4 L: id .

    $výchozí  reduce using rule 4 (L)


State 2

    3 L: . '*' R
    3  | '*' . R
    4  | . id
    5 R: . L

    id   posunout a přejít do stavu 1
    '*'  posunout a přejít do stavu 2

    L  přejít do stavu 6
    R  přejít do stavu 7


State 3

    0 $accept: S . $end

    $end  posunout a přejít do stavu 8


State 4

    1 S: L . '=' R
    5 R: L .  [$end]

    '='  posunout a přejít do stavu 9

    $výchozí  reduce using rule 5 (R)


State 5

    2 S: R .

    $výchozí  reduce using rule 2 (S)


State 6

    5 R: L .

    $výchozí  reduce using rule 5 (R)


State 7

    3 L: '*' R .

    $výchozí  reduce using rule 3 (L)


State 8

    0 $accept: S $end .

    $výchozí  přijmout


State 9

    1 S: L '=' . R
    3 L: . '*' R
    4  | . id
    5 R: . L

    id   posunout a přejít do stavu 1
    '*'  posunout a přejít do stavu 2

    L  přejít do stavu 6
    R  přejít do stavu 10


State 10

    1 S: L '=' R .

    $výchozí  reduce using rule 1 (S)
