Gramatika

    0 $accept: S $end

    1 S: L '=' R

    2 L: '*' R
    3  | id

    4 R: L


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'*' (42) 2
'=' (61) 1
error (256)
id (258) 3


Neterminály s pravidly, ve kterých se objevují

$accept (6)
    vlevo: 0
S (7)
    vlevo: 1, vpravo: 0
L (8)
    vlevo: 2 3, vpravo: 1 4
R (9)
    vlevo: 4, vpravo: 1 2


State 0

    0 $accept: . S $end
    1 S: . L '=' R
    2 L: . '*' R
    3  | . id

    id   posunout a přejít do stavu 1
    '*'  posunout a přejít do stavu 2

    S  přejít do stavu 3
    L  přejít do stavu 4


State 1

    3 L: id .

    $výchozí  reduce using rule 3 (L)


State 2

    2 L: . '*' R
    2  | '*' . R
    3  | . id
    4 R: . L

    id   posunout a přejít do stavu 1
    '*'  posunout a přejít do stavu 2

    L  přejít do stavu 5
    R  přejít do stavu 6


State 3

    0 $accept: S . $end

    $end  posunout a přejít do stavu 7


State 4

    1 S: L . '=' R

    '='  posunout a přejít do stavu 8


State 5

    4 R: L .

    $výchozí  reduce using rule 4 (R)


State 6

    2 L: '*' R .

    $výchozí  reduce using rule 2 (L)


State 7

    0 $accept: S $end .

    $výchozí  přijmout


State 8

    1 S: L '=' . R
    2 L: . '*' R
    3  | . id
    4 R: . L

    id   posunout a přejít do stavu 1
    '*'  posunout a přejít do stavu 2

    L  přejít do stavu 5
    R  přejít do stavu 9


State 9

    1 S: L '=' R .

    $výchozí  reduce using rule 1 (S)
