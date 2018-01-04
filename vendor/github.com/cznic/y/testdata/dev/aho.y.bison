Terminals unused in grammar

   id


Gramatika

    0 $accept: S $end

    1 S: C C

    2 C: 'c' C
    3  | 'd'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'c' (99) 2
'd' (100) 3
error (256)
id (258)


Neterminály s pravidly, ve kterých se objevují

$accept (6)
    vlevo: 0
S (7)
    vlevo: 1, vpravo: 0
C (8)
    vlevo: 2 3, vpravo: 1 2


State 0

    0 $accept: . S $end
    1 S: . C C
    2 C: . 'c' C
    3  | . 'd'

    'c'  posunout a přejít do stavu 1
    'd'  posunout a přejít do stavu 2

    S  přejít do stavu 3
    C  přejít do stavu 4


State 1

    2 C: . 'c' C
    2  | 'c' . C
    3  | . 'd'

    'c'  posunout a přejít do stavu 1
    'd'  posunout a přejít do stavu 2

    C  přejít do stavu 5


State 2

    3 C: 'd' .

    $výchozí  reduce using rule 3 (C)


State 3

    0 $accept: S . $end

    $end  posunout a přejít do stavu 6


State 4

    1 S: C . C
    2 C: . 'c' C
    3  | . 'd'

    'c'  posunout a přejít do stavu 1
    'd'  posunout a přejít do stavu 2

    C  přejít do stavu 7


State 5

    2 C: 'c' C .

    $výchozí  reduce using rule 2 (C)


State 6

    0 $accept: S $end .

    $výchozí  přijmout


State 7

    1 S: C C .

    $výchozí  reduce using rule 1 (S)
