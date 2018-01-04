Gramatika

    0 $accept: start $end

    1 start: start expr
    2      | %empty

    3 expr: NR
    4     | expr '+' expr


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'+' (43) 4
error (256)
NR (258) 3


Neterminály s pravidly, ve kterých se objevují

$accept (5)
    vlevo: 0
start (6)
    vlevo: 1 2, vpravo: 0 1
expr (7)
    vlevo: 3 4, vpravo: 1 4


State 0

    0 $accept: . start $end
    1 start: . start expr
    2      | . %empty

    $výchozí  reduce using rule 2 (start)

    start  přejít do stavu 1


State 1

    0 $accept: start . $end
    1 start: start . expr
    3 expr: . NR
    4     | . expr '+' expr

    $end  posunout a přejít do stavu 2
    NR    posunout a přejít do stavu 3

    expr  přejít do stavu 4


State 2

    0 $accept: start $end .

    $výchozí  přijmout


State 3

    3 expr: NR .

    $výchozí  reduce using rule 3 (expr)


State 4

    1 start: start expr .  [$end, NR]
    4 expr: expr . '+' expr

    '+'  posunout a přejít do stavu 5

    $výchozí  reduce using rule 1 (start)


State 5

    3 expr: . NR
    4     | . expr '+' expr
    4     | expr '+' . expr

    NR  posunout a přejít do stavu 3

    expr  přejít do stavu 6


State 6

    4 expr: expr . '+' expr
    4     | expr '+' expr .  [$end, NR, '+']

    $výchozí  reduce using rule 4 (expr)

    Conflict between rule 4 and token '+' resolved as reduce (%left '+').
