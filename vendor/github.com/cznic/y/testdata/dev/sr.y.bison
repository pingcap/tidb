Stav 5 conflicts: 1 shift/reduce


Gramatika

    0 $accept: expr $end

    1 expr: expr '-' expr
    2     | '1'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'-' (45) 1
'1' (49) 2
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (5)
    vlevo: 0
expr (6)
    vlevo: 1 2, vpravo: 0 1


State 0

    0 $accept: . expr $end
    1 expr: . expr '-' expr
    2     | . '1'

    '1'  posunout a přejít do stavu 1

    expr  přejít do stavu 2


State 1

    2 expr: '1' .

    $výchozí  reduce using rule 2 (expr)


State 2

    0 $accept: expr . $end
    1 expr: expr . '-' expr

    $end  posunout a přejít do stavu 3
    '-'   posunout a přejít do stavu 4


State 3

    0 $accept: expr $end .

    $výchozí  přijmout


State 4

    1 expr: . expr '-' expr
    1     | expr '-' . expr
    2     | . '1'

    '1'  posunout a přejít do stavu 1

    expr  přejít do stavu 5


State 5

    1 expr: expr . '-' expr
    1     | expr '-' expr .  [$end, '-']

    '-'  posunout a přejít do stavu 4

    '-'         [reduce using rule 1 (expr)]
    $výchozí  reduce using rule 1 (expr)
