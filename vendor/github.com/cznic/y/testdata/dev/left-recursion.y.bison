Gramatika

    0 $accept: S $end

    1 S: %empty
    2  | S 'z'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'z' (122) 2
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (4)
    vlevo: 0
S (5)
    vlevo: 1 2, vpravo: 0 2


State 0

    0 $accept: . S $end
    1 S: . %empty
    2  | . S 'z'

    $výchozí  reduce using rule 1 (S)

    S  přejít do stavu 1


State 1

    0 $accept: S . $end
    2 S: S . 'z'

    $end  posunout a přejít do stavu 2
    'z'   posunout a přejít do stavu 3


State 2

    0 $accept: S $end .

    $výchozí  přijmout


State 3

    2 S: S 'z' .

    $výchozí  reduce using rule 2 (S)
