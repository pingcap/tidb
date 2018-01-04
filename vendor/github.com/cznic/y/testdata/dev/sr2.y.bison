Gramatika

    0 $accept: E $end

    1 E: '1' E
    2  | '1'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'1' (49) 1 2
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (4)
    vlevo: 0
E (5)
    vlevo: 1 2, vpravo: 0 1


State 0

    0 $accept: . E $end
    1 E: . '1' E
    2  | . '1'

    '1'  posunout a přejít do stavu 1

    E  přejít do stavu 2


State 1

    1 E: . '1' E
    1  | '1' . E
    2  | . '1'
    2  | '1' .  [$end]

    '1'  posunout a přejít do stavu 1

    $výchozí  reduce using rule 2 (E)

    E  přejít do stavu 3


State 2

    0 $accept: E . $end

    $end  posunout a přejít do stavu 4


State 3

    1 E: '1' E .

    $výchozí  reduce using rule 1 (E)


State 4

    0 $accept: E $end .

    $výchozí  přijmout
