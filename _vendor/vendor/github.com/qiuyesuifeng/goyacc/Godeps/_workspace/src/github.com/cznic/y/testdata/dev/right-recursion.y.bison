Gramatika

    0 $accept: S $end

    1 S: %empty
    2  | 'a' S


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'a' (97) 2
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (4)
    vlevo: 0
S (5)
    vlevo: 1 2, vpravo: 0 2


State 0

    0 $accept: . S $end
    1 S: . %empty  [$end]
    2  | . 'a' S

    'a'  posunout a přejít do stavu 1

    $výchozí  reduce using rule 1 (S)

    S  přejít do stavu 2


State 1

    1 S: . %empty  [$end]
    2  | . 'a' S
    2  | 'a' . S

    'a'  posunout a přejít do stavu 1

    $výchozí  reduce using rule 1 (S)

    S  přejít do stavu 3


State 2

    0 $accept: S . $end

    $end  posunout a přejít do stavu 4


State 3

    2 S: 'a' S .

    $výchozí  reduce using rule 2 (S)


State 4

    0 $accept: S $end .

    $výchozí  přijmout
