Gramatika

    0 $accept: S $end

    1 S: %empty
    2  | 'a' S 'z'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'a' (97) 2
'z' (122) 2
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (5)
    vlevo: 0
S (6)
    vlevo: 1 2, vpravo: 0 2


State 0

    0 $accept: . S $end
    1 S: . %empty  [$end]
    2  | . 'a' S 'z'

    'a'  posunout a přejít do stavu 1

    $výchozí  reduce using rule 1 (S)

    S  přejít do stavu 2


State 1

    1 S: . %empty  ['z']
    2  | . 'a' S 'z'
    2  | 'a' . S 'z'

    'a'  posunout a přejít do stavu 1

    $výchozí  reduce using rule 1 (S)

    S  přejít do stavu 3


State 2

    0 $accept: S . $end

    $end  posunout a přejít do stavu 4


State 3

    2 S: 'a' S . 'z'

    'z'  posunout a přejít do stavu 5


State 4

    0 $accept: S $end .

    $výchozí  přijmout


State 5

    2 S: 'a' S 'z' .

    $výchozí  reduce using rule 2 (S)
