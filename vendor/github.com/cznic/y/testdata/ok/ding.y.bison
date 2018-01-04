Gramatika

    0 $accept: rhyme $end

    1 rhyme: sound place

    2 sound: DING DONG

    3 place: DELL


Terminály s pravidly, ve kterých se objevují

$end (0) 0
error (256)
DING (258) 2
DONG (259) 2
DELL (260) 3


Neterminály s pravidly, ve kterých se objevují

$accept (6)
    vlevo: 0
rhyme (7)
    vlevo: 1, vpravo: 0
sound (8)
    vlevo: 2, vpravo: 1
place (9)
    vlevo: 3, vpravo: 1


State 0

    0 $accept: . rhyme $end
    1 rhyme: . sound place
    2 sound: . DING DONG

    DING  posunout a přejít do stavu 1

    rhyme  přejít do stavu 2
    sound  přejít do stavu 3


State 1

    2 sound: DING . DONG

    DONG  posunout a přejít do stavu 4


State 2

    0 $accept: rhyme . $end

    $end  posunout a přejít do stavu 5


State 3

    1 rhyme: sound . place
    3 place: . DELL

    DELL  posunout a přejít do stavu 6

    place  přejít do stavu 7


State 4

    2 sound: DING DONG .

    $výchozí  reduce using rule 2 (sound)


State 5

    0 $accept: rhyme $end .

    $výchozí  přijmout


State 6

    3 place: DELL .

    $výchozí  reduce using rule 3 (place)


State 7

    1 rhyme: sound place .

    $výchozí  reduce using rule 1 (rhyme)
