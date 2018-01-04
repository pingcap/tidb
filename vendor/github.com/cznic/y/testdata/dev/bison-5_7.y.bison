Stav 1 conflicts: 1 reduce/reduce


Gramatika

    0 $accept: def $end

    1 def: param_spec return_spec ','

    2 param_spec: type
    3           | name_list ':' type

    4 return_spec: type
    5            | name ':' type

    6 type: id

    7 name: id

    8 name_list: name
    9          | name ',' name_list


Terminály s pravidly, ve kterých se objevují

$end (0) 0
',' (44) 1 9
':' (58) 3 5
error (256)
id (258) 6 7


Neterminály s pravidly, ve kterých se objevují

$accept (6)
    vlevo: 0
def (7)
    vlevo: 1, vpravo: 0
param_spec (8)
    vlevo: 2 3, vpravo: 1
return_spec (9)
    vlevo: 4 5, vpravo: 1
type (10)
    vlevo: 6, vpravo: 2 3 4 5
name (11)
    vlevo: 7, vpravo: 5 8 9
name_list (12)
    vlevo: 8 9, vpravo: 3 9


State 0

    0 $accept: . def $end
    1 def: . param_spec return_spec ','
    2 param_spec: . type
    3           | . name_list ':' type
    6 type: . id
    7 name: . id
    8 name_list: . name
    9          | . name ',' name_list

    id  posunout a přejít do stavu 1

    def         přejít do stavu 2
    param_spec  přejít do stavu 3
    type        přejít do stavu 4
    name        přejít do stavu 5
    name_list   přejít do stavu 6


State 1

    6 type: id .  [id, ',']
    7 name: id .  [',', ':']

    ','         reduce using rule 6 (type)
    ','         [reduce using rule 7 (name)]
    ':'         reduce using rule 7 (name)
    $výchozí  reduce using rule 6 (type)


State 2

    0 $accept: def . $end

    $end  posunout a přejít do stavu 7


State 3

    1 def: param_spec . return_spec ','
    4 return_spec: . type
    5            | . name ':' type
    6 type: . id
    7 name: . id

    id  posunout a přejít do stavu 1

    return_spec  přejít do stavu 8
    type         přejít do stavu 9
    name         přejít do stavu 10


State 4

    2 param_spec: type .

    $výchozí  reduce using rule 2 (param_spec)


State 5

    8 name_list: name .  [':']
    9          | name . ',' name_list

    ','  posunout a přejít do stavu 11

    $výchozí  reduce using rule 8 (name_list)


State 6

    3 param_spec: name_list . ':' type

    ':'  posunout a přejít do stavu 12


State 7

    0 $accept: def $end .

    $výchozí  přijmout


State 8

    1 def: param_spec return_spec . ','

    ','  posunout a přejít do stavu 13


State 9

    4 return_spec: type .

    $výchozí  reduce using rule 4 (return_spec)


State 10

    5 return_spec: name . ':' type

    ':'  posunout a přejít do stavu 14


State 11

    7 name: . id
    8 name_list: . name
    9          | . name ',' name_list
    9          | name ',' . name_list

    id  posunout a přejít do stavu 15

    name       přejít do stavu 5
    name_list  přejít do stavu 16


State 12

    3 param_spec: name_list ':' . type
    6 type: . id

    id  posunout a přejít do stavu 17

    type  přejít do stavu 18


State 13

    1 def: param_spec return_spec ',' .

    $výchozí  reduce using rule 1 (def)


State 14

    5 return_spec: name ':' . type
    6 type: . id

    id  posunout a přejít do stavu 17

    type  přejít do stavu 19


State 15

    7 name: id .

    $výchozí  reduce using rule 7 (name)


State 16

    9 name_list: name ',' name_list .

    $výchozí  reduce using rule 9 (name_list)


State 17

    6 type: id .

    $výchozí  reduce using rule 6 (type)


State 18

    3 param_spec: name_list ':' type .

    $výchozí  reduce using rule 3 (param_spec)


State 19

    5 return_spec: name ':' type .

    $výchozí  reduce using rule 5 (return_spec)
