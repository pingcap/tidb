Gramatika

    0 $accept: input $end

    1 input: %empty
    2      | input line

    3 line: '\n'
    4     | exp '\n'

    5 exp: NUM
    6    | exp '+' exp
    7    | exp '-' exp
    8    | exp '*' exp
    9    | exp '/' exp
   10    | '-' exp
   11    | exp '^' exp
   12    | '(' exp ')'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'\n' (10) 3 4
'(' (40) 12
')' (41) 12
'*' (42) 8
'+' (43) 6
'-' (45) 7 10
'/' (47) 9
'^' (94) 11
error (256)
NUM (258) 5
NEG (259)


Neterminály s pravidly, ve kterých se objevují

$accept (13)
    vlevo: 0
input (14)
    vlevo: 1 2, vpravo: 0 2
line (15)
    vlevo: 3 4, vpravo: 2
exp (16)
    vlevo: 5 6 7 8 9 10 11 12, vpravo: 4 6 7 8 9 10 11 12


State 0

    0 $accept: . input $end
    1 input: . %empty
    2      | . input line

    $výchozí  reduce using rule 1 (input)

    input  přejít do stavu 1


State 1

    0 $accept: input . $end
    2 input: input . line
    3 line: . '\n'
    4     | . exp '\n'
    5 exp: . NUM
    6    | . exp '+' exp
    7    | . exp '-' exp
    8    | . exp '*' exp
    9    | . exp '/' exp
   10    | . '-' exp
   11    | . exp '^' exp
   12    | . '(' exp ')'

    $end  posunout a přejít do stavu 2
    NUM   posunout a přejít do stavu 3
    '-'   posunout a přejít do stavu 4
    '\n'  posunout a přejít do stavu 5
    '('   posunout a přejít do stavu 6

    line  přejít do stavu 7
    exp   přejít do stavu 8


State 2

    0 $accept: input $end .

    $výchozí  přijmout


State 3

    5 exp: NUM .

    $výchozí  reduce using rule 5 (exp)


State 4

    5 exp: . NUM
    6    | . exp '+' exp
    7    | . exp '-' exp
    8    | . exp '*' exp
    9    | . exp '/' exp
   10    | . '-' exp
   10    | '-' . exp
   11    | . exp '^' exp
   12    | . '(' exp ')'

    NUM  posunout a přejít do stavu 3
    '-'  posunout a přejít do stavu 4
    '('  posunout a přejít do stavu 6

    exp  přejít do stavu 9


State 5

    3 line: '\n' .

    $výchozí  reduce using rule 3 (line)


State 6

    5 exp: . NUM
    6    | . exp '+' exp
    7    | . exp '-' exp
    8    | . exp '*' exp
    9    | . exp '/' exp
   10    | . '-' exp
   11    | . exp '^' exp
   12    | . '(' exp ')'
   12    | '(' . exp ')'

    NUM  posunout a přejít do stavu 3
    '-'  posunout a přejít do stavu 4
    '('  posunout a přejít do stavu 6

    exp  přejít do stavu 10


State 7

    2 input: input line .

    $výchozí  reduce using rule 2 (input)


State 8

    4 line: exp . '\n'
    6 exp: exp . '+' exp
    7    | exp . '-' exp
    8    | exp . '*' exp
    9    | exp . '/' exp
   11    | exp . '^' exp

    '-'   posunout a přejít do stavu 11
    '+'   posunout a přejít do stavu 12
    '*'   posunout a přejít do stavu 13
    '/'   posunout a přejít do stavu 14
    '^'   posunout a přejít do stavu 15
    '\n'  posunout a přejít do stavu 16


State 9

    6 exp: exp . '+' exp
    7    | exp . '-' exp
    8    | exp . '*' exp
    9    | exp . '/' exp
   10    | '-' exp .  ['-', '+', '*', '/', '\n', ')']
   11    | exp . '^' exp

    '^'  posunout a přejít do stavu 15

    $výchozí  reduce using rule 10 (exp)

    Conflict between rule 10 and token '-' resolved as reduce ('-' < NEG).
    Conflict between rule 10 and token '+' resolved as reduce ('+' < NEG).
    Conflict between rule 10 and token '*' resolved as reduce ('*' < NEG).
    Conflict between rule 10 and token '/' resolved as reduce ('/' < NEG).
    Conflict between rule 10 and token '^' resolved as shift (NEG < '^').


State 10

    6 exp: exp . '+' exp
    7    | exp . '-' exp
    8    | exp . '*' exp
    9    | exp . '/' exp
   11    | exp . '^' exp
   12    | '(' exp . ')'

    '-'  posunout a přejít do stavu 11
    '+'  posunout a přejít do stavu 12
    '*'  posunout a přejít do stavu 13
    '/'  posunout a přejít do stavu 14
    '^'  posunout a přejít do stavu 15
    ')'  posunout a přejít do stavu 17


State 11

    5 exp: . NUM
    6    | . exp '+' exp
    7    | . exp '-' exp
    7    | exp '-' . exp
    8    | . exp '*' exp
    9    | . exp '/' exp
   10    | . '-' exp
   11    | . exp '^' exp
   12    | . '(' exp ')'

    NUM  posunout a přejít do stavu 3
    '-'  posunout a přejít do stavu 4
    '('  posunout a přejít do stavu 6

    exp  přejít do stavu 18


State 12

    5 exp: . NUM
    6    | . exp '+' exp
    6    | exp '+' . exp
    7    | . exp '-' exp
    8    | . exp '*' exp
    9    | . exp '/' exp
   10    | . '-' exp
   11    | . exp '^' exp
   12    | . '(' exp ')'

    NUM  posunout a přejít do stavu 3
    '-'  posunout a přejít do stavu 4
    '('  posunout a přejít do stavu 6

    exp  přejít do stavu 19


State 13

    5 exp: . NUM
    6    | . exp '+' exp
    7    | . exp '-' exp
    8    | . exp '*' exp
    8    | exp '*' . exp
    9    | . exp '/' exp
   10    | . '-' exp
   11    | . exp '^' exp
   12    | . '(' exp ')'

    NUM  posunout a přejít do stavu 3
    '-'  posunout a přejít do stavu 4
    '('  posunout a přejít do stavu 6

    exp  přejít do stavu 20


State 14

    5 exp: . NUM
    6    | . exp '+' exp
    7    | . exp '-' exp
    8    | . exp '*' exp
    9    | . exp '/' exp
    9    | exp '/' . exp
   10    | . '-' exp
   11    | . exp '^' exp
   12    | . '(' exp ')'

    NUM  posunout a přejít do stavu 3
    '-'  posunout a přejít do stavu 4
    '('  posunout a přejít do stavu 6

    exp  přejít do stavu 21


State 15

    5 exp: . NUM
    6    | . exp '+' exp
    7    | . exp '-' exp
    8    | . exp '*' exp
    9    | . exp '/' exp
   10    | . '-' exp
   11    | . exp '^' exp
   11    | exp '^' . exp
   12    | . '(' exp ')'

    NUM  posunout a přejít do stavu 3
    '-'  posunout a přejít do stavu 4
    '('  posunout a přejít do stavu 6

    exp  přejít do stavu 22


State 16

    4 line: exp '\n' .

    $výchozí  reduce using rule 4 (line)


State 17

   12 exp: '(' exp ')' .

    $výchozí  reduce using rule 12 (exp)


State 18

    6 exp: exp . '+' exp
    7    | exp . '-' exp
    7    | exp '-' exp .  ['-', '+', '\n', ')']
    8    | exp . '*' exp
    9    | exp . '/' exp
   11    | exp . '^' exp

    '*'  posunout a přejít do stavu 13
    '/'  posunout a přejít do stavu 14
    '^'  posunout a přejít do stavu 15

    $výchozí  reduce using rule 7 (exp)

    Conflict between rule 7 and token '-' resolved as reduce (%left '-').
    Conflict between rule 7 and token '+' resolved as reduce (%left '+').
    Conflict between rule 7 and token '*' resolved as shift ('-' < '*').
    Conflict between rule 7 and token '/' resolved as shift ('-' < '/').
    Conflict between rule 7 and token '^' resolved as shift ('-' < '^').


State 19

    6 exp: exp . '+' exp
    6    | exp '+' exp .  ['-', '+', '\n', ')']
    7    | exp . '-' exp
    8    | exp . '*' exp
    9    | exp . '/' exp
   11    | exp . '^' exp

    '*'  posunout a přejít do stavu 13
    '/'  posunout a přejít do stavu 14
    '^'  posunout a přejít do stavu 15

    $výchozí  reduce using rule 6 (exp)

    Conflict between rule 6 and token '-' resolved as reduce (%left '-').
    Conflict between rule 6 and token '+' resolved as reduce (%left '+').
    Conflict between rule 6 and token '*' resolved as shift ('+' < '*').
    Conflict between rule 6 and token '/' resolved as shift ('+' < '/').
    Conflict between rule 6 and token '^' resolved as shift ('+' < '^').


State 20

    6 exp: exp . '+' exp
    7    | exp . '-' exp
    8    | exp . '*' exp
    8    | exp '*' exp .  ['-', '+', '*', '/', '\n', ')']
    9    | exp . '/' exp
   11    | exp . '^' exp

    '^'  posunout a přejít do stavu 15

    $výchozí  reduce using rule 8 (exp)

    Conflict between rule 8 and token '-' resolved as reduce ('-' < '*').
    Conflict between rule 8 and token '+' resolved as reduce ('+' < '*').
    Conflict between rule 8 and token '*' resolved as reduce (%left '*').
    Conflict between rule 8 and token '/' resolved as reduce (%left '/').
    Conflict between rule 8 and token '^' resolved as shift ('*' < '^').


State 21

    6 exp: exp . '+' exp
    7    | exp . '-' exp
    8    | exp . '*' exp
    9    | exp . '/' exp
    9    | exp '/' exp .  ['-', '+', '*', '/', '\n', ')']
   11    | exp . '^' exp

    '^'  posunout a přejít do stavu 15

    $výchozí  reduce using rule 9 (exp)

    Conflict between rule 9 and token '-' resolved as reduce ('-' < '/').
    Conflict between rule 9 and token '+' resolved as reduce ('+' < '/').
    Conflict between rule 9 and token '*' resolved as reduce (%left '*').
    Conflict between rule 9 and token '/' resolved as reduce (%left '/').
    Conflict between rule 9 and token '^' resolved as shift ('/' < '^').


State 22

    6 exp: exp . '+' exp
    7    | exp . '-' exp
    8    | exp . '*' exp
    9    | exp . '/' exp
   11    | exp . '^' exp
   11    | exp '^' exp .  ['-', '+', '*', '/', '\n', ')']

    '^'  posunout a přejít do stavu 15

    $výchozí  reduce using rule 11 (exp)

    Conflict between rule 11 and token '-' resolved as reduce ('-' < '^').
    Conflict between rule 11 and token '+' resolved as reduce ('+' < '^').
    Conflict between rule 11 and token '*' resolved as reduce ('*' < '^').
    Conflict between rule 11 and token '/' resolved as reduce ('/' < '^').
    Conflict between rule 11 and token '^' resolved as shift (%right '^').
