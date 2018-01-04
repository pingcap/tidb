Gramatika

    0 $accept: program $end

    1 program: declaration_list

    2 declaration_list: declaration_list declaration
    3                 | declaration

    4 declaration: var_declaration
    5            | fun_declaration

    6 var_declaration: type_specifier ID ';'
    7                | type_specifier ID '[' NUM ']' ';'

    8 type_specifier: INT
    9               | VOID

   10 fun_declaration: type_specifier ID '(' params ')' compound_stmt

   11 params: param_list
   12       | VOID

   13 param_list: param_list ',' param
   14           | param

   15 param: type_specifier ID
   16      | type_specifier ID '[' ']'

   17 compound_stmt: '{' local_declarations statement_list '}'

   18 local_declarations: local_declarations var_declaration
   19                   | %empty

   20 statement_list: statement_list statement
   21               | %empty

   22 statement: expression_stmt
   23          | compound_stmt
   24          | selection_stmt
   25          | iteration_stmt
   26          | return_stmt

   27 expression_stmt: expression ';'
   28                | ';'

   29 selection_stmt: IF '(' expression ')' statement
   30               | IF '(' expression ')' statement ELSE statement

   31 iteration_stmt: WHILE '(' expression ')' statement

   32 return_stmt: RETURN ';'
   33            | RETURN expression ';'

   34 expression: var '=' expression
   35           | simple_expression

   36 var: ID
   37    | ID '[' expression ']'

   38 simple_expression: additive_expression relop additive_expression
   39                  | additive_expression

   40 relop: LTE
   41      | '<'
   42      | '>'
   43      | GTE
   44      | EQUAL
   45      | NOTEQUAL

   46 additive_expression: additive_expression addop term
   47                    | term

   48 addop: '+'
   49      | '-'

   50 term: term mulop factor
   51     | factor

   52 mulop: '*'
   53      | '/'

   54 factor: '(' expression ')'
   55       | var
   56       | call
   57       | NUM

   58 call: ID '(' args ')'

   59 args: arg_list
   60     | %empty

   61 arg_list: arg_list ',' expression
   62         | expression


Terminály s pravidly, ve kterých se objevují

$end (0) 0
'(' (40) 10 29 30 31 54 58
')' (41) 10 29 30 31 54 58
'*' (42) 52
'+' (43) 48
',' (44) 13 61
'-' (45) 49
'/' (47) 53
';' (59) 6 7 27 28 32 33
'<' (60) 41
'=' (61) 34
'>' (62) 42
'[' (91) 7 16 37
']' (93) 7 16 37
'{' (123) 17
'}' (125) 17
error (256)
ELSE (258) 30
IF (259) 29 30
INT (260) 8
RETURN (261) 32 33
VOID (262) 9 12
WHILE (263) 31
ID (264) 6 7 10 15 16 36 37 58
NUM (265) 7 57
LTE (266) 40
GTE (267) 43
EQUAL (268) 44
NOTEQUAL (269) 45
LOWER_THAN_ELSE (270)


Neterminály s pravidly, ve kterých se objevují

$accept (31)
    vlevo: 0
program (32)
    vlevo: 1, vpravo: 0
declaration_list (33)
    vlevo: 2 3, vpravo: 1 2
declaration (34)
    vlevo: 4 5, vpravo: 2 3
var_declaration (35)
    vlevo: 6 7, vpravo: 4 18
type_specifier (36)
    vlevo: 8 9, vpravo: 6 7 10 15 16
fun_declaration (37)
    vlevo: 10, vpravo: 5
params (38)
    vlevo: 11 12, vpravo: 10
param_list (39)
    vlevo: 13 14, vpravo: 11 13
param (40)
    vlevo: 15 16, vpravo: 13 14
compound_stmt (41)
    vlevo: 17, vpravo: 10 23
local_declarations (42)
    vlevo: 18 19, vpravo: 17 18
statement_list (43)
    vlevo: 20 21, vpravo: 17 20
statement (44)
    vlevo: 22 23 24 25 26, vpravo: 20 29 30 31
expression_stmt (45)
    vlevo: 27 28, vpravo: 22
selection_stmt (46)
    vlevo: 29 30, vpravo: 24
iteration_stmt (47)
    vlevo: 31, vpravo: 25
return_stmt (48)
    vlevo: 32 33, vpravo: 26
expression (49)
    vlevo: 34 35, vpravo: 27 29 30 31 33 34 37 54 61 62
var (50)
    vlevo: 36 37, vpravo: 34 55
simple_expression (51)
    vlevo: 38 39, vpravo: 35
relop (52)
    vlevo: 40 41 42 43 44 45, vpravo: 38
additive_expression (53)
    vlevo: 46 47, vpravo: 38 39 46
addop (54)
    vlevo: 48 49, vpravo: 46
term (55)
    vlevo: 50 51, vpravo: 46 47 50
mulop (56)
    vlevo: 52 53, vpravo: 50
factor (57)
    vlevo: 54 55 56 57, vpravo: 50 51
call (58)
    vlevo: 58, vpravo: 56
args (59)
    vlevo: 59 60, vpravo: 58
arg_list (60)
    vlevo: 61 62, vpravo: 59 61


State 0

    0 $accept: . program $end
    1 program: . declaration_list
    2 declaration_list: . declaration_list declaration
    3                 | . declaration
    4 declaration: . var_declaration
    5            | . fun_declaration
    6 var_declaration: . type_specifier ID ';'
    7                | . type_specifier ID '[' NUM ']' ';'
    8 type_specifier: . INT
    9               | . VOID
   10 fun_declaration: . type_specifier ID '(' params ')' compound_stmt

    INT   posunout a přejít do stavu 1
    VOID  posunout a přejít do stavu 2

    program           přejít do stavu 3
    declaration_list  přejít do stavu 4
    declaration       přejít do stavu 5
    var_declaration   přejít do stavu 6
    type_specifier    přejít do stavu 7
    fun_declaration   přejít do stavu 8


State 1

    8 type_specifier: INT .

    $výchozí  reduce using rule 8 (type_specifier)


State 2

    9 type_specifier: VOID .

    $výchozí  reduce using rule 9 (type_specifier)


State 3

    0 $accept: program . $end

    $end  posunout a přejít do stavu 9


State 4

    1 program: declaration_list .  [$end]
    2 declaration_list: declaration_list . declaration
    4 declaration: . var_declaration
    5            | . fun_declaration
    6 var_declaration: . type_specifier ID ';'
    7                | . type_specifier ID '[' NUM ']' ';'
    8 type_specifier: . INT
    9               | . VOID
   10 fun_declaration: . type_specifier ID '(' params ')' compound_stmt

    INT   posunout a přejít do stavu 1
    VOID  posunout a přejít do stavu 2

    $výchozí  reduce using rule 1 (program)

    declaration      přejít do stavu 10
    var_declaration  přejít do stavu 6
    type_specifier   přejít do stavu 7
    fun_declaration  přejít do stavu 8


State 5

    3 declaration_list: declaration .

    $výchozí  reduce using rule 3 (declaration_list)


State 6

    4 declaration: var_declaration .

    $výchozí  reduce using rule 4 (declaration)


State 7

    6 var_declaration: type_specifier . ID ';'
    7                | type_specifier . ID '[' NUM ']' ';'
   10 fun_declaration: type_specifier . ID '(' params ')' compound_stmt

    ID  posunout a přejít do stavu 11


State 8

    5 declaration: fun_declaration .

    $výchozí  reduce using rule 5 (declaration)


State 9

    0 $accept: program $end .

    $výchozí  přijmout


State 10

    2 declaration_list: declaration_list declaration .

    $výchozí  reduce using rule 2 (declaration_list)


State 11

    6 var_declaration: type_specifier ID . ';'
    7                | type_specifier ID . '[' NUM ']' ';'
   10 fun_declaration: type_specifier ID . '(' params ')' compound_stmt

    ';'  posunout a přejít do stavu 12
    '['  posunout a přejít do stavu 13
    '('  posunout a přejít do stavu 14


State 12

    6 var_declaration: type_specifier ID ';' .

    $výchozí  reduce using rule 6 (var_declaration)


State 13

    7 var_declaration: type_specifier ID '[' . NUM ']' ';'

    NUM  posunout a přejít do stavu 15


State 14

    8 type_specifier: . INT
    9               | . VOID
   10 fun_declaration: type_specifier ID '(' . params ')' compound_stmt
   11 params: . param_list
   12       | . VOID
   13 param_list: . param_list ',' param
   14           | . param
   15 param: . type_specifier ID
   16      | . type_specifier ID '[' ']'

    INT   posunout a přejít do stavu 1
    VOID  posunout a přejít do stavu 16

    type_specifier  přejít do stavu 17
    params          přejít do stavu 18
    param_list      přejít do stavu 19
    param           přejít do stavu 20


State 15

    7 var_declaration: type_specifier ID '[' NUM . ']' ';'

    ']'  posunout a přejít do stavu 21


State 16

    9 type_specifier: VOID .  [ID]
   12 params: VOID .  [')']

    ')'         reduce using rule 12 (params)
    $výchozí  reduce using rule 9 (type_specifier)


State 17

   15 param: type_specifier . ID
   16      | type_specifier . ID '[' ']'

    ID  posunout a přejít do stavu 22


State 18

   10 fun_declaration: type_specifier ID '(' params . ')' compound_stmt

    ')'  posunout a přejít do stavu 23


State 19

   11 params: param_list .  [')']
   13 param_list: param_list . ',' param

    ','  posunout a přejít do stavu 24

    $výchozí  reduce using rule 11 (params)


State 20

   14 param_list: param .

    $výchozí  reduce using rule 14 (param_list)


State 21

    7 var_declaration: type_specifier ID '[' NUM ']' . ';'

    ';'  posunout a přejít do stavu 25


State 22

   15 param: type_specifier ID .  [')', ',']
   16      | type_specifier ID . '[' ']'

    '['  posunout a přejít do stavu 26

    $výchozí  reduce using rule 15 (param)


State 23

   10 fun_declaration: type_specifier ID '(' params ')' . compound_stmt
   17 compound_stmt: . '{' local_declarations statement_list '}'

    '{'  posunout a přejít do stavu 27

    compound_stmt  přejít do stavu 28


State 24

    8 type_specifier: . INT
    9               | . VOID
   13 param_list: param_list ',' . param
   15 param: . type_specifier ID
   16      | . type_specifier ID '[' ']'

    INT   posunout a přejít do stavu 1
    VOID  posunout a přejít do stavu 2

    type_specifier  přejít do stavu 17
    param           přejít do stavu 29


State 25

    7 var_declaration: type_specifier ID '[' NUM ']' ';' .

    $výchozí  reduce using rule 7 (var_declaration)


State 26

   16 param: type_specifier ID '[' . ']'

    ']'  posunout a přejít do stavu 30


State 27

   17 compound_stmt: '{' . local_declarations statement_list '}'
   18 local_declarations: . local_declarations var_declaration
   19                   | . %empty

    $výchozí  reduce using rule 19 (local_declarations)

    local_declarations  přejít do stavu 31


State 28

   10 fun_declaration: type_specifier ID '(' params ')' compound_stmt .

    $výchozí  reduce using rule 10 (fun_declaration)


State 29

   13 param_list: param_list ',' param .

    $výchozí  reduce using rule 13 (param_list)


State 30

   16 param: type_specifier ID '[' ']' .

    $výchozí  reduce using rule 16 (param)


State 31

    6 var_declaration: . type_specifier ID ';'
    7                | . type_specifier ID '[' NUM ']' ';'
    8 type_specifier: . INT
    9               | . VOID
   17 compound_stmt: '{' local_declarations . statement_list '}'
   18 local_declarations: local_declarations . var_declaration
   20 statement_list: . statement_list statement
   21               | . %empty  [IF, RETURN, WHILE, ID, NUM, ';', '(', '{', '}']

    INT   posunout a přejít do stavu 1
    VOID  posunout a přejít do stavu 2

    $výchozí  reduce using rule 21 (statement_list)

    var_declaration  přejít do stavu 32
    type_specifier   přejít do stavu 33
    statement_list   přejít do stavu 34


State 32

   18 local_declarations: local_declarations var_declaration .

    $výchozí  reduce using rule 18 (local_declarations)


State 33

    6 var_declaration: type_specifier . ID ';'
    7                | type_specifier . ID '[' NUM ']' ';'

    ID  posunout a přejít do stavu 35


State 34

   17 compound_stmt: . '{' local_declarations statement_list '}'
   17              | '{' local_declarations statement_list . '}'
   20 statement_list: statement_list . statement
   22 statement: . expression_stmt
   23          | . compound_stmt
   24          | . selection_stmt
   25          | . iteration_stmt
   26          | . return_stmt
   27 expression_stmt: . expression ';'
   28                | . ';'
   29 selection_stmt: . IF '(' expression ')' statement
   30               | . IF '(' expression ')' statement ELSE statement
   31 iteration_stmt: . WHILE '(' expression ')' statement
   32 return_stmt: . RETURN ';'
   33            | . RETURN expression ';'
   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    IF      posunout a přejít do stavu 36
    RETURN  posunout a přejít do stavu 37
    WHILE   posunout a přejít do stavu 38
    ID      posunout a přejít do stavu 39
    NUM     posunout a přejít do stavu 40
    ';'     posunout a přejít do stavu 41
    '('     posunout a přejít do stavu 42
    '{'     posunout a přejít do stavu 27
    '}'     posunout a přejít do stavu 43

    compound_stmt        přejít do stavu 44
    statement            přejít do stavu 45
    expression_stmt      přejít do stavu 46
    selection_stmt       přejít do stavu 47
    iteration_stmt       přejít do stavu 48
    return_stmt          přejít do stavu 49
    expression           přejít do stavu 50
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 35

    6 var_declaration: type_specifier ID . ';'
    7                | type_specifier ID . '[' NUM ']' ';'

    ';'  posunout a přejít do stavu 12
    '['  posunout a přejít do stavu 13


State 36

   29 selection_stmt: IF . '(' expression ')' statement
   30               | IF . '(' expression ')' statement ELSE statement

    '('  posunout a přejít do stavu 57


State 37

   32 return_stmt: RETURN . ';'
   33            | RETURN . expression ';'
   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    ';'  posunout a přejít do stavu 58
    '('  posunout a přejít do stavu 42

    expression           přejít do stavu 59
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 38

   31 iteration_stmt: WHILE . '(' expression ')' statement

    '('  posunout a přejít do stavu 60


State 39

   36 var: ID .  [LTE, GTE, EQUAL, NOTEQUAL, ';', ']', ')', ',', '=', '<', '>', '+', '-', '*', '/']
   37    | ID . '[' expression ']'
   58 call: ID . '(' args ')'

    '['  posunout a přejít do stavu 61
    '('  posunout a přejít do stavu 62

    $výchozí  reduce using rule 36 (var)


State 40

   57 factor: NUM .

    $výchozí  reduce using rule 57 (factor)


State 41

   28 expression_stmt: ';' .

    $výchozí  reduce using rule 28 (expression_stmt)


State 42

   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   54       | '(' . expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    expression           přejít do stavu 63
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 43

   17 compound_stmt: '{' local_declarations statement_list '}' .

    $výchozí  reduce using rule 17 (compound_stmt)


State 44

   23 statement: compound_stmt .

    $výchozí  reduce using rule 23 (statement)


State 45

   20 statement_list: statement_list statement .

    $výchozí  reduce using rule 20 (statement_list)


State 46

   22 statement: expression_stmt .

    $výchozí  reduce using rule 22 (statement)


State 47

   24 statement: selection_stmt .

    $výchozí  reduce using rule 24 (statement)


State 48

   25 statement: iteration_stmt .

    $výchozí  reduce using rule 25 (statement)


State 49

   26 statement: return_stmt .

    $výchozí  reduce using rule 26 (statement)


State 50

   27 expression_stmt: expression . ';'

    ';'  posunout a přejít do stavu 64


State 51

   34 expression: var . '=' expression
   55 factor: var .  [LTE, GTE, EQUAL, NOTEQUAL, ';', ']', ')', ',', '<', '>', '+', '-', '*', '/']

    '='  posunout a přejít do stavu 65

    $výchozí  reduce using rule 55 (factor)


State 52

   35 expression: simple_expression .

    $výchozí  reduce using rule 35 (expression)


State 53

   38 simple_expression: additive_expression . relop additive_expression
   39                  | additive_expression .  [';', ']', ')', ',']
   40 relop: . LTE
   41      | . '<'
   42      | . '>'
   43      | . GTE
   44      | . EQUAL
   45      | . NOTEQUAL
   46 additive_expression: additive_expression . addop term
   48 addop: . '+'
   49      | . '-'

    LTE       posunout a přejít do stavu 66
    GTE       posunout a přejít do stavu 67
    EQUAL     posunout a přejít do stavu 68
    NOTEQUAL  posunout a přejít do stavu 69
    '<'       posunout a přejít do stavu 70
    '>'       posunout a přejít do stavu 71
    '+'       posunout a přejít do stavu 72
    '-'       posunout a přejít do stavu 73

    $výchozí  reduce using rule 39 (simple_expression)

    relop  přejít do stavu 74
    addop  přejít do stavu 75


State 54

   47 additive_expression: term .  [LTE, GTE, EQUAL, NOTEQUAL, ';', ']', ')', ',', '<', '>', '+', '-']
   50 term: term . mulop factor
   52 mulop: . '*'
   53      | . '/'

    '*'  posunout a přejít do stavu 76
    '/'  posunout a přejít do stavu 77

    $výchozí  reduce using rule 47 (additive_expression)

    mulop  přejít do stavu 78


State 55

   51 term: factor .

    $výchozí  reduce using rule 51 (term)


State 56

   56 factor: call .

    $výchozí  reduce using rule 56 (factor)


State 57

   29 selection_stmt: IF '(' . expression ')' statement
   30               | IF '(' . expression ')' statement ELSE statement
   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    expression           přejít do stavu 79
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 58

   32 return_stmt: RETURN ';' .

    $výchozí  reduce using rule 32 (return_stmt)


State 59

   33 return_stmt: RETURN expression . ';'

    ';'  posunout a přejít do stavu 80


State 60

   31 iteration_stmt: WHILE '(' . expression ')' statement
   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    expression           přejít do stavu 81
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 61

   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   37    | ID '[' . expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    expression           přejít do stavu 82
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 62

   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'
   58     | ID '(' . args ')'
   59 args: . arg_list
   60     | . %empty  [')']
   61 arg_list: . arg_list ',' expression
   62         | . expression

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    $výchozí  reduce using rule 60 (args)

    expression           přejít do stavu 83
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56
    args                 přejít do stavu 84
    arg_list             přejít do stavu 85


State 63

   54 factor: '(' expression . ')'

    ')'  posunout a přejít do stavu 86


State 64

   27 expression_stmt: expression ';' .

    $výchozí  reduce using rule 27 (expression_stmt)


State 65

   34 expression: . var '=' expression
   34           | var '=' . expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    expression           přejít do stavu 87
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 66

   40 relop: LTE .

    $výchozí  reduce using rule 40 (relop)


State 67

   43 relop: GTE .

    $výchozí  reduce using rule 43 (relop)


State 68

   44 relop: EQUAL .

    $výchozí  reduce using rule 44 (relop)


State 69

   45 relop: NOTEQUAL .

    $výchozí  reduce using rule 45 (relop)


State 70

   41 relop: '<' .

    $výchozí  reduce using rule 41 (relop)


State 71

   42 relop: '>' .

    $výchozí  reduce using rule 42 (relop)


State 72

   48 addop: '+' .

    $výchozí  reduce using rule 48 (addop)


State 73

   49 addop: '-' .

    $výchozí  reduce using rule 49 (addop)


State 74

   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: additive_expression relop . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    var                  přejít do stavu 88
    additive_expression  přejít do stavu 89
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 75

   36 var: . ID
   37    | . ID '[' expression ']'
   46 additive_expression: additive_expression addop . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    var     přejít do stavu 88
    term    přejít do stavu 90
    factor  přejít do stavu 55
    call    přejít do stavu 56


State 76

   52 mulop: '*' .

    $výchozí  reduce using rule 52 (mulop)


State 77

   53 mulop: '/' .

    $výchozí  reduce using rule 53 (mulop)


State 78

   36 var: . ID
   37    | . ID '[' expression ']'
   50 term: term mulop . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    var     přejít do stavu 88
    factor  přejít do stavu 91
    call    přejít do stavu 56


State 79

   29 selection_stmt: IF '(' expression . ')' statement
   30               | IF '(' expression . ')' statement ELSE statement

    ')'  posunout a přejít do stavu 92


State 80

   33 return_stmt: RETURN expression ';' .

    $výchozí  reduce using rule 33 (return_stmt)


State 81

   31 iteration_stmt: WHILE '(' expression . ')' statement

    ')'  posunout a přejít do stavu 93


State 82

   37 var: ID '[' expression . ']'

    ']'  posunout a přejít do stavu 94


State 83

   62 arg_list: expression .

    $výchozí  reduce using rule 62 (arg_list)


State 84

   58 call: ID '(' args . ')'

    ')'  posunout a přejít do stavu 95


State 85

   59 args: arg_list .  [')']
   61 arg_list: arg_list . ',' expression

    ','  posunout a přejít do stavu 96

    $výchozí  reduce using rule 59 (args)


State 86

   54 factor: '(' expression ')' .

    $výchozí  reduce using rule 54 (factor)


State 87

   34 expression: var '=' expression .

    $výchozí  reduce using rule 34 (expression)


State 88

   55 factor: var .

    $výchozí  reduce using rule 55 (factor)


State 89

   38 simple_expression: additive_expression relop additive_expression .  [';', ']', ')', ',']
   46 additive_expression: additive_expression . addop term
   48 addop: . '+'
   49      | . '-'

    '+'  posunout a přejít do stavu 72
    '-'  posunout a přejít do stavu 73

    $výchozí  reduce using rule 38 (simple_expression)

    addop  přejít do stavu 75


State 90

   46 additive_expression: additive_expression addop term .  [LTE, GTE, EQUAL, NOTEQUAL, ';', ']', ')', ',', '<', '>', '+', '-']
   50 term: term . mulop factor
   52 mulop: . '*'
   53      | . '/'

    '*'  posunout a přejít do stavu 76
    '/'  posunout a přejít do stavu 77

    $výchozí  reduce using rule 46 (additive_expression)

    mulop  přejít do stavu 78


State 91

   50 term: term mulop factor .

    $výchozí  reduce using rule 50 (term)


State 92

   17 compound_stmt: . '{' local_declarations statement_list '}'
   22 statement: . expression_stmt
   23          | . compound_stmt
   24          | . selection_stmt
   25          | . iteration_stmt
   26          | . return_stmt
   27 expression_stmt: . expression ';'
   28                | . ';'
   29 selection_stmt: . IF '(' expression ')' statement
   29               | IF '(' expression ')' . statement
   30               | . IF '(' expression ')' statement ELSE statement
   30               | IF '(' expression ')' . statement ELSE statement
   31 iteration_stmt: . WHILE '(' expression ')' statement
   32 return_stmt: . RETURN ';'
   33            | . RETURN expression ';'
   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    IF      posunout a přejít do stavu 36
    RETURN  posunout a přejít do stavu 37
    WHILE   posunout a přejít do stavu 38
    ID      posunout a přejít do stavu 39
    NUM     posunout a přejít do stavu 40
    ';'     posunout a přejít do stavu 41
    '('     posunout a přejít do stavu 42
    '{'     posunout a přejít do stavu 27

    compound_stmt        přejít do stavu 44
    statement            přejít do stavu 97
    expression_stmt      přejít do stavu 46
    selection_stmt       přejít do stavu 47
    iteration_stmt       přejít do stavu 48
    return_stmt          přejít do stavu 49
    expression           přejít do stavu 50
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 93

   17 compound_stmt: . '{' local_declarations statement_list '}'
   22 statement: . expression_stmt
   23          | . compound_stmt
   24          | . selection_stmt
   25          | . iteration_stmt
   26          | . return_stmt
   27 expression_stmt: . expression ';'
   28                | . ';'
   29 selection_stmt: . IF '(' expression ')' statement
   30               | . IF '(' expression ')' statement ELSE statement
   31 iteration_stmt: . WHILE '(' expression ')' statement
   31               | WHILE '(' expression ')' . statement
   32 return_stmt: . RETURN ';'
   33            | . RETURN expression ';'
   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    IF      posunout a přejít do stavu 36
    RETURN  posunout a přejít do stavu 37
    WHILE   posunout a přejít do stavu 38
    ID      posunout a přejít do stavu 39
    NUM     posunout a přejít do stavu 40
    ';'     posunout a přejít do stavu 41
    '('     posunout a přejít do stavu 42
    '{'     posunout a přejít do stavu 27

    compound_stmt        přejít do stavu 44
    statement            přejít do stavu 98
    expression_stmt      přejít do stavu 46
    selection_stmt       přejít do stavu 47
    iteration_stmt       přejít do stavu 48
    return_stmt          přejít do stavu 49
    expression           přejít do stavu 50
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 94

   37 var: ID '[' expression ']' .

    $výchozí  reduce using rule 37 (var)


State 95

   58 call: ID '(' args ')' .

    $výchozí  reduce using rule 58 (call)


State 96

   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'
   61 arg_list: arg_list ',' . expression

    ID   posunout a přejít do stavu 39
    NUM  posunout a přejít do stavu 40
    '('  posunout a přejít do stavu 42

    expression           přejít do stavu 99
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 97

   29 selection_stmt: IF '(' expression ')' statement .  [IF, RETURN, WHILE, ID, NUM, ';', '(', '{', '}']
   30               | IF '(' expression ')' statement . ELSE statement

    ELSE  posunout a přejít do stavu 100

    $výchozí  reduce using rule 29 (selection_stmt)

    Conflict between rule 29 and token ELSE resolved as shift (LOWER_THAN_ELSE < ELSE).


State 98

   31 iteration_stmt: WHILE '(' expression ')' statement .

    $výchozí  reduce using rule 31 (iteration_stmt)


State 99

   61 arg_list: arg_list ',' expression .

    $výchozí  reduce using rule 61 (arg_list)


State 100

   17 compound_stmt: . '{' local_declarations statement_list '}'
   22 statement: . expression_stmt
   23          | . compound_stmt
   24          | . selection_stmt
   25          | . iteration_stmt
   26          | . return_stmt
   27 expression_stmt: . expression ';'
   28                | . ';'
   29 selection_stmt: . IF '(' expression ')' statement
   30               | . IF '(' expression ')' statement ELSE statement
   30               | IF '(' expression ')' statement ELSE . statement
   31 iteration_stmt: . WHILE '(' expression ')' statement
   32 return_stmt: . RETURN ';'
   33            | . RETURN expression ';'
   34 expression: . var '=' expression
   35           | . simple_expression
   36 var: . ID
   37    | . ID '[' expression ']'
   38 simple_expression: . additive_expression relop additive_expression
   39                  | . additive_expression
   46 additive_expression: . additive_expression addop term
   47                    | . term
   50 term: . term mulop factor
   51     | . factor
   54 factor: . '(' expression ')'
   55       | . var
   56       | . call
   57       | . NUM
   58 call: . ID '(' args ')'

    IF      posunout a přejít do stavu 36
    RETURN  posunout a přejít do stavu 37
    WHILE   posunout a přejít do stavu 38
    ID      posunout a přejít do stavu 39
    NUM     posunout a přejít do stavu 40
    ';'     posunout a přejít do stavu 41
    '('     posunout a přejít do stavu 42
    '{'     posunout a přejít do stavu 27

    compound_stmt        přejít do stavu 44
    statement            přejít do stavu 101
    expression_stmt      přejít do stavu 46
    selection_stmt       přejít do stavu 47
    iteration_stmt       přejít do stavu 48
    return_stmt          přejít do stavu 49
    expression           přejít do stavu 50
    var                  přejít do stavu 51
    simple_expression    přejít do stavu 52
    additive_expression  přejít do stavu 53
    term                 přejít do stavu 54
    factor               přejít do stavu 55
    call                 přejít do stavu 56


State 101

   30 selection_stmt: IF '(' expression ')' statement ELSE statement .

    $výchozí  reduce using rule 30 (selection_stmt)
