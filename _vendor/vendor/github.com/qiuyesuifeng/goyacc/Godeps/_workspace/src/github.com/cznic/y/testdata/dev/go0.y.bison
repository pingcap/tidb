Gramatika

    0 $accept: File $end

    1 $@1: %empty

    2 File: PackageDecl $@1 Imports

    3 PackageDecl: 'P' Symbol ';'

    4 Imports: 'I'

    5 Symbol: 'S'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
';' (59) 3
'I' (73) 4
'P' (80) 3
'S' (83) 5
error (256)


Neterminály s pravidly, ve kterých se objevují

$accept (7)
    vlevo: 0
File (8)
    vlevo: 2, vpravo: 0
$@1 (9)
    vlevo: 1, vpravo: 2
PackageDecl (10)
    vlevo: 3, vpravo: 2
Imports (11)
    vlevo: 4, vpravo: 2
Symbol (12)
    vlevo: 5, vpravo: 3


State 0

    0 $accept: . File $end
    2 File: . PackageDecl $@1 Imports
    3 PackageDecl: . 'P' Symbol ';'

    'P'  posunout a přejít do stavu 1

    File         přejít do stavu 2
    PackageDecl  přejít do stavu 3


State 1

    3 PackageDecl: 'P' . Symbol ';'
    5 Symbol: . 'S'

    'S'  posunout a přejít do stavu 4

    Symbol  přejít do stavu 5


State 2

    0 $accept: File . $end

    $end  posunout a přejít do stavu 6


State 3

    1 $@1: . %empty
    2 File: PackageDecl . $@1 Imports

    $výchozí  reduce using rule 1 ($@1)

    $@1  přejít do stavu 7


State 4

    5 Symbol: 'S' .

    $výchozí  reduce using rule 5 (Symbol)


State 5

    3 PackageDecl: 'P' Symbol . ';'

    ';'  posunout a přejít do stavu 8


State 6

    0 $accept: File $end .

    $výchozí  přijmout


State 7

    2 File: PackageDecl $@1 . Imports
    4 Imports: . 'I'

    'I'  posunout a přejít do stavu 9

    Imports  přejít do stavu 10


State 8

    3 PackageDecl: 'P' Symbol ';' .

    $výchozí  reduce using rule 3 (PackageDecl)


State 9

    4 Imports: 'I' .

    $výchozí  reduce using rule 4 (Imports)


State 10

    2 File: PackageDecl $@1 Imports .

    $výchozí  reduce using rule 2 (File)
