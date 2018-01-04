Terminals unused in grammar

   illegal
   tkRCurl


Gramatika

    0 $accept: spec $end

    1 spec: defs tkMark rules tail

    2 tail: %empty
    3     | tkMark

    4 defs: %empty
    5     | defs def

    6 def: tkStart tkIdent
    7    | tkUnion
    8    | tkLCurl
    9    | tkErrorVerbose
   10    | rword tag nlist

   11 rword: tkToken
   12      | tkLeft
   13      | tkRight
   14      | tkNonAssoc
   15      | tkType

   16 tag: %empty
   17    | '<' tkIdent '>'

   18 nlist: nmno
   19      | nlist nmno

   20 nmno: tkIdent
   21     | tkIdent tkNumber

   22 rules: tkCIdent rbody prec
   23      | rules rule

   24 rule: tkCIdent rbody prec
   25     | '|' rbody prec

   26 rbody: %empty
   27      | rbody tkIdent
   28      | rbody act

   29 act: '{'

   30 prec: %empty
   31     | tkPrec tkIdent
   32     | tkPrec tkIdent act
   33     | prec ';'


Terminály s pravidly, ve kterých se objevují

$end (0) 0
';' (59) 33
'<' (60) 17
'>' (62) 17
'{' (123) 29
'|' (124) 25
error (256)
illegal (258)
tkIdent (259) 6 17 20 21 27 31 32
tkCIdent (260) 22 24
tkNumber (261) 21
tkLeft (262) 12
tkRight (263) 13
tkNonAssoc (264) 14
tkToken (265) 11
tkPrec (266) 31 32
tkType (267) 15
tkStart (268) 6
tkUnion (269) 7
tkErrorVerbose (270) 9
tkMark (271) 1 3
tkLCurl (272) 8
tkRCurl (273)


Neterminály s pravidly, ve kterých se objevují

$accept (24)
    vlevo: 0
spec (25)
    vlevo: 1, vpravo: 0
tail (26)
    vlevo: 2 3, vpravo: 1
defs (27)
    vlevo: 4 5, vpravo: 1 5
def (28)
    vlevo: 6 7 8 9 10, vpravo: 5
rword (29)
    vlevo: 11 12 13 14 15, vpravo: 10
tag (30)
    vlevo: 16 17, vpravo: 10
nlist (31)
    vlevo: 18 19, vpravo: 10 19
nmno (32)
    vlevo: 20 21, vpravo: 18 19
rules (33)
    vlevo: 22 23, vpravo: 1 23
rule (34)
    vlevo: 24 25, vpravo: 23
rbody (35)
    vlevo: 26 27 28, vpravo: 22 24 25 27 28
act (36)
    vlevo: 29, vpravo: 28 32
prec (37)
    vlevo: 30 31 32 33, vpravo: 22 24 25 33


State 0

    0 $accept: . spec $end
    1 spec: . defs tkMark rules tail
    4 defs: . %empty
    5     | . defs def

    $výchozí  reduce using rule 4 (defs)

    spec  přejít do stavu 1
    defs  přejít do stavu 2


State 1

    0 $accept: spec . $end

    $end  posunout a přejít do stavu 3


State 2

    1 spec: defs . tkMark rules tail
    5 defs: defs . def
    6 def: . tkStart tkIdent
    7    | . tkUnion
    8    | . tkLCurl
    9    | . tkErrorVerbose
   10    | . rword tag nlist
   11 rword: . tkToken
   12      | . tkLeft
   13      | . tkRight
   14      | . tkNonAssoc
   15      | . tkType

    tkLeft          posunout a přejít do stavu 4
    tkRight         posunout a přejít do stavu 5
    tkNonAssoc      posunout a přejít do stavu 6
    tkToken         posunout a přejít do stavu 7
    tkType          posunout a přejít do stavu 8
    tkStart         posunout a přejít do stavu 9
    tkUnion         posunout a přejít do stavu 10
    tkErrorVerbose  posunout a přejít do stavu 11
    tkMark          posunout a přejít do stavu 12
    tkLCurl         posunout a přejít do stavu 13

    def    přejít do stavu 14
    rword  přejít do stavu 15


State 3

    0 $accept: spec $end .

    $výchozí  přijmout


State 4

   12 rword: tkLeft .

    $výchozí  reduce using rule 12 (rword)


State 5

   13 rword: tkRight .

    $výchozí  reduce using rule 13 (rword)


State 6

   14 rword: tkNonAssoc .

    $výchozí  reduce using rule 14 (rword)


State 7

   11 rword: tkToken .

    $výchozí  reduce using rule 11 (rword)


State 8

   15 rword: tkType .

    $výchozí  reduce using rule 15 (rword)


State 9

    6 def: tkStart . tkIdent

    tkIdent  posunout a přejít do stavu 16


State 10

    7 def: tkUnion .

    $výchozí  reduce using rule 7 (def)


State 11

    9 def: tkErrorVerbose .

    $výchozí  reduce using rule 9 (def)


State 12

    1 spec: defs tkMark . rules tail
   22 rules: . tkCIdent rbody prec
   23      | . rules rule

    tkCIdent  posunout a přejít do stavu 17

    rules  přejít do stavu 18


State 13

    8 def: tkLCurl .

    $výchozí  reduce using rule 8 (def)


State 14

    5 defs: defs def .

    $výchozí  reduce using rule 5 (defs)


State 15

   10 def: rword . tag nlist
   16 tag: . %empty  [tkIdent]
   17    | . '<' tkIdent '>'

    '<'  posunout a přejít do stavu 19

    $výchozí  reduce using rule 16 (tag)

    tag  přejít do stavu 20


State 16

    6 def: tkStart tkIdent .

    $výchozí  reduce using rule 6 (def)


State 17

   22 rules: tkCIdent . rbody prec
   26 rbody: . %empty
   27      | . rbody tkIdent
   28      | . rbody act

    $výchozí  reduce using rule 26 (rbody)

    rbody  přejít do stavu 21


State 18

    1 spec: defs tkMark rules . tail
    2 tail: . %empty  [$end]
    3     | . tkMark
   23 rules: rules . rule
   24 rule: . tkCIdent rbody prec
   25     | . '|' rbody prec

    tkCIdent  posunout a přejít do stavu 22
    tkMark    posunout a přejít do stavu 23
    '|'       posunout a přejít do stavu 24

    $výchozí  reduce using rule 2 (tail)

    tail  přejít do stavu 25
    rule  přejít do stavu 26


State 19

   17 tag: '<' . tkIdent '>'

    tkIdent  posunout a přejít do stavu 27


State 20

   10 def: rword tag . nlist
   18 nlist: . nmno
   19      | . nlist nmno
   20 nmno: . tkIdent
   21     | . tkIdent tkNumber

    tkIdent  posunout a přejít do stavu 28

    nlist  přejít do stavu 29
    nmno   přejít do stavu 30


State 21

   22 rules: tkCIdent rbody . prec
   27 rbody: rbody . tkIdent
   28      | rbody . act
   29 act: . '{'
   30 prec: . %empty  [$end, tkCIdent, tkMark, '|', ';']
   31     | . tkPrec tkIdent
   32     | . tkPrec tkIdent act
   33     | . prec ';'

    tkIdent  posunout a přejít do stavu 31
    tkPrec   posunout a přejít do stavu 32
    '{'      posunout a přejít do stavu 33

    $výchozí  reduce using rule 30 (prec)

    act   přejít do stavu 34
    prec  přejít do stavu 35


State 22

   24 rule: tkCIdent . rbody prec
   26 rbody: . %empty
   27      | . rbody tkIdent
   28      | . rbody act

    $výchozí  reduce using rule 26 (rbody)

    rbody  přejít do stavu 36


State 23

    3 tail: tkMark .

    $výchozí  reduce using rule 3 (tail)


State 24

   25 rule: '|' . rbody prec
   26 rbody: . %empty
   27      | . rbody tkIdent
   28      | . rbody act

    $výchozí  reduce using rule 26 (rbody)

    rbody  přejít do stavu 37


State 25

    1 spec: defs tkMark rules tail .

    $výchozí  reduce using rule 1 (spec)


State 26

   23 rules: rules rule .

    $výchozí  reduce using rule 23 (rules)


State 27

   17 tag: '<' tkIdent . '>'

    '>'  posunout a přejít do stavu 38


State 28

   20 nmno: tkIdent .  [tkIdent, tkLeft, tkRight, tkNonAssoc, tkToken, tkType, tkStart, tkUnion, tkErrorVerbose, tkMark, tkLCurl]
   21     | tkIdent . tkNumber

    tkNumber  posunout a přejít do stavu 39

    $výchozí  reduce using rule 20 (nmno)


State 29

   10 def: rword tag nlist .  [tkLeft, tkRight, tkNonAssoc, tkToken, tkType, tkStart, tkUnion, tkErrorVerbose, tkMark, tkLCurl]
   19 nlist: nlist . nmno
   20 nmno: . tkIdent
   21     | . tkIdent tkNumber

    tkIdent  posunout a přejít do stavu 28

    $výchozí  reduce using rule 10 (def)

    nmno  přejít do stavu 40


State 30

   18 nlist: nmno .

    $výchozí  reduce using rule 18 (nlist)


State 31

   27 rbody: rbody tkIdent .

    $výchozí  reduce using rule 27 (rbody)


State 32

   31 prec: tkPrec . tkIdent
   32     | tkPrec . tkIdent act

    tkIdent  posunout a přejít do stavu 41


State 33

   29 act: '{' .

    $výchozí  reduce using rule 29 (act)


State 34

   28 rbody: rbody act .

    $výchozí  reduce using rule 28 (rbody)


State 35

   22 rules: tkCIdent rbody prec .  [$end, tkCIdent, tkMark, '|']
   33 prec: prec . ';'

    ';'  posunout a přejít do stavu 42

    $výchozí  reduce using rule 22 (rules)


State 36

   24 rule: tkCIdent rbody . prec
   27 rbody: rbody . tkIdent
   28      | rbody . act
   29 act: . '{'
   30 prec: . %empty  [$end, tkCIdent, tkMark, '|', ';']
   31     | . tkPrec tkIdent
   32     | . tkPrec tkIdent act
   33     | . prec ';'

    tkIdent  posunout a přejít do stavu 31
    tkPrec   posunout a přejít do stavu 32
    '{'      posunout a přejít do stavu 33

    $výchozí  reduce using rule 30 (prec)

    act   přejít do stavu 34
    prec  přejít do stavu 43


State 37

   25 rule: '|' rbody . prec
   27 rbody: rbody . tkIdent
   28      | rbody . act
   29 act: . '{'
   30 prec: . %empty  [$end, tkCIdent, tkMark, '|', ';']
   31     | . tkPrec tkIdent
   32     | . tkPrec tkIdent act
   33     | . prec ';'

    tkIdent  posunout a přejít do stavu 31
    tkPrec   posunout a přejít do stavu 32
    '{'      posunout a přejít do stavu 33

    $výchozí  reduce using rule 30 (prec)

    act   přejít do stavu 34
    prec  přejít do stavu 44


State 38

   17 tag: '<' tkIdent '>' .

    $výchozí  reduce using rule 17 (tag)


State 39

   21 nmno: tkIdent tkNumber .

    $výchozí  reduce using rule 21 (nmno)


State 40

   19 nlist: nlist nmno .

    $výchozí  reduce using rule 19 (nlist)


State 41

   29 act: . '{'
   31 prec: tkPrec tkIdent .  [$end, tkCIdent, tkMark, '|', ';']
   32     | tkPrec tkIdent . act

    '{'  posunout a přejít do stavu 33

    $výchozí  reduce using rule 31 (prec)

    act  přejít do stavu 45


State 42

   33 prec: prec ';' .

    $výchozí  reduce using rule 33 (prec)


State 43

   24 rule: tkCIdent rbody prec .  [$end, tkCIdent, tkMark, '|']
   33 prec: prec . ';'

    ';'  posunout a přejít do stavu 42

    $výchozí  reduce using rule 24 (rule)


State 44

   25 rule: '|' rbody prec .  [$end, tkCIdent, tkMark, '|']
   33 prec: prec . ';'

    ';'  posunout a přejít do stavu 42

    $výchozí  reduce using rule 25 (rule)


State 45

   32 prec: tkPrec tkIdent act .

    $výchozí  reduce using rule 32 (prec)
