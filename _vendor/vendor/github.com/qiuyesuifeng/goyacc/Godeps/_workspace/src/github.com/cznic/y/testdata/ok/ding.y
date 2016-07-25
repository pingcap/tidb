// http://dinosaur.compilertools.net/yacc/

%token  DING  DONG  DELL
%%
rhyme   :       sound  place
        ;
sound   :       DING  DONG
        ;
place   :       DELL
        ;

/*

state 0
	$accept: .rhyme $end 

	DING  shift 3
	.  error

	rhyme  goto 1
	sound  goto 2

state 1
	$accept:  rhyme.$end 

	$end  accept
	.  error


state 2
	rhyme:  sound.place 

	DELL  shift 5
	.  error

	place  goto 4

state 3
	sound:  DING.DONG 

	DONG  shift 6
	.  error


state 4
	rhyme:  sound place.    (1)

	.  reduce 1 (src line 5)


state 5
	place:  DELL.    (3)

	.  reduce 3 (src line 9)


state 6
	sound:  DING DONG.    (2)

	.  reduce 2 (src line 7)


6 terminals, 4 nonterminals
4 grammar rules, 7/2000 states
0 shift/reduce, 0 reduce/reduce conflicts reported
53 working sets used
memory: parser 2/30000
0 extra closures
3 shift entries, 1 exceptions
3 goto entries
0 entries saved by goto default
Optimizer space used: output 6/30000
6 table entries, 0 zero
maximum spread: 6, maximum offset: 6

*/
