// Dragon book example 4.42, p. 231

%token id

%%

S:	C C
C:	'c' C
|	'd'
