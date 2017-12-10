// Dragon book example 4.46, p. 241

%token id

%%

S:	L '=' R
L:	'*' R
|	id
R:	L
