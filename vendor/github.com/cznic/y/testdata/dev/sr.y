// http://dinosaur.compilertools.net/yacc/

%%
expr    :       expr  '-'  expr
	|	'1'
