// http://en.wikipedia.org/wiki/LR_parser#Conflicts_in_the_constructed_tables

%%
E:	A '1'	// 1
|	B '2'	// 2

A:	'1'	// 3

B:	'1'	// 4
