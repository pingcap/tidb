// http://en.wikipedia.org/wiki/LR_parser#Additional_example_1.2B1

%%
E:
	E '*' B
|	E '+' B
|	B

B: 
 	'0'
|	'1'
