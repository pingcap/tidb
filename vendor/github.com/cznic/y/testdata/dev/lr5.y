// http://en.wikipedia.org/wiki/LALR_parser#LR_parsers
//
// The standard example of an LR(1) grammar that cannot be parsed with the
// LALR(1) parser, exhibiting such a reduce/reduce conflict.

%%

S:	'a' E 'c'
|	'a' F 'd'
|	'b' F 'c'
|	'b' E 'd'

E:	'e'

F:	'e'
