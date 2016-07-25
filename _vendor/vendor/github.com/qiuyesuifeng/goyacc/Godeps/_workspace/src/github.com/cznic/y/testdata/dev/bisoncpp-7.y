// http://bisoncpp.sourceforge.net/bisonc++07.html

%token NR
    
%left '+'

%%

start:
    start expr
|
    // empty
;

expr:
    NR
|
    expr '+' expr
;
