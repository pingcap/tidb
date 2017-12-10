// http://sites.tufts.edu/comp181/2013/10/05/first-and-follow-sets/

%token id

%%

E:
 T E2

E2:
  '+' T E2
|

T:
 F T2

T2:
  '*' F T2
|

F:
 '(' E ')'
| id
