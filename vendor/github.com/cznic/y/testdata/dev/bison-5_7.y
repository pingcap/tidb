// http://www.gnu.org/software/bison/manual/html_node/Mysterious-Conflicts.html#Mysterious-Conflicts

%token id

%%

def: param_spec return_spec ',';
param_spec:
  type
| name_list ':' type
;
return_spec:
  type
| name ':' type
;
type: id;

name: id;
name_list:
  name
| name ',' name_list
;
