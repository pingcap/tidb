//

%union {
	int int
	str str
}

%token	<int>	NUM
%type	<str>	idList

%%

start:
	idList ids ids2 ids3 ids4 ids5

idList:
|	NUM 'X'

ids:
	'A' NUM NUM
	{
		$<int>$ = $2
	}

ids2:
	NUM NUM
	{
		$<str>$ = $1
	}

ids3:
	NUM NUM
	{
		$<foo>$ = $1
	}

ids4:
	NUM NUM
	{
		$$ = $1
	}

ids5:
	NUM NUM
	{
		$$ = $<bar>1
	}
