// Copyright 2013 The parser Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

%right 'r'
%nonassoc 'n' foo

%token TOK 1234

%%

x:
	'-'  vexp       %prec  UMINUS
y:
