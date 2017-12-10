/*

Copyright:

	© 2001-2004 The IEEE and The Open Group, All Rights reserved.

Source:

	http://pubs.opengroup.org/onlinepubs/009695399/utilities/yacc.html

*/

/* Grammar for the input to yacc. */
/* Basic entries. */
/* The following are recognized by the lexical analyzer. */


%token    IDENTIFIER      /* Includes identifiers and literals */
%token    C_IDENTIFIER    /* identifier (but not literal)
                             followed by a :. */
%token    NUMBER          /* [0-9][0-9]* */


/* Reserved words : %type=>TYPE %left=>LEFT, and so on */


%token    LEFT RIGHT NONASSOC TOKEN PREC TYPE START UNION


%token    MARK            /* The %% mark. */
%token    LCURL           /* The %{ mark. */
%token    RCURL           /* The %} mark. */


/* 8-bit character literals stand for themselves; */
/* tokens have to be defined for multi-byte characters. */


%start    spec


%%


spec  : defs MARK rules tail
      ;
tail  : MARK
      {
        /* In this action, set up the rest of the file. */
      }
      | /* Empty; the second MARK is optional. */
      ;
defs  : /* Empty. */
      |    defs def
      ;
def   : START IDENTIFIER
      |    UNION
      {
        /* Copy union definition to output. */
      }
      |    LCURL
      {
        /* Copy C code to output file. */
      }
        RCURL
      |    rword tag nlist
      ;
rword : TOKEN
      | LEFT
      | RIGHT
      | NONASSOC
      | TYPE
      ;
tag   : /* Empty: union tag ID optional. */
      | '<' IDENTIFIER '>'
      ;
nlist : nmno
      | nlist nmno
      ;
nmno  : IDENTIFIER         /* Note: literal invalid with % type. */
      | IDENTIFIER NUMBER  /* Note: invalid with % type. */
      ;


/* Rule section */


rules : C_IDENTIFIER rbody prec
      | rules  rule
      ;
rule  : C_IDENTIFIER rbody prec
      | '|' rbody prec
      ;
rbody : /* empty */
      | rbody IDENTIFIER
      | rbody act
      ;
act   : '{'
        {
          /* Copy action, translate $$, and so on. */
        }
        '}'
      ;
prec  : /* Empty */
      | PREC IDENTIFIER
      | PREC IDENTIFIER act
      | prec ';'
      ;
