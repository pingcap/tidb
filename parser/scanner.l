%{
// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"fmt"
	"math"
	"errors"
	"strconv"
	"unicode"
	"strings"
	
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/stmt"
)

type lexer struct {
	c		int
	col		int
	errs		[]error
	expr		expression.Expression
	i		int
	inj		int
	lcol		int
	line		int
	list		[]stmt.Statement
	ncol		int
	nline		int
	sc		int
	src		string
	val		[]byte
	ungetBuf	[]byte
	root		bool
	prepare		bool 
	ParamList	[]*expressions.ParamMarker
	stmtStartPos 	int
}

// NewLexer builds a new lexer.
func NewLexer(src string) (l *lexer) {
	l = &lexer{
		src:	src,
		nline:	1,
		ncol:	0,
	}
	l.next()
	return
}

func (l *lexer) Errors() []error {
	return l.errs
}

func (l *lexer) Stmts() []stmt.Statement{
	return l.list
}

func (l *lexer) Expr() expression.Expression {
	return l.expr
}

func (l *lexer) Inj() int {
	return l.inj
}

func (l *lexer) SetPrepare() {
	l.prepare = true	
}

func (l *lexer) IsPrepare() bool {
	return l.prepare	
}

func (l *lexer) SetInj(inj int) {
	l.inj = inj
}

func (l *lexer) Root() bool {
	return l.root
}

func (l *lexer) SetRoot(root bool) {
	l.root = root
}

func (l *lexer) unget(b byte) {
	l.ungetBuf = append(l.ungetBuf, b)
	l.i--
	l.ncol--
}

func (l *lexer) next() int {
	if un := len(l.ungetBuf); un > 0 {
		nc := l.ungetBuf[0]
		l.ungetBuf = l.ungetBuf[1:]
		l.c = int(nc)
		return  l.c
	}

	if l.c != 0 {
		l.val = append(l.val, byte(l.c))
	}
	l.c = 0
	if l.i < len(l.src) {
		l.c = int(l.src[l.i])
		l.i++
	}
	switch l.c {
	case '\n':
		l.lcol = l.ncol
		l.nline++
		l.ncol = 0
	default:
		l.ncol++
	}
	return l.c
}

func (l *lexer) err0(ln, c int, args ...interface{}) {
	var argStr string
	if len(args) > 0 {
		argStr = fmt.Sprintf(" " + args[0].(string), args[1:]...)
	}
	err := errors.New(fmt.Sprintf("line %d column %d near \"%s\"", ln, c, l.val) + argStr)
	l.errs = append(l.errs, err)
}

func (l *lexer) err(s string, args ...interface{}) {
	l.err0(l.line, l.col, args...)
}

func (l *lexer) Error(s string) {
	l.err(s)
}

func (l *lexer) stmtText() string {
	endPos := l.i
	if l.src[l.i-1] == '\n' {
		endPos = l.i-1 // trim new line	
	}
	if l.src[l.stmtStartPos] == '\n' {
		l.stmtStartPos++	
	}

	text := l.src[l.stmtStartPos:endPos] 

	l.stmtStartPos = l.i
	return text
}


func (l *lexer) Lex(lval *yySymType) (r int) {
	defer func() {
		lval.line, lval.col = l.line, l.col
	}()
	const (
		INITIAL = iota
		S1
		S2
		S3
	)

	if n := l.inj; n != 0 {
		l.inj = 0
		return n
	}

	c0, c := 0, l.c
%}

int_lit		{decimal_lit}|{octal_lit}
decimal_lit	[1-9][0-9]*
octal_lit	0[0-7]*
hex_lit		0[xX][0-9a-fA-F]+|[xX]"'"[0-9a-fA-F]+"'"

float_lit	{D}"."{D}?{E}?|{D}{E}|"."{D}{E}?
D		[0-9]+
E		[eE][-+]?[0-9]+

imaginary_ilit	{D}i
imaginary_lit	{float_lit}i

a		[aA]
b		[bB]
c		[cC]
d		[dD]
e		[eE]
f		[fF]
g		[gG]
h		[hH]
i		[iI]
j		[jJ]
k		[kK]
l		[lL]
m		[mM]
n		[nN]
o		[oO]
p		[pP]
q		[qQ]
r		[rR]
s		[sS]
t		[tT]
u		[uU]
v		[vV]
w		[wW]
x		[xX]
y		[yY]
z		[zZ]

abs		{a}{b}{s}
add		{a}{d}{d}
after		{a}{f}{t}{e}{r}
all		{a}{l}{l}
alter		{a}{l}{t}{e}{r}
and		{a}{n}{d}
any 		{a}{n}{y}
as		{a}{s}
asc		{a}{s}{c}
auto_increment	{a}{u}{t}{o}_{i}{n}{c}{r}{e}{m}{e}{n}{t}
avg		{a}{v}{g}
begin		{b}{e}{g}{i}{n}
between		{b}{e}{t}{w}{e}{e}{n}
by		{b}{y}
case		{c}{a}{s}{e}
cast		{c}{a}{s}{t}
character	{c}{h}{a}{r}{a}{c}{t}{e}{r}
charset		{c}{h}{a}{r}{s}{e}{t}
coalesce	{c}{o}{a}{l}{e}{s}{c}{e}
collate		{c}{o}{l}{l}{a}{t}{e}
column		{c}{o}{l}{u}{m}{n}
columns		{c}{o}{l}{u}{m}{n}{s}
commit		{c}{o}{m}{m}{i}{t}
concat		{c}{o}{n}{c}{a}{t}
concat_ws	{c}{o}{n}{c}{a}{t}_{w}{s}
constraint	{c}{o}{n}{s}{t}{r}{a}{i}{n}{t}
convert		{c}{o}{n}{v}{e}{r}{t}
count		{c}{o}{u}{n}{t}
create		{c}{r}{e}{a}{t}{e}
cross		{c}{r}{o}{s}{s}
database	{d}{a}{t}{a}{b}{a}{s}{e}
databases	{d}{a}{t}{a}{b}{a}{s}{e}{s}
day		{d}{a}{y}
dayofweek	{d}{a}{y}{o}{f}{w}{e}{e}{k}
dayofmonth	{d}{a}{y}{o}{f}{m}{o}{n}{t}{h}
dayofyear	{d}{a}{y}{o}{f}{y}{e}{a}{r}
deallocate	{d}{e}{a}{l}{l}{o}{c}{a}{t}{e}
default		{d}{e}{f}{a}{u}{l}{t}
delayed		{d}{e}{l}{a}{y}{e}{d}
delete		{d}{e}{l}{e}{t}{e}
drop		{d}{r}{o}{p}
desc		{d}{e}{s}{c}
describe	{d}{e}{s}{c}{r}{i}{b}{e}
distinct	{d}{i}{s}{t}{i}{n}{c}{t}
div		{d}{i}{v}
do		{d}{o}
duplicate	{d}{u}{p}{l}{i}{c}{a}{t}{e}
else		{e}{l}{s}{e}
end		{e}{n}{d}
engine		{e}{n}{g}{i}{n}{e}
engines		{e}{n}{g}{i}{n}{e}{s}
execute		{e}{x}{e}{c}{u}{t}{e}
exists		{e}{x}{i}{s}{t}{s}
explain		{e}{x}{p}{l}{a}{i}{n}
first		{f}{i}{r}{s}{t}
for		{f}{o}{r}
foreign		{f}{o}{r}{e}{i}{g}{n}
found_rows	{f}{o}{u}{n}{d}_{r}{o}{w}{s}
from		{f}{r}{o}{m}
full		{f}{u}{l}{l}
fulltext	{f}{u}{l}{l}{t}{e}{x}{t}
global		{g}{l}{o}{b}{a}{l}
group		{g}{r}{o}{u}{p}
group_concat	{g}{r}{o}{u}{p}_{c}{o}{n}{c}{a}{t}
having		{h}{a}{v}{i}{n}{g}
high_priority	{h}{i}{g}{h}_{p}{r}{i}{o}{r}{i}{t}{y}
hour		{h}{o}{u}{r}
if		{i}{f}
ifnull		{i}{f}{n}{u}{l}{l}
ignore		{i}{g}{n}{o}{r}{e}
in		{i}{n}
index		{i}{n}{d}{e}{x}
inner 		{i}{n}{n}{e}{r}
insert		{i}{n}{s}{e}{r}{t}
into		{i}{n}{t}{o}
is		{i}{s}
join		{j}{o}{i}{n}
key		{k}{e}{y}
left		{l}{e}{f}{t}
length		{l}{e}{n}{g}{t}{h}
like		{l}{i}{k}{e}
limit		{l}{i}{m}{i}{t}
local		{l}{o}{c}{a}{l}
lock		{l}{o}{c}{k}
low_priority	{l}{o}{w}_{p}{r}{i}{o}{r}{i}{t}{y}
microsecond	{m}{i}{c}{r}{o}{s}{e}{c}{o}{n}{d}
minute		{m}{i}{n}{u}{t}{e}
mod 		{m}{o}{d}
mode		{m}{o}{d}{e}
month		{m}{o}{n}{t}{h}
names		{n}{a}{m}{e}{s}
not		{n}{o}{t}
offset		{o}{f}{f}{s}{e}{t}
on		{o}{n}
or		{o}{r}
order		{o}{r}{d}{e}{r}
outer		{o}{u}{t}{e}{r}
password	{p}{a}{s}{s}{w}{o}{r}{d}
prepare		{p}{r}{e}{p}{a}{r}{e}
primary		{p}{r}{i}{m}{a}{r}{y}
quick		{q}{u}{i}{c}{k}
repeat		{r}{e}{p}{e}{a}{t}
references	{r}{e}{f}{e}{r}{e}{n}{c}{e}{s}
regexp		{r}{e}{g}{e}{x}{p}
right		{r}{i}{g}{h}{t}
rlike		{r}{l}{i}{k}{e}
rollback	{r}{o}{l}{l}{b}{a}{c}{k}
row 		{r}{o}{w}
schema		{s}{c}{h}{e}{m}{a}
schemas		{s}{c}{h}{e}{m}{a}{s}
second		{s}{e}{c}{o}{n}{d}
select		{s}{e}{l}{e}{c}{t}
session		{s}{e}{s}{s}{i}{o}{n}
set		{s}{e}{t}
share		{s}{h}{a}{r}{e}
show		{s}{h}{o}{w}
some		{s}{o}{m}{e}
start		{s}{t}{a}{r}{t}
substring	{s}{u}{b}{s}{t}{r}{i}{n}{g}
sum		{s}{u}{m}
table		{t}{a}{b}{l}{e}
tables		{t}{a}{b}{l}{e}{s}
then		{t}{h}{e}{n}
transaction	{t}{r}{a}{n}{s}{a}{c}{t}{i}{o}{n}
truncate	{t}{r}{u}{n}{c}{a}{t}{e}
max		{m}{a}{x}
min		{m}{i}{n}
unknown		{u}{n}{k}{n}{o}{w}{n}
union		{u}{n}{i}{o}{n}
unique		{u}{n}{i}{q}{u}{e}
nullif		{n}{u}{l}{l}{i}{f}
update		{u}{p}{d}{a}{t}{e}
value		{v}{a}{l}{u}{e}
values		{v}{a}{l}{u}{e}{s}
variables	{v}{a}{r}{i}{a}{b}{l}{e}{s}
warnings	{w}{a}{r}{n}{i}{n}{g}{s}
week		{w}{e}{e}{k}
weekday		{w}{e}{e}{k}{d}{a}{y}
weekofyear	{w}{e}{e}{k}{o}{f}{y}{e}{a}{r}
where		{w}{h}{e}{r}{e}
when		{w}{h}{e}{n}
xor		{x}{o}{r}
yearweek	{y}{e}{a}{r}{w}{e}{e}{k}

null		{n}{u}{l}{l}
false		{f}{a}{l}{s}{e}
true		{t}{r}{u}{e}

calc_found_rows	{s}{q}{l}_{c}{a}{l}{c}_{f}{o}{u}{n}{d}_{r}{o}{w}{s}

current_ts	{c}{u}{r}{r}{e}{n}{t}_{t}{i}{m}{e}{s}{t}{a}{m}{p}
localtime	{l}{o}{c}{a}{l}{t}{i}{m}{e}
localts		{l}{o}{c}{a}{l}{t}{i}{m}{e}{s}{t}{a}{m}{p}
now		{n}{o}{w}

bit		{b}{i}{t}
tiny		{t}{i}{n}{y}
tinyint		{t}{i}{n}{y}{i}{n}{t}
smallint	{s}{m}{a}{l}{l}{i}{n}{t}
mediumint	{m}{e}{d}{i}{u}{m}{i}{n}{t}
int		{i}{n}{t}
integer		{i}{n}{t}{e}{g}{e}{r}
bigint		{b}{i}{g}{i}{n}{t}
real		{r}{e}{a}{l}
double		{d}{o}{u}{b}{l}{e}
float		{f}{l}{o}{a}{t}
decimal		{d}{e}{c}{i}{m}{a}{l}
numeric		{n}{u}{m}{e}{r}{i}{c}
date		{d}{a}{t}{e}
time		{t}{i}{m}{e}
timestamp	{t}{i}{m}{e}{s}{t}{a}{m}{p}
datetime	{d}{a}{t}{e}{t}{i}{m}{e}
year		{y}{e}{a}{r}
char		{c}{h}{a}{r}
varchar		{v}{a}{r}{c}{h}{a}{r}
binary		{b}{i}{n}{a}{r}{y}
varbinary	{v}{a}{r}{b}{i}{n}{a}{r}{y}
tinyblob	{t}{i}{n}{y}{b}{l}{o}{b}
blob		{b}{l}{o}{b}
mediumblob	{m}{e}{d}{i}{u}{m}{b}{l}{o}{b}
longblob	{l}{o}{n}{g}{b}{l}{o}{b}
tinytext	{t}{i}{n}{y}{t}{e}{x}{t}
text		{t}{e}{x}{t}
mediumtext	{m}{e}{d}{i}{u}{m}{t}{e}{x}{t}
longtext	{l}{o}{n}{g}{t}{e}{x}{t}
enum		{e}{n}{u}{m}
precision	{p}{r}{e}{c}{i}{s}{i}{o}{n}

signed		{s}{i}{g}{n}{e}{d}
unsigned	{u}{n}{s}{i}{g}{n}{e}{d}
zerofill	{z}{e}{r}{o}{f}{i}{l}{l}

bigrat		{b}{i}{g}{r}{a}{t}
bool		{b}{o}{o}{l}
boolean		{b}{o}{o}{l}{e}{a}{n}
byte		{b}{y}{t}{e}
duration	{d}{u}{r}{a}{t}{i}{o}{n}
rune		{r}{u}{n}{e}
string		{s}{t}{r}{i}{n}{g}
use		{u}{s}{e}
using		{u}{s}{i}{n}{g}

idchar0		[a-zA-Z_]
idchars		{idchar0}|[0-9]
ident		{idchar0}{idchars}*
quotedIdent	`{ident}`

user_var	"@"{ident}
sys_var		"@@"(({global}".")|({session}".")|{local}".")?{ident}

%yyc c
%yyn c = l.next()
%yyt l.sc

%x S1 S2 S3

%%
		l.val = l.val[:0]
		c0, l.line, l.col = l.c, l.nline, l.ncol
                        
<*>\0		return 0

[ \t\n\r]+
#.*
\/\/.*
\/\*([^*]|\*+[^*/])*\*+\/
--			l.sc = S3
<S3>[ \t]+.*		{l.sc = 0} 
<S3>[^ \t]		{
				l.sc = 0
				l.c = '-'
				n := len(l.val)
				l.unget(l.val[n-1])
				return '-' 
			}

{int_lit}		return l.int(lval)
{float_lit}		return l.float(lval)
{hex_lit}		return l.hex(lval)

\"			l.sc = S1
'			l.sc = S2

<S1>(\\.|[^\"])*\"	return l.str(lval, "\"")
<S2>((\\')|[^']|\n)*'	return l.str(lval, "'")

"&&"			return andand
"&^"			return andnot
"<<"			return lsh
"<="			return le
"=" 			return eq
">="			return ge
"!="			return neq
"<>"			return neq
"||"			return oror
">>"			return rsh

"?"			return placeholder

{abs}			lval.item = string(l.val)
			return abs
{add}			return add
{after}			lval.item = string(l.val)
			return after
{all}			return all
{alter}			return alter
{and}			return and
{any}			lval.item = string(l.val)
			return any
{asc}			return asc
{as}			return as
{auto_increment}	lval.item = string(l.val)
			return autoIncrement
{avg}			lval.item = string(l.val)
			return avg
{begin}			lval.item = string(l.val)
			return begin
{between}		return between
{by}			return by
{case}			return caseKwd
{cast}			return cast
{character}		return character
{charset}		lval.item = string(l.val)
			return charsetKwd
{coalesce}		lval.item = string(l.val)
			return coalesce
{collate}		return collation
{column}		return column
{columns}		lval.item = string(l.val)
			return columns
{commit}		lval.item = string(l.val)
			return commit
{concat}		lval.item = string(l.val)
			return concat
{concat_ws}		lval.item = string(l.val)
			return concatWs
{constraint}		return constraint
{convert}		return convert
{count}			lval.item = string(l.val)
			return count
{create}		return create
{cross}			return cross
{database}		lval.item = string(l.val)
			return database
{databases}		return databases
{day}			lval.item = string(l.val)
			return day
{dayofweek}		lval.item = string(l.val)
			return dayofweek
{dayofmonth}		lval.item = string(l.val)
			return dayofmonth
{dayofyear}		lval.item = string(l.val)
			return dayofyear
{deallocate}		lval.item = string(l.val)
			return deallocate
{default}		return defaultKwd
{delayed}		return delayed
{delete}		return deleteKwd
{desc}			return desc
{describe}		return describe
{drop}			return drop
{distinct}		return distinct
{div}			return div
{do}			lval.item = string(l.val)
			return do
{duplicate}		lval.item = string(l.val)
			return duplicate
{else}			return elseKwd
{end}			lval.item = string(l.val)
			return end
{engine}		lval.item = string(l.val)
			return engine
{engines}		lval.item = string(l.val)
			return engines
{execute}		lval.item = string(l.val)
			return execute
{exists}		return exists
{explain}		return explain
{first}			lval.item = string(l.val)
			return first
{for}			return forKwd
{foreign}		return foreign
{found_rows}		lval.item = string(l.val)
			return foundRows
{from}			return from
{full}			lval.item = string(l.val)
			return full
{fulltext}		return fulltext
{group}			return group
{group_concat}		lval.item = string(l.val)
			return groupConcat
{having}		return having
{high_priority}		return highPriority
{hour}			lval.item = string(l.val)
			return hour
{if}			lval.item = string(l.val)
			return ifKwd
{ifnull}		lval.item = string(l.val)
			return ifNull
{ignore}		return ignore
{index}			return index
{inner} 		return inner
{insert}		return insert
{into}			return into
{in}			return in
{is}			return is
{join}			return join
{key}			return key
{left}			lval.item = string(l.val)
			return left
{length}		lval.item = string(l.val)
			return length
{like}			return like
{limit}			return limit
{local}			lval.item = string(l.val)
			return local
{lock}			return lock
{low_priority}		return lowPriority
{max}			lval.item = string(l.val)
			return max
{microsecond}		lval.item = string(l.val)
			return microsecond
{min}			lval.item = string(l.val)
			return min
{minute}		lval.item = string(l.val)
			return minute
{mod}			return mod
{mode}			lval.item = string(l.val)
			return mode
{month}			lval.item = string(l.val)
			return month
{names}			lval.item = string(l.val)
			return names
{not}			return not
{offset}		lval.item = string(l.val)
			return offset
{on}			return on
{order}			return order
{or}			return or
{outer}			return outer
{password}		lval.item = string(l.val)
			return password
{prepare}		lval.item = string(l.val)
			return prepare
{primary}		return primary
{quick}			lval.item = string(l.val)
			return quick
{right}			return right
{rollback}		lval.item = string(l.val)
			return rollback
{row}			lval.item = string(l.val)
			return row
{schema}		lval.item = string(l.val)
			return schema
{schemas}		return schemas
{session}		lval.item = string(l.val)
			return session
{some}			lval.item = string(l.val)
			return some
{start}			lval.item = string(l.val)
			return start
{global}		lval.item = string(l.val)
			return global
{repeat}		lval.item = string(l.val)
			return repeat
{regexp}		return regexp
{references}		return references
{rlike}			return rlike

{sys_var}		lval.item = string(l.val)
			return sysVar

{user_var}		lval.item = string(l.val)
			return userVar
{second}		lval.item = string(l.val)
			return second
{select}		return selectKwd

{set}			return set
{share}			return share
{show}			return show
{substring}		lval.item = string(l.val)
			return substring
{sum}			lval.item = string(l.val)
			return sum
{table}			return tableKwd
{tables}		lval.item = string(l.val)
			return tables
{then}			return then
{transaction}		lval.item = string(l.val)
			return transaction
{truncate}		lval.item = string(l.val)
			return truncate
{union}			return union
{unique}		return unique
{unknown}		lval.item = string(l.val)
			return unknown
{nullif}		lval.item = string(l.val)
			return nullIf
{update}		return update
{use}			return use
{using}			return using
{value}			lval.item = string(l.val)
			return value
{values}		return values
{variables}		lval.item = string(l.val)
			return variables
{warnings}		lval.item = string(l.val)
			return warnings
{week}			lval.item = string(l.val)
			return week
{weekday}		lval.item = string(l.val)
			return weekday
{weekofyear}		lval.item = string(l.val)
			return weekofyear
{when}			return when
{where}			return where
{xor}			return xor
{yearweek}		lval.item = string(l.val)
			return yearweek
			
{signed}		lval.item = string(l.val)
			return signed
{unsigned}		return unsigned
{zerofill}		return zerofill

{null}			lval.item = nil
			return null

{false}			return falseKwd

{true}			return trueKwd

{calc_found_rows}	lval.item = string(l.val)
			return calcFoundRows

{current_ts}		return currentTs
{localtime}		return localTime
{localts}		return localTs
{now}			lval.item = string(l.val)
			return now

{bit}			lval.item = string(l.val) 
			return bitType

{tiny}			lval.item = string(l.val) 
			return tinyIntType

{tinyint}		lval.item = string(l.val) 
			return tinyIntType

{smallint}		lval.item = string(l.val) 
			return smallIntType

{mediumint}		lval.item = string(l.val)
			return mediumIntType

{bigint}		lval.item = string(l.val)
			return bigIntType

{decimal}		lval.item = string(l.val)
			return decimalType

{numeric}		lval.item = string(l.val)
			return numericType

{float}			lval.item = string(l.val)
			return floatType

{double}		lval.item = string(l.val)
			return doubleType

{precision}		lval.item = string(l.val)
			return precisionType

{real}			lval.item = string(l.val)
			return realType

{date}			lval.item = string(l.val)
			return dateType

{time}			lval.item = string(l.val) 
			return timeType

{timestamp}		lval.item = string(l.val)
			return timestampType

{datetime}		lval.item = string(l.val)
			return datetimeType

{year}			lval.item = string(l.val)
			return yearType

{char}			lval.item = string(l.val)
			return charType

{varchar}		lval.item = string(l.val)
			return varcharType

{binary}		lval.item = string(l.val)
			return binaryType

{varbinary}		lval.item = string(l.val)
			return varbinaryType

{tinyblob}		lval.item = string(l.val)
			return tinyblobType

{blob}			lval.item = string(l.val)
			return blobType

{mediumblob}		lval.item = string(l.val)
			return mediumblobType

{longblob}		lval.item = string(l.val)
			return longblobType

{tinytext}		lval.item = string(l.val)
			return tinytextType

{mediumtext}		lval.item = string(l.val)
			return mediumtextType

{text}			lval.item = string(l.val)
			return textType

{longtext}		lval.item = string(l.val)
			return longtextType

{bool}			lval.item = string(l.val) 
			return boolType

{boolean}		lval.item = string(l.val)
			return booleanType

{byte}			lval.item = string(l.val) 
			return byteType

{int}			lval.item = string(l.val)
			return intType

{integer}		lval.item = string(l.val)
			return integerType

{ident}			lval.item = string(l.val)
			return identifier

{quotedIdent}		lval.item = l.trimIdent(string(l.val))
			return identifier

.			return c0

%%
			return int(unicode.ReplacementChar)
}

func (l *lexer) npos() (line, col int) {
	if line, col = l.nline, l.ncol; col == 0 {
		line--
		col = l.lcol+1
	}
	return
} 

func (l *lexer) str(lval *yySymType, pref string) int {
	l.sc = 0
	s := string(l.val)
	// TODO: performance issue.
	if pref == "'" {
		s = strings.Replace(s, "\\'", "'", -1)    
		s = strings.TrimSuffix(s, "'") + "\""
		pref = "\""
	}
	v, err := strconv.Unquote(pref + s)
	if err != nil {
		v = strings.TrimSuffix(s, pref)
	}
	lval.item = v
	return stringLit
}

func (l *lexer) trimIdent(idt string) string {
	idt = strings.TrimPrefix(idt, "`")    
	idt = strings.TrimSuffix(idt, "`")    
	return idt
}

func (l *lexer) int(lval *yySymType) int {
	n, err := strconv.ParseUint(string(l.val), 0, 64)
	if err != nil {
		l.err("", "integer literal: %v", err)
		return int(unicode.ReplacementChar)
	}

	switch {
	case n < math.MaxInt64:
		lval.item = int64(n)
	default:
		lval.item = uint64(n)
	}
	return intLit
}

func (l *lexer) float(lval *yySymType) int {
	n, err := strconv.ParseFloat(string(l.val), 64)
	if err != nil {
		l.err("", "float literal: %v", err)
		return int(unicode.ReplacementChar)
	}

	lval.item = float64(n)
	return floatLit
}

// https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func (l *lexer) hex(lval *yySymType) int {
	s := string(l.val)
	// convert x'12' to general 0x12
	s = strings.Replace(s, "'", "", -1)
	if s[0] != '0' {
		s = "0" + s
	}
	lval.item = s 
	return stringLit
}
