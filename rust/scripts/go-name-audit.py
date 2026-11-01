#!/usr/bin/env python3
"""Go-name audit: Rust public names with no Go counterpart.

Each crate's owning Go package is its path (`tidb-infoschema-issyncer` is
`pkg/infoschema/issyncer`), else the unique `pkg/` directory its name ends
with, else the package its `lib.rs` docs name. Every `pub` / `pub(crate)` fn
and type is then classified by its snake_case name against that package's
Go identifiers (funcs, methods, types, fields, consts, vars):

  ok         the owning package has it
  elsewhere  only another Go package has it
  invented   no Go package has it

    python3 rust/scripts/go-name-audit.py audit.json   # JSON rows per crate
"""
import os,re,sys,json,collections
ROOT=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PKG=os.path.join(ROOT,'pkg'); CR=os.path.join(ROOT,'rust/crates')
PRODUCTS=(('TiKV','Tikv'),('TiDB','Tidb'),('TiFlash','Tiflash'),('MySQL','Mysql'),('IDs','Ids'),
          ('SCtx','Sctx'),('DBkey','DbKey'),('UnSynced','Unsynced'))
# A sized type name keeps its digits (Float64 -> float64, Int32 -> int32).
SIZED=re.compile(r'(Int|Uint|Float|int|uint|float)(8|16|32|64)(?=[A-Z_]|$)')
def snake(n):
    # Go CamelCase -> snake: GetHTTPServer -> get_http_server, LoadWithTS -> load_with_ts,
    # product names stay one word (TiKV -> tikv), digits are their own word (Task4Logical -> task_4_logical)
    for a,b in PRODUCTS: n=n.replace(a,b)
    n=SIZED.sub(lambda m:m.group(1)+'\x00'+m.group(2),n)
    n=re.sub(r'(?<=[A-Za-z])(\d+)',r'_\1_',n)
    n=n.replace('\x00_','').replace('\x00','')
    s=re.sub(r'([A-Z]+)([A-Z][a-z])',r'\1_\2',n)
    s=re.sub(r'([a-z0-9])([A-Z])',r'\1_\2',s)
    return re.sub(r'_+','_',s.lower()).strip('_')
go_re=[re.compile(r'^func\s+(?:\([^)]*\)\s*)?([A-Za-z_]\w*)\s*[\[(]',re.M),
       re.compile(r'^type\s+([A-Za-z_]\w*)',re.M),
       re.compile(r'^\t([A-Z]\w*)\s+[\*\[\]\w.]',re.M),   # struct fields / iface methods
       re.compile(r'^\t([A-Z]\w*)\s*\(',re.M),
       re.compile(r'^(?:const|var)\s+([A-Za-z_]\w*)',re.M),
       re.compile(r'^\t([A-Za-z_]\w*)\s*(?:=|\w+\s*=)',re.M)]
def go_names(d):
    names=set()
    for f in os.listdir(d):
        if f.endswith('.go') and not f.endswith('_test.go'):
            t=open(os.path.join(d,f),errors='ignore').read()
            for r in go_re:
                for m in r.finditer(t): names.add(snake(m.group(1)))
    return names
allgo=set(); pkgnames={}
for dp,dn,fn in os.walk(PKG):
    if any(f.endswith('.go') for f in fn):
        n=go_names(dp); pkgnames[os.path.relpath(dp,PKG)]=n; allgo|=n
def owning(crate):
    parts=crate[len('tidb-'):].split('-') if crate.startswith('tidb-') else crate.split('-')
    # longest prefix path that exists, allowing '_' joins inside a segment
    best=None
    def rec(i,path):
        nonlocal best
        if path and path in pkgnames and (best is None or i>best[0] or (i==best[0])): best=(i,path)
        if i==len(parts): return
        for j in range(i+1,len(parts)+1):
            seg='_'.join(parts[i:j])
            for cand in (seg,seg.replace('_','')):
                p=(path+'/' if path else '')+cand
                if os.path.isdir(os.path.join(PKG,p)): rec(j,p)
    rec(0,'')
    return best[1] if best and best[0]==len(parts) else (best[1] if best else None)
_dirs=sorted(pkgnames)
def owning2(crate):
    p=owning(crate)
    tail=crate[len('tidb-'):] if crate.startswith('tidb-') else crate
    if p and p.replace('/','-').replace('_','-')==tail: return p,'path'
    segs=tail.split('-')
    for k in range(len(segs),0,-1):
        want='/'.join(segs[-k:]); want2='_'.join(segs[-k:]); want3=''.join(segs[-k:])
        hits=[d for d in _dirs if d==want or d.endswith('/'+want) or d.endswith('/'+want2) or d.endswith('/'+want3) or d==want2 or d==want3]
        if len(hits)==1: return hits[0],'suffix'
    lib=os.path.join(CR,crate,'src','lib.rs')
    if os.path.exists(lib):
        head=open(lib,errors='ignore').read()[:6000]
        for m in re.finditer(r'`?pkg/([a-z0-9_/]+?)`?[\s`/,.:)]',head):
            cand=m.group(1).rstrip('/')
            cand=re.sub(r'/[a-z0-9_]+\.go$','',cand)
            while cand and cand not in pkgnames: cand=cand.rsplit('/',1)[0] if '/' in cand else ''
            if cand: return cand,'doc'
    return p,'path' if p else 'none'
rs_fn=re.compile(r'^\s*pub(?:\([^)]*\))?\s+(?:const\s+|async\s+|unsafe\s+)*fn\s+([a-z_]\w*)',re.M)
rs_ty=re.compile(r'^\s*pub(?:\([^)]*\))?\s+(?:struct|enum|trait|type)\s+([A-Z]\w*)',re.M)
RUST_IDIOMS={'new','default','as_str','as_bytes','as_slice','as_ref','as_mut','into_inner','into_parts','from_parts','from_raw','into_raw','ptr_eq','get_ref','get_mut','iter','iter_mut','into_iter','len','is_empty','with_capacity','clone_from','fmt','from_str','to_owned','spawn','drop','deref','borrow','try_from','from','into'}
out={}
for crate in sorted(os.listdir(CR)):
    src=os.path.join(CR,crate,'src')
    if not os.path.isdir(src): continue
    pkg,how=owning2(crate)
    own=pkgnames.get(pkg,set())
    rows=[]
    for dp,dn,fn in os.walk(src):
        for f in fn:
            if not f.endswith('.rs') or 'test' in f: continue
            p=os.path.join(dp,f); t=open(p,errors='ignore').read()
            # drop #[cfg(test)] mod bodies crudely: cut at '#[cfg(test)]\nmod'
            k=t.find('#[cfg(test)]\nmod')
            if k>=0: t=t[:k]
            for kind,r in (('fn',rs_fn),('type',rs_ty)):
                for m in r.finditer(t):
                    n=m.group(1); key=snake(n) if kind=='type' else n
                    if key in RUST_IDIOMS: continue
                    if key in own: cls='ok'
                    elif key in allgo: cls='elsewhere'
                    else: cls='invented'
                    line=t.count('\n',0,m.start())+1
                    rows.append((cls,kind,n,os.path.relpath(p,CR)+':'+str(line)))
    out[crate]={'pkg':pkg,'how':how,'rows':rows}
json.dump(out,open(sys.argv[1],'w'))
tot=collections.Counter(); 
for c,v in out.items():
    for r in v['rows']: tot[r[0]]+=1
print(tot)
