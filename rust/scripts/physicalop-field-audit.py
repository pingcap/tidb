"""Go physicalop struct fields with no snake_case field on the Rust struct of
the same name (register row P01). Run from anywhere: python3 rust/scripts/physicalop-field-audit.py"""
import re,glob,os
def snake(n):
    s=re.sub(r'([A-Z]+)([A-Z][a-z])',r'\1_\2',n); s=re.sub(r'([a-z0-9])([A-Z])',r'\1_\2',s); return s.lower()
go={}
for f in glob.glob(__import__('os').path.join(__import__('os').path.dirname(__file__), '..', '..', 'pkg', 'planner')+'/core/operator/physicalop/*.go'):
    if f.endswith('_test.go'): continue
    src=open(f).read()
    for m in re.finditer(r'^type (Physical\w+) struct \{\n(.*?)^\}', src, re.S|re.M):
        name,body=m.group(1),m.group(2)
        fields=[]
        for line in body.split('\n'):
            line=line.strip()
            if not line or line.startswith('//'): continue
            mm=re.match(r'([A-Za-z_]\w*)\s+\S',line)
            if mm and not line.startswith('*') and mm.group(1)[0].isupper():
                fields.append(mm.group(1))
        go[name]=fields
rs={}
root=__import__('os').path.join(__import__('os').path.dirname(__file__), '..', 'crates', 'tidb-planner-core-operator-physicalop', 'src')
for f in glob.glob(root+'/**/*.rs',recursive=True):
    src=open(f).read()
    for m in re.finditer(r'pub struct (Physical\w+)\s*\{(.*?)\n\}', src, re.S):
        rs.setdefault(m.group(1),set()).update(re.findall(r'pub (\w+):',m.group(2)))
total=0
for name in sorted(go):
    if name not in rs: continue
    missing=[g for g in go[name] if snake(g) not in rs[name] and snake(g).rstrip('s') not in rs[name]]
    if missing:
        total+=len(missing)
        print(f"{name}: {', '.join(missing)}")
print('missing',total,'| go structs',len(go),'| rust',len([n for n in go if n in rs]))
print('go structs absent in rust:', ', '.join(n for n in sorted(go) if n not in rs))
