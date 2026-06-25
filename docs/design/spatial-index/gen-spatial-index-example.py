#!/usr/bin/env python3
# Generates spatial-index-example.svg, the figure for the triangle worked example in
# research.md (and the design doc). It draws the quadtree grid levels, the three sample
# geometries (triangle id 42 "T", rectangle id 75, triangle id 1), their covering cells,
# and the shared cell 0320. Domain [0,8)^2, unit = 64px.
#
# Regenerate:  python3 gen-spatial-index-example.py   (writes the .svg next to this file)
# To a PNG:    rsvg-convert -o spatial-index-example.png spatial-index-example.svg
#
# Colors are a blue-forward approximation; replace the hex values below with the official
# PingCAP brand palette if desired (only the C_* / G_* constants need changing).
import os

S = 64                      # px per unit
MX, MY_TOP = 50, 60         # left margin, top margin (title)
LEGEND_W = 230
W = MX + 8*S + LEGEND_W
H = MY_TOP + 8*S + 46
Y0 = MY_TOP + 8*S           # svg y of math y=0

def sx(x): return MX + x*S
def sy(y): return Y0 - y*S

# Official TiDB brand palette (TiDB brand guidelines): blue / teal / red primary hues,
# violet for the shared-cell highlight, Carbon neutrals for the grid.
C_T      = "#2C80CE"   # triangle id42      -> Medium Blue
C_R      = "#1AA8A8"   # rectangle id75     -> Medium Teal
C_S      = "#DC150B"   # small triangle id1 -> Brand Red
C_SHARE  = "#5D137D"   # shared-cell border/label -> Dark Violet
F_SHARE  = "#C76FF2"   # shared-cell fill   -> Light Violet
# Carbon neutrals: 100 (unit grid), 300 (L3), 500 (L2), 800 (L1 box + text)
G_UNIT, G_L3, G_L2, G_L1 = "#E5E8EB", "#B9C2CA", "#8B96A2", "#424D57"
TXT = "#424D57"
C_TEAL_DARK = "#0F5353"  # Dark Teal, for the id 75 label/leader
C_NOTE = "#74808B"       # Carbon 600, for the small legend note

out = []
out.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" font-family="Helvetica,Arial,sans-serif">')
out.append(f'<rect width="{W}" height="{H}" fill="#ffffff"/>')
out.append(f'<text x="{MX}" y="34" font-size="20" font-weight="700" fill="{TXT}">Spatial index: covering cells for three geometries (domain [0,8)²)</text>')

# --- grid: unit, then L3 (size2), L2 (size4), L1 (size8) on top ---
def vline(x,color,w): out.append(f'<line x1="{sx(x)}" y1="{sy(0)}" x2="{sx(x)}" y2="{sy(8)}" stroke="{color}" stroke-width="{w}"/>')
def hline(y,color,w): out.append(f'<line x1="{sx(0)}" y1="{sy(y)}" x2="{sx(8)}" y2="{sy(y)}" stroke="{color}" stroke-width="{w}"/>')
for i in range(9):
    vline(i, G_UNIT, 1); hline(i, G_UNIT, 1)
for i in (0,2,4,6,8):
    vline(i, G_L3, 1.5); hline(i, G_L3, 1.5)
for i in (0,4,8):
    vline(i, G_L2, 2.5); hline(i, G_L2, 2.5)
out.append(f'<rect x="{sx(0)}" y="{sy(8)}" width="{8*S}" height="{8*S}" fill="none" stroke="{G_L1}" stroke-width="3"/>')

# --- covering cell borders (dashed). Labels are queued and drawn LAST, on top, with a
#     white halo, so gridlines and geometry edges never cross the text. ---
labels = []   # (cx, cy, text, color, weight)
def cell(label, x, y, w, h, color):
    out.append(f'<rect x="{sx(x)}" y="{sy(y+h)}" width="{w*S}" height="{h*S}" fill="none" '
               f'stroke="{color}" stroke-width="2" stroke-dasharray="5,3" opacity="0.85"/>')
    labels.append((sx(x+w/2), sy(y+h/2), label, color, "600"))

# T id42 covering (blue), excluding the shared cell 0320 drawn below
for lab,x,y,w,h in [("030",4,4,2,2),("0310",6,4,1,1),("0311",7,4,1,1),
                    ("0312",6,5,1,1),("0321",5,6,1,1),("0322",4,7,1,1)]:
    cell(lab,x,y,w,h,C_T)
# rectangle id75 covering (teal); 0300/0301 sit inside T's 030 (descendants)
for lab,x,y,w,h in [("0033",3,3,1,1),("0122",4,3,1,1),("0123",5,3,1,1),
                    ("0211",3,4,1,1),("0300",4,4,1,1),("0301",5,4,1,1)]:
    cell(lab,x,y,w,h,C_R)

# --- shared cell 0320 (gold fill + magenta border); label queued for the top pass ---
out.append(f'<rect x="{sx(4)}" y="{sy(7)}" width="{S}" height="{S}" fill="{F_SHARE}" fill-opacity="0.40" '
           f'stroke="{C_SHARE}" stroke-width="3"/>')
labels.append((sx(4.5), sy(6.5), "0320 ★", C_SHARE, "700"))

# --- geometries (filled translucent + solid stroke) ---
def poly(pts, color, op):
    p = " ".join(f"{sx(x)},{sy(y)}" for x,y in pts)
    out.append(f'<polygon points="{p}" fill="{color}" fill-opacity="{op}" stroke="{color}" stroke-width="2.5"/>')
poly([(4,4),(8,4),(4,8)], C_T, 0.18)              # T id42
poly([(3,3),(6,3),(6,5),(3,5)], C_R, 0.22)        # rectangle id75
poly([(4,6),(5,6),(4,7)], C_S, 0.55)              # small triangle id1

# geometry id labels (in empty areas, with short leader lines where needed)
out.append(f'<text x="{sx(6.55)}" y="{sy(6.6)}" font-size="13" fill="{C_T}" font-weight="700">T (id 42)</text>')
out.append(f'<line x1="{sx(2.95)}" y1="{sy(6.5)}" x2="{sx(4)}" y2="{sy(6.5)}" stroke="{C_S}" stroke-width="1"/>')
out.append(f'<text x="{sx(2.0)}" y="{sy(6.45)}" font-size="12" fill="{C_S}" font-weight="700">id 1</text>')
out.append(f'<line x1="{sx(4.5)}" y1="{sy(2.7)}" x2="{sx(4.5)}" y2="{sy(3)}" stroke="{C_TEAL_DARK}" stroke-width="1"/>')
out.append(f'<text x="{sx(4.5)}" y="{sy(2.4)}" font-size="13" fill="{C_TEAL_DARK}" font-weight="700" text-anchor="middle">id 75</text>')

# --- axes ticks ---
for i in range(9):
    out.append(f'<text x="{sx(i)}" y="{sy(0)+18}" font-size="11" fill="{TXT}" text-anchor="middle">{i}</text>')
    out.append(f'<text x="{sx(0)-14}" y="{sy(i)+4}" font-size="11" fill="{TXT}" text-anchor="middle">{i}</text>')

# --- legend ---
lx = MX + 8*S + 18
ly = MY_TOP + 10
def lg(dy, color, text, fill=None, dash=False):
    yy = ly+dy
    f = fill if fill else color
    op = "0.25" if not fill else "0.55"
    extra = ' stroke-dasharray="5,3"' if dash else ''
    out.append(f'<rect x="{lx}" y="{yy}" width="18" height="14" fill="{f}" fill-opacity="{op}" stroke="{color}" stroke-width="2"{extra}/>')
    out.append(f'<text x="{lx+26}" y="{yy+12}" font-size="12" fill="{TXT}">{text}</text>')
out.append(f'<text x="{lx}" y="{ly-6}" font-size="13" font-weight="700" fill="{TXT}">Legend</text>')
lg(8,  C_T, "Triangle id 42 (T)")
lg(34, C_R, "Rectangle id 75")
lg(60, C_S, "Triangle id 1")
lg(86, C_SHARE, "Shared cell 0320 (id 1 + id 42)", fill=F_SHARE)
lg(112, C_T, "covering cell (dashed)", dash=True)
out.append(f'<text x="{lx}" y="{ly+150}" font-size="11.5" fill="{TXT}" font-weight="700">Quadtree levels (grid):</text>')
for k,(c,t) in enumerate([(G_L1,"L1 size 8 (cell 0)"),(G_L2,"L2 size 4"),(G_L3,"L3 size 2"),(G_UNIT,"unit size 1")]):
    yy = ly+168+k*20
    out.append(f'<line x1="{lx}" y1="{yy}" x2="{lx+24}" y2="{yy}" stroke="{c}" stroke-width="{[3,2.5,1.5,1][k]}"/>')
    out.append(f'<text x="{lx+30}" y="{yy+4}" font-size="11.5" fill="{TXT}">{t}</text>')
out.append(f'<text x="{lx}" y="{ly+260}" font-size="11" fill="{C_NOTE}">0300/0301 (id 75) sit inside</text>')
out.append(f'<text x="{lx}" y="{ly+275}" font-size="11" fill="{C_NOTE}">030 (id 42): descendants, not</text>')
out.append(f'<text x="{lx}" y="{ly+290}" font-size="11" fill="{C_NOTE}">a shared key.</text>')

# --- cell-id labels LAST, each on a white halo so nothing crosses the text ---
for cx, cy, text, color, weight in labels:
    tw = len(text)*6.6 + 8
    out.append(f'<rect x="{cx-tw/2:.1f}" y="{cy-9:.1f}" width="{tw:.1f}" height="15" rx="2.5" fill="#ffffff" fill-opacity="0.92"/>')
    out.append(f'<text x="{cx:.1f}" y="{cy+4:.1f}" font-size="11" fill="{color}" text-anchor="middle" font-weight="{weight}">{text}</text>')

out.append('</svg>')

path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spatial-index-example.svg")
open(path, "w").write("\n".join(out))
print("wrote", path, "bytes:", len("\n".join(out)))
