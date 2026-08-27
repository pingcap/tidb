#!/usr/bin/env bash
set -euo pipefail

# Extracts a deterministic, relationally useful slice from a TPC-DS SF1
# directory.  The generator itself is intentionally not vendored here: this
# script only selects rows from files produced by tidb-bench/tpcds/dsdgen.
if [[ $# -ne 2 ]]; then
    echo "usage: $0 <sf1-tools-dir> <output-dir>" >&2
    exit 2
fi

src=$1
out=$2
if [[ ! -d "$src" || ! -f "$src/tpcds.sql" ]]; then
    echo "source must be the tidb-bench/tpcds/tools directory" >&2
    exit 2
fi
if [[ -e "$out" ]]; then
    echo "refusing to overwrite existing output directory: $out" >&2
    exit 2
fi
mkdir -p "$out"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/tpcds-slice.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# Q3/Q6/Q7/Q10/Q13/Q15/Q19/Q25/Q26/Q29/Q34/Q35/Q42/Q43/Q45/Q46/Q48/Q50/
# Q52/Q55/Q61/Q62/Q65/Q66/Q68/Q69/Q71/Q72/Q73/Q76/Q79/Q85/Q91 use dates in
# this five-year band.  Keeping the complete date dimension for that band
# retains the date predicates while reducing 73,049 rows to 1,826.
awk -F'|' '$7 >= 1998 && $7 <= 2002' "$src/date_dim.dat" > "$out/date_dim.dat"
cut -d'|' -f1 "$out/date_dim.dat" > "$tmp/date.keys"

select_fact() {
    local table=$1
    local max_rows=$2
    awk -F'|' -v limit="$max_rows" \
        'NR == FNR { keys[$1] = 1; next } ($1 in keys) && n++ < limit { print }' \
        "$tmp/date.keys" "$src/$table.dat" > "$out/$table.dat"
}

# Keep enough rows to exercise joins and aggregates, but avoid loading the
# full 1.2 GB SF1 corpus into the local playground.  The same date-key rule
# is applied to every fact table, so repeated runs are byte-for-byte stable.
select_fact store_sales 50000
select_fact store_returns 50000
select_fact catalog_sales 50000
select_fact catalog_returns 50000
select_fact web_sales 50000
select_fact web_returns 50000
select_fact inventory 50000

# Build foreign-key sets from the selected facts.  Dimension rows not reached
# by these facts are still included up to a bounded prefix so literal-only
# and dimension-only queries (for example Q41/Q84) remain meaningful.
awk -F'|' '{print $3}' "$out/store_sales.dat" "$out/store_returns.dat" \
    "$out/catalog_returns.dat" "$out/web_returns.dat" > "$tmp/item.keys"
awk -F'|' '{print $16}' "$out/catalog_sales.dat" "$out/web_sales.dat" >> "$tmp/item.keys"
sort -u "$tmp/item.keys" -o "$tmp/item.keys"
awk -F'|' '{print $4}' "$out/store_sales.dat" "$out/catalog_sales.dat" "$out/web_sales.dat" > "$tmp/customer.keys"
sort -u "$tmp/customer.keys" -o "$tmp/customer.keys"
awk -F'|' '{print $8}' "$out/store_sales.dat" > "$tmp/store.keys"
sort -u "$tmp/store.keys" -o "$tmp/store.keys"
awk -F'|' '{print $5}' "$out/store_sales.dat" "$out/catalog_sales.dat" "$out/web_sales.dat" > "$tmp/demo.keys"
sort -u "$tmp/demo.keys" -o "$tmp/demo.keys"
awk -F'|' '{print $6}' "$out/store_sales.dat" "$out/catalog_sales.dat" "$out/web_sales.dat" > "$tmp/hdemo.keys"
sort -u "$tmp/hdemo.keys" -o "$tmp/hdemo.keys"
awk -F'|' '{print $7}' "$out/store_sales.dat" "$out/catalog_sales.dat" "$out/web_sales.dat" > "$tmp/address.keys"
sort -u "$tmp/address.keys" -o "$tmp/address.keys"
awk -F'|' '{print $9}' "$out/store_sales.dat" > "$tmp/promo.keys"
awk -F'|' '{print $17}' "$out/catalog_sales.dat" "$out/web_sales.dat" >> "$tmp/promo.keys"
sort -u "$tmp/promo.keys" -o "$tmp/promo.keys"
awk -F'|' '{print $2}' "$out/inventory.dat" > "$tmp/inventory-item.keys"
sort -u "$tmp/inventory-item.keys" -o "$tmp/inventory-item.keys"

select_dimension() {
    local table=$1
    local key_file=$2
    local max_rows=$3
    awk -F'|' -v limit="$max_rows" \
        'NR == FNR { keys[$1] = 1; next } (($1 in keys) || n++ < limit) { print }' \
        "$key_file" "$src/$table.dat" > "$out/$table.dat"
}

select_dimension item "$tmp/item.keys" 20000
select_dimension customer "$tmp/customer.keys" 20000
select_dimension customer_address "$tmp/address.keys" 20000
select_dimension customer_demographics "$tmp/demo.keys" 20000
select_dimension household_demographics "$tmp/hdemo.keys" 10000
select_dimension store "$tmp/store.keys" 10000
select_dimension promotion "$tmp/promo.keys" 10000

# These dimensions are small enough to retain in full.  time_dim is bounded
# because Q88/Q90/Q96 only need a representative prefix for this smoke slice.
for table in dbgen_version warehouse ship_mode reason income_band call_center web_site web_page catalog_page; do
    cp "$src/$table.dat" "$out/$table.dat"
done
head -n 20000 "$src/time_dim.dat" > "$out/time_dim.dat"

cp "$src/tpcds.sql" "$out/tpcds.sql"
{
    echo "source=$src"
    echo "date_years=1998..2002"
    echo "fact_cap=50000_each"
    echo "generated=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "sha256="
    LC_ALL=POSIX shasum -a 256 "$out"/*.dat | sort
} > "$out/MANIFEST"
