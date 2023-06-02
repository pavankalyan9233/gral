#!/usr/bin/fish
set -xg PATH (pwd)/target/debug $PATH

gral > gral.log &

grupload randomize --vertices V --edges E --vertex-coll-name V --max-vertices 10000 --max-edges 5000 --key-size 20
grupload upload --vertices V --edges E --threads 2
grupload compute --graph 0 --algorithm wcc | tee computation.log
set comp_id (tail -n 1 computation.log)

for i in (seq 0 3) 
    grupload progress --comp-id "$comp_id"
    sleep 1
end

grupload vertexresults --comp-id "$comp_id" --vertices V --output V_E_wcomps
grupload dropcomp --comp-id "$comp_id"

grupload compute --graph 0 --algorithm scc | tee computation.log
set comp_id (tail -n 1 computation.log)

for i in (seq 0 3) 
    grupload progress --comp-id "$comp_id"
    sleep 1
end

grupload vertexresults --comp-id "$comp_id" --vertices V --output V_E_scomps
grupload dropcomp --comp-id "$comp_id"

grupload dropGraph --graph 0

jobs

kill -INT %1
