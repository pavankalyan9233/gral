#!/usr/bin/fish
set -xg nr 10000000
set -xg endpoint tcp://localhost:8529
set -xg password ""

time target/release/grupload randomize --vertices V --edges E --max-vertices="$nr" --max-edges="$nr"
time ~/cpp/graphutils/build/smartifier2 vertices --input V --output Vsmart --smart-value _key --smart-index=2 --smart-graph-attribute=sm --type jsonl
cp E Esmart
time ~/cpp/graphutils/build/smartifier2 edges --vertices V:V --edges Esmart:V:V --smart-value _key --smart-index=2 --smart-graph-attribute=sm --type jsonl
arangosh --server.endpoint "$endpoint" --server.password "$password" --javascript.execute doit.js
arangoimport --server.endpoint "$endpoint" --server.password "$password" --collection V --type jsonl --file Vsmart --create-collection false --overwrite false
arangoimport --server.endpoint "$endpoint" --server.password "$password" --collection E --type jsonl --file Esmart --create-collection false --overwrite false

# var pregel = require("@arangodb/pregel");
# var execution = pregel.start("wcc", "G", {parallelism:4});
# var status = pregel.status(execution);
# 4398056530829  2023-05-02T12:10:03Z"
# while (true) { status = pregel.status(execution); print(status); if (status.state === "done") { break; } ; require("internal").wait(1); print(new Date()); }
# require("fs").writeFileSync("3.10.parall4.json", JSON.stringify(status));
