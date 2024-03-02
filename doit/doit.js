let g = require("@arangodb/smart-graph");
try {
  g._drop("G", true);
} catch(e) {
}
g._create("G", [g._relation("E", ["V"],["V"])], [], {smartGraphAttribute:"sm", numberOfShards:30, replicationFactor:2})
