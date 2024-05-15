import {hideBin} from 'yargs/helpers';
import {config} from '../config/environment.js';
import {createRequire} from 'module';
import * as environment from "../config/environment.js";

const require = createRequire(import.meta.url);
const yargs = require('yargs/yargs');

function parseArgs() {
  const argv = yargs(hideBin(process.argv))
    .option('graphName', {
      alias: 'g',
      type: 'string',
      description: 'Name of the graph',
      default: 'twitter_mpi',
    })
    .option('neo4jEndpoint', {
      alias: 'e',
      type: 'string',
      description: 'Neo4j endpoint',
      default: environment.config.neo4j.endpoint,
    })
    .option('neo4jUser', {
      alias: 'u',
      type: 'string',
      description: 'Neo4j username',
      default: environment.config.neo4j.username,
    })
    .option('neo4jPassword', {
      alias: 'p',
      type: 'string',
      description: 'Neo4j password',
      default: environment.config.neo4j.password,
    })
    .option('databaseName', {
      alias: 'n',
      type: 'string',
      description: 'Name of the database',
      default: '_system',
    })
    .option('dropLabels', {
      alias: 'd',
      type: 'boolean',
      description: 'Drop all related labels before creating new ones',
      default: false,
    })
    .options('skipVertices', {
      alias: 'sv',
      type: 'boolean',
      description: 'Skip vertices insertion',
      default: false,
    })
    .options('skipEdges', {
      alias: 'se',
      type: 'boolean',
      description: 'Skip edges insertion',
      default: false,
    })
    .options('concurrency', {
      alias: 'con',
      type: 'number',
      description: 'Number of concurrent operations',
      default: environment.config.import_configuration.concurrency,
    })
    .options('maxQueueSize', {
      alias: 'mqs',
      type: 'number',
      description: 'Maximum queue size',
      default: environment.config.import_configuration.max_queue_size
    })
    .options('verifyGraph', {
      alias: 'v',
      type: 'boolean',
      description: 'Verify insertions, will check if the number of inserted vertices and edges is correct',
      default: false,
    })
    .help()
    .argv;

  return argv;
}

export {parseArgs};
