import { hideBin } from 'yargs/helpers';
import { config } from '../config/environment.js';
import { createRequire } from 'module';
import * as environment from "../config/environment.js";
const require = createRequire(import.meta.url);
const yargs = require('yargs/yargs');

function parseArgs() {
  const argv = yargs(hideBin(process.argv))
    .option('graphName', {
      alias: 'g',
      type: 'string',
      description: 'Name of the graph',
      default: 'twitter_mpi', // Default value if not provided
    })
    .option('arangoEndpoint', {
      alias: 'e',
      type: 'string',
      description: 'ArangoDB endpoint',
      default: environment.config.arangodb.endpoint, // Default value if not provided
    })
    .option('arangoUser', {
      alias: 'u',
      type: 'string',
      description: 'ArangoDB username',
      default: environment.config.arangodb.username, // Default value if not provided
    })
    .option('arangoPassword', {
      alias: 'p',
      type: 'string',
      description: 'ArangoDB password',
      default: environment.config.arangodb.password, // Default value if not provided
    })
    .option('databaseName', {
      alias: 'n',
      type: 'string',
      description: 'Name of the database',
      default: '_system', // Default value if not provided
    })
    .option('dropGraph', {
      alias: 'd',
      type: 'boolean',
      description: 'Drop the graph before creating it',
      default: false, // Default value if not provided
    })
    .help()
    .argv;

  return argv;
}

export { parseArgs };
