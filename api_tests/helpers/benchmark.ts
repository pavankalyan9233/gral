import {config} from '../environment.config';

function getPagerankGraphs() {
  let graphs = [];
  for (const key in config.benchmark.graphs) {
    if (Object.prototype.hasOwnProperty.call(config.benchmark.graphs, key)) {
      const element = config.benchmark.graphs[key];
      if (element.algos.includes('pagerank')) {
        graphs.push(key)
      }
    }
  }
  return graphs;
}


export const benchmarkHelper = {
  getPagerankGraphs
};


module.exports = benchmarkHelper;