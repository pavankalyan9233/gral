import {readFileSync} from 'fs';

export const common = {
  readGraphNameToGralIdMap: () => {
    let data = null;

    try {
      data = readFileSync('./modules/graphNameToGralIdMap.json', 'utf8');
    } catch (error) {
      console.log(error);
    }

    return JSON.parse(data);
  },
  getGralGraphId: (graphName: string) => {
    const graphNameToGralIdMap = common.readGraphNameToGralIdMap();
    return graphNameToGralIdMap[graphName];
  }

}

module.exports = common;