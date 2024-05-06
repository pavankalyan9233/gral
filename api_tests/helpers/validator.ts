import {ArrayCursor} from "arangojs/cursor";
import {expect} from 'vitest';

const fs = require('fs');
const path = require('path');

function readResultLines(graphName: string, algorithm: string) {
  const data = fs.readFileSync(path.join('../', 'examples', 'data', `${graphName}`,`${graphName}-${algorithm}`));
  return data.toString().split('\n');
}

async function verifyPageRankDocuments(graphName: string, actual: ArrayCursor) {
  const lines = readResultLines(graphName, 'PR');
  let expected = {};

  for (const line of lines) {
    const parts = line.split(' ');
    if (parts.length === 2) {
      expected[parseInt(parts[0])] = parseFloat(parts[1]);
    }
  }

  await actual.forEach((doc: any) => {
    let docId = doc[0];
    expect(doc[1]).toBeCloseTo(expected[docId], 13);
  });
}

export const validator = {
  verifyPageRankDocuments
};


module.exports = validator;