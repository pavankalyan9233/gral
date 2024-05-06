import {ArrayCursor} from "arangojs/cursor";
import {expect} from 'vitest';

const fs = require('fs');
const path = require('path');

function readResultLines(graphName: string, algorithm: string) {
  const data = fs.readFileSync(path.join('../', 'examples', 'data', `${graphName}`, `${graphName}-${algorithm}`));
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

async function verifyWCCResults(graphName: string, actual: ArrayCursor) {
  let groups = {};
  await actual.forEach((doc) => {
    groups[doc[0]] = {
      actual: doc[1]
    };
  });

  const lines = readResultLines(graphName, 'WCC');

  for (const line of lines) {
    const parts = line.split(' ');
    if (parts.length === 2) {
      // expect that entry exists
      expect(groups[parseInt(parts[0])]).toBeDefined();
      groups[parseInt(parts[0])].expected = parseInt(parts[1]);
    }
  }

  let unique = {};
  for (const key in groups) {
    let actual = groups[key].actual;
    let expected = groups[key].expected;
    if (unique[actual] === undefined) {
      unique[actual] = expected;
    } else {
      expect(unique[actual]).toBe(expected);
    }
  }
}

export const validator = {
  verifyPageRankDocuments, verifyWCCResults
};


module.exports = validator;