import { CypherEngine } from '../packages/core/src/CypherEngine';
import { JsonAdapter } from '../packages/adapters/json-adapter/src/index';

const dataset = {
  nodes: [
    { id: 1, labels: ['Person'], properties: { name: 'Keanu Reeves' } },
    { id: 2, labels: ['Movie'], properties: { title: 'The Matrix', released: 1999 } }
  ],
  relationships: [
    { id: 3, type: 'ACTED_IN', startNode: 1, endNode: 2, properties: {} }
  ]
};

const adapter = new JsonAdapter({ dataset });
const engine = new CypherEngine({ adapter });

function refreshGraph() {
  const graphElem = document.getElementById('graph');
  if (graphElem) graphElem.textContent = JSON.stringify(adapter.exportData(), null, 2);
}

refreshGraph();

const btn = document.getElementById('runBtn') as HTMLButtonElement;
btn.addEventListener('click', async () => {
  const query = (document.getElementById('query') as HTMLTextAreaElement).value;
  const resultsElem = document.getElementById('results');
  if (!resultsElem) return;
  resultsElem.textContent = '';
  try {
    for await (const row of engine.run(query)) {
      resultsElem.textContent += JSON.stringify(row, null, 2) + '\n';
    }
    refreshGraph();
  } catch (err) {
    resultsElem.textContent = 'Error: ' + (err as Error).message;
  }
});
