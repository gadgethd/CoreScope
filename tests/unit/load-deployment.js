'use strict';

const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../../public/deployment.js'), 'utf8');
const contexts = new WeakSet();

module.exports = function loadDeployment(context) {
  if (contexts.has(context)) return;
  vm.runInContext(source, context, { filename: 'deployment.js' });
  contexts.add(context);
};
