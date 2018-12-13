const _ = require('lodash/fp');
const axios = require('axios');
const cheerio = require('cheerio');

const _a = require('./lib/lodash-a');

const html = _a.pipe([axios, _.get('data'), cheerio.load]);

exports.html = html;

function cheerioText(node) {
  if (!node) return null;
  if (node.type === 'text') return node.data;
  return _.pipe(
    _.map(cheerioText),
    _.join(' ')
  )(node.children);
}

exports.cheerioText = cheerioText;
