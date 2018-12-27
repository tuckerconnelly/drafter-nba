const fs = require('fs');
const crypto = require('crypto');
const assert = require('assert');

const _ = require('lodash');

module.exports = _.curry(async function memoizeFs(path, fn, arg) {
  const fnHash = crypto
    .createHash('md5')
    .update(fn.toString())
    .digest('hex');

  let memoized;
  try {
    memoized = JSON.parse(fs.readFileSync(path, 'utf8'));
    assert(memoized.fnHash === fnHash, 'fnHash needs to match');
  } catch (err) {
    console.warn(err);
    memoized = { fnHash, results: {} };
  }

  const argsHash = crypto
    .createHash('md5')
    .update(JSON.stringify(arg))
    .digest('hex');

  if (memoized.results[argsHash]) return memoized.results[argsHash];

  const res = await fn(arg);

  fs.writeFileSync(
    path,
    JSON.stringify({
      fnHash,
      results: { ...memoized.results, [argsHash]: res }
    })
  );

  return res;
});
