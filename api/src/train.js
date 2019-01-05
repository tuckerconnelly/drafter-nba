const path = require('path');
const fs = require('fs');

require('@tensorflow/tfjs-node');
const tf = require('@tensorflow/tfjs');
const _ = require('lodash/fp');
const ProgressBar = require('progress');
const Table = require('cli-table');

const {
  makeMapFunctions,
  loadData,
  loadPlayerData,
  getPlayers
} = require('./data');

let MAX_SAMPLES = undefined;
let BATCH_SIZE = 5000;
let EPOCHS = 20;
let PLAYER_LOSS_PLAYER_LIMIT = undefined;
let PLAYER_EPOCHS = 20;

if (process.env.DEBUG) {
  MAX_SAMPLES = 1000;
  BATCH_SIZE = 50;
  EPOCHS = 5;
  PLAYER_EPOCHS = 5;
  PLAYER_LOSS_PLAYER_LIMIT = 10;
}

async function train({ finalModel = false } = {}) {
  const {
    trainX,
    trainY,
    validationX,
    validationY,
    testX,
    testY,

    features,
    labels,
    trainSamples
  } = await loadData({
    maxSamples: MAX_SAMPLES,
    trainSplit: finalModel ? 1 : 0.8,
    validationSplit: finalModel ? 0 : 0.1,
    testSplit: finalModel ? 0.2 : 0.1,
    batchSize: BATCH_SIZE
  });

  console.log({ features, labels, trainSamples });

  const MODEL_NAME = `model_${Date.now()}`;
  const MODEL_SAVE_PATH = `${MODEL_SAVE_DIR}/${MODEL_NAME}`;
  const NUM_BATCHES = Math.ceil(trainSamples / BATCH_SIZE);

  const model = tf.sequential();

  model.add(tf.layers.inputLayer({ inputShape: features }));

  model.add(tf.layers.dense({ units: 420 }));
  model.add(tf.layers.batchNormalization());
  model.add(tf.layers.elu());
  model.add(tf.layers.dropout({ rate: 0.5 }));

  model.add(tf.layers.dense({ units: 420 }));
  model.add(tf.layers.batchNormalization());
  model.add(tf.layers.elu());
  model.add(tf.layers.dropout({ rate: 0.5 }));

  model.add(tf.layers.dense({ units: labels, activation: 'linear' }));

  model.compile({
    optimizer: 'adam',
    loss: 'meanSquaredError',
    metrics: ['mae']
  });
  model.summary();

  console.log({ MODEL_SAVE_PATH });

  for (let i = 0; i < EPOCHS; i++) {
    console.log('\n');
    console.log({ epoch: i + 1, of: EPOCHS });

    const bar = new ProgressBar('[ :bar ] :percent :etas rmse::rmse', {
      total: NUM_BATCHES,
      width: 40
    });

    for (let j = 0; j < NUM_BATCHES; j++) {
      const [mse] = await model.trainOnBatch(trainX[j], trainY[j]);

      bar.tick(1, { rmse: Math.sqrt(mse).toFixed(3) });
    }

    let losses = {};

    if (!finalModel) {
      const [mse, mae] = await model.evaluate(validationX, validationY, {
        batchSize: BATCH_SIZE
      });

      losses = {
        mse: mse.dataSync()[0],
        rmse: Math.sqrt(mse.dataSync()[0]),
        mae: mae.dataSync()[0]
      };
      console.log(_.mapValues(it => parseFloat(it.toFixed(3)), losses));
    }
  }

  const [mse, mae] = await model.evaluate(testX, testY, {
    batchSize: BATCH_SIZE
  });

  const losses = {
    mse: mse.dataSync()[0],
    rmse: Math.sqrt(mse.dataSync()[0]),
    mae: mae.dataSync()[0]
  };
  console.log(_.mapValues(it => parseFloat(it.toFixed(3)), losses));

  await saveModel(model, MODEL_NAME, losses);

  await makePlayerModels(MODEL_NAME);

  return model;
}

async function makePlayerModels(modelName, finalModel) {
  let players = await getPlayers();
  if (PLAYER_LOSS_PLAYER_LIMIT)
    players = players.slice(0, PLAYER_LOSS_PLAYER_LIMIT);

  const { datumToX, datumToY } = await makeMapFunctions();

  let losses = [];

  console.log('makePlayerModels');
  const bar = new ProgressBar('[ :bar ] :current/:total :percent :etas', {
    width: 40,
    total: players.length
  });

  for (let playerBasketballReferenceId of players) {
    const datum = await loadPlayerData({
      playerBasketballReferenceId,
      datumToX,
      datumToY,
      testSplit: finalModel ? 0 : 0.1
    });

    if (!datum) continue;

    const model = await loadModel(modelName);
    model.layers[0].trainable = false;
    model.layers[1].trainable = false;
    model.layers[2].trainable = false;
    model.layers[3].trainable = false;
    model.compile({
      optimizer: 'adam',
      loss: 'meanSquaredError',
      metrics: ['mae']
    });
    for (let i = 0; i < PLAYER_EPOCHS; i++) {
      await model.trainOnBatch(datum.trainX, datum.trainY);
    }
    const [mse, mae] = await model.evaluate(
      finalModel ? datum.trainX : datum.testX,
      finalModel ? datum.trainY : datum.testY
    );

    const playerLosses = {
      playerBasketballReferenceId,

      trainSamples: datum.trainX.shape[0],
      testSamples: datum.trainY.shape[0],

      mse: mse.dataSync()[0],
      rmse: Math.sqrt(mse.dataSync()[0]),
      mae: mae.dataSync()[0]
    };

    await saveModel(
      model,
      `${modelName}/${playerBasketballReferenceId}`,
      playerLosses
    );

    losses.push(playerLosses);
    bar.tick(1);
  }

  losses = _.sortBy('rmse')(losses);
  const table = new Table({
    head: _.keys(losses[0]),
    colWidths: [20, 8, 8, 8, 8, 8]
  });
  table.push(
    ...losses
      .map(_.values)
      .map(l => l.map(c => (_.isNumber(c) ? c.toFixed(3) : c)))
  );
  console.log(table.toString());
}

async function predict(model, batch) {
  const { datumToX, yToDatum } = await makeMapFunctions();

  const predictions = await model.predictOnBatch(
    tf.tensor2d(_.map(datumToX)(batch))
  );

  return _.pipe([
    _.toArray,
    _.chunk(predictions.shape[1]),
    _.map(yToDatum),
    data => {
      const newData = [];
      for (let i = 0; i < data.length; i++)
        newData[i] = {
          ...batch[i],
          _predictions: data[i],
          _losses: model.drafterLosses
        };
      return newData;
    }
  ])(await predictions.data());
}

exports.predict = predict;

const MODEL_SAVE_DIR = path.join(__dirname, '../../tmp');
if (!fs.existsSync(MODEL_SAVE_DIR)) fs.mkdirSync(MODEL_SAVE_DIR);

async function saveModel(model, modelName, losses = {}) {
  await model.save(`file://${MODEL_SAVE_DIR}/${modelName}`);

  fs.writeFileSync(
    path.join(`${MODEL_SAVE_DIR}/${modelName}`, 'losses.json'),
    JSON.stringify(losses)
  );
}

global.fetch = url => ({
  text: () => fs.readFileSync(url, 'utf8'),
  json: () => JSON.parse(fs.readFileSync(url, 'utf8')),
  arrayBuffer: () => fs.readFileSync(url).buffer
});

async function loadModel(modelName) {
  const modelJson = JSON.parse(
    fs.readFileSync(`${MODEL_SAVE_DIR}/${modelName}/model.json`, 'utf8')
  );
  modelJson.weightsManifest[0].paths[0] = `${MODEL_SAVE_DIR}/${modelName}/weights.bin`;
  const model = await tf.models.modelFromJSON(modelJson);

  const losses = JSON.parse(
    fs.readFileSync(`${MODEL_SAVE_DIR}/${modelName}/losses.json`, 'utf8')
  );

  model.drafterLosses = losses;

  return model;
}

exports.loadModel = loadModel;

if (process.env.TASK === 'train')
  train({ finalModel: process.env.FINAL === '1' });
