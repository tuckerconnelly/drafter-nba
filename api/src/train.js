const path = require('path');
const fs = require('fs');

require('@tensorflow/tfjs-node');
const tf = require('@tensorflow/tfjs');
const _ = require('lodash/fp');
const ProgressBar = require('progress');

const {
  makeMapFunctions,
  getDataFromPg,
  loadData,
  makeTrainingData
} = require('./data');

const MODEL_SAVE_DIR = path.join(__dirname, '../../tmp');
if (!fs.existsSync(MODEL_SAVE_DIR)) fs.mkdirSync(MODEL_SAVE_DIR);

async function train() {
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
  } = await makeTrainingData(undefined, await loadData());

  console.log({ features, labels, trainSamples });

  const MODEL_SAVE_PATH = `${MODEL_SAVE_DIR}/model_${Date.now()}`;
  const BATCH_SIZE = 5000;
  const EPOCHS = 30;
  const NUM_BATCHES = Math.ceil(trainSamples / BATCH_SIZE);

  const model = tf.sequential();

  model.add(tf.layers.inputLayer({ inputShape: features }));
  // model.add(tf.layers.dense({ units: 512, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.25 }));
  // model.add(tf.layers.dense({ units: 64, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.5 }));
  // model.add(tf.layers.dense({ units: 64, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.5 }));
  model.add(tf.layers.dense({ units: 420, activation: 'relu' }));
  model.add(tf.layers.dropout({ rate: 0.25 }));
  model.add(tf.layers.dense({ units: 420, activation: 'relu' }));
  model.add(tf.layers.dropout({ rate: 0.25 }));
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

    const bar = new ProgressBar('[ :bar ] :percent :etas loss::loss mae::mae', {
      total: NUM_BATCHES,
      width: 40
    });

    for (let j = 0; j < NUM_BATCHES; j++) {
      const [loss, mae] = await model.trainOnBatch(
        trainX.slice(
          j * BATCH_SIZE,
          j === NUM_BATCHES - 1 ? trainSamples % BATCH_SIZE : BATCH_SIZE
        ),
        trainY.slice(
          j * BATCH_SIZE,
          j === NUM_BATCHES - 1 ? trainSamples % BATCH_SIZE : BATCH_SIZE
        )
      );

      bar.tick(1, {
        loss: loss.toFixed(5),
        mae: mae.toFixed(3)
      });
    }

    const [valLoss, valMae] = await model.evaluate(validationX, validationY, {
      batchSize: BATCH_SIZE
    });

    // const predictionsTensor = await model.predictOnBatch(validationX);
    // const predictions = _.pipe([
    //   _.toArray,
    //   _.chunk(predictionsTensor.shape[1]),
    //   mapYToData,
    //   data => {
    //     const newData = [];
    //     for (let i = 0; i < data.length; i++)
    //       newData[i] = {
    //         a: validationData[i].points,
    //         p: Math.round(data[i].points)
    //       };
    //     return newData;
    //   }
    // ])(await predictionsTensor.data());
    // console.log(predictions);

    console.log({
      valLoss: parseFloat(valLoss.dataSync()[0].toFixed(5)),
      valMae: parseFloat(valMae.dataSync()[0].toFixed(3))
    });

    await model.save(`file://${MODEL_SAVE_PATH}`);
  }

  const [testLoss, testMae] = await model.evaluate(testX, testY, {
    batchSize: BATCH_SIZE
  });

  console.log({
    testLoss: parseFloat(testLoss.dataSync()[0].toFixed(5)),
    testMae: parseFloat(testMae.dataSync()[0].toFixed(3))
  });

  return model;
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
    _.tap(console.log),
    data => {
      const newData = [];
      for (let i = 0; i < data.length; i++)
        newData[i] = { a: batch[i].points, p: data[i].points };
      return newData;
    }
  ])(await predictions.data());
}

async function testPredictions(model) {
  console.time('getDataFromPg');
  let data = await getDataFromPg({ limit: 1000, offset: 10000 });
  console.timeEnd('getDataFromPg');

  console.time('mapData');
  let currentPlayerSeason = null;
  let pointsLastGames = [];
  for (let i in data) {
    if (
      currentPlayerSeason !==
      `${data[i].playerBasketballReferenceId}${data[i].season}`
    ) {
      currentPlayerSeason = `${data[i].playerBasketballReferenceId}${
        data[i].season
      }`;
      pointsLastGames = [];
    }

    data[i].pointsLastGames = pointsLastGames;

    pointsLastGames.unshift(data[i].points);
  }
  console.timeEnd('mapData');

  const res = await predict(model, data.slice(10));

  console.log(JSON.stringify(res, null, 2));
}

// train().then(testPredictions);

// global.fetch = url => ({
//   text: () => fs.readFileSync(url, 'utf8'),
//   json: () => JSON.parse(fs.readFileSync(url, 'utf8')),
//   arrayBuffer: () => fs.readFileSync(url).buffer
// });
//
// (async () => {
//   console.time('Load model');
//   const modelJson = JSON.parse(
//     fs.readFileSync(`${MODEL_SAVE_DIR}/model_1545790795878/model.json`, 'utf8')
//   );
//   modelJson.weightsManifest[0].paths[0] = `${MODEL_SAVE_DIR}/model_1545790795878/weights.bin`;
//
//   const model = await tf.models.modelFromJSON(modelJson);
//   console.timeEnd('Load model');
//
//   await testPredictions(model);
// })();

// Double-double double-triple calculations

// [
//   parseInt(d.points) >= 10,
//   parseInt(d.totalRebounds) >= 10,
//   parseInt(d.assists) >= 10,
//   parseInt(d.blocks) >= 10,
//   parseInt(d.steals) >= 10
// ].filter(Boolean).length >= 2
//   ? 1
//   : 0,
// [
//   parseInt(d.points) >= 10,
//   parseInt(d.totalRebounds) >= 10,
//   parseInt(d.assists) >= 10,
//   parseInt(d.blocks) >= 10,
//   parseInt(d.steals) >= 10
// ].filter(Boolean).length >= 3
//   ? 1
//   : 0
