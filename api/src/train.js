const path = require('path');
const fs = require('fs');

require('@tensorflow/tfjs-node');
const tf = require('@tensorflow/tfjs');
const _ = require('lodash/fp');
const ProgressBar = require('progress');

const { wsq } = require('./services');

const MODEL_SAVE_DIR = path.join(__dirname, '../../tmp');

console.log({ MODEL_SAVE_DIR });

if (!fs.existsSync(MODEL_SAVE_DIR)) fs.mkdirSync(MODEL_SAVE_DIR);

async function _getTotalSamples() {
  return parseInt(
    (await wsq.l`select count(*) from training_data`.one()).count
  );
}

async function _getData({ offset = 0, limit = 'all' } = {}) {
  return await wsq.l`
    select * from training_data
    limit ${limit}
    offset ${offset}
  `;
}

async function _getStats() {
  return await wsq.l`
    select
    	avg(floor(date_part('days', g.time_of_game - p.date_of_birth) / 365)) as age_at_time_of_game_avg,
    	min(floor(date_part('days', g.time_of_game - p.date_of_birth) / 365)) as age_at_time_of_game_min,
    	max(floor(date_part('days', g.time_of_game - p.date_of_birth) / 365)) as age_at_time_of_game_max,

    	avg(date_part('year', g.time_of_game)) as year_of_game_avg,
    	min(date_part('year', g.time_of_game)) as year_of_game_min,
    	max(date_part('year', g.time_of_game)) as year_of_game_max,

    	avg(date_part('month', g.time_of_game)) as month_of_game_avg,
    	min(date_part('month', g.time_of_game)) as month_of_game_min,
    	max(date_part('month', g.time_of_game)) as month_of_game_max,

    	avg(date_part('day', g.time_of_game)) as day_of_game_avg,
    	min(date_part('day', g.time_of_game)) as day_of_game_min,
    	max(date_part('day', g.time_of_game)) as day_of_game_max,

    	avg(date_part('hour', g.time_of_game)) as hour_of_game_avg,
    	min(date_part('hour', g.time_of_game)) as hour_of_game_min,
    	max(date_part('hour', g.time_of_game)) as hour_of_game_max,

    	avg(tp.experience) as experience_avg,
    	min(tp.experience) as experience_min,
    	max(tp.experience) as experience_max,

    	avg(tp.height_inches) as height_inches_avg,
    	min(tp.height_inches) as height_inches_min,
    	max(tp.height_inches) as height_inches_max,

    	avg(tp.weight_lbs) as weight_lbs_avg,
    	min(tp.weight_lbs) as weight_lbs_min,
    	max(tp.weight_lbs) as weight_lbs_max,



    	avg(gp.points) as points_avg,
    	min(gp.points) as points_min,
    	max(gp.points) as points_max,

    	avg(gp.three_point_field_goals) as three_point_field_goals_avg,
    	min(gp.three_point_field_goals) as three_point_field_goals_min,
    	max(gp.three_point_field_goals) as three_point_field_goals_max,

    	avg(gp.total_rebounds) as total_rebounds_avg,
    	min(gp.total_rebounds) as total_rebounds_min,
    	max(gp.total_rebounds) as total_rebounds_max,

    	avg(gp.assists) as assists_avg,
    	min(gp.assists) as assists_min,
    	max(gp.assists) as assists_max,

    	avg(gp.steals) as steals_avg,
    	min(gp.steals) as steals_min,
    	max(gp.steals) as steals_max,

    	avg(gp.blocks) as blocks_avg,
    	min(gp.blocks) as blocks_min,
    	max(gp.blocks) as blocks_max,

    	avg(gp.turnovers) as turnovers_avg,
    	min(gp.turnovers) as turnovers_min,
    	max(gp.turnovers) as turnovers_max,

    	avg(gp.free_throws) as free_throws_avg,
    	min(gp.free_throws) as free_throws_min,
    	max(gp.free_throws) as free_throws_max
    from games_players gp
    inner join players p
    	on p.basketball_reference_id = gp.player_basketball_reference_id
    inner join games g
    	on g.basketball_reference_id = gp.game_basketball_reference_id
    inner join teams_players tp
    	on tp.player_basketball_reference_id = gp.player_basketball_reference_id
    	and tp.season = g.season
    inner join teams t
    	on t.basketball_reference_id = tp.team_basketball_reference_id;
  `.one();
}

async function _getPlayers() {
  return _.map('basketballReferenceId')(
    await wsq.l`
      select distinct basketball_reference_id
      from players p
      inner join teams_players tp on tp.player_basketball_reference_id = p.basketball_reference_id
      where tp.season = 2019;
    `
  );
}

async function _getArenas() {
  return _.map('arena')(await wsq.l`select distinct arena from games`);
}

async function _getBirthCountries() {
  return _.map('birthCountry')(
    await wsq.l`
    select distinct birth_country
    from players p
    inner join teams_players tp on tp.player_basketball_reference_id = p.basketball_reference_id
    where tp.season = 2019
  `
  );
}

async function _getTeams() {
  return _.map('basketballReferenceId')(
    await wsq.l`select distinct basketball_reference_id from teams`
  );
}

async function _getPositions() {
  return _.map('position')(
    await wsq.l`select distinct position from teams_players`
  );
}

function makeOneHotEncoders(possibleValues) {
  return {
    encode: value => possibleValues.map(pv => (pv === value ? 1 : 0))
  };
}

const makeEncoders = function makeEncoders(average, min, max) {
  if (!average && average !== 0)
    throw new Error('Expected average in makeEncoders()');
  if (!min && min !== 0) throw new Error('Expected min in makeEncoders()');
  if (!max && max !== 0) throw new Error('Expected max in makeEncoders()');

  const numberAverage = parseFloat(average);
  const numberMin = parseFloat(min);
  const numberMax = parseFloat(max);

  return {
    encode: rawValue =>
      (parseFloat(rawValue) - numberAverage) / (numberMax - numberMin),
    decode: encodedValue =>
      parseFloat(encodedValue) * (numberMax - numberMin) + numberAverage
  };
};

async function makeMapFunctions() {
  const teams = makeOneHotEncoders(await _getTeams());

  const players = makeOneHotEncoders(await _getPlayers());

  const arenas = makeOneHotEncoders(await _getArenas());

  const birthCountries = makeOneHotEncoders(await _getBirthCountries());

  const positions = makeOneHotEncoders(await _getPositions());

  const stats = await _getStats();
  const ageAtTimeOfGame = makeEncoders(
    stats.ageAtTimeOfGameAvg,
    stats.ageAtTimeOfGameMin,
    stats.ageAtTimeOfGameMax
  );
  const yearOfGame = makeEncoders(
    stats.yearOfGameAvg,
    stats.yearOfGameMin,
    stats.yearOfGameMax
  );
  const monthOfGame = makeEncoders(
    stats.monthOfGameAvg,
    stats.monthOfGameMin,
    stats.monthOfGameMax
  );
  const dayOfGame = makeEncoders(
    stats.dayOfGameAvg,
    stats.dayOfGameMin,
    stats.dayOfGameMax
  );
  const hourOfGame = makeEncoders(
    stats.hourOfGameAvg,
    stats.hourOfGameMin,
    stats.hourOfGameMax
  );
  const experience = makeEncoders(
    stats.experienceAvg,
    stats.experienceMin,
    stats.experienceMax
  );
  const heightInches = makeEncoders(
    stats.heightInchesAvg,
    stats.heightInchesMin,
    stats.heightInchesMax
  );
  const weightLbs = makeEncoders(
    stats.weightLbsAvg,
    stats.weightLbsMin,
    stats.weightLbsMax
  );

  const points = makeEncoders(
    stats.pointsAvg,
    stats.pointsMin,
    stats.pointsMax
  );

  const threePointFieldGoals = makeEncoders(
    stats.threePointFieldGoalsAvg,
    stats.threePointFieldGoalsMin,
    stats.threePointFieldGoalsMax
  );

  const totalRebounds = makeEncoders(
    stats.totalReboundsAvg,
    stats.totalReboundsMin,
    stats.totalReboundsMax
  );

  const assists = makeEncoders(
    stats.assistsAvg,
    stats.assistsMin,
    stats.assistsMax
  );

  const steals = makeEncoders(
    stats.stealsAvg,
    stats.stealsMin,
    stats.stealsMax
  );

  const blocks = makeEncoders(
    stats.blocksAvg,
    stats.blocksMin,
    stats.blocksMax
  );

  const turnovers = makeEncoders(
    stats.turnoversAvg,
    stats.turnoversMin,
    stats.turnoversMax
  );

  const freeThrows = makeEncoders(
    stats.freeThrowsAvg,
    stats.freeThrowsMin,
    stats.freeThrowsMax
  );

  const mapDataToX = _.map(d => [
    ...players.encode(d.playerBasketballReferenceId),
    ...arenas.encode(d.arena),
    ...birthCountries.encode(d.birthCountry),
    ...teams.encode(d.homeTeamBasketballReferenceId),
    ...teams.encode(d.awayTeamBasketballReferenceId),
    ...teams.encode(d.playerTeamBasketballReferenceId),
    ...positions.encode(d.position),
    ageAtTimeOfGame.encode(d.ageAtTimeOfGame),
    yearOfGame.encode(d.yearOfGame),
    monthOfGame.encode(d.monthOfGame),
    dayOfGame.encode(d.dayOfGame),
    hourOfGame.encode(d.hourOfGame),
    experience.encode(d.experience),
    heightInches.encode(d.heightInches),
    weightLbs.encode(d.weightLbs),
    d.playingAtHome ? 1 : 0
  ]);

  const mapDataToY = _.map(d => [
    points.encode(d.points),
    threePointFieldGoals.encode(d.threePointFieldGoals),
    totalRebounds.encode(d.totalRebounds),
    assists.encode(d.assists),
    steals.encode(d.steals),
    blocks.encode(d.blocks),
    turnovers.encode(d.turnovers),
    freeThrows.encode(d.freeThrows)
  ]);

  const mapYToData = _.map(y => ({
    points: points.decode(y.points),
    threePointFieldGoals: threePointFieldGoals.decode(y.threePointFieldGoals),
    totalRebounds: totalRebounds.decode(y.totalRebounds),
    assists: assists.decode(y.assists),
    steals: steals.decode(y.steals),
    blocks: blocks.decode(y.blocks),
    turnovers: turnovers.decode(y.turnovers),
    freeThrows: freeThrows.decode(y.freeThrows)
  }));

  return { mapDataToX, mapDataToY, mapYToData };
}

async function train() {
  const { mapDataToX, mapDataToY } = await makeMapFunctions();

  const sampleSample = await _getData({ limit: 1 });
  const sampleX = mapDataToX([sampleSample])[0];
  const sampleY = mapDataToY([sampleSample])[0];

  console.log({
    features: sampleX.length,
    labels: sampleY.length
  });

  const TOTAL_SAMPLES = (await _getTotalSamples()) * 0.1;
  const TRAINING_SPLIT = 0.7;
  const VALIDATION_SPLIT = 0.15;
  const TEST_SPLIT = 0.15;
  const BATCH_SIZE = 500;
  const MODEL_SAVE_PATH = `${MODEL_SAVE_DIR}/model_${Date.now()}.json`;

  const TRAINING_SAMPLES = Math.floor(TRAINING_SPLIT * TOTAL_SAMPLES);
  const VALIDATION_SAMPLES = Math.floor(VALIDATION_SPLIT * TOTAL_SAMPLES);
  const TEST_SAMPLES = Math.floor(TEST_SPLIT * TOTAL_SAMPLES);
  const EPOCHS = 25;
  const NUM_BATCHES = Math.ceil(TRAINING_SAMPLES / BATCH_SIZE);

  const model = tf.sequential();

  model.add(tf.layers.inputLayer({ inputShape: sampleX.length }));
  // model.add(tf.layers.dense({ units: 512, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.25 }));
  // model.add(tf.layers.dense({ units: 256, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.25 }));
  // model.add(tf.layers.dense({ units: 128, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.25 }));
  model.add(tf.layers.dense({ units: 64, activation: 'relu' }));
  model.add(tf.layers.dropout({ rate: 0.2 }));
  // model.add(tf.layers.dense({ units: 32, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.2 }));
  model.add(tf.layers.dense({ units: sampleY.length, activation: 'linear' }));

  model.compile({
    optimizer: 'rmsprop',
    loss: 'meanSquaredError',
    metrics: ['accuracy']
  });
  model.summary();

  console.log({ MODEL_SAVE_PATH });

  for (let i = 0; i <= EPOCHS; i++) {
    console.log('\n');
    console.log({ epoch: i + 1, of: EPOCHS });

    const bar = new ProgressBar('[ :bar ] :percent :etas loss::loss acc::acc', {
      total: NUM_BATCHES + 1,
      width: 40
    });

    for (let j = 0; j <= NUM_BATCHES; j++) {
      const data = await _getData({
        limit: BATCH_SIZE,
        offset: j * BATCH_SIZE
      });

      const [loss, acc] = await model.trainOnBatch(
        tf.tensor2d(mapDataToX(data)),
        tf.tensor2d(mapDataToY(data))
      );

      bar.tick(1, {
        loss: loss.toFixed(5),
        acc: acc.toFixed(3)
      });
    }

    const validationData = await _getData({
      limit: VALIDATION_SAMPLES,
      offset: TRAINING_SAMPLES
    });

    const [valLoss, valAcc] = await model.evaluate(
      tf.tensor2d(mapDataToX(validationData)),
      tf.tensor2d(mapDataToY(validationData)),
      { batchSize: BATCH_SIZE }
    );

    console.log({
      valLoss: parseFloat(valLoss.dataSync()[0].toFixed(5)),
      valAcc: parseFloat(valAcc.dataSync()[0].toFixed(3))
    });

    fs.writeFileSync(
      MODEL_SAVE_PATH,
      JSON.stringify(model.toJSON(null, false))
    );
  }

  const testData = await _getData({
    limit: TRAINING_SAMPLES + VALIDATION_SAMPLES,
    offset: TEST_SAMPLES
  });

  const [testLoss, testAcc] = await model.evaluate(
    tf.tensor2d(mapDataToX(testData)),
    tf.tensor2d(mapDataToY(testData)),
    { batchSize: BATCH_SIZE }
  );

  console.log({
    valLoss: parseFloat(testLoss.dataSync()[0].toFixed(5)),
    valAcc: parseFloat(testAcc.dataSync()[0].toFixed(3))
  });
}

async function predict(batch) {
  const { mapDataToX } = await makeMapFunctions();

  const model = await tf.models.modelFromJSON(
    fs.readFileSync(`${MODEL_SAVE_DIR}/model_1545529383943.json`, 'utf8')
  );
  const predictions = await model.predictOnBatch(
    tf.tensor2d(mapDataToX(batch))
  );
  return predictions.dataSync();
}

// train();

_getData({ limit: 1 })
  .then(predict)
  .then(console.log);

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
