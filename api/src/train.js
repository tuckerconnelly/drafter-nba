require('@tensorflow/tfjs-node');
const tf = require('@tensorflow/tfjs');
const _ = require('lodash/fp');
const shuffleSeed = require('shuffle-seed');

const { wsq } = require('./services');

async function _getData() {
  return await wsq.l`
    select
    	gp.game_basketball_reference_id,
    	p.name as player_name,
    	t.name as team_name,

      gp.player_basketball_reference_id, -- enum
    	p.birth_country, -- enum
    	floor(date_part('days', g.time_of_game - p.date_of_birth) / 365) as age_at_time_of_game,
    	g.arena, -- enum
    	date_part('year', g.time_of_game) as year_of_game,
    	date_part('month', g.time_of_game) as month_of_game,
    	date_part('day', g.time_of_game) as day_of_game,
    	date_part('hour', g.time_of_game) as hour_of_game,
    	g.home_team_basketball_reference_id, -- enum
    	g.away_team_basketball_reference_id, -- enum
    	tp.team_basketball_reference_id as player_team_basketball_reference_id, -- enum
    	tp.experience,
    	tp.height_inches,
    	tp.weight_lbs,
    	tp.position, -- enum
    	(g.home_team_basketball_reference_id = t.basketball_reference_id) as playing_at_home,

    	gp.points,
    	gp.three_point_field_goals,
    	gp.total_rebounds,
    	gp.assists,
    	gp.steals,
    	gp.blocks,
    	gp.turnovers
    from games_players gp
    inner join players p
    	on p.basketball_reference_id = gp.player_basketball_reference_id
    inner join games g
    	on g.basketball_reference_id = gp.game_basketball_reference_id
    inner join teams_players tp
    	on tp.player_basketball_reference_id = gp.player_basketball_reference_id
    	and tp.season = g.season
    inner join teams t
    	on t.basketball_reference_id = tp.team_basketball_reference_id
    order by gp.id desc
    limit 100000
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
    	max(gp.turnovers) as turnovers_max
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

function testTrainSplit(X, y, { testSize = 0.25, randomState = 0 } = {}) {
  const i = Math.floor(X.length * (1 - testSize));

  const shuffledX = shuffleSeed.shuffle(X, randomState);
  const shuffledY = shuffleSeed.shuffle(y, randomState);
  return [
    shuffledX.slice(0, i),
    shuffledX.slice(i),
    shuffledY.slice(0, i),
    shuffledY.slice(i)
  ];
}

(async () => {
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

  console.time('Fetching data');
  const data = await _getData();
  console.timeEnd('Fetching data');

  console.time('Mapping X');
  const X = _.map(
    d => [
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
    ],
    data
  );
  console.timeEnd('Mapping X');

  console.time('Mapping y');
  const y = _.map(
    d => [
      points.encode(d.points),
      threePointFieldGoals.encode(d.threePointFieldGoals),
      totalRebounds.encode(d.totalRebounds),
      assists.encode(d.assists),
      steals.encode(d.steals),
      blocks.encode(d.blocks),
      turnovers.encode(d.turnovers),
      [
        parseInt(d.points) >= 10,
        parseInt(d.totalRebounds) >= 10,
        parseInt(d.assists) >= 10,
        parseInt(d.blocks) >= 10,
        parseInt(d.steals) >= 10
      ].filter(Boolean).length >= 2
        ? 1
        : 0,
      [
        parseInt(d.points) >= 10,
        parseInt(d.totalRebounds) >= 10,
        parseInt(d.assists) >= 10,
        parseInt(d.blocks) >= 10,
        parseInt(d.steals) >= 10
      ].filter(Boolean).length >= 3
        ? 1
        : 0
    ],
    data
  );
  console.timeEnd('Mapping y');

  console.time('Test train split');
  const [trainX, testX, trainY, testY] = testTrainSplit(X, y);
  console.timeEnd('Test train split');

  console.log({
    features: trainX[0].length,
    labels: trainY[0].length,
    trainX: trainX.length,
    testX: testX.length,
    trainY: trainY.length,
    testY: testY.length
  });

  const BATCH_SIZE = 5000;
  const EPOCHS = 25;
  const VALIDATION_SPLIT = 0.10;

  const model = tf.sequential();

  model.add(tf.layers.inputLayer({ inputShape: trainX[0].length }));
  // model.add(tf.layers.dense({ units: 512, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.25 }));
  // model.add(tf.layers.dense({ units: 256, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.25 }));
  // model.add(tf.layers.dense({ units: 128, activation: 'relu' }));
  // model.add(tf.layers.dropout({ rate: 0.25 }));
  model.add(tf.layers.dense({ units: 64, activation: 'relu' }));
  model.add(tf.layers.dropout({ rate: 0.2 }));
  model.add(tf.layers.dense({ units: 32, activation: 'relu' }));
  model.add(tf.layers.dropout({ rate: 0.2 }));
  model.add(tf.layers.dense({ units: testY[0].length, activation: 'linear' }));

  model.compile({
    optimizer: 'rmsprop',
    loss: 'meanSquaredError',
    metrics: ['accuracy']
  });
  model.summary();

  await model.fit(tf.tensor2d(trainX), tf.tensor2d(trainY), {
    epochs: EPOCHS,
    batchSize: BATCH_SIZE,
    validationSplit: VALIDATION_SPLIT
  });

  const evalOutput = model.evaluate(tf.tensor2d(testX), tf.tensor2d(testY));
  console.log(
    `\nEvaluation result:\n` +
      `  Loss = ${evalOutput[0].dataSync()[0].toFixed(3)}; ` +
      `Accuracy = ${evalOutput[1].dataSync()[0].toFixed(3)}`
  );
})();
