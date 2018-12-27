const fs = require('fs');
const path = require('path');
const readline = require('readline');

const _ = require('lodash/fp');
require('@tensorflow/tfjs-node');
const tf = require('@tensorflow/tfjs');

const { wsq } = require('./services');

const DATA_FILE_PATH = path.join(__dirname, '../../tmp/data.csv');

async function getDataFromPg({ limit = null, offset = 0 } = {}) {
  return await wsq.l`
    select
      gp.game_basketball_reference_id,
      p.name as player_name,
      t.name as team_name,
      g.season as season,

      gp.player_basketball_reference_id, -- enum
      p.birth_country, -- enum
      floor(date_part('days', g.time_of_game - p.date_of_birth) / 365) as age_at_time_of_game,
      date_part('year', g.time_of_game) as year_of_game,
      date_part('month', g.time_of_game) as month_of_game,
      date_part('day', g.time_of_game) as day_of_game,
      date_part('hour', g.time_of_game) as hour_of_game,
      (case (g.home_team_basketball_reference_id = t.basketball_reference_id) when true then g.away_team_basketball_reference_id else g.home_team_basketball_reference_id end) as opposing_team_basketball_reference_id, --enum
      tp.team_basketball_reference_id as player_team_basketball_reference_id, -- enum
      tp.experience,
      tp.position, -- enum
      (g.home_team_basketball_reference_id = t.basketball_reference_id) as playing_at_home,
      gp.seconds_played,

      gp.points,
      gp.three_point_field_goals,
      gp.total_rebounds,
      gp.assists,
      gp.steals,
      gp.blocks,
      gp.turnovers,
      gp.free_throws
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
    where p.basketball_reference_id in (
    	select player_basketball_reference_id
    	from teams_players
    	where season = 2019
    )
    order by gp.player_basketball_reference_id asc, g.time_of_game asc
    limit ${limit}
    offset ${offset}
  `;
}

exports.getDataFromPg = getDataFromPg;

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

    	avg(gp.seconds_played) as seconds_played_avg,
    	min(gp.seconds_played) as seconds_played_min,
    	max(gp.seconds_played) as seconds_played_max,



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
    encode: value => possibleValues.map(pv => (pv === value ? 1 : 0)),
    decode: values => possibleValues[_.findIndex(_.identity, values)]
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

exports.makeMapFunctions = async function makeMapFunctions() {
  const teamsValues = await _getTeams();
  const teams = makeOneHotEncoders(teamsValues);

  const playersValues = await _getPlayers();
  const players = makeOneHotEncoders(playersValues);

  const birthCountriesValues = await _getBirthCountries();
  const birthCountries = makeOneHotEncoders(birthCountriesValues);

  const positionsValues = await _getPositions();
  const positions = makeOneHotEncoders(positionsValues);

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
  const secondsPlayed = makeEncoders(
    stats.secondsPlayedAvg,
    stats.secondsPlayedMin,
    stats.secondsPlayedMax
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

  const plgEncode = average => plg =>
    points.encode(plg || plg === 0 ? plg : average);

  const splgEncode = average => sp =>
    secondsPlayed.encode(sp || sp === 0 ? sp : average);

  const datumToX = d => {
    const pointsLastGamesAverage =
      _.mean(_.filter(_.isInteger, d.pointsLastGames)) || 0;
    const secondsPlayedLastGamesAverage =
      _.mean(_.filter(_.isInteger, d.secondsPlayedLastGames)) || 0;

    return [
      ...players.encode(d.playerBasketballReferenceId),
      ...teams.encode(d.playerTeamBasketballReferenceId),
      ...teams.encode(d.opposingTeamBasketballReferenceId),
      ...positions.encode(d.position),
      ...birthCountries.encode(d.birthCountry),
      ageAtTimeOfGame.encode(d.ageAtTimeOfGame),
      yearOfGame.encode(d.yearOfGame),
      monthOfGame.encode(d.monthOfGame),
      dayOfGame.encode(d.dayOfGame),
      hourOfGame.encode(d.hourOfGame),
      experience.encode(d.experience),
      ..._.map(i => plgEncode(pointsLastGamesAverage)(d.pointsLastGames[i]))(
        _.range(0, 7)
      ),
      ..._.map(i =>
        splgEncode(secondsPlayedLastGamesAverage)(d.secondsPlayedLastGames[i])
      )(_.range(0, 7)),
      d.playingAtHome ? 1 : 0
    ];
  };

  const datumToY = d => [
    points.encode(d.points),
    threePointFieldGoals.encode(d.threePointFieldGoals),
    totalRebounds.encode(d.totalRebounds),
    assists.encode(d.assists),
    steals.encode(d.steals),
    blocks.encode(d.blocks),
    turnovers.encode(d.turnovers),
    freeThrows.encode(d.freeThrows)
  ];

  const yToDatum = y => ({
    points: points.decode(y[0]),
    threePointFieldGoals: threePointFieldGoals.decode(y[1]),
    totalRebounds: totalRebounds.decode(y[2]),
    assists: assists.decode(y[3]),
    steals: steals.decode(y[4]),
    blocks: blocks.decode(y[5]),
    turnovers: turnovers.decode(y[6]),
    freeThrows: freeThrows.decode(y[7])
  });

  return { datumToX, datumToY, yToDatum };
};

async function cacheData() {
  fs.writeFileSync(DATA_FILE_PATH, '');

  console.time('makeMapFunctions');
  const { datumToX, datumToY } = await exports.makeMapFunctions();
  console.timeEnd('makeMapFunctions');

  console.time('getDataFromPg');
  let data = await getDataFromPg();
  console.timeEnd('getDataFromPg');

  console.time('mapData');
  let currentPlayerSeason = null;
  let pointsLastGames = [];
  let secondsPlayedLastGames = [];
  for (let i in data) {
    if (
      currentPlayerSeason !==
      `${data[i].playerBasketballReferenceId}${data[i].season}`
    ) {
      currentPlayerSeason = `${data[i].playerBasketballReferenceId}${
        data[i].season
      }`;
      pointsLastGames = [];
      secondsPlayedLastGames = [];
    }

    data[i].pointsLastGames = pointsLastGames;
    data[i].secondsPlayedLastGames = secondsPlayedLastGames;
    pointsLastGames.unshift(data[i].points);
    secondsPlayedLastGames.unshift(data[i].secondsPlayed);

    data[i] = [...datumToX(data[i]), ...datumToY(data[i])];
  }
  console.timeEnd('mapData');

  console.time('shuffleData');
  data = _.shuffle(data);
  console.timeEnd('shuffleData');

  console.time('writeData');
  for (let datum of data) {
    fs.writeFileSync(DATA_FILE_PATH, datum.join(',') + '\n', { flag: 'a' });
  }
  console.timeEnd('writeData');
}

exports.loadData = async function loadData({ maxSamples = Infinity } = {}) {
  const fileStream = fs.createReadStream(DATA_FILE_PATH, 'utf8');
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  let parsedX = [];
  let parsedY = [];
  let i = 0;

  for await (const line of rl) {
    if (i > maxSamples) break;
    i++;

    let parsedLine = line.split(',').map(parseFloat);

    parsedX.push(parsedLine.slice(0, -8));
    parsedY.push(parsedLine.slice(-8));
  }

  return { parsedX, parsedY };
};

exports.makeTrainingData = async function makeTrainingData(
  { validationSplit = 0.1, testSplit = 0.1 } = {},
  { parsedX, parsedY }
) {
  const totalSamples = parsedX.length;
  const trainSamples = Math.floor(
    totalSamples * (1 - testSplit - validationSplit)
  );
  const validationSamples = Math.floor(totalSamples * validationSplit);

  const res = {
    trainX: tf.tensor2d(parsedX.slice(0, trainSamples)),
    trainY: tf.tensor2d(parsedY.slice(0, trainSamples)),
    validationX: tf.tensor2d(
      parsedX.slice(trainSamples, trainSamples + validationSamples)
    ),
    validationY: tf.tensor2d(
      parsedY.slice(trainSamples, trainSamples + validationSamples)
    ),
    testX: tf.tensor2d(parsedX.slice(trainSamples + validationSamples)),
    testY: tf.tensor2d(parsedY.slice(trainSamples + validationSamples)),
    features: parsedX[0].length,
    labels: parsedY[0].length,
    trainSamples
  };

  return res;
};

if (process.env.TASK === 'cacheData') cacheData();
