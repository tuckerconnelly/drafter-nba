const fs = require('fs');
const path = require('path');
const assert = require('assert');
const util = require('util');

const readline = require('readline');
const ProgressBar = require('progress');
const cmd = require('node-cmd');
const _ = require('lodash/fp');
require('@tensorflow/tfjs-node');
const tf = require('@tensorflow/tfjs');

const { wsq } = require('./services');

const cmdGetAsync = util.promisify(cmd.get);

const DATA_FILE_PATH = path.join(__dirname, '../../tmp/data.csv');

async function getDataFromPg({ limit = null, offset = 0 } = {}) {
  return await wsq.l`
    select
      gp.game_basketball_reference_id,
      p.name as player_name,
      t.name as team_name,
      g.season as season,
      g.time_of_game as time_of_game,

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
      gp.starter,

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
      and currently_on_this_team = true
    )
    order by gp.player_basketball_reference_id asc, g.time_of_game asc
    limit ${limit}
    offset ${offset}
  `;
}

exports.getDataFromPg = getDataFromPg;

function calculateFantasyScore(stats) {
  return Math.max(
    0,
    Math.round(
      stats.points +
        stats.threePointFieldGoals * 0.5 +
        stats.totalRebounds * 1.25 +
        stats.assists * 1.5 +
        stats.steals * 2 +
        stats.blocks * 2 +
        stats.turnovers * -0.5 +
        ([
          stats.points >= 10,
          stats.totalRebounds >= 10,
          stats.assists >= 10,
          stats.blocks >= 10,
          stats.steals >= 10
        ].filter(Boolean).length >= 2
          ? 1.5
          : 0) +
        ([
          stats.points >= 10,
          stats.totalRebounds >= 10,
          stats.assists >= 10,
          stats.blocks >= 10,
          stats.steals >= 10
        ].filter(Boolean).length >= 3
          ? 3
          : 0)
    )
  );
}

exports.calculateFantasyScore = calculateFantasyScore;

async function getStatsLastGamesFromPg({
  playerBasketballReferenceId,
  season,
  currentGameDate
}) {
  assert(playerBasketballReferenceId);
  assert(season);
  assert(currentGameDate);

  const statsLastGames = await wsq.l`
    select
      gp.points,
      gp.three_point_field_goals,
      gp.total_rebounds,
      gp.assists,
      gp.blocks,
      gp.steals,
      gp.turnovers,
      gp.seconds_played
    from games g
    left join games_players gp
      on gp.game_basketball_reference_id = g.basketball_reference_id
      and gp.player_basketball_reference_id = ${playerBasketballReferenceId}
    inner join teams_players tp
      on tp.player_basketball_reference_id = gp.player_basketball_reference_id
      and tp.season = ${season}
    where g.season = ${season}
      and (
        g.home_team_basketball_reference_id = tp.team_basketball_reference_id
        or g.away_team_basketball_reference_id = tp.team_basketball_reference_id
      )
    and g.time_of_game < ${currentGameDate}
    order by g.time_of_game desc
    limit 7
  `;

  return {
    pointsLastGames: _.map('points', statsLastGames),
    threePointFieldGoalsLastGames: _.map(
      'threePointFieldGoals',
      statsLastGames
    ),
    totalReboundsLastGames: _.map('totalRebounds', statsLastGames),
    assistsLastGames: _.map('assists', statsLastGames),
    blocksLastGames: _.map('blocks', statsLastGames),
    stealsLastGames: _.map('steals', statsLastGames),
    turnoversLastGames: _.map('turnovers', statsLastGames),
    secondsPlayedLastGames: _.map('secondsPlayed', statsLastGames),
    fantasyPointsLastGames: _.map(calculateFantasyScore, statsLastGames)
  };
}

exports.getStatsLastGamesFromPg = getStatsLastGamesFromPg;

async function addLastGamesStatsMutates(data) {
  console.log('addLastGamesStatsMutates');
  const bar = new ProgressBar('[ :bar ] :current/:total :percent :etas', {
    width: 40,
    total: data.length
  });

  const BATCH_SIZE = 8;

  for (let i = 0; i < data.length / BATCH_SIZE; i++) {
    await Promise.all(
      _.range(i * BATCH_SIZE, (i + 1) * BATCH_SIZE).map(async n => {
        if (!data[n]) return;

        const statsLastGames = await getStatsLastGamesFromPg({
          playerBasketballReferenceId: data[n].playerBasketballReferenceId,
          season: data[n].season,
          currentGameDate: data[n].timeOfGame
        });
        data[n].pointsLastGames = statsLastGames.pointsLastGames;
        data[n].threePointFieldGoalsLastGames =
          statsLastGames.threePointFieldGoalsLastGames;
        data[n].totalReboundsLastGames = statsLastGames.totalReboundsLastGames;
        data[n].assistsLastGames = statsLastGames.assistsLastGames;
        data[n].blocksLastGames = statsLastGames.blocksLastGames;
        data[n].stealsLastGames = statsLastGames.stealsLastGames;
        data[n].turnoversLastGames = statsLastGames.turnoversLastGames;
        data[n].secondsPlayedLastGames = statsLastGames.secondsPlayedLastGames;
        data[n].fantasyPointsLastGames = statsLastGames.fantasyPointsLastGames;

        bar.tick(1);
      })
    );
  }
  return data;
}

exports.addLastGamesStatsMutates = addLastGamesStatsMutates;

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

  const lgEncode = _.curry((num, encodeFn, lastGamesStats) => {
    const average =
      _.pipe([_.slice(0, num), _.filter(_.isInteger), _.mean])(
        lastGamesStats
      ) || 0;

    return _.map(
      i =>
        encodeFn(
          lastGamesStats[i] || lastGamesStats[i] === 0
            ? lastGamesStats[i]
            : average
        ),
      _.range(0, 7)
    );
  });

  const datumToX = d => {
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
      ...lgEncode(7, points.encode, d.pointsLastGames),
      ...lgEncode(
        7,
        threePointFieldGoals.encode,
        d.threePointFieldGoalsLastGames
      ),
      ...lgEncode(7, totalRebounds.encode, d.totalReboundsLastGames),
      ...lgEncode(7, assists.encode, d.assistsLastGames),
      ...lgEncode(7, blocks.encode, d.blocksLastGames),
      ...lgEncode(7, steals.encode, d.stealsLastGames),
      ...lgEncode(7, turnovers.encode, d.turnoversLastGames),
      ...lgEncode(7, secondsPlayed.encode, d.secondsPlayedLastGames),
      d.playingAtHome ? 1 : 0,
      d.starter ? 1 : 0
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

async function getTotalSamples() {
  const totalSamples = _.parseInt(
    10,
    _.trim(await cmdGetAsync(`wc -l ${DATA_FILE_PATH}`))
  );

  assert(totalSamples);

  return totalSamples;
}

async function cacheData(i) {
  console.time('makeMapFunctions');
  const { datumToX, datumToY } = await exports.makeMapFunctions();
  console.timeEnd('makeMapFunctions');

  console.time('getDataFromPg');
  let data = await getDataFromPg();
  console.timeEnd('getDataFromPg');

  console.time('addLastGamesStatsMutates');
  await addLastGamesStatsMutates(data);
  console.time('addLastGamesStatsMutates');

  console.time('mapData');
  for (let i in data) {
    data[i] = [...datumToX(data[i]), ...datumToY(data[i])];
  }
  console.timeEnd('mapData');

  console.time('shuffleData');
  data = _.shuffle(data);
  console.timeEnd('shuffleData');

  console.time('writeData');
  fs.writeFileSync(DATA_FILE_PATH, '');
  for (let datum of data) {
    fs.writeFileSync(DATA_FILE_PATH, datum.join(',') + '\n', { flag: 'a' });
  }
  console.timeEnd('writeData');
}

exports.loadData = async function loadData({
  maxSamples = Infinity,
  batchSize = 5000,
  validationSplit = 0.1,
  testSplit = 0.1
} = {}) {
  const totalSamples = Math.min(maxSamples, await getTotalSamples());

  const trainSamples = Math.round(
    totalSamples * (1 - testSplit - validationSplit)
  );
  const validationSamples = Math.round(totalSamples * validationSplit);
  const testSamples = Math.round(totalSamples * testSplit);

  console.debug({
    trainSamples,
    validationSamples,
    testSamples
  });

  const fileStream = fs.createReadStream(DATA_FILE_PATH, 'utf8');
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  const trainX = [];
  const trainY = [];
  const validationX = [];
  const validationY = [];
  const testX = [];
  const testY = [];
  let i = 0;

  for await (const line of rl) {
    if (i >= maxSamples) break;

    let parsedLine = line.split(',').map(parseFloat);

    if (i > trainSamples + validationSamples) {
      testX.push(parsedLine.slice(0, -8));
      testY.push(parsedLine.slice(-8));
      i++;
      continue;
    }

    if (i > trainSamples) {
      validationX.push(parsedLine.slice(0, -8));
      validationY.push(parsedLine.slice(-8));
      i++;
      continue;
    }

    trainX.push(parsedLine.slice(0, -8));
    trainY.push(parsedLine.slice(-8));

    i++;
  }

  console.debug({
    trainXLength: trainX.length,
    trainYLength: trainY.length,
    validationXLength: validationX.length,
    validationYLength: validationY.length,
    testXLength: testX.length,
    testYLength: testY.length
  });

  return {
    trainX: _.chunk(batchSize, trainX).map(it => tf.tensor2d(it)),
    trainY: _.chunk(batchSize, trainY).map(it => tf.tensor2d(it)),
    validationX: validationSamples ? tf.tensor2d(validationX) : null,
    validationY: validationSamples ? tf.tensor2d(validationY) : null,
    testX: testSamples ? tf.tensor2d(testX) : null,
    testY: testSamples ? tf.tensor2d(testY) : null,
    features: trainX[0].length,
    labels: trainY[0].length,
    trainSamples: trainX.length
  };
};

if (process.env.TASK === 'cacheData') cacheData();
