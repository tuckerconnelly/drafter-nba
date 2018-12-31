const assert = require('assert');

const ProgressBar = require('progress');
const _ = require('lodash/fp');
require('@tensorflow/tfjs-node');
const tf = require('@tensorflow/tfjs');
const shuffleSeed = require('shuffle-seed');

const _a = require('./lib/lodash-a');
const { wsq } = require('./services');

/*** js-ml ***/

function makeOneHotEncoders(possibleValues) {
  return {
    encode: values => possibleValues.map(pv => (values.includes(pv) ? 1 : 0))
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

/*** cacheData ***/

function calculateFantasyScore(stats) {
  return Math.max(
    0,
    Math.round(
      (stats.points +
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
          : 0)) *
        1000
    ) / 1000
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
    dkFantasyPointsLastGames: _.map(calculateFantasyScore, statsLastGames)
  };
}

exports.getStatsLastGamesFromPg = getStatsLastGamesFromPg;

async function cacheSingleGamesPlayer(gamesPlayer) {
  assert(gamesPlayer.season);
  assert(gamesPlayer.timeOfGame);

  await wsq.from`games_players_computed`.delete.where({
    gameBasketballReferenceId: gamesPlayer.gameBasketballReferenceId,
    playerBasketballReferenceId: gamesPlayer.playerBasketballReferenceId
  });

  const statsLastGames = await getStatsLastGamesFromPg({
    playerBasketballReferenceId: gamesPlayer.playerBasketballReferenceId,
    season: gamesPlayer.season,
    currentGameDate: gamesPlayer.timeOfGame
  });

  await wsq.from`games_players_computed`.insert({
    gameBasketballReferenceId: gamesPlayer.gameBasketballReferenceId,
    playerBasketballReferenceId: gamesPlayer.playerBasketballReferenceId,
    dkFantasyPoints: calculateFantasyScore(gamesPlayer),
    dkFantasyPointsLastGames: statsLastGames.dkFantasyPointsLastGames,
    secondsPlayedLastGames: statsLastGames.secondsPlayedLastGames
  });
}

exports.cacheSingleGamesPlayer = cacheSingleGamesPlayer;

async function cacheSingleGame(game) {
  const awayStarters = (await wsq.l`
    select gp.player_basketball_reference_id
    from games_players gp
    inner join teams_players tp
      on tp.player_basketball_reference_id = gp.player_basketball_reference_id
      and tp.season = ${game.season}
    where gp.game_basketball_reference_id = ${game.basketballReferenceId}
    and gp.starter = true
    and tp.team_basketball_reference_id = ${game.awayTeamBasketballReferenceId}
  `).map(it => it.playerBasketballReferenceId);

  const homeStarters = (await wsq.l`
    select gp.player_basketball_reference_id
    from games_players gp
    inner join teams_players tp
      on tp.player_basketball_reference_id = gp.player_basketball_reference_id
      and tp.season = ${game.season}
    where gp.game_basketball_reference_id = ${game.basketballReferenceId}
    and gp.starter = true
    and tp.team_basketball_reference_id = ${game.homeTeamBasketballReferenceId}
  `).map(it => it.playerBasketballReferenceId);

  await wsq.from`games_computed`.delete.where({
    gameBasketballReferenceId: game.basketballReferenceId
  });

  await wsq.from`games_computed`.insert({
    gameBasketballReferenceId: game.basketballReferenceId,
    awayStarters,
    homeStarters
  });
}

exports.cacheSingleGame = cacheSingleGame;

async function cacheData() {
  console.log('Caching games');

  const games = await wsq.l`
    select
      season,
      basketball_reference_id,
      away_team_basketball_reference_id,
      home_team_basketball_reference_id
    from games
  `;

  const gBar = new ProgressBar('[ :bar ] :current/:total :percent :etas', {
    width: 40,
    total: games.length
  });

  await _a.mapBatches(
    10,
    async g => {
      await cacheSingleGame(g);
      gBar.tick(1);
    },
    games
  );

  console.log('Caching games_players');

  const gamesPlayers = await wsq.l`
    select
      gp.game_basketball_reference_id,
      gp.player_basketball_reference_id,
      gp.seconds_played,
      gp.points,
      gp.three_point_field_goals,
      gp.total_rebounds,
      gp.assists,
      gp.steals,
      gp.blocks,
      gp.turnovers,
      gp.free_throws,

      g.season,
      g.time_of_game
    from games_players gp
    inner join games g
      on g.basketball_reference_id = gp.game_basketball_reference_id
    inner join teams_players tp
      on tp.player_basketball_reference_id = gp.player_basketball_reference_id
      and tp.currently_on_this_team = true
  `;

  const gpBar = new ProgressBar('[ :bar ] :current/:total :percent :etas', {
    width: 40,
    total: gamesPlayers.length
  });

  await _a.mapBatches(
    10,
    async gp => {
      await cacheSingleGamesPlayer(gp);
      gpBar.tick(1);
    },
    gamesPlayers
  );
}

/*** loadData ***/

async function getDataFromPg({
  limit = null,
  offset = 0,
  playerBasketballReferenceId = null,
  gameBasketballReferenceId = null
} = {}) {
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
      gpc.dk_fantasy_points,
      gpc.seconds_played_last_games,
      gpc.dk_fantasy_points_last_games,
      gc.away_starters,
      gc.home_starters,

      gpc.dk_fantasy_points
    from games_players gp
    inner join players p
      on p.basketball_reference_id = gp.player_basketball_reference_id
    inner join games g
      on g.basketball_reference_id = gp.game_basketball_reference_id
    inner join teams_players tp
      on tp.player_basketball_reference_id = gp.player_basketball_reference_id
      and tp.currently_on_this_team = true
    inner join teams t
      on t.basketball_reference_id = tp.team_basketball_reference_id
    left join games_players_computed gpc
      on gpc.game_basketball_reference_id = gp.game_basketball_reference_id
      and gpc.player_basketball_reference_id = gp.player_basketball_reference_id
    left join games_computed gc
      on gc.game_basketball_reference_id = g.basketball_reference_id
    where true
    ${
      playerBasketballReferenceId
        ? wsq.raw(
            `and gp.player_basketball_reference_id = ${playerBasketballReferenceId}`
          )
        : wsq.raw('')
    }
    ${
      gameBasketballReferenceId
        ? wsq.raw(
            `and gp.game_basketball_reference_id = ${gameBasketballReferenceId}`
          )
        : wsq.raw('')
    }
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

    	avg(gp.seconds_played) as seconds_played_avg,
    	min(gp.seconds_played) as seconds_played_min,
    	max(gp.seconds_played) as seconds_played_max,


    	avg(gpc.dk_fantasy_points) as dk_fantasy_points_avg,
    	min(gpc.dk_fantasy_points) as dk_fantasy_points_min,
    	max(gpc.dk_fantasy_points) as dk_fantasy_points_max

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
    inner join games_players_computed gpc
      on gpc.game_basketball_reference_id = gp.game_basketball_reference_id
      and gpc.player_basketball_reference_id = gp.player_basketball_reference_id
  `.one();
}

async function _getPlayers() {
  return _.uniq(
    _.map('basketballReferenceId')(
      await wsq.l`
        select basketball_reference_id
        from players p
        inner join teams_players tp on tp.player_basketball_reference_id = p.basketball_reference_id
        where currently_on_this_team = true;
      `
    )
  );
}

const suffixes = ['Jr.', 'II', 'III', 'IV', 'V'];

function formatPlayerName(name) {
  const parts = _.split(' ', _.trim(name));
  const first = _.head(parts)[0];
  let last = _.last(parts);

  if (suffixes.includes(last)) {
    last = parts[parts.length - 2];
  }

  return `${first}. ${last}`;
}

exports.formatPlayerName = formatPlayerName;

async function getPlayersByTeamAndFormattedName() {
  const players = await wsq.l`
    select p.*, tp.team_basketball_reference_id
    from players p
    inner join teams_players tp on tp.player_basketball_reference_id = p.basketball_reference_id
    where currently_on_this_team = true;
  `;

  const playersByTeamAndFormattedName = _.keyBy(
    it => `${it.teamBasketballReferenceId} ${formatPlayerName(it.name)}`
  )(players);

  assert(_.values(playersByTeamAndFormattedName).length === players.length);

  return playersByTeamAndFormattedName;
}

exports.getPlayersByTeamAndFormattedName = getPlayersByTeamAndFormattedName;

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

async function makeMapFunctions() {
  const teamsValues = await _getTeams();
  const teams = makeOneHotEncoders(teamsValues);

  const playersValues = await _getPlayers();
  const players = makeOneHotEncoders(playersValues);

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

  const dkFantasyPoints = makeEncoders(
    stats.dkFantasyPointsAvg,
    stats.dkFantasyPointsMin,
    stats.dkFantasyPointsMax
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
      ageAtTimeOfGame.encode(d.ageAtTimeOfGame),
      yearOfGame.encode(d.yearOfGame),
      monthOfGame.encode(d.monthOfGame),
      dayOfGame.encode(d.dayOfGame),
      hourOfGame.encode(d.hourOfGame),
      experience.encode(d.experience),
      d.playingAtHome ? 1 : 0,
      d.starter ? 1 : 0,
      ...lgEncode(7, dkFantasyPoints.encode, d.dkFantasyPointsLastGames),
      ...lgEncode(7, secondsPlayed.encode, d.secondsPlayedLastGames),
      ...players.encode([d.playerBasketballReferenceId]),
      ...teams.encode([d.playerTeamBasketballReferenceId]),
      ...teams.encode([d.opposingTeamBasketballReferenceId]),
      ...positions.encode([d.position]),
      ...players.encode([...d.awayStarters, ...d.homeStarters])
    ];
  };

  const datumToY = d => [d.dkFantasyPoints];

  const yToDatum = y => ({
    dkFantasyPoints: parseFloat((Math.round(y[0] * 4) / 4).toFixed(2))
  });

  return { datumToX, datumToY, yToDatum };
}

exports.makeMapFunctions = makeMapFunctions;

async function loadData({
  maxSamples = Infinity,
  batchSize = 5000,
  validationSplit = 0.1,
  testSplit = 0.1
} = {}) {
  console.time('getDataFromPg');
  let data = await getDataFromPg({
    limit: maxSamples === Infinity ? null : maxSamples
  });
  console.timeEnd('getDataFromPg');

  data = shuffleSeed.shuffle(data, 0);

  const trainSamples = Math.round(
    data.length * (1 - testSplit - validationSplit)
  );
  const validationSamples = Math.round(data.length * validationSplit);
  const testSamples = Math.round(data.length * testSplit);

  console.time('makeMapFunctions');
  const { datumToX, datumToY } = await makeMapFunctions();
  console.timeEnd('makeMapFunctions');

  console.time('slice and map');
  const trainX = data.slice(0, trainSamples).map(datumToX);
  const trainY = data.slice(0, trainSamples).map(datumToY);
  const validationX = data
    .slice(trainSamples, trainSamples + validationSamples)
    .map(datumToX);
  const validationY = data
    .slice(trainSamples, trainSamples + validationSamples)
    .map(datumToY);
  const testX = data.slice(trainSamples + validationSamples).map(datumToX);
  const testY = data.slice(trainSamples + validationSamples).map(datumToY);
  console.timeEnd('slice and map');

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
}

exports.loadData = loadData;

if (process.env.TASK === 'cacheData') cacheData();
if (process.env.TASK === 'loadData') loadData({ maxSamples: 100 });
