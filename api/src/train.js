require('@tensorflow/tfjs-node');
const tf = require('@tensorflow/tfjs');
const _ = require('lodash/fp');

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
    limit 4
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

function makeOneHotEncoders(values) {
  const keyedValues = _.reduce(
    (prev, curr) => ({
      ...prev,
      [curr]: 0
    }),
    {},
    _.filter(_.identity, values)
  );
  return {
    encode: value => {
      const encoded = _.cloneDeep(keyedValues);
      if (!value) return encoded;

      encoded[value] = 1;
      return encoded;
    }
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

(async () => {
  const model = tf.sequential();

  model.add(
    tf.layers.dense({
      units: 128,
      activation: 'relu',
      batchInputShape: [null, 50]
    })
  );
  model.add(tf.layers.dropout({ rate: 0.5 }));
  model.add(tf.layers.dense({ units: 128, activation: 'relu' }));
  model.add(tf.layers.dropout({ rate: 0.5 }));
  model.add(tf.layers.dense({ units: 10, activation: 'linear' }));

  model.compile({
    optimizer: 'rmsprop',
    loss: 'meanSquaredError',
    metrics: ['accuracy', 'mae', 'mse']
  });

  // model.summary();

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

  const keyWith = prefix => _.mapKeys(k => `${prefix}_${_.camelCase(k)}`);

  const data = _.shuffle(await _getData());
  const X = _.pipe(
    _.map(d => ({
      ...keyWith('player')(players.encode(d.playerBasketballReferenceId)),
      ...keyWith('arena')(arenas.encode(d.arena)),
      ...keyWith('birthCountry')(birthCountries.encode(d.birthCountry)),
      ...keyWith('homeTeam')(teams.encode(d.homeTeamBasketballReferenceId)),
      ...keyWith('awayTeam')(teams.encode(d.awayTeamBasketballReferenceId)),
      ...keyWith('playerTeam')(teams.encode(d.playerTeamBasketballReferenceId)),
      ...keyWith('position')(positions.encode(d.position)),
      ageAtTimeOfGame: ageAtTimeOfGame.encode(d.ageAtTimeOfGame),
      yearOfGame: yearOfGame.encode(d.yearOfGame),
      monthOfGame: monthOfGame.encode(d.monthOfGame),
      dayOfGame: dayOfGame.encode(d.dayOfGame),
      hourOfGame: hourOfGame.encode(d.hourOfGame),
      experience: experience.encode(d.experience),
      heightInches: heightInches.encode(d.heightInches),
      weightLbs: weightLbs.encode(d.weightLbs),
      playingAtHome: d.playingAtHome ? 1 : 0
    })),
    _.map(_.values)
  )(data);

  const y = _.pipe(
    _.map(d => ({
      points: points.encode(d.points),
      threePointFieldGoals: threePointFieldGoals.encode(d.threePointFieldGoals),
      totalRebounds: totalRebounds.encode(d.totalRebounds),
      assists: assists.encode(d.assists),
      steals: steals.encode(d.steals),
      blocks: blocks.encode(d.blocks),
      turnovers: turnovers.encode(d.turnovers)
    })),
    _.map(_.values)
  )(data);

  console.log(X[2], y[2]);

  // model.fit({
  //
  // });
})();
