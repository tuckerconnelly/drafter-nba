const path = require('path');
const fs = require('fs');

require('@tensorflow/tfjs-node');
const _ = require('lodash/fp');
const ProgressBar = require('progress');
const chrono = require('chrono-node');
const moment = require('moment');
const Table = require('cli-table');

const _a = require('./lib/lodash-a');
const memoizeFs = require('./lib/memoize-fs');
const {
  formatPlayerName,
  getPlayersByTeamAndFormattedName,
  getStatsLastGamesFromPg
} = require('./data');
const { wsq } = require('./services');
const { predict, loadModel } = require('./train');
const { getLineups } = require('./getLineups');

const ABBREVIATIONS = {
  NO: 'NOP',
  DAL: 'DAL',
  CHA: 'CHO',
  BKN: 'BRK',
  TOR: 'TOR',
  MIA: 'MIA',
  WAS: 'WAS',
  DET: 'DET',
  PHO: 'PHO',
  ORL: 'ORL',
  MIN: 'MIN',
  CHI: 'CHI',
  CLE: 'CLE',
  MEM: 'MEM',
  DEN: 'DEN',
  SA: 'SAS',
  SAC: 'SAC',
  LAC: 'LAC',
  IND: 'IND',
  ATL: 'ATL',
  BOS: 'BOS',
  HOU: 'HOU',
  POR: 'POR',
  GS: 'GSW',
  MIL: 'MIL',
  NY: 'NYK',
  LAL: 'LAL',
  PHI: 'PHI',
  UTA: 'UTA',
  OKC: 'OKC'
};

const parseSalaryFile = _a.pipe([
  path => fs.readFileSync(path, 'utf8'),
  _.split('\n'),
  _.filter(_.identity),
  _.slice(1, Infinity),
  _.map(
    _.pipe([
      _.split(','),
      line => ({
        positions: _.pipe([_.get(0), _.split('/')])(line),
        name: _.pipe([_.get(2), _.trim])(line),
        rosterPositions: _.pipe([_.get(4), _.split('/')])(line),
        salaryDollars: _.pipe([_.get(5), _.toInteger])(line),
        awayTeamBasketballReferenceId: _.pipe([
          _.get(6),
          c => c.match(/(\w\w\w?)@\w\w\w?/i),
          _.get(1),
          it => ABBREVIATIONS[it]
        ])(line),
        homeTeamBasketballReferenceId: _.pipe([
          _.get(6),
          c => c.match(/\w\w\w?@(\w\w\w?)/i),
          _.get(1),
          it => ABBREVIATIONS[it]
        ])(line),
        timeOfGame: _.pipe([
          _.get(6),
          c => c.match(/\w\w\w?@\w\w\w?\s(.*)/i),
          _.get(1),
          chrono.parseDate
        ])(line),
        playerTeamBasketballReferenceId: _.pipe([
          _.get(7),
          it => ABBREVIATIONS[it]
        ])(line),
        pointsPerGame: _.pipe([_.get(8), _.toInteger])(line)
      }),
      _.tap(it => {
        if (_.values(_.filter(it => it !== undefined, it)).length === 9) return;

        console.error({
          message: 'Invalid salary record',
          salaryRecord: it
        });
        throw new Error(`Invalid salary record`);
      })
    ])
  )
]);

const embellishSalaryData = memoizeFs(
  path.join(__dirname, '../../tmp/embellishSalaryData.json'),
  async salaryData => {
    const bar = new ProgressBar('[ :bar ] :current/:total :percent :etas', {
      width: 40,
      total: salaryData.length
    });

    const playersByTeamAndFormattedName = await getPlayersByTeamAndFormattedName();
    const lineups = await getLineups();

    return _a.mapBatches(10, async s => {
      const inDbPlayer = await wsq.l`
          select
            p.basketball_reference_id,
            p.birth_country,
            p.date_of_birth,
            tp.experience,
            tp.position
          from players p
          inner join teams_players tp
            on tp.player_basketball_reference_id = p.basketball_reference_id
            and tp.season = '2019'
          where p.name = ${s.name}
      `.one();

      if (!inDbPlayer) {
        throw new Error(
          `No player found with name ${s.name} on team ${
            s.playerTeamBasketballReferenceId
          }`
        );
      }

      const statsLastGames = await getStatsLastGamesFromPg({
        playerBasketballReferenceId: inDbPlayer.basketballReferenceId,
        season: 2019,
        currentGameDate: new Date()
      });

      const teamRoster = lineups[s.playerTeamBasketballReferenceId];

      if (!teamRoster) {
        throw new Error(
          `Team roster not found in lineups: ${
            s.playerTeamBasketballReferenceId
          }`
        );
      }

      const starter = _.find({ name: formatPlayerName(s.name) })(
        teamRoster.starters
      );
      const injured = _.find({ name: formatPlayerName(s.name) })(
        teamRoster.injured
      );

      bar.tick(1);

      const awayStarters = lineups[
        s.awayTeamBasketballReferenceId
      ].starters.map(
        starter =>
          playersByTeamAndFormattedName[
            `${s.awayTeamBasketballReferenceId} ${starter.name}`
          ].basketballReferenceId
      );

      const homeStarters = lineups[
        s.homeTeamBasketballReferenceId
      ].starters.map(
        starter =>
          playersByTeamAndFormattedName[
            `${s.homeTeamBasketballReferenceId} ${starter.name}`
          ].basketballReferenceId
      );

      return {
        name: s.name,
        salaryDollars: s.salaryDollars,
        pointsPerGame: s.pointsPerGame,
        rosterPositions: s.rosterPositions,
        playerBasketballReferenceId: inDbPlayer.basketballReferenceId,
        playerTeamBasketballReferenceId: s.playerTeamBasketballReferenceId,
        opposingTeamBasketballReferenceId:
          s.playerTeamBasketballReferenceId === s.homeTeamBasketballReferenceId
            ? s.awayTeamBasketballReferenceId
            : s.homeTeamBasketballReferenceId,
        position: s.positions[0],
        ageAtTimeOfGame: moment(s.timeOfGame).diff(
          moment(inDbPlayer.dateOfBirth),
          'years'
        ),
        yearOfGame: moment(s.timeOfGame).year(),
        monthOfGame: moment(s.timeOfGame).month(),
        dayOfGame: moment(s.timeOfGame).date(),
        hourOfGame: moment(s.timeOfGame).hour(),
        experience: inDbPlayer.experience,
        playingAtHome:
          s.playerTeamBasketballReferenceId === s.homeTeamBasketballReferenceId,
        ...statsLastGames,
        starter: !!starter,
        injured: !!injured,
        injury:
          _.getOr(null, 'injury', starter) || _.getOr(null, 'injury', injured),
        awayStarters,
        homeStarters
      };
    })(salaryData);
  }
);

async function checkStarters(data) {
  const startersByTeam = _.pipe([
    _.filter('starter'),
    _.reduce(
      (prev, curr) => ({
        ...prev,
        [curr.playerTeamBasketballReferenceId]: [
          ...(prev[curr.playerTeamBasketballReferenceId] || []),
          {
            name: formatPlayerName(curr.name),
            position: curr.position,
            injury: curr.injury
          }
        ]
      }),
      {}
    ),
    _.mapValues(starters => ({ starters }))
  ])(data);

  const injuredByTeam = _.pipe([
    _.filter('injured'),
    _.reduce(
      (prev, curr) => ({
        ...prev,
        [curr.playerTeamBasketballReferenceId]: [
          ...(prev[curr.playerTeamBasketballReferenceId] || []),
          {
            name: formatPlayerName(curr.name),
            position: curr.position,
            injury: curr.injury
          }
        ]
      }),
      {}
    ),
    _.mapValues(injured => ({ injured }))
  ])(data);

  const returnedLineups = _.merge(startersByTeam, injuredByTeam);
  const trueLineups = await getLineups();

  _.keys(trueLineups).forEach(team => {
    if (
      _.getOr(0, [team, 'starters', 'length'], returnedLineups) !==
      _.getOr(0, [team, 'starters', 'length'], trueLineups)
    ) {
      console.warn({
        type: 'starters',
        team,
        returned: returnedLineups[team].starters,
        actual: trueLineups[team].starters
      });
    }
    if (
      _.getOr(0, [team, 'injured', 'length'], returnedLineups) !==
      _.getOr(0, [team, 'injured', 'length'], trueLineups)
    ) {
      console.error({
        type: 'injured',
        team,
        returned: returnedLineups[team].injured,
        actual: trueLineups[team].injured
      });
    }
  });

  return data;
}

const predictWithModel = memoizeFs(
  path.join(__dirname, '../../tmp/predictWithModel.json'),
  async data => predict(await loadModel(), data)
);

function outputTable(data) {
  const tableData = _.pipe([
    _.map(it => {
      return {
        name: `${it.name.split(' ')[0][0]}.${it.name
          .split(' ')
          .slice(1)
          .join(' ')}`,
        rosterPositions: it.rosterPositions,
        salaryDollars: it.salaryDollars,
        difference: it._predictions.dkFantasyPoints - it.pointsPerGame,
        pointsPerGame: it.pointsPerGame,
        predictedFantasyScore: it._predictions.dkFantasyPoints,
        dollarsPerFantasyPoint: Math.round(
          it.salaryDollars / it._predictions.dkFantasyPoints
        ),
        dkFantasyPointsLastGames: it.dkfantasyPointsLastGames.join(',')
      };
    }),
    _.sortBy(['salaryDollars']),
    _.reverse,
    _.map(it => ({
      name: it.name,
      pos: it.rosterPositions.join(','),
      sal: it.salaryDollars,
      diff: it.difference,
      ppg: it.pointsPerGame,
      pred: it.predictedFantasyScore,
      dpfp: it.dollarsPerFantasyPoint,
      fplg: it.fantasyPointsLastGames
    }))
  ])(data);

  const table = new Table({
    head: _.keys(tableData),
    colWidths: [14, 14, 8, 6, 6, 6, 8, 20]
  });
  table.push(...tableData.map(_.values));
  console.log(table.toString());

  return data;
}

if (process.env.TASK === 'getPredictions') {
  parseSalaryFile(path.join(__dirname, '../../tmp/DKSalaries.csv'))
    .then(embellishSalaryData)
    .then(checkStarters)
    .then(data => data.filter(datum => !datum.injured))
    .then(predictWithModel)
    .then(outputTable);
}
