// nr loadGames && nr getPredictions
// nr trainFinal && echo "copy model"
// nr getPredictions

const path = require('path');
const fs = require('fs');

require('@tensorflow/tfjs-node');
const _ = require('lodash/fp');
const ProgressBar = require('progress');
const chrono = require('chrono-node');
const moment = require('moment');
const Table = require('cli-table');
const Combinatorics = require('js-combinatorics');

const _a = require('./lib/lodash-a');
const memoizeFs = require('./lib/memoize-fs');
const { wsq } = require('./services');

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

function _generateGameId(homeTeamBasketballReferenceId) {
  return `${moment()
    .subtract(1, 'day')
    .format('YYYYMMDD')}0${homeTeamBasketballReferenceId}`;
}

const embellishSalaryData = memoizeFs(
  path.join(__dirname, '../../tmp/idealEmbellishSalaryData.json'),
  async salaryData => {
    const bar = new ProgressBar('[ :bar ] :current/:total :percent :etas', {
      width: 40,
      total: salaryData.length
    });

    return _a.mapBatches(10, async s => {
      const inDbPlayer = await wsq.l`
          select
            p.basketball_reference_id,
            p.birth_country,
            p.date_of_birth,
            tp.experience,
            tp.position,
            gpc.dk_fantasy_points
          from players p
          inner join teams_players tp
            on tp.player_basketball_reference_id = p.basketball_reference_id
            and tp.season = '2019'
          left join games_players_computed gpc
            on gpc.game_basketball_reference_id = ${_generateGameId(
              s.homeTeamBasketballReferenceId
            )}
            and gpc.player_basketball_reference_id = p.basketball_reference_id
          where p.name = ${s.name}
      `.one();

      if (!inDbPlayer) {
        throw new Error(
          `No player found with name ${s.name} on team ${
            s.playerTeamBasketballReferenceId
          }`
        );
      }

      bar.tick(1);

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
        dkFantasyPoints: inDbPlayer.dkFantasyPoints
      };
    })(salaryData);
  }
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
        pointsPerGame: it.pointsPerGame,
        actualFantasyScore: it.dkFantasyPoints
      };
    }),
    _.sortBy(['actualFantasyScore']),
    _.reverse,
    _.map(it => ({
      name: it.name,
      pos: it.rosterPositions.join(','),
      sal: it.salaryDollars,
      ppg: it.pointsPerGame,
      act: it.actualFantasyScore || ''
    }))
  ])(data);

  const table = new Table({
    head: _.keys(tableData[0]),
    colWidths: [16, 16, 8, 8, 8]
  });
  table.push(...tableData.map(_.values));
  console.log(table.toString());

  return data;
}

const MIN_SPEND = 42000;
const BUDGET = 50000;
const MIN_ACCEPTABLE_POINTS = 250;

function _getSalaryOfRoster(roster) {
  return _.values(roster).reduce(
    (prev, curr) => prev + _.getOr(0, 'salaryDollars', curr),
    0
  );
}

const _getTotalPointsOfRoster = _.curry(function _getExpectedPointsOfRoster(
  field = 'dkFantasyPointsExpected',
  roster
) {
  return _.values(roster).reduce(
    (prev, curr) => prev + _.getOr(0, field, curr),
    0
  );
});

function _isValidRoster(roster) {
  return (
    _.pipe([_.flatMap(p => p.rosterPositions), _.uniq])(roster).length === 8
  );
}

async function outputActualBest(data) {
  const players = _.pipe([
    _.map(p => ({
      playerBasketballReferenceId: p.playerBasketballReferenceId,
      rosterPositions: p.rosterPositions,
      name: p.name,
      salaryDollars: p.salaryDollars,
      dkFantasyPointsActual: p.dkFantasyPoints,
      dollarsPerFantasyPoint: Math.round(p.salaryDollars / p.dkFantasyPoints)
    })),
    _.filter(p => p.dkFantasyPointsActual),
    _.sortBy('dollarsPerFantasyPoint'),
    _.slice(0, 30)
  ])(data);

  const rosters = [];
  const cmb = Combinatorics.bigCombination(players, 8);
  const bar = new ProgressBar('[ :bar ] :current/:total :percent :etas', {
    width: 40,
    total: cmb.length
  });
  let rosterPlayers;
  while ((rosterPlayers = cmb.next())) {
    bar.tick(1);
    const salary = _getSalaryOfRoster(rosterPlayers);
    if (salary < MIN_SPEND || salary > BUDGET) continue;
    const actualPoints = _getTotalPointsOfRoster(
      'dkFantasyPointsActual',
      rosterPlayers
    );
    if (actualPoints < MIN_ACCEPTABLE_POINTS) continue;
    if (!_isValidRoster(rosterPlayers)) continue;

    rosters.push({
      players: rosterPlayers,
      salary,
      actualPoints
    });
  }

  const tableData = _.pipe([
    _.sortBy(['actualPoints']),
    _.reverse,
    _.slice(0, 5),
    _.map(it => ({
      names: it.players
        .map(rp => `${_.padEnd(30, rp.name)} ${rp.salaryDollars}`)
        .join('\n'),
      salary: it.salary,
      actualPoints: it.actualPoints
    }))
  ])(rosters);
  const table = new Table({
    head: _.keys(tableData[0]),
    colWidths: [40, 8, 8]
  });
  table.push(...tableData.map(_.values));
  console.log(table.toString());

  return data;
}

if (process.env.TASK === 'getIdealRoster') {
  parseSalaryFile(path.join(__dirname, '../../tmp/DKSalaries.csv'))
    .then(embellishSalaryData)
    .then(outputTable)
    .then(outputActualBest);
}
