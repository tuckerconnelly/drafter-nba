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
const { wsq } = require('./services');
const { getStatsLastGamesFromPg, calculateFantasyScore } = require('./data');
const { predict, loadModel } = require('./train');

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
  UTA: 'UTA'
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
  salaryData => {
    const bar = new ProgressBar('[ :bar ] :current/:total :percent :etas', {
      width: 40,
      total: salaryData.length
    });

    return _a.mapBatches(10, async s => {
      const inDbPlayer = await wsq.l`
      select p.basketball_reference_id, p.birth_country, p.date_of_birth, tp.experience, tp.position
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
        teamBasketballReferenceId: s.playerTeamBasketballReferenceId
      });

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
        birthCountry: inDbPlayer.birthCountry,
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
        ...statsLastGames
      };
    })(salaryData);
  }
);

const predictWithModel = memoizeFs(
  path.join(__dirname, '../../tmp/predictWithModel.json'),
  async data => predict(await loadModel(), data)
);

if (process.env.TASK === 'getPredictions') {
  parseSalaryFile(path.join(__dirname, '../../tmp/DKSalaries.csv'))
    .then(embellishSalaryData)
    .then(predictWithModel)
    .then(
      _.map(it => {
        const predictedFantasyScore = calculateFantasyScore(it._predictions);
        return {
          name: `${it.name.split(' ')[0][0]}.${it.name
            .split(' ')
            .slice(1)
            .join(' ')}`,
          rosterPositions: it.rosterPositions,
          salaryDollars: it.salaryDollars,
          difference: predictedFantasyScore - it.pointsPerGame,
          pointsPerGame: it.pointsPerGame,
          predictedFantasyScore: predictedFantasyScore,
          dollarsPerFantasyPoint: Math.round(
            it.salaryDollars / predictedFantasyScore
          ),
          fantasyPointsLastGames: it.fantasyPointsLastGames.join(',')
        };
      })
    )
    // PG, SG, SF, PF, C, G, F, UTIL
    // .then(_.filter(it => it.rosterPositions.includes('G')))
    .then(_.sortBy(['dollarsPerFantasyPoint']))
    .then(
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
    )
    .then(data => {
      const table = new Table({
        head: _.keys(data[0]),
        colWidths: [14, 14, 8, 6, 6, 6, 8, 20]
      });
      table.push(...data.map(_.values));
      console.log(table.toString());
    });
}
