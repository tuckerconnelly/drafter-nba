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

      const pointsLastGames = await wsq.l`
        select gp.points
        from games g
        left join games_players gp
          on gp.game_basketball_reference_id = g.basketball_reference_id
          and gp.player_basketball_reference_id = ${
            inDbPlayer.basketballReferenceId
          }
        where g.season = '2019'
          and (
            g.home_team_basketball_reference_id = ${
              s.playerTeamBasketballReferenceId
            }
            or g.away_team_basketball_reference_id = ${
              s.playerTeamBasketballReferenceId
            }
          )
        order by g.time_of_game desc
        limit 7
      `;

      const secondsPlayedLastGames = await wsq.l`
        select gp.seconds_played
        from games g
        left join games_players gp
          on gp.game_basketball_reference_id = g.basketball_reference_id
          and gp.player_basketball_reference_id = ${
            inDbPlayer.basketballReferenceId
          }
        where g.season = '2019'
          and (
            g.home_team_basketball_reference_id = ${
              s.playerTeamBasketballReferenceId
            }
            or g.away_team_basketball_reference_id = ${
              s.playerTeamBasketballReferenceId
            }
          )
        order by g.time_of_game desc
        limit 7
      `;

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
        pointsLastGames: pointsLastGames.map(it => it.points),
        secondsPlayedLastGames: secondsPlayedLastGames.map(
          it => it.secondsPlayed
        ),
        playingAtHome:
          s.playerTeamBasketballReferenceId === s.homeTeamBasketballReferenceId
      };
    })(salaryData);
  }
);

const predictWithModel = memoizeFs(
  path.join(__dirname, '../../tmp/predictWithModel.json'),
  async data => predict(await loadModel(), data)
);

parseSalaryFile(path.join(__dirname, '../../tmp/DKSalaries.csv'))
  // .then(async it => it.slice(0, 10))
  .then(embellishSalaryData)
  .then(predictWithModel)
  .then(
    _.map(it => {
      const predictedFantasyScore =
        it._predictions.points +
        it._predictions.threePointFieldGoals * 0.5 +
        it._predictions.totalRebounds * 1.25 +
        it._predictions.assists * 1.5 +
        it._predictions.steals * 2 +
        it._predictions.blocks * 2 +
        it._predictions.turnovers * -0.5 +
        ([
          it._predictions.points >= 10,
          it._predictions.totalRebounds >= 10,
          it._predictions.assists >= 10,
          it._predictions.blocks >= 10,
          it._predictions.steals >= 10
        ].filter(Boolean).length >= 2
          ? 1.5
          : 0) +
        ([
          it._predictions.points >= 10,
          it._predictions.totalRebounds >= 10,
          it._predictions.assists >= 10,
          it._predictions.blocks >= 10,
          it._predictions.steals >= 10
        ].filter(Boolean).length >= 3
          ? 3
          : 0);
      return {
        name: it.name,
        rosterPositions: it.rosterPositions,
        salaryDollars: it.salaryDollars,
        difference: Math.round(predictedFantasyScore - it.pointsPerGame),
        pointsPerGame: it.pointsPerGame,
        predictedFantasyScore: Math.round(predictedFantasyScore),
        dollarsPerFantasyPoint: Math.round(
          it.salaryDollars / predictedFantasyScore
        )
      };
    })
  )
  .then(_.filter(it => it.pointsPerGame > 0))
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
      dpfp: it.dollarsPerFantasyPoint
    }))
  )
  .then(data => {
    const table = new Table({
      head: _.keys(data[0]),
      colWidths: [20, 14, 8, 6, 6, 6, 8]
    });
    table.push(...data.map(_.values));
    console.log(table.toString());
  });
