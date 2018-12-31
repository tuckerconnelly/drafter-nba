const path = require('path');
const fs = require('fs');
const cluster = require('cluster');
const os = require('os');

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

  _.keys(returnedLineups).forEach(team => {
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
  if (cluster.isWorker) return data;

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
        dkFantasyPointsLastGames: it.dkFantasyPointsLastGames.join(',')
      };
    }),
    _.sortBy(['dollarsPerFantasyPoint']),
    _.map(it => ({
      name: it.name,
      pos: it.rosterPositions.join(','),
      sal: it.salaryDollars,
      diff: it.difference,
      ppg: it.pointsPerGame,
      pred: it.predictedFantasyScore,
      dpfp: it.dollarsPerFantasyPoint,
      fplg: it.dkFantasyPointsLastGames
    }))
  ])(data);

  const table = new Table({
    head: _.keys(tableData[0]),
    colWidths: [16, 16, 8, 8, 8, 8, 8, 32]
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

function _getExpectedPointsOfRoster(roster) {
  return _.values(roster).reduce(
    (prev, curr) => prev + _.getOr(0, 'dkFantasyPoints', curr),
    0
  );
}

async function pickLineups(data) {
  const players = _.pipe([
    _.map(p => ({
      playerBasketballReferenceId: p.playerBasketballReferenceId,
      rosterPositions: p.rosterPositions,
      name: p.name,
      salaryDollars: p.salaryDollars,
      dkFantasyPoints: p._predictions.dkFantasyPoints,
      dollarsPerFantasyPoint: Math.round(
        p.salaryDollars / p._predictions.dkFantasyPoints
      )
    })),
    _.sortBy('dollarsPerFantasyPoint'),
    _.slice(0, 27)
  ])(data);

  const rpFilter = rp =>
    _.pipe([_.filter(p => p.rosterPositions.includes(rp))]);

  const PGs = rpFilter('PG')(players);
  const SGs = rpFilter('SG')(players);
  const SFs = rpFilter('SF')(players);
  const PFs = rpFilter('PF')(players);
  const Cs = rpFilter('C')(players);
  const Gs = rpFilter('G')(players);
  const Fs = rpFilter('F')(players);
  const UTILs = rpFilter('UTIL')(players);

  const positionsLengths = [
    PGs.length,
    SGs.length,
    SFs.length,
    PFs.length,
    Cs.length,
    Gs.length,
    Fs.length,
    UTILs.length
  ];

  if (cluster.isWorker) {
    const workerId = parseInt(cluster.worker.id) - 1;
    const totalThreads = Math.min(os.cpus().length, PGs.length);
    const pgsPerThread = Math.ceil(PGs.length / totalThreads);
    const start = workerId * pgsPerThread;
    const end = Math.min(PGs.length, start + pgsPerThread);

    let bar;

    if (workerId === 0) {
      console.log({
        workerId,
        totalThreads,
        pgsPerThread,
        start,
        end
      });

      bar = new ProgressBar('[ :bar ] :current/:total :percent :etas', {
        width: 40,
        total: (end - start) * SGs.length
      });
    }

    for (let i = start; i < end; i++) {
      const pg = PGs[i];

      for (let sg of SGs) {
        if (bar) bar.tick(1);
        if ([pg].map(p => p.name).includes(sg.name)) continue;

        for (let sf of SFs) {
          if ([pg, sg].map(p => p.name).includes(sf.name)) continue;

          for (let pf of PFs) {
            if ([pg, sg, sf].map(p => p.name).includes(pf.name)) continue;
            if (_getSalaryOfRoster([pg, sg, sf, pf]) > BUDGET - 4 * 3000)
              continue;

            for (let c of Cs) {
              if ([pg, sg, sf, pf].map(p => p.name).includes(c.name)) continue;
              if (_getSalaryOfRoster([pg, sg, sf, pf, c]) > BUDGET - 3 * 3000)
                continue;

              for (let g of Gs) {
                if ([pg, sg, sf, pf, c].map(p => p.name).includes(g.name))
                  continue;
                if (
                  _getSalaryOfRoster([pg, sg, sf, pf, c, g]) >
                  BUDGET - 2 * 3000
                )
                  continue;

                for (let f of Fs) {
                  if ([pg, sg, sf, pf, c, g].map(p => p.name).includes(f.name))
                    continue;
                  if (
                    _getSalaryOfRoster([pg, sg, sf, pf, c, g, f]) >
                    BUDGET - 3000
                  )
                    continue;

                  for (let util of UTILs) {
                    if (
                      [pg, sg, sf, pf, c, g, f]
                        .map(p => p.name)
                        .includes(util.name)
                    )
                      continue;
                    const salaryOfRoster = _getSalaryOfRoster([
                      pg,
                      sg,
                      sf,
                      pf,
                      c,
                      g,
                      f,
                      util
                    ]);
                    if (salaryOfRoster > BUDGET) continue;
                    if (salaryOfRoster < MIN_SPEND) continue;
                    const expectedPointsOfRoster = _getExpectedPointsOfRoster([
                      pg,
                      sg,
                      sf,
                      pf,
                      c,
                      g,
                      f,
                      util
                    ]);
                    if (expectedPointsOfRoster < MIN_ACCEPTABLE_POINTS)
                      continue;
                    process.send(
                      JSON.stringify({
                        salaryOfRoster,
                        expectedPointsOfRoster,
                        players: {
                          pg,
                          sg,
                          sf,
                          pf,
                          c,
                          g,
                          f,
                          util
                        }
                      })
                    );
                  }
                }
              }
            }
          }
        }
      }
    }

    process.exit();
  }

  console.log(`Master ${process.pid} is running`);

  console.log({
    positionsLengths,
    total: positionsLengths.reduce((prev, curr) => prev * curr, 1)
  });

  let rosters = [];

  const totalThreads = Math.min(os.cpus().length, PGs.length);
  for (let i = 0; i < totalThreads; i++) cluster.fork();

  let finishedWorkers = 0;

  for (const id in cluster.workers) {
    cluster.workers[id].on('message', json => {
      const newRosters = JSON.parse(json);
      rosters = rosters.concat(newRosters);
    });
    cluster.workers[id].on('exit', () => {
      console.log(`${id} finished`);
      finishedWorkers++;
    });
  }

  while (finishedWorkers < totalThreads) {
    await new Promise(resolve => setTimeout(resolve, 1000));
  }

  console.dir(
    _.pipe([_.sortBy('expectedPointsOfRoster'), _.reverse, _.slice(0, 20)])(
      rosters
    ),
    { depth: 4 }
  );
}

if (process.env.TASK === 'getPredictions') {
  parseSalaryFile(path.join(__dirname, '../../tmp/DKSalaries.csv'))
    .then(embellishSalaryData)
    .then(checkStarters)
    .then(data => data.filter(datum => !datum.injured))
    .then(predictWithModel)
    .then(outputTable)
    .then(pickLineups);
}
