const _ = require('lodash/fp');
const chrono = require('chrono-node');
const moment = require('moment');

const _a = require('./lib/lodash-a');
const { html, cheerioText } = require('./helpers');
const { wsq } = require('./services');
const { cacheSingleGame, cacheSingleGamesPlayer } = require('./data');

const numberFromColumn = column =>
  _.pipe([_.get(['children', column]), cheerioText, _.toInteger]);

// #all_four_factors + div + div tbody tr

const scrapeGamesPlayers = gameBasketballReferenceId => trSelector => $ => {
  let starter = true;
  return _.pipe([
    $ => $(trSelector),
    _.toArray,
    _.map(tr => {
      if (_.trim(cheerioText(tr.children[1])) === 'Reserves') {
        starter = false;
        return null;
      }

      if (_.lowerCase(_.trim(cheerioText(tr.children[1]))) === 'did not play') {
        return null;
      }

      return { starter, tr };
    }),
    _.filter(_.identity),
    _.map(it => ({
      ..._.omit('tr', it),
      gameBasketballReferenceId,
      playerBasketballReferenceId: _.pipe([
        _.get('children.0.children.0.attribs.href'),
        _.split('/'),
        _.get(3),
        _.split('.'),
        _.get(0)
      ])(it.tr),
      secondsPlayed: _.pipe([
        _.get('children.1'),
        cheerioText,
        _.split(':'),
        _.map(_.toInteger),
        ([minutes, seconds]) => (minutes || 0) * 60 + (seconds || 0)
      ])(it.tr),
      fieldGoals: numberFromColumn(2)(it.tr),
      fieldGoalsAttempted: numberFromColumn(3)(it.tr),
      threePointFieldGoals: numberFromColumn(5)(it.tr),
      threePointFieldGoalsAttempted: numberFromColumn(6)(it.tr),
      freeThrows: numberFromColumn(8)(it.tr),
      freeThrowsAttempted: numberFromColumn(9)(it.tr),
      offensiveRebounds: numberFromColumn(11)(it.tr),
      defensiveRebounds: numberFromColumn(12)(it.tr),
      totalRebounds: numberFromColumn(13)(it.tr),
      assists: numberFromColumn(14)(it.tr),
      steals: numberFromColumn(15)(it.tr),
      blocks: numberFromColumn(16)(it.tr),
      turnovers: numberFromColumn(17)(it.tr),
      personalFouls: numberFromColumn(18)(it.tr),
      points: numberFromColumn(19)(it.tr)
    }))
  ])($);
};

_a.pipe([
  _.flatMap(year =>
    _.flatMap(
      month =>
        _.flatMap(
          day =>
            `https://www.basketball-reference.com/boxscores/?month=${month}&day=${day}&year=${year}`,
          _.range(1, 32)
        ),
      // NOTE Start 2002 at month 8, the beginning of the 2003 season
      // _.range(year === 2002 ? 10 : 1, 13)
      _.range(year === 2018 ? 12 : 13)
    )
  ),

  // // DEBUG
  // _.slice(0, 1),

  _a.mapSequential(
    _a.pipe([
      _.tap(console.log),
      html,
      $ => $('.game_summary .gamelink a'),
      _.map('attribs.href'),
      _a.mapParallel(
        _a.pipe([
          _.split('/'),
          _.get(2),
          _.split('.'),
          _.get(0),
          id =>
            _a.pipe([
              id => `https://www.basketball-reference.com/boxscores/${id}.html`,
              _.tap(console.log),
              html,
              _a.applyValues({
                basketballReferenceId: _.constant(id),
                awayTeamBasketballReferenceId: _.pipe([
                  $ => $('.scorebox div:first-of-type [itemprop="name"]'),
                  _.get('0.attribs.href'),
                  _.split('/'),
                  _.get(2)
                ]),
                homeTeamBasketballReferenceId: _.pipe([
                  $ => $('.scorebox div:nth-of-type(2) [itemprop="name"]'),
                  _.get('0.attribs.href'),
                  _.split('/'),
                  _.get(2)
                ]),
                awayScore: _.pipe([
                  $ => $('.scorebox div:first-of-type .score'),
                  _.get(0),
                  cheerioText,
                  _.toInteger
                ]),
                homeScore: _.pipe([
                  $ => $('.scorebox div:nth-of-type(2) .score'),
                  _.get(0),
                  cheerioText,
                  _.toInteger
                ]),
                timeOfGame: _.pipe([
                  $ => $('.scorebox_meta div:nth-of-type(1)'),
                  _.get(0),
                  cheerioText,
                  it => `${it} EST`,
                  chrono.parseDate
                ]),
                arena: _.pipe([
                  $ => $('.scorebox_meta div:nth-of-type(2)'),
                  _.get(0),
                  cheerioText
                ]),
                awayGamesPlayers: scrapeGamesPlayers(id)(
                  '#all_four_factors + div + div tbody tr'
                ),
                homeGamesPlayers: scrapeGamesPlayers(id)(
                  '#all_four_factors + div + div + div + div tbody tr'
                )
              })
            ])(id),
          async scrapedGame => {
            let game = await wsq.from`games`
              .where(_.pick('basketballReferenceId', scrapedGame))
              .one();

            if (!game) {
              game = await wsq.from`games`.insert({
                ..._.pick(
                  [
                    'basketballReferenceId',
                    'homeTeamBasketballReferenceId',
                    'awayTeamBasketballReferenceId',
                    'homeScore',
                    'awayScore',
                    'arena',
                    'timeOfGame'
                  ],
                  scrapedGame
                ),
                season:
                  moment(scrapedGame.timeOfGame).month() > 7
                    ? moment(scrapedGame.timeOfGame).year() + 1
                    : moment(scrapedGame.timeOfGame).year()
              }).return`*`.one();
            }

            const gamesPlayers = await _a.mapSequential(async gp => {
              let inDbGp = await wsq.from`games_players`
                .where(
                  _.pick(
                    [
                      'gameBasketballReferenceId',
                      'playerBasketballReferenceId'
                    ],
                    gp
                  )
                )
                .one();

              if (inDbGp) return inDbGp;

              inDbGp = await wsq.from`games_players`.insert(gp).return`*`.one();

              await cacheSingleGamesPlayer({
                ...inDbGp,
                season: game.season,
                timeOfGame: game.timeOfGame
              });

              return inDbGp;
            })([
              ...scrapedGame.awayGamesPlayers,
              ...scrapedGame.homeGamesPlayers
            ]);

            await cacheSingleGame(game);

            return { game, gamesPlayers };
          }
        ])
      )
    ])
  )

  // // DEBUG
  // _.tap(it => console.dir(it, { depth: 6 }))
  // ])(_.range(2002, 2019));
])(_.range(2018, 2019));
