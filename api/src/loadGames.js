const _ = require('lodash/fp');
const chrono = require('chrono-node');
const moment = require('moment');

const _a = require('./lib/lodash-a');
const { html, cheerioText } = require('./helpers');
const { wsq } = require('./services');

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
  _a.tap(
    async () =>
      await wsq.l`
        delete from games_players;
        alter sequence games_players_id_seq RESTART WITH 1;

        delete from games;
        alter sequence games_id_seq RESTART WITH 1;
      `
  ),

  _.flatMap(year =>
    _.flatMap(
      month =>
        _.flatMap(
          day =>
            `https://www.basketball-reference.com/boxscores/?month=${month}&day=${day}&year=${year}`,
          _.range(1, 31)
        ),
      _.range(1, 12)
    )
  ),

  // DEBUG
  _.slice(0, 1),

  _a.mapSequential(
    _a.pipe([
      html,
      $ => $('.game_summary .gamelink a'),
      _.map('attribs.href'),
      _a.mapSequential(
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
                awayTeamBasketbalReferenceId: _.pipe([
                  $ => $('.scorebox div:first-of-type [itemprop="name"]'),
                  _.get('0.attribs.href'),
                  _.split('/'),
                  _.get(2)
                ]),
                homeTeamBasketbalReferenceId: _.pipe([
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
            const toDbGame = {
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
                moment(scrapedGame.timeOfGame).month() > 9
                  ? moment(scrapedGame.timeOfGame).year() + 1
                  : moment(scrapedGame.timeOfGame).year()
            };

            const game = await wsq.from`games`.insert(toDbGame).return`*`.one();
            const awayGamesPlayers = await _a.mapSequential(async gp => {
              return await wsq.from`games_players`.insert(gp).return`*`.one();
            })(scrapedGame.awayGamesPlayers);
            const homeGamesPlayers = await _a.mapSequential(async gp => {
              return await wsq.from`games_players`.insert(gp).return`*`.one();
            })(scrapedGame.homeGamesPlayers);

            return { game, awayGamesPlayers, homeGamesPlayers };
          }
        ])
      )
    ])
  ),

  // DEBUG
  _.tap(it => console.dir(it, { depth: 6 }))

  // ])(_.range(2002, 2019));
])(_.range(2018, 2019));
