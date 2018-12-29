const _ = require('lodash/fp');
const chrono = require('chrono-node');

const _a = require('./lib/lodash-a');
const { html, cheerioText } = require('./helpers');
const { wsq } = require('./services');

const MIN_SEASON = 2019;
// const MIN_SEASON = 2003;

const _scrapeTeamSeason = _a.pipe([
  _.tap(console.log),
  html,
  $ =>
    _a.pipe([
      _a.tap(
        _a.pipe([
          _a.applyValues({
            name: _.pipe([
              $ => $('#meta [itemprop="name"] span'),
              _.get(1),
              cheerioText,
              _.trim
            ]),
            basketballReferenceId: _.pipe([
              $ => $('[rel="canonical"]'),
              _.get('0.attribs.href'),
              _.split('/'),
              _.get(4)
            ])
          }),
          async team => {
            const existingTeam = await wsq.from`teams`
              .where(_.pick('basketballReferenceId', team))
              .one();

            if (existingTeam) return existingTeam;

            return await wsq.from`teams`.insert(team).return`*`.one();
          }
        ])
      ),
      $ => $('#roster tr'),
      _.slice(1, Infinity),
      _.map('children'),
      _a.mapSequential(
        _a.pipe([
          _a.applyValues({
            season: _.constant(
              _.pipe([
                $ => $('#meta [itemprop="name"] span:first-of-type'),
                _.get(0),
                cheerioText,
                it => parseInt(it),
                it => it + 1
              ])($)
            ),
            teamBasketballReferenceId: _.constant(
              _.pipe([
                $ => $('[rel="canonical"]'),
                _.get('0.attribs.href'),
                _.split('/'),
                _.get(4)
              ])($)
            ),
            playerNumber: _.pipe([_.get(0), cheerioText, _.toInteger]),
            name: _.pipe([
              _.get(1),
              cheerioText,
              _.split('(TW)'),
              _.get(0),
              _.trim
            ]),
            playerBasketballReferenceId: _.pipe([
              _.get('1.children.0.attribs.href'),
              _.split('/'),
              _.get(3),
              _.split('.html'),
              _.get(0)
            ]),
            position: _.pipe([_.get('2'), cheerioText, _.trim]),
            heightInches: _.pipe([
              _.get('3'),
              cheerioText,
              _.split('-'),
              _.map(_.toInteger),
              ([feet, inches]) => feet * 12 + inches
            ]),
            weightLbs: _.pipe([_.get('4'), cheerioText, _.toInteger]),
            dateOfBirth: _.pipe([_.get('5'), cheerioText, chrono.parseDate]),
            birthCountry: _.pipe([_.get('6'), cheerioText]),
            experience: _.pipe([_.get('7'), cheerioText, _.toInteger])
          }),
          async teamPlayer => {
            let inDbPlayer = await wsq.from`players`
              .where({
                basketballReferenceId: teamPlayer.playerBasketballReferenceId
              })
              .one();

            if (!inDbPlayer) {
              inDbPlayer = await wsq.from`players`.insert({
                basketballReferenceId: teamPlayer.playerBasketballReferenceId,
                ..._.pick(['name', 'dateOfBirth', 'birthCountry'], teamPlayer)
              }).return`*`.one();
            }

            let inDbTeamPlayer = await wsq.from`teams_players`
              .where(
                _.pick(
                  [
                    'playerBasketballReferenceId',
                    'teamBasketballReferenceId',
                    'season'
                  ],
                  teamPlayer
                )
              )
              .one();

            if (!inDbTeamPlayer) {
              await wsq.from`teams_players`
                .set({ currentlyOnThisTeam: false })
                .where(_.pick(['playerBasketballReferenceId'], teamPlayer));
              inDbTeamPlayer = await wsq.from`teams_players`.insert({
                ..._.pick(
                  [
                    'playerBasketballReferenceId',
                    'teamBasketballReferenceId',
                    'season',
                    'playerNumber',
                    'position',
                    'heightInches',
                    'weightLbs',
                    'experience'
                  ],
                  teamPlayer
                ),
                currentlyOnThisTeam: true
              }).return`*`.one();
            }

            return inDbTeamPlayer;
          }
        ])
      )
    ])($)
]);

_a.pipe([
  html,
  $ => $('#teams_active a'),
  _.map('attribs.href'),
  _.map(it => `https://www.basketball-reference.com${it}`),
  _.tap(console.log),
  _a.mapSequential(
    _a.pipe([
      html,
      $ => $('[data-stat="season"] a'),
      _.map('attribs.href'),
      _.filter(
        _.pipe([_.split('/'), _.get(3), it => parseInt(it), _.lte(MIN_SEASON)])
      ),
      _.map(it => `https://www.basketball-reference.com${it}`),
      _a.mapSequential(_scrapeTeamSeason)
    ])
  ),

  _a.mapSequential(_scrapeTeamSeason),

  _.tap(it => console.dir(it, { depth: 4 }))
])('https://www.basketball-reference.com/teams/');
