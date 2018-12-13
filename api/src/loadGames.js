const _ = require('lodash/fp');

const _a = require('./lib/lodash-a');
const { html } = require('./helpers');

// -- steps —
//
// create cloud sql instance
//
// /teams
// go to all teams
// scrape team
// scrape roster (players, teams_players)
// press back recursively
//
// /games
// 2002 - 2019; jan - june, oct - dec
// go to all games on date
// scrape games

_a.pipe([
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
          _.get(0)
          // async id => _a.pipe([
          //   _.map(id => `https://www.basketball-reference.com/boxscores/${id}.html`),
          //   html,
          //
          // ])
        ])
      )
    ])
  ),

  // _a.mapSequential(
  //   _a.pipe([
  //     it => `https://www.basketball-reference.com${it}2019.html`,
  //     html,
  //     $ => $('#roster tr')
  //   ])
  // ),

  _.tap(console.log)
])(_.range(2003, 2019));
