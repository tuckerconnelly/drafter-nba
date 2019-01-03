const _ = require('lodash/fp');
const cheerio = require('cheerio');

const _a = require('./lib/lodash-a');
const { html, cheerioText } = require('./helpers');
const { formatPlayerName } = require('./data');

function mapPlayerNames(playerName) {
  if (playerName === 'W. Hernangomez') return 'G. Hernangomez';
  return playerName;
}

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

async function getLineups() {
  return _a.pipe([
    html,
    $ => $('.lineup.is-nba:not(.is-tools)'),
    _a.mapParallel(
      _a.pipe([
        cheerio.load,
        _a.applyValues({
          awayTeamAbbreviation: _.pipe([
            $ => $('.lineup__abbr:first-of-type'),
            _.get(0),
            cheerioText,
            _.trim
          ]),
          homeTeamAbbreviation: _.pipe([
            $ => $('.lineup__abbr:first-of-type'),
            _.get(1),
            cheerioText,
            _.trim
          ]),
          awayRoster: _.pipe([
            $ => $('.lineup__list.is-visit .lineup__player'),
            _.map(
              _.pipe([
                cheerio.load,
                $ => ({
                  position: _.trim($('.lineup__pos').text()),
                  name: mapPlayerNames(
                    formatPlayerName($('.lineup__pos + a').text())
                  ),
                  injury: _.trim($('.lineup__inj').text()) || null
                })
              ])
            )
          ]),
          homeRoster: _.pipe([
            $ => $('.lineup__list.is-home .lineup__player'),
            _.map(
              _.pipe([
                cheerio.load,
                $ => ({
                  position: _.trim($('.lineup__pos').text()),
                  name: mapPlayerNames(
                    formatPlayerName($('.lineup__pos + a').text())
                  ),
                  injury: _.trim($('.lineup__inj').text()) || null
                })
              ])
            )
          ])
        }),
        it => ({
          [ABBREVIATIONS[it.awayTeamAbbreviation]]: {
            starters: it.awayRoster.slice(0, 5),
            injured: it.awayRoster.slice(5)
          },
          [ABBREVIATIONS[it.homeTeamAbbreviation]]: {
            starters: it.homeRoster.slice(0, 5),
            injured: it.homeRoster.slice(5)
          }
        })
      ])
    ),
    _.reduce(_.merge, {})
    // _.tap(it => console.dir(it, { depth: 4 }))
  ])('https://www.rotowire.com/basketball/nba-lineups.php');
}

exports.getLineups = getLineups;

// getLineups();
