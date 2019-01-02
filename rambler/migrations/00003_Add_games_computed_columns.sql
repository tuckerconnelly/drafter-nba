-- rambler up

delete from games_computed;

alter table games_computed
  add column away_wins int,
  add column away_losses int,
  add column home_wins int,
  add column home_losses int,
  add column away_dk_fantasy_points_allowed real,
  add column home_dk_fantasy_points_allowed real,
  add column away_dk_fantasy_points_allowed_last_games jsonb default '[]',
  add column home_dk_fantasy_points_allowed_last_games jsonb default '[]';

-- rambler down

alter table games_computed
  drop column away_wins,
  drop column away_losses,
  drop column home_wins,
  drop column home_losses,
  drop column away_dk_fantasy_points_allowed,
  drop column home_dk_fantasy_points_allowed,
  drop column away_dk_fantasy_points_allowed_last_games,
  drop column home_dk_fantasy_points_allowed_last_games;
