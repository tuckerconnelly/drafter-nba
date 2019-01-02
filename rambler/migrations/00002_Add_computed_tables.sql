-- rambler up

create table games_players_computed (
  id serial not null primary key,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now(),

  game_basketball_reference_id text references games(basketball_reference_id) on delete cascade,
  player_basketball_reference_id text references players(basketball_reference_id) on delete cascade,
  unique(game_basketball_reference_id, player_basketball_reference_id),

  dk_fantasy_points real,
  dk_fantasy_points_last_games jsonb default '[]',
  seconds_played_last_games jsonb default '[]'
);

create trigger update_updated_at before update
  on games_players_computed for each row execute procedure update_updated_at();

create table games_computed (
  id serial not null primary key,
  created_at timestamp not null default now(),
  updated_at timestamp not null default now(),

  game_basketball_reference_id text references games(basketball_reference_id) on delete cascade,
  away_starters jsonb default '[]',
  home_starters jsonb default '[]'
);

create trigger update_updated_at before update
  on games_computed for each row execute procedure update_updated_at();

-- rambler down

drop table games_computed;
drop table games_players_computed;
