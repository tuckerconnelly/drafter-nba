-- rambler up

create materialized view training_data
as
  select
    gp.game_basketball_reference_id,
    p.name as player_name,
    t.name as team_name,

    gp.player_basketball_reference_id, -- enum
    p.birth_country, -- enum
    floor(date_part('days', g.time_of_game - p.date_of_birth) / 365) as age_at_time_of_game,
    g.arena, -- enum
    date_part('year', g.time_of_game) as year_of_game,
    date_part('month', g.time_of_game) as month_of_game,
    date_part('day', g.time_of_game) as day_of_game,
    date_part('hour', g.time_of_game) as hour_of_game,
    g.home_team_basketball_reference_id, -- enum
    g.away_team_basketball_reference_id, -- enum
    tp.team_basketball_reference_id as player_team_basketball_reference_id, -- enum
    tp.experience,
    tp.height_inches,
    tp.weight_lbs,
    tp.position, -- enum
    (g.home_team_basketball_reference_id = t.basketball_reference_id) as playing_at_home,

    gp.points,
    gp.three_point_field_goals,
    gp.total_rebounds,
    gp.assists,
    gp.steals,
    gp.blocks,
    gp.turnovers,
    gp.free_throws
  from games_players gp
  inner join players p
    on p.basketball_reference_id = gp.player_basketball_reference_id
  inner join games g
    on g.basketball_reference_id = gp.game_basketball_reference_id
  inner join teams_players tp
    on tp.player_basketball_reference_id = gp.player_basketball_reference_id
    and tp.season = g.season
  inner join teams t
    on t.basketball_reference_id = tp.team_basketball_reference_id
  order by random()
with data;

-- rambler down

drop materialized view training_data;
