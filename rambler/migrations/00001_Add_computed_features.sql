-- rambler up

create table computed_features (
    id integer primary key autoincrement,
    created_at datetime default current_timestamp not null,
    updated_at datetime default current_timestamp not null,
    game_basketball_reference_id text not null,
    player_basketball_reference_id text not null,
    season int not null,

    x text not null default '[]',
    y text not null default '[]',
    sw text not null default '1',

    unique(game_basketball_reference_id, player_basketball_reference_id)
);

-- rambler down

drop table computed_features;
