-- +goose Up
create table task_definitions
(
    id uuid primary key default gen_random_uuid(),
    created_at  int not null,
    updated_at  int not null,
    metadata jsonb,
    expire_after int64,
    expire_after_interval interval,
    in_progress bool,
    last_fire_time timestamptz,
    next_fire_time timestamptz,
    completed_at timestamptz,
    recurring bool,
    INVERTED INDEX metadata_idx (metadata)
);

create table task_instances
(
    id uuid primary key default gen_random_uuid(),
    created_at  int not null,
    updated_at  int not null,
    expires_at timestamptz,
    execute_at timestamptz,
    started_at timestamptz,
    completed_at timestamptz,
    task_definition_id uuid not null references task_definitions (id) on delete cascade
);

create table execute_once_triggers
(
    id uuid primary key default gen_random_uuid(),
    created_at  bigint not null,
    updated_at  bigint not null,
    task_definition_id uuid not null references task_definitions (id) on delete cascade,
    fire_at timestamptz not null
);

create table cron_triggers
(
    id uuid primary key default gen_random_uuid(),
    created_at  bigint not null,
    updated_at  bigint not null,
    task_definition_id uuid not null references task_definitions (id) on delete cascade,
    expression string not null
);

-- +goose Down
drop table task_definitions;
drop table task_instances;
drop table execute_once_triggers;
drop table cron_triggers;