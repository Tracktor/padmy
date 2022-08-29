SET SCHEMA 'public';

DO
$$
    BEGIN
        CREATE TYPE MIGRATION_TYPE AS ENUM (
            'up',
            'down'
            );
    EXCEPTION
        WHEN duplicate_object THEN null;
    END
$$;

CREATE TABLE IF NOT EXISTS public.migration
(
    id             serial PRIMARY KEY NOT NULL,
    applied_at     timestamp          NOT NULL DEFAULT now(),
    migration_type MIGRATION_TYPE     NOT NULL,
    file_name      text               NOT NULL,
    file_ts        TIMESTAMP          NOT NULL,
    file_id        varchar(10)        NOT NULL
);
