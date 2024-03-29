SET SCHEMA 'public';

DO
$$
    BEGIN
        CREATE DOMAIN MIGRATION_TYPE AS TEXT;
    EXCEPTION
        WHEN duplicate_object THEN NULL;
    END
$$;

ALTER DOMAIN MIGRATION_TYPE DROP CONSTRAINT IF EXISTS migration_type_check;
ALTER DOMAIN MIGRATION_TYPE ADD CONSTRAINT migration_type_check CHECK (value = ANY (ARRAY [
    'up',
    'down']));


CREATE TABLE IF NOT EXISTS public.migration
(
    id             serial PRIMARY KEY NOT NULL,
    applied_at     timestamp          NOT NULL DEFAULT now(),
    migration_type MIGRATION_TYPE     NOT NULL,
    file_name      text               NOT NULL,
    file_ts        TIMESTAMP          NOT NULL,
    file_id        varchar(10)        NOT NULL
);

ALTER TABLE public.migration
    ADD COLUMN IF NOT EXISTS meta JSONB;
