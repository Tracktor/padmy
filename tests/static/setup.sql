CREATE EXTENSION IF NOT EXISTS tsm_system_rows;


CREATE TABLE IF NOT EXISTS public.table_1
(
    id  SERIAL PRIMARY KEY,
    foo TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.table_2
(
    id         SERIAL PRIMARY KEY,
    table_1_id int references public.table_1 ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.table_3
(
    id         SERIAL PRIMARY KEY,
    table_1_id int references public.table_1 ON DELETE CASCADE,
    table_2_id int references public.table_2 ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.table_4
(
    id SERIAL PRIMARY KEY
);

-- Single Circular

CREATE TABLE public.single_circular
(
    id        SERIAL PRIMARY KEY,
    parent_id INT REFERENCES public.single_circular
);

-- Multiple circular

CREATE TABLE public.multiple_circular
(
    id        SERIAL PRIMARY KEY,
    parent_id INT REFERENCES public.multiple_circular
);


CREATE TABLE public.multiple_circular_2
(
    id                   SERIAL PRIMARY KEY,
    multiple_circular_id INT REFERENCES public.multiple_circular
);
