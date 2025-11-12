create schema if not exists s_sql_dds;

create table if not exists s_sql_dds.t_sql_source_unstructured (
    id text,
    name text,
    age integer,
    category text,
    value numeric(12,2),
    country text,
    city text,
    signup_date date,
    email text
);
