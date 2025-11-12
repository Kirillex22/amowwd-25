create table if not exists s_sql_dds.t_sql_source_structured (
    id text primary key,
    name text not null,
    age integer check (age between 0 and 120),
    category text,
    value numeric(12,2),
    country text,
    city text,
    signup_date date not null,
    email text
);
