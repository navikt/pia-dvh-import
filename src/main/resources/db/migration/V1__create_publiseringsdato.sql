create table publiseringsdato
(
    id          serial primary key,
    arstall     smallint  not null,
    kvartal     smallint  not null,
    dato        date      not null,
    prosessert  boolean   not null default false,
    opprettet   timestamp not null default current_timestamp,
    constraint publiseringsdato_arstall_kvartal unique (arstall, kvartal)
);
