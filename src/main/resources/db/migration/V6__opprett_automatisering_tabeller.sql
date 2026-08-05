create table automatisering_import_lock
(
    id                  serial primary key,
    publiseringsdato_id integer   not null references publiseringsdato (id),
    status              varchar   not null,
    start_dato          timestamp not null default current_timestamp,
    slutt_dato          timestamp,
    constraint automatisering_import_lock_publiseringsdato_id_unique unique (publiseringsdato_id)
);

create table automatisering_import_steg
(
    id                     serial primary key,
    publiseringsdato_id    integer   not null references publiseringsdato (id),
    navn                   varchar   not null,
    rekkefolge             smallint  not null,
    status                 varchar   not null,
    kontroll               varchar,
    start_dato             timestamp,
    slutt_dato             timestamp,
    antall_rader_lest      integer   not null default 0,
    antall_sendt_paa_kafka integer   not null default 0,
    sf_prosent             numeric,
    constraint automatisering_import_steg_publiseringsdato_id_navn_unique unique (publiseringsdato_id, navn)
);
