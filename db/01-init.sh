#!/bin/bash
set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER $APP_DB_USER WITH PASSWORD '$APP_DB_PASS';
  CREATE DATABASE $APP_DB_NAME;
  GRANT ALL PRIVILEGES ON DATABASE $APP_DB_NAME TO $APP_DB_USER;
  \connect $APP_DB_NAME $APP_DB_USER
  BEGIN;
    CREATE TABLE public.clean_historic_values_all_states (
	"index" int8 NULL,
	"date" timestamp NULL,
	state text NULL,
	positive float8 NULL,
	"deathConfirmed" float8 NULL
);
CREATE INDEX ix_clean_historic_values_all_states_index ON public.clean_historic_values_all_states USING btree (index);
ALTER TABLE public.clean_historic_values_all_states OWNER TO docker;
GRANT ALL ON TABLE public.clean_historic_values_all_states TO docker;
CREATE or replace VIEW historic_values_resume as
    SELECT state, 
    MIN("date") as primera_fecha, 
    MAX("date") as ultima_fecha, 
    count("date") as num_dias, 
    sum(positive)::numeric(12) as positivos, 
    sum("deathConfirmed")::numeric(12) as muertes_confirmadas, 
    (sum(positive) / count("date"))::numeric(12,2) AS promedio_positivos_dia,
    (sum("deathConfirmed") / count("date"))::numeric(12,2) AS promedio_muertes_confirmadas_dia
    FROM public.clean_historic_values_all_states
    GROUP BY state;
COMMIT;
create or replace procedure nombre_proc(
   state_to_delete_dates text,
   date_to_delete timestamp
)
language plpgsql    
as \$$
begin
    DELETE FROM public.clean_historic_values_all_states
    WHERE state = state_to_delete_dates
    and "date"= date_to_delete;

    commit;
end;\$$
EOSQL