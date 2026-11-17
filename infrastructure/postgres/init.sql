\set ON_ERROR_STOP on
SELECT format('CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD %L', :'debezium_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium')
\gexec
ALTER ROLE debezium WITH LOGIN REPLICATION PASSWORD :'debezium_password';

SELECT format('CREATE ROLE vehicle_writer WITH LOGIN PASSWORD %L', :'writer_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vehicle_writer')
\gexec
ALTER ROLE vehicle_writer WITH LOGIN PASSWORD :'writer_password';

CREATE TABLE IF NOT EXISTS public.vehicles (
    id uuid PRIMARY KEY,
    state text NOT NULL CHECK (
        state IN ('REGISTERED', 'ACTIVE', 'IDLE', 'MAINTENANCE', 'DECOMMISSIONED')
    ),
    state_version bigint NOT NULL CHECK (state_version > 0),
    state_updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS public.positions (
    id uuid PRIMARY KEY,
    vehicle_id uuid NOT NULL,
    lat double precision NOT NULL CHECK (lat BETWEEN -90 AND 90),
    lng double precision NOT NULL CHECK (lng BETWEEN -180 AND 180),
    recorded_at timestamptz NOT NULL
);

ALTER TABLE public.vehicles REPLICA IDENTITY FULL;
ALTER TABLE public.positions REPLICA IDENTITY FULL;

GRANT CONNECT ON DATABASE vehicle_cache TO debezium, vehicle_writer;
GRANT USAGE ON SCHEMA public TO debezium, vehicle_writer;
GRANT SELECT ON public.vehicles, public.positions TO debezium;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.vehicles TO vehicle_writer;
GRANT SELECT, INSERT ON public.positions TO vehicle_writer;

SELECT 'CREATE PUBLICATION vehicle_cache_publication'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_publication WHERE pubname = 'vehicle_cache_publication'
)
\gexec
ALTER PUBLICATION vehicle_cache_publication
    SET TABLE public.vehicles, public.positions;

\if :seed_canary
INSERT INTO public.vehicles (id, state, state_version, state_updated_at)
VALUES (
    '00000000-0000-4000-8000-000000000001',
    'REGISTERED',
    1,
    TIMESTAMPTZ '2026-01-01 00:00:00+00'
)
ON CONFLICT (id) DO NOTHING;
\endif
