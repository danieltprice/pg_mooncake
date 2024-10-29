CREATE FUNCTION columnstore_handler(internal) RETURNS table_am_handler
    AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE ACCESS METHOD columnstore TYPE TABLE HANDLER columnstore_handler;

CREATE SCHEMA mooncake;

CREATE TABLE mooncake.tables (
    oid OID NOT NULL,
    path TEXT NOT NULL
);
CREATE UNIQUE INDEX tables_oid ON mooncake.tables (oid);

CREATE TABLE mooncake.data_files (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    oid OID NOT NULL,
    file_name TEXT NOT NULL
);
CREATE INDEX data_files_oid ON mooncake.data_files (oid);
CREATE UNIQUE INDEX data_files_file_name ON mooncake.data_files (file_name);

CREATE TABLE mooncake.secrets (
    secret_name NAME NOT NULL PRIMARY KEY,
    secret_type NAME NOT NULL,
    delta_storage_property TEXT NOT NULL,
    duckdb_secret_string TEXT NOT NULL,
    scope TEXT NOT NULL
);

CREATE INDEX secrets_key ON mooncake.secrets (secret_name);

CREATE SEQUENCE mooncake.secrets_table_seq START WITH 1 INCREMENT BY 1;
SELECT setval('mooncake.secrets_table_seq', 1);

CREATE OR REPLACE FUNCTION mooncake.create_secret(
    name TEXT,
    type TEXT,
    key_id TEXT,
    secret TEXT,
    extra_params JSONB DEFAULT '{}'::JSONB
)
RETURNS VOID
LANGUAGE plpgsql
AS $create_secret$
DECLARE
    allowed_keys TEXT[] := ARRAY['USE_SSL', 'REGION', 'END_POINT', 'SCOPE'];
    param_keys TEXT[];
    invalid_keys TEXT[];
BEGIN
    IF type = 'S3' THEN
        param_keys := ARRAY(SELECT jsonb_object_keys(extra_params));

        invalid_keys := ARRAY(SELECT unnest(param_keys) EXCEPT SELECT unnest(allowed_keys));

        -- If there are any invalid keys, raise an exception
        IF array_length(invalid_keys, 1) IS NOT NULL THEN
            RAISE EXCEPTION 'Invalid extra parameters: %', array_to_string(invalid_keys, ', ')
            USING HINT = 'Allowed parameters are USE_SSL, REGION, END_POINT, SCOPE.';
        END IF;

        INSERT INTO mooncake.secrets(secret_name, secret_type, delta_storage_property, duckdb_secret_string, scope)
        VALUES (
            name,
            type,
            jsonb_build_object(
                'AWS_ACCESS_KEY_ID', key_id,
                'AWS_SECRET_ACCESS_KEY', secret
            ) ||
            jsonb_strip_nulls(jsonb_build_object(
                'ALLOW_HTTP', NOT((extra_params->>'USE_SSL')::boolean),
                'AWS_REGION', extra_params->>'REGION',
                'AWS_ENDPOINT', extra_params->>'end_point'
            )),
            FORMAT('CREATE SECRET pgduckb_secret_%s (TYPE %s, KEY_ID %L, SECRET %L',
                name, type, key_id, secret) ||
            CASE WHEN extra_params->>'REGION' IS NULL THEN '' ELSE FORMAT(', REGION %L', extra_params->>'REGION') END ||
            CASE WHEN extra_params->>'END_POINT' IS NULL THEN '' ELSE FORMAT(', ENDPOINT %L', extra_params->>'END_POINT') END ||
            CASE WHEN (extra_params->>'USE_SSL')::boolean = false THEN ', USE_SSL FALSE' ELSE '' END ||
            CASE WHEN extra_params->>'SCOPE' IS NULL THEN '' ELSE FORMAT(', SCOPE %L', extra_params->>'SCOPE') END ||
            ');',
            COALESCE(extra_params->>'SCOPE', '')
        );
        PERFORM nextval('mooncake.secrets_table_seq');
    ELSE
        RAISE EXCEPTION 'Unsupported secret type: %', type
        USING HINT = 'Only secrets of type S3 are supported.';
    END IF;
END;
$create_secret$ SECURITY DEFINER;

CREATE OR REPLACE FUNCTION mooncake.drop_secret(
    name TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $drop_secret$
BEGIN
    DELETE from mooncake.secrets where secret_name=name;
END;
$drop_secret$ SECURITY DEFINER;

CREATE VIEW mooncake.columnstore_tables as
    select relname as table_name, path
    from pg_class p join mooncake.tables m
    on p.oid = m.oid;

CREATE VIEW mooncake.cloud_secrets as
    select secret_name, secret_type, scope from mooncake.secrets;

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA mooncake FROM PUBLIC;
GRANT USAGE ON SCHEMA mooncake TO PUBLIC;
GRANT SELECT ON mooncake.columnstore_tables TO PUBLIC;
GRANT SELECT ON mooncake.cloud_secrets TO PUBLIC;
