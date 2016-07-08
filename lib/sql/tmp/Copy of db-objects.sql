/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */

CREATE OR REPLACE FUNCTION insert_into_geojson_table(t text, obj jsonb) RETURNS jsonb AS $$
    var plan = plv8.prepare(
        'INSERT INTO ' + t + '(properties, geometry) ' + 
        'VALUES (to_json($1), ST_SetSRID(ST_GeomFromGeoJSON($2),4326))',
        ['jsonb', 'text']
    );
    try {
        plan.execute([obj.properties, JSON.stringify(obj.geometry)])
    } finally {
        plan.free();
    }
    return 1;
$$ LANGUAGE plv8 STRICT IMMUTABLE;


/* -------------------------------------------------------------------------- */

drop table if exists objects cascade;
select create_geojson_table('objects');

create or replace view objects_webmercator as
select
    objects.id,
    objects.type,
    objects.properties,
    objects.geometry,
    st_transform(objects.geometry, 3857) as the_geom_webmercator
from objects;
create index on objects(type);

-- -------------------------------------------------------------------------------

/* * /
DROP FUNCTION IF EXISTS insert_into_geojson_table(t text, obj jsonb);
CREATE OR REPLACE FUNCTION insert_into_geojson_table(t text, obj jsonb) RETURNS jsonb AS $$
    var plan = plv8.prepare(
        'INSERT INTO ' + t + '(properties, geometry) ' + 
        'VALUES (to_json($1), ST_SetSRID(ST_GeomFromGeoJSON($2),4326))',
        ['jsonb', 'text']
    );
    try {
        plan.execute([obj.properties, JSON.stringify(obj.geometry)])
    } finally {
        plan.free();
    }
    return 1;
$$ LANGUAGE plv8 STRICT IMMUTABLE;
-- */

-- -- example of usage:

-- -------------------------------------------------------------------------------
/* */
DROP FUNCTION IF EXISTS get_table_columns(t varchar(255));
CREATE OR REPLACE FUNCTION get_table_columns(t varchar(255))
RETURNS jsonb LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT to_jsonb(array_agg(column_name::text)) FROM information_schema.columns WHERE table_name=t;
$$;

/* Copies content of a table to a JSON table. Table columns are transformed to field names. */
DROP FUNCTION IF EXISTS convert_table(from_table varchar(255), excluded varchar(255), geometry varchar(255), id varchar(255), to_table varchar(255));
CREATE OR REPLACE FUNCTION convert_table(from_table varchar(255), excluded varchar(255), geometry varchar(255), id varchar(255), to_table varchar(255))
RETURNS jsonb AS $$
    var query = "SELECT get_table_columns('" + from_table + "') AS columns";
    var columnsInfo = plv8.execute(query)[0];
    var select = [];
    var excludedIndex = {};
    excludedIndex[geometry.toLowerCase()] = true;
    excluded.split(/[\s;,]+/).forEach(function(field){
        field = field.toLowerCase();
        excludedIndex[field] = true;
    });
    columnsInfo.columns.forEach(function(name, i){
        var n = name.toLowerCase();
        if (excludedIndex[n]) return ;
        name = JSON.stringify(name);
        select.push(name + ' AS '  + name);
    });
    var sql = 'INSERT INTO ' + to_table + '(properties, type, geometry)  ' + 
    'SELECT row_to_json(N.*) AS properties, \'Feature\' AS type, ST_Transform(M.' + geometry + ', 4326) AS geometry ' + 
    'FROM (SELECT ' + select.join(',') + ' FROM ' +  from_table + ') AS N, ' + from_table + ' AS M ' +
    'WHERE N.' + id + ' = M.' + id;
    return plv8.execute(sql);
$$ LANGUAGE plv8 STRICT IMMUTABLE;
-- */
-- example of usage:
-- select convert_table('planet_osm_polygon', '', 'way', 'osm_id', 'objects');
-- select convert_table('planet_osm_line', '', 'way', 'osm_id', 'objects');
-- select convert_table('planet_osm_point', '', 'way', 'osm_id', 'objects');

/* -------------------------------------------------------------------------- */


drop function if exists jsonb_object_set_key(
    "json" jsonb,
    "key_to_set"    TEXT,
    "value_to_set"  anyelement);
CREATE OR REPLACE FUNCTION "jsonb_object_set_key"(
  "json"          jsonb,
  "key_to_set"    TEXT,
  "value_to_set"  anyelement
)
RETURNS jsonb LANGUAGE sql IMMUTABLE STRICT
AS $function$
SELECT COALESCE(
  (SELECT ('{' || string_agg(to_jsonb("key") || ':' || "value", ',') || '}')
     FROM (SELECT *
             FROM jsonb_each("json")
            WHERE "key" <> "key_to_set"
            UNION ALL
           SELECT "key_to_set", to_jsonb("value_to_set")) AS "fields"),
  '{}'
)::jsonb
$function$;


/* -------------------------------------------------------------------------- */

drop function if exists json_object_set_keys(
    "json"          jsonb,
    "keys_to_set"   TEXT[],
    "values_to_set" anyarray);
CREATE OR REPLACE FUNCTION "jsonb_object_set_keys"(
    "json"          jsonb,
    "keys_to_set"   TEXT[],
    "values_to_set" anyarray
)
  RETURNS jsonb
  LANGUAGE sql
  IMMUTABLE
  STRICT
AS $function$
SELECT COALESCE(
  (SELECT ('{' || string_agg(to_jsonb("key") || ':' || "value", ',') || '}')
     FROM (SELECT *
             FROM jsonb_each("json")
            WHERE "key" <> ALL ("keys_to_set")
            UNION ALL
           SELECT DISTINCT ON ("keys_to_set"["index"])
                  "keys_to_set"["index"],
                  CASE
                    WHEN "values_to_set"["index"] IS NULL THEN 'null'
                    ELSE to_jsonb("values_to_set"["index"])
                  END
             FROM generate_subscripts("keys_to_set", 1) AS "keys"("index")
             JOIN generate_subscripts("values_to_set", 1) AS "values"("index")
            USING ("index")) AS "fields"),
  '{}'
)::jsonb
$function$;

/* -------------------------------------------------------------------------- */

drop function if exists jsonb_object_remove_key(
    "json"          jsonb,
    "key_to_remove"    TEXT);
CREATE OR REPLACE FUNCTION "jsonb_object_remove_key"(
  "json"          jsonb,
  "key_to_remove"    TEXT
)
  RETURNS jsonb
  LANGUAGE sql
  IMMUTABLE
  STRICT
AS $function$
SELECT COALESCE(
  (SELECT ('{' || string_agg(to_jsonb("key") || ':' || "value", ',') || '}')
     FROM (SELECT *
             FROM jsonb_each("json")
            WHERE "key" <> "key_to_remove"
    ) AS "fields"),
  '{}'
)::jsonb
$function$;


/* -------------------------------------------------------------------------- */

drop function if exists jsonb_object_rename_key(
  "json"       jsonb,
  "old_key"    TEXT,
  "new_key"    TEXT);
CREATE OR REPLACE FUNCTION "jsonb_object_rename_key"(
  "json"       jsonb,
  "old_key"    TEXT,
  "new_key"    TEXT
)
  RETURNS jsonb
  LANGUAGE sql
  IMMUTABLE
  STRICT
AS $function$
SELECT COALESCE(
  (SELECT ('{' || string_agg(to_jsonb("key") || ':' || "value", ',') || '}')
     FROM (SELECT * FROM jsonb_each("json") WHERE "key" <> "old_key"
            UNION ALL
            SELECT "new_key", "value" FROM jsonb_each("json")
            WHERE "key" = "old_key") AS "fields"),
  '{}'
)::jsonb
$function$;

-- -----------------------------------------------------------------------------
