CREATE OR REPLACE FUNCTION drop_geojson(t varchar(255)) RETURNS boolean AS $$
    plv8.execute("DROP VIEW IF EXISTS " + t + " CASCADE");
    plv8.execute("DROP TABLE IF EXISTS " + t + "_json CASCADE");
    plv8.execute("DROP TABLE IF EXISTS " + t + "_geometry CASCADE");
    return 1;
$$ LANGUAGE plv8 STRICT IMMUTABLE