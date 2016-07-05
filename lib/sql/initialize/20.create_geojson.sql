CREATE OR REPLACE FUNCTION create_geojson(t varchar(255)) RETURNS boolean AS $$
    plv8.execute("SELECT create_json_table('" + t + "_json')");
    plv8.execute("SELECT create_geometry_table('" + t + "_geometry')");
    plv8.execute("" + 
        "CREATE OR REPLACE VIEW " + t + " AS " + 
        "SELECT J.id, J.type, J.properties, G.geometry " + 
        "FROM " + t + "_json AS J, " + t + "_geometry AS G " +
        "WHERE J.id = G.id"
    );
    return 1;
$$ LANGUAGE plv8 STRICT IMMUTABLE