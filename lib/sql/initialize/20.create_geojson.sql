CREATE OR REPLACE FUNCTION create_geojson(t varchar(255), lang varchar(255)) RETURNS boolean AS $$
    lang = lang || 'default';
    var viewName = 'collection_' + t + '_' + lang;
    var collectionName = viewName + '_json';
    var geometryName = viewName + '_geometry';

    plv8.execute("SELECT create_json_table('" + collectionName + "')");
    plv8.execute("SELECT create_geometry_table('" + geometryName + "')");
    plv8.execute("" + 
        "CREATE OR REPLACE VIEW " + viewName + " AS " + 
        "SELECT J.id, J.type, J.properties, G.geometry " + 
        "FROM " + collectionName + " AS J, " + geometryName + " AS G " +
        "WHERE J.id = G.id"
    );
    return 1;
$$ LANGUAGE plv8 STRICT IMMUTABLE