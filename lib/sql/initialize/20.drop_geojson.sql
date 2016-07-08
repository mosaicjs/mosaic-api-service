CREATE OR REPLACE FUNCTION drop_geojson(t varchar(255), lang varchar(255)) RETURNS boolean AS $$
    lang = lang || 'default';
    var viewName = 'collection_' + t + '_' + lang;
    var collectionName = viewName + '_json';
    var geometryName = viewName + '_geometry';
    plv8.execute("DROP VIEW IF EXISTS " + viewName + " CASCADE");
    plv8.execute("DROP TABLE IF EXISTS " + collectionName + " CASCADE");

    // This geometry table can be used by other locales.
    // Drop only if the default table is removed
    if (lang === 'default') {
	    plv8.execute("DROP TABLE IF EXISTS " + geometryName + " CASCADE");
	}
    return 1;
$$ LANGUAGE plv8 STRICT IMMUTABLE