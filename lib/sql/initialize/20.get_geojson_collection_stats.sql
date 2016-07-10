CREATE OR REPLACE FUNCTION get_geojson_collection_stats(t varchar(255), lang varchar(255), options jsonb) RETURNS jsonb AS $$
    // { field : 'properties.address.city', name : 'city' }
    // { field : 'properties.address.building', name : 'building', type : 'string' }
    // { field : 'properties.info.tags', name : 'tag', type : 'string', array: true }
    lang = lang || 'default';
    var viewName = 'collection_' + t + '_' + lang;
    
    var collectionName = viewName + '_json';
    var get_geojson_stats = plv8.find_function('get_geojson_stats');
    
    return get_geojson_stats(collectionName, options);
$$ LANGUAGE plv8 STRICT IMMUTABLE
