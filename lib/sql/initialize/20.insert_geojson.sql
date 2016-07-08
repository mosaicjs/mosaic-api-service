CREATE OR REPLACE FUNCTION insert_geojson(t varchar(255), lang varchar(255), list jsonb) RETURNS boolean AS $$
    lang = lang || 'default';
    var viewName = 'collection_' + t + '_' + lang;
    var collectionName = viewName + '_json';
    var geometryName = viewName + '_geometry';
    
    if (!Array.isArray(list)) {
        list = !!list ? [list] : [];
    }
    var jsonPlan = plv8.prepare(
        'INSERT INTO ' + collectionName + '(id, properties) VALUES ($1, to_json($2))',
        ['uuid', 'jsonb']
    );
    try {
        var geometryPlan = plv8.prepare(
            'INSERT INTO ' + geometryName + '(id, geometry) ' + 
               'VALUES ($1, ST_SetSRID(ST_GeomFromGeoJSON($2),4326))',
            ['uuid', 'text']
        );
        try {
             list.forEach(function(obj){
                var uuid = obj.id || '';
                var properties = obj.properties || {};
                if (!uuid.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/)) {
	                if (properties.id) {
	                   uuid = plv8.execute('SELECT uuid5(' + plv8.quote_literal(properties.id) + ') AS id')[0].id;
	                } else {
	                   uuid = plv8.execute('SELECT gen_random_uuid() AS id')[0].id;
	                }
                }
                jsonPlan.execute([uuid, properties]);
                if (obj.geometry){
                    geometryPlan.execute([uuid, JSON.stringify(obj.geometry)]);
                }
             });
        } finally {
            geometryPlan.free();
        }
    } finally {
        jsonPlan.free();
    }
    return true;
$$ LANGUAGE plv8 STRICT IMMUTABLE