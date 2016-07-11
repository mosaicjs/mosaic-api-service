CREATE OR REPLACE FUNCTION insert_geojson(t varchar(255), lang varchar(255), list jsonb) RETURNS boolean AS $$
    lang = lang || 'default';
    var viewName = 'collection_' + t + '_' + lang;
    var collectionName = viewName + '_json';
    var geometryName = viewName + '_geometry';
    
    if (!Array.isArray(list)) {
        list = !!list ? [list] : [];
    }
    var jsonPlan = plv8.prepare(
        'INSERT INTO ' + collectionName + '(id, properties) VALUES ($1, to_json($2)) ' +
        'ON CONFLICT(id) DO UPDATE SET (properties) = (to_json($2))',
        ['uuid', 'jsonb']
    );
    try {
        var geometryPlan = plv8.prepare(
            'INSERT INTO ' + geometryName + '(id, geometry) ' + 
               'VALUES ($1, ST_SetSRID(ST_GeomFromGeoJSON($2),4326)) ' +
               'ON CONFLICT(id) DO UPDATE SET (geometry) = (ST_SetSRID(ST_GeomFromGeoJSON($2),4326))',
            ['uuid', 'text']
        );
        try {
             list.forEach(function(obj){
                var id = generateId(obj);
                obj.id = id;
                var properties = obj.properties || {};
                jsonPlan.execute([id, properties]);
                if (obj.geometry){
                    geometryPlan.execute([id, JSON.stringify(obj.geometry)]);
                }
             });
        } finally {
            geometryPlan.free();
        }
    } finally {
        jsonPlan.free();
    }
    return true;

	function generateId(obj) {
	    var regexp = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/;
	    var properties = obj.properties || {};
	    var id = obj.id || properties.id || '';
	    if (!id.match(regexp)) {
	        if (id) {
	            id = generateUUID5(id);
	        } else {
	            id = generateUUID4();
	        }
	    }
	    return id;
	}

	function generateUUID4() {
       return plv8.execute('SELECT gen_random_uuid() AS id')[0].id;
	}
	
	function generateUUID5(str) {
	    return plv8.execute('SELECT uuid5(' + plv8.quote_literal(str) + ') AS id')[0].id;
	}

$$ LANGUAGE plv8 STRICT IMMUTABLE