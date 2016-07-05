CREATE OR REPLACE FUNCTION insert_geojson(t varchar(255), list jsonb) RETURNS boolean AS $$
    if (!Array.isArray(list)) {
    list = !!list ? [list] : [];
    }
    var jsonPlan = plv8.prepare(
        'INSERT INTO ' + t + '_json(id, properties) VALUES ($1, to_json($2))',
        ['uuid', 'jsonb']
    );
    try {
        var geometryPlan = plv8.prepare(
            'INSERT INTO ' + t + '_geometry(id, geometry) ' + 
               'VALUES ($1, ST_SetSRID(ST_GeomFromGeoJSON($2),4326))',
            ['uuid', 'text']
        );
        try {
             list.forEach(function(obj){
                var uuid = plv8.execute('SELECT gen_random_uuid() AS id')[0].id;
                jsonPlan.execute([uuid, obj.properties || {}]);
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