CREATE OR REPLACE FUNCTION get_geojson_stats(t varchar(255), lang varchar(255), options jsonb) RETURNS jsonb AS $$
    // { field : 'properties.address.city', name : 'city' }
    // { field : 'properties.address.building', name : 'building', type : 'string' }
    // { field : 'properties.info.tags', name : 'tag', type : 'string', array: true }

    lang = lang || 'default';
    var viewName = 'collection_' + t + '_' + lang;
    var collectionName = viewName + '_json';

    var fields = options.fields || [];
    var maxSize = options.maxSize || 1000;
    var queries = [];
    Object.keys(fields).forEach(function(info, i){
        var array = info.field.split('.');
        var f = array[0];
        if (array.length > 1) {
            f = f + '->' + array.slice(1).map(function(n) {
                return "'" + n + "'";
            }).join('->');
        }
        f = '(' + f + ')';
        if (info.array) {
            f = 'jsonb_array_elements' + f;
        }
        f += ' AS val ';
        
        var p = '(val)';
        if (info.type === 'boolean') {
            p = 'to_boolean((val)::jsonb)';
        } else if (info.type === 'string'){
            p = "to_string((val)::jsonb)";
        }
        var query = 'SELECT ' + p + ' AS value, count(val) AS count FROM ' + 
            '(SELECT ' + f + ' FROM ' + collectionName + ') AS T '+
            'GROUP BY val ORDER BY count DESC, value ' +
            'LIMIT ' + maxSize;
        queries.push(query);
    });

    var stats = {};
    queries.forEach(function(query, i){
        var info = fields[i];
        var results = plv8.execute(query);
        results.forEach(function(res){
            stats[info.name] = {
                value : res.value,
                count : res.count
            };
        })
    });
    return stats;

$$ LANGUAGE plv8 STRICT IMMUTABLE
