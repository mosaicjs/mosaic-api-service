CREATE OR REPLACE FUNCTION get_geojson_stats(tbl text, fields jsonb) RETURNS jsonb AS $$
    // { "fields" : { "city": "properties.address.city" } }
    // { "fields" : { "building": { "field" : "properties.address.building", "type" : "string" } } }
    // { "fields" : { "tags": { "field" : "properties.info.tags", "type" : "string", "array" : true } } }

    var json_field_selector = plv8.find_function('json_field_selector');
    
    var maxSize = fields.$maxSize || 1000;
    delete fields.$maxSize;
    var queries = [];
    var fieldsInfo = [];
    Object.keys(fields).forEach(function(name, i){
        var info = fields[name];
        if (typeof info === 'string') {
            info = {
                field : info
            };
        }
        info.name = info.name || name;
        fieldsInfo.push(info);

        var x = json_field_selector(info);
        var f = x.field + ' AS val ';
        var p = x.select;

        var sql = 'SELECT ' + p + ' AS value, count(val) AS count FROM ' + 
            '(SELECT ' + f + ' FROM ' + tbl + ') AS T '+
            'GROUP BY val ORDER BY count DESC, value ' +
            'LIMIT ' + maxSize;
        // plv8.elog(NOTICE, sql);
        queries.push(sql);
    });

    var stats = {};
    queries.forEach(function(query, i){
        var info = fieldsInfo[i];
        var s = stats[info.name] = {};
        var results = plv8.execute(query);
        results.forEach(function(res){
            s[res.value] = res.count;
        })
    });
    return stats;

$$ LANGUAGE plv8 STRICT IMMUTABLE
