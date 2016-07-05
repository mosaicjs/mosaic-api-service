CREATE OR REPLACE FUNCTION convert_to_tsvector(obj jsonb, lang text, fields jsonb)
RETURNS tsvector AS $$
    var properties = obj || {};
    if (!fields){
        fields = {};
        for (var field in properties) {
            fields[field] = 1;
        }
    }
    var values = [];
    for (var field in fields){
        selectJSON(obj, field, values);
    }
    var result = expandValues(values);   
    var str = result.join(' ').replace(/[\"\']/gim, ' ');
    str = "'" + str + "'";
    var vectors = plv8.execute("SELECT to_tsvector('" + lang + "'::regconfig," + str + ") as vec" );
    return vectors.map(function(v){ 
        return v.vec;
    })

    function expandValues(values, results) {
        results = results || [];
        if (values === null || values === undefined)
	        return results;
	    if (!Array.isArray(values)) {
	        values = [ values || '' ];
	    }
	    values.forEach(function(val) {
	        if (val === undefined || val === null)
	            return;
	        if (typeof val === 'object') {
	            for ( var field in val) {
	                expandValues(val[field], results);
	            }
	        } else {
	            results.push(val);
	        }
	    });
	    return results;
	}
    function selectJSON(obj, path, results) {
        if (!Array.isArray(path)) {
            path = path.split('.');
        }
        return doSelect(obj, path, 0, results);
        function doSelect(obj, path, pos, results) {
            if (obj === undefined)
                return results;
            if (pos === path.length) {
                results.push(obj);
                return results;
            }
            var field = path[pos];
            var value = obj[field];
            if (Array.isArray(value)) {
                value.forEach(function(cell) {
                    doSelect(cell, path, pos + 1, results);
                });
            } else {
                doSelect(value, path, pos + 1, results);
            }
            return results;
        }
    }
$$ LANGUAGE plv8 STRICT IMMUTABLE