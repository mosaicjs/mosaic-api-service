CREATE OR REPLACE FUNCTION select_json(obj jsonb, path text)
RETURNS jsonb AS $$
    var results = [];
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
$$ LANGUAGE plv8 STRICT IMMUTABLE