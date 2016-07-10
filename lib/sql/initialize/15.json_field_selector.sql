CREATE OR REPLACE FUNCTION json_field_selector(info jsonb) RETURNS jsonb AS $$
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
    var p = '(val)';
    if (info.type === 'boolean') {
        p = 'to_boolean((val)::jsonb)';
    } else if (info.type === 'string'){
        p = "to_string((val)::jsonb, ',')";
    }
    return {
        field : f,
        select : p
    };
$$ LANGUAGE plv8 STRICT IMMUTABLE