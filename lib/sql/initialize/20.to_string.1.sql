CREATE OR REPLACE FUNCTION to_string(o jsonb) RETURNS text AS $$
    if (Array.isArray(o)) {
        if (!o.length)
            return '';
        return o[0] + '';
    } else if (o) {
        return o + '';
    } else {
        return '';
    }
$$ LANGUAGE plv8 STRICT IMMUTABLE