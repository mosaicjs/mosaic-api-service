CREATE OR REPLACE FUNCTION to_string(o jsonb, c text) RETURNS text AS $$
    if (Array.isArray(o)) {
        if (!o.length)
            return '';
        return c ? o.join(c) : o[0] + '';
    } else if (o) {
        return o + '';
    } else {
        return '';
    }
$$ LANGUAGE plv8 STRICT IMMUTABLE