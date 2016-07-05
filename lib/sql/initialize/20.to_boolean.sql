CREATE OR REPLACE FUNCTION to_boolean(o jsonb) RETURNS boolean AS $$
    return toBoolean(o) ? 't' : 'f';
    function toBoolean(o){
        if (Array.isArray(o)) {
            return o.length ? toBoolean(o[0]) : false;
        } else if (o === 'true' || o === 1) {
            return true;
        } else if (o === 'false' || o === 0) {
            return false;
        } else {
            return !!o;
        }
    }
$$ LANGUAGE plv8 STRICT IMMUTABLE