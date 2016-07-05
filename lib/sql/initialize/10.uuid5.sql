CREATE OR REPLACE FUNCTION uuid5(str text) RETURNS UUID AS $$
    var plan = plv8.prepare("SELECT ENCODE(digest($1, 'sha1'), 'hex') AS hash", ['text']);
    try {
        var hash = plan.execute( [str] )[0].hash;
        var val = parseInt(hash.substring(16, 18), 16);
        val = val & 0x3f | 0xa0; // set variant
        return '' + 
            hash.substring( 0,  8) + '-' + //
            hash.substring( 8, 12) + '-' + //
            '5' + // set version
            hash.substring(13, 16) + '-' + //
            val.toString(16) + hash.substring(18, 20) + '-' + //
            hash.substring(20, 32);
    } finally {
        plan.free();
    }
$$ LANGUAGE plv8 STRICT IMMUTABLE