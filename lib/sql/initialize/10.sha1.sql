CREATE OR REPLACE FUNCTION sha1(str text) returns text AS $$
    select encode(digest(str, 'sha1'), 'hex') as hash;
$$ LANGUAGE SQL STRICT IMMUTABLE