-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------
-- ----------------------------------------------------------------------------

/* * /
DROP MATERIALIZED VIEW IF EXISTS object_fts_q CASCADE;
DROP FUNCTION IF EXISTS convert_to_tsvector(obj jsonb, lang text, fields jsonb) CASCADE;
--* /

DROP MATERIALIZED VIEW IF EXISTS object_fts_q CASCADE;
CREATE MATERIALIZED VIEW object_fts_q AS
SELECT id, convert_to_tsvector(properties, 'french', '{"name":1, "addr:street":1}'::jsonb) as document
FROM objects;

CREATE INDEX idx_object_fts_q ON object_fts_q USING gist(document);

-- Faster (LEFT JOIN):

SELECT O.* FROM object_fts_q AS I LEFT JOIN prm AS O ON I.id = O.id 
WHERE I.document @@ to_tsquery('geo:*')
ORDER BY ts_rank(I.document, to_tsquery('geo:*')) DESC
LIMIT 100;

SELECT O.properties->>'name', O.properties->>'description', O.properties->>'url' FROM prm_fts_q AS I LEFT JOIN prm AS O ON I.id = O.id 
WHERE I.document @@ to_tsquery('geo:*')
ORDER BY ts_rank(I.document, to_tsquery('geo:*')) DESC
LIMIT 100;


-- SELECT O.* FROM object_fts_q AS I LEFT JOIN objects AS O ON I.id = O.id 
-- WHERE I.document @@ to_tsquery('saint:*')
-- SELECT O.* FROM object_fts_q AS I, objects AS O
-- WHERE I.document @@ to_tsquery('saint:*') AND O.id = I.id
-- ORDER BY ts_rank(I.document, to_tsquery('saint:*')) DESC limit 10;

/* * /
DROP EXTENSION IF EXISTS unaccent;
CREATE EXTENSION unaccent;
SELECT unaccent('èéêë');
-- */



select vec, vec @@ to_tsquery(lang, 'rue & roc:*') from (
   select
      to_tsvector(lang, convert_to_text(properties, '{"name":1, "addr:street":1}'::jsonb)) as vec,
      L.lang as lang
   from objects, (select 'french'::regconfig as lang ) as L
) as T;