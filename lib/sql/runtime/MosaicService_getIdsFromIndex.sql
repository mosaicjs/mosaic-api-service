SELECT A.id AS id, jsonb_build_object( 'id', A.id )::jsonb AS feature FROM
(
    SELECT I.id FROM ${collection}_view_${index}_${lang} AS I ORDER BY id
) AS A
