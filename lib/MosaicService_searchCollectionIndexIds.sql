SELECT B.id, jsonb_build_object( 'id', B.id )::jsonb AS feature FROM
(
    SELECT I.id FROM ${collection}_view_${index}_${lang} AS I
    WHERE I.document @@ to_tsquery('${query}')
) AS B
ORDER BY id