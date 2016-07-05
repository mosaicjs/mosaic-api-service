SELECT jsonb_build_object( 'id', A.id, 'pos', A.pos - 1)::jsonb AS feature  FROM
(
    SELECT ROW_NUMBER() OVER (ORDER BY id) AS pos, I.id
    FROM ${collection}_view_${index}_${lang} AS I
    WHERE I.document @@ to_tsquery('${query}')
    ORDER BY I.id
) AS A
WHERE A.id IN (${ids})