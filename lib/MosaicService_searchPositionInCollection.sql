SELECT jsonb_build_object( 'id', A.id, 'pos', A.pos - 1)::jsonb AS feature  FROM
(
    SELECT ROW_NUMBER() OVER (ORDER BY id) AS pos, I.id
    FROM ${collection} AS I
    ORDER BY I.id
) AS A
WHERE A.id IN (${ids})