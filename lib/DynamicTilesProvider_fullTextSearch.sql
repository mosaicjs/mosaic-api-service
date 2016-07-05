SELECT B.* FROM 
(
    SELECT I.*
    FROM ${collection}_view_${index}_${locale} AS I
    WHERE I.document @@ to_tsquery('${query}')
    ORDER BY ts_rank(I.document, to_tsquery('${query}')) DESC
) AS A LEFT JOIN (
    SELECT
        id AS id,
        ${projection}
        geometry AS geometry
    FROM ${collection}
) AS B ON A.id = B.id
ORDER BY ${order} id