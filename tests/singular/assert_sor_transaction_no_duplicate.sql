SELECT
    transaction_id,
    count(*) AS occurrences,
    min(transaction_date) AS min_transaction_date,
    min(transaction_date) AS max_transaction_date,
    min(amount) AS min_amount,
    max(amount) AS max_amount
FROM {{ ref('sor_transaction') }}
GROUP BY transaction_id
HAVING count(*) > 1