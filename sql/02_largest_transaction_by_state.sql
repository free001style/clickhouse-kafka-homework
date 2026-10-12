-- One category for the largest SINGLE transaction in each state.
-- Equal amounts are resolved by the lexicographically greatest transaction_id.
SELECT
    us_state,
    tupleElement(winner, 1) AS cat_id,
    tupleElement(winner, 2) AS max_amount,
    tupleElement(winner, 3) AS transaction_id
FROM
(
    SELECT
        us_state,
        argMax(
            tuple(toString(cat_id), amount, transaction_id),
            tuple(amount, transaction_id)
        ) AS winner
    FROM homework.transactions
    GROUP BY us_state
)
ORDER BY us_state;
