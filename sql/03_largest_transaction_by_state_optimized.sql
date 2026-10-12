-- Merge states at query time: correct even before background part merges.
SELECT
    us_state,
    tupleElement(winner, 1) AS cat_id,
    tupleElement(winner, 2) AS max_amount,
    tupleElement(winner, 3) AS transaction_id
FROM
(
    SELECT
        us_state,
        argMaxMerge(winner_state) AS winner
    FROM homework.largest_transaction_by_state
    GROUP BY us_state
)
ORDER BY us_state;
