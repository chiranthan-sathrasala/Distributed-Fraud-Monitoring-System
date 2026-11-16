# Design notes

## Pipeline
`Kafka -> parse JSON -> watermark 1 min -> sliding window (2 min, every 30 s) per source account -> collect all payments of the
window -> foreachBatch: R2/R3/R4 alerts + one decision per payment -> MERGE into two Iceberg tables`.

## Rules (exactly as the assignment, validated against the ground truth)
* **R1** single INR payment >= 50,000. **R2** >= 5 payments in a window. **R3** >= 2 INR payments with total > 600,000.
  **R4** >= 3 payments from one account to the same destination. **R5** score = number of rules active; >= 2 BLOCK, 1 REVIEW, 0 APPROVE.
* Windows are `[start, start+120 s)` with starts on a 30 s grid, so one payment lies in 4 windows and is flagged if any fires.
* **Findings from the ground truth** (they are not in the assignment text, `tests/reference_check.py` proves them):
  1. *Linking is account-level:* an alert applies to every payment of that account whose event time lies inside the alert window
     (even a payment to a different destination than the R4 pattern). `related_alert_ids` and `r2..r4_triggered` follow from that.
  2. *One R4 alert per account and window:* if two destinations qualify in the same window, only one is kept
     (most payments, then earliest trigger, then lowest destination number). `alert_id = R4-<account>-<window start>`.
  3. `trigger_transaction_id` is the latest payment (by event time) in the window; R3 counts and sums INR payments only.
  4. `evaluated_at` / `triggered_at` are empty in the ground truth; the verifier ignores them.

## Why one aggregation + foreachBatch
Spark 3.3 cannot chain stateful operators in append mode (for example aggregation -> stream-stream join), and
`dropDuplicatesWithinWatermark` only exists from Spark 3.5. So the only stateful step is one windowed aggregation that
collects the payments of each (account, window). Everything after it is plain batch logic on the windows that just closed.
* **Duplicates:** the same transaction_id inside a window is removed before anything is counted; decisions are written with
  insert-if-absent on `transaction_id`, so a payment is never decided twice.
* **Decision timing:** a payment is in 4 windows; the last one to close starts at `floor(event time, 30 s)`. The payment is decided in
  the batch where that window closes, when all its alerts exist. So a decision appears about 2.5-3 minutes (event time) after the payment.

## Lateness (watermark = 1 minute)
Spark closes a window once the watermark (newest event time seen minus 1 min) passes its end. Payments up to 1 minute late are
always inside every open window. Payments later than that are missing from the windows that already closed: they may be
evaluated on fewer windows (decision with fewer rules) or, if all their windows closed, get no decision at all. This is the
expected result of Experiment 4.

## End-of-stream flush marker
The watermark only moves when newer data arrives, so the last windows of the dataset would never close. The producer therefore sends one extra event
for the fake account `__FLUSH__` stamped 30 minutes after the last payment. It is dropped from all outputs.

## Exactly-once and the restart experiment
The streaming checkpoint (on HDFS) stores offsets and window state. If the driver dies, a restart with the **same run name** resumes from the
checkpoint and replays the unfinished micro-batch. Both writes are `MERGE ... WHEN NOT MATCHED THEN INSERT` on deterministic keys
(`alert_id`, `transaction_id`), so a replay cannot create duplicates.

## Choosing speed and trigger
The producer replays at 40x (8 h of data in about 12 min). Spark processes micro-batches back to back; each batch covers
`batch duration x 40` seconds of event time. Experiment 3 delays are 10-50 s (always inside the 60 s watermark, so exact match expected).
Experiment 4 delays are 300-600 s, large enough that the late events are beyond the watermark whatever the batch size.
