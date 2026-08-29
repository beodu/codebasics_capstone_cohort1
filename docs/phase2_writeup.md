# Phase 2 — Batch vs stream

![Phase 2 architecture](images/phase2_architecture.png)

Phase 1 is the overnight sync. That’s what I would use for month-end and
anything finance will sign off on — complete day, reconciled, cheap to run.

Ops also asked “how many orders in the last 10 minutes?” The batch job can’t
answer that until tomorrow. So Phase 2 is a second lane: events go to Kafka as
they happen, Structured Streaming writes Bronze → Silver → Gold continuously.
I kept it separate from Phase 1 on purpose (same company, different job).

Streaming is faster but thinner. It only sees what landed on the topic. If
someone later fixes an order in OLTP, the stream won’t know. I would not use
it for official revenue.

## Why the Kafka key is `order_id`

Kafka only keeps order inside a partition, and the same key always hashes to
the same partition. If I key by `order_id`, `order_placed` / `payment_received`
/ `order_shipped` for one order stay in sequence. If I keyed randomly, a
consumer could see shipped before paid.

## Why the 5-minute Gold windows show up late

I used a 10-minute watermark and 5-minute tumbling windows on
`payment_received`. A window is only written after the watermark passes its
end, in append mode, so the number is final.

Example: event_ts 10:04 can arrive at 10:09. The 10:00–10:05 window is not
safe to close at 10:05. It closes once we have seen events around 10:15.
That’s why the ticker is ~10–15 minutes behind — late data, not a bug.

If I emitted running totals and then corrected them, revenue on the dashboard
could go down. I’d rather wait.

Same watermark is used in Silver with `dropDuplicates(["event_id"])` so a
replayed event doesn’t land twice, and old state can be dropped. `event_id`
here is doing the same job `updated_at` does in the batch pipeline.
