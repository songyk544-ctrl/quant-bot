# Preserve archived score metadata during accumulation

`load_score_trend_safe()` previously let pandas infer numeric stock codes and returned a fixed subset of columns that omitted V5 fields. Rewriting the daily score CSV could remove leading zeroes from older codes and erase older V5 scores/status/reasons even though the newest rows were correct.

The loader now reads stock codes as source strings and retains all existing metadata columns. It does not pad, repair or reconstruct existing malformed codes, recalculate scores, change weights, or alter V3/V4/V5 strategy functions. Previously lost fields remain unavailable in the current CSV; their Git snapshots must be kept separately.

An existing unreadable/empty/wrong-schema/conflicted score CSV stops with a fixed `score_trend_existing_snapshot_unreadable` or `score_trend_existing_snapshot_invalid` error before replacement. A missing file still allows an initial snapshot. Malformed source files require review; this patch does not repair or reset them.

Apply after the independent history-read guard from fa90f33. All 28 offline unit tests pass, including seven new Mock/temporary CSV tests for metadata retention and safe failure. Source scraper startup, APIs, credentials, Telegram, operational workflows and original CSVs were not executed or changed during validation. Production/remote CI validation is still required after the patch is pushed by an authorized maintainer. Do not auto-merge.
