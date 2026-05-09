from option_data_manager.cli.collect_market_parallel import build_contiguous_shards


def test_build_contiguous_shards_creates_non_overlapping_boundaries() -> None:
    shards = build_contiguous_shards(
        ["A", "B", "C", "D", "E"],
        worker_count=2,
    )

    assert shards[0].start_after_underlying is None
    assert shards[0].end_before_underlying == "C"
    assert shards[0].first_underlying == "A"
    assert shards[0].last_underlying == "B"
    assert shards[1].start_after_underlying == "B"
    assert shards[1].end_before_underlying is None
    assert shards[1].first_underlying == "C"
    assert shards[1].last_underlying == "E"


def test_build_contiguous_shards_handles_more_workers_than_symbols() -> None:
    shards = build_contiguous_shards(["A"], worker_count=3)

    assert [shard.underlying_count for shard in shards] == [0, 0, 1]
    assert shards[-1].first_underlying == "A"
