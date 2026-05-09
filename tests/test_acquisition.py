import sqlite3

from option_data_manager.acquisition import AcquisitionRepository


def test_finish_stale_running_runs_marks_old_runs_failed() -> None:
    connection = sqlite3.connect(":memory:")
    repository = AcquisitionRepository(connection)
    old_run = repository.start_run(
        trigger="test",
        started_at="2026-05-09T00:00:00+00:00",
    )
    fresh_run = repository.start_run(
        trigger="test",
        started_at="2026-05-09T01:00:00+00:00",
    )

    updated = repository.finish_stale_running_runs(
        started_before="2026-05-09T00:30:00+00:00",
        finished_at="2026-05-09T02:00:00+00:00",
    )

    assert updated == 1
    assert repository.get_run(old_run.run_id).status == "failed"  # type: ignore[union-attr]
    assert repository.get_run(fresh_run.run_id).status == "running"  # type: ignore[union-attr]
