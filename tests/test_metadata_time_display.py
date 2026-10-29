from datetime import datetime, timezone
import uuid
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa

from gui.services import metadata_service
from gui.services.metadata_service import _localize_metadata_timestamps


def test_metadata_timestamps_render_in_local_timezone_without_changing_instant():
    source = datetime(2026, 9, 9, 6, 47, 22)
    frame = pd.DataFrame({"insert_date": [source]})

    rendered = _localize_metadata_timestamps(frame)
    expected = source.replace(tzinfo=timezone.utc).astimezone().replace(tzinfo=None)

    assert rendered.loc[0, "insert_date"] == expected


def test_recent_activity_converts_uuid_run_ids_before_streamlit_rendering(monkeypatch):
    frame = pd.DataFrame({"run_id": [uuid.uuid4()], "status": ["SUCCESS"]})
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = MagicMock()
    monkeypatch.setattr(metadata_service, "get_engine", lambda: engine)
    monkeypatch.setattr(metadata_service.pd, "read_sql", lambda *_args, **_kwargs: frame.copy())
    if hasattr(metadata_service.get_recent_activity, "clear"):
        metadata_service.get_recent_activity.clear()

    activity = metadata_service.get_recent_activity(limit=1)

    assert activity.loc[0, "run_id"] == str(frame.loc[0, "run_id"])
    pa.Table.from_pandas(activity)
