from unittest.mock import patch

from neptune_scale.sync.sequence_tracker import SequenceTracker


def test_update_sequence_id():
    with patch("time.time", return_value=123.456):
        tracker = SequenceTracker()

        # Update with a positive sequence ID
        tracker.update_sequence_id(5)
        assert tracker.last_sequence_id == 5
        assert tracker.last_timestamp == 123.456

        # Update with a higher sequence ID
        tracker.update_sequence_id(10)
        assert tracker.last_sequence_id == 10
        assert tracker.last_timestamp == 123.456

        # Update with a lower sequence ID
        tracker.update_sequence_id(7)
        assert tracker.last_sequence_id == 10  # Should not decrease
        assert tracker.last_timestamp == 123.456
