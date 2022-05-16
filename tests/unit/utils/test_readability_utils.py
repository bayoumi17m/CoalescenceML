from coalescenceml.utils import readability_utils


def test_get_human_readable_time():
    """Check get_human_readable_time formats string properly."""
    assert readability_utils.get_human_readable_time(3661) == "1h1m1s"
    assert readability_utils.get_human_readable_time(301) == "5m1s"
    assert readability_utils.get_human_readable_time(0.1234) == "0.123s"
    assert readability_utils.get_human_readable_time(172799) == "1d23h59m59s"
