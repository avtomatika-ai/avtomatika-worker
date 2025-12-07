from importlib import reload
from importlib.metadata import PackageNotFoundError
from unittest.mock import patch

import avtomatika_worker


def test_version_unknown_when_package_not_found():
    """
    Tests that __version__ is set to 'unknown' when the package is not found.
    """
    with patch("importlib.metadata.version") as mock_version:
        mock_version.side_effect = PackageNotFoundError
        reload(avtomatika_worker)
        assert avtomatika_worker.__version__ == "unknown"


def test_version_is_set_when_package_is_found():
    """
    Tests that __version__ is set correctly when the package is found.
    """
    with patch("importlib.metadata.version") as mock_version:
        mock_version.return_value = "1.2.3"
        reload(avtomatika_worker)
        assert avtomatika_worker.__version__ == "1.2.3"
