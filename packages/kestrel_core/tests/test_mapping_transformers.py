import pandas as pd
import pytest

from kestrel.mapping.transformers import (run_transformer,
                                          run_transformer_on_series)


@pytest.mark.parametrize(
    "transform, value, expected", [
        ("dirname", "/tmp", "/"),
        ("basename", "/tmp", "tmp"),
        ("dirname", "/usr/local/bin/thing", "/usr/local/bin"),
        ("basename", "/usr/local/bin/thing", "thing"),
        ("posixpath_startswith", "/var", "/var/%"),
        ("posixpath_endswith", "tmp.sh", r"%/tmp.sh"),
        ("dirname", r"C:\Windows\System32\cmd.exe", r"C:\Windows\System32"),
        ("basename", r"C:\Windows\System32\cmd.exe", r"cmd.exe"),
        ("winpath_startswith", r"C:\Windows\System32", r"C:\Windows\System32\%"),
        ("winpath_endswith", "cmd.exe", r"%\cmd.exe"),
        ("to_int", 1234, 1234),
        ("to_int", 1234.1234, 1234),  # Maybe this should fail?
        ("to_int", "1234", 1234),
        ("to_int", "0x4d2", 1234),
        ("to_str", "1234", "1234"),
        ("to_str", 1234, "1234"),
        ("to_epoch_ms", "2024-03-29T12:57:56.926Z", 1711717076926),
        ("to_epoch_ms", "2024-03-29T12:57:56.92Z", 1711717076920),
        ("to_epoch_ms", "2024-03-29T12:57:56.9Z", 1711717076900),
        ("to_epoch_ms", "2024-03-29T12:57:56Z", 1711717076000),
        ("lowercase", "WORKSTATION5.example.com", "workstation5.example.com"),
    ]
)
def test_run_transformer(transform, value, expected):
    assert run_transformer(transform, value) == expected


def test_run_series_basename():
    data = pd.Series([r"C:\Windows\System32\cmd.exe", r"C:\TMP"])
    result = list(run_transformer_on_series("basename", data))
    assert result == ["cmd.exe", "TMP"]
