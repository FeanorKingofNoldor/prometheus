from importlib.util import find_spec


def test_prometheus_package_importable() -> None:
    assert find_spec("prometheus") is not None
