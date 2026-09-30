"""Build a contaminated source tree and inspect both release artifacts."""

import configparser
from email.parser import Parser
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import zipfile


def test_release_artifacts_exclude_bytecode(tmp_path):
    root = Path(__file__).resolve().parents[1]
    source = tmp_path / "source"
    shutil.copytree(
        root,
        source,
        ignore=shutil.ignore_patterns(
            ".git",
            ".env",
            ".venv*",
            ".mypy_cache",
            ".pytest_cache",
            "dist",
            "build",
            "*.egg-info",
        ),
    )
    # Simulate imports and pytest's rewritten test cache before a release build.
    for relative in (
        "es/__pycache__/baseapi.cpython-311.pyc",
        "es/tests/__pycache__/test_dbapi.cpython-311-pytest-8.1.1.pyc",
        "es/elastic/stale.pyo",
        "es/old.pyc",
    ):
        cache = source / relative
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(b"stale bytecode")
    subprocess.run(
        [sys.executable, "setup.py", "sdist", "bdist_wheel"],
        cwd=source,
        check=True,
        capture_output=True,
        text=True,
    )
    wheel = source / "dist/elasticsearch_dbapi-0.3.0-py3-none-any.whl"
    sdists = list((source / "dist").glob("*.tar.gz"))
    assert len(sdists) == 1
    with tarfile.open(sdists[0]) as archive:
        sdist_names = archive.getnames()
        metadata = (
            archive.extractfile(next(n for n in sdist_names if n.endswith("/PKG-INFO")))
            .read()
            .decode()
        )
        assert Parser().parsestr(metadata)["Version"] == "0.3.0"
    with zipfile.ZipFile(wheel) as archive:
        wheel_names = archive.namelist()
        metadata = Parser().parsestr(
            archive.read("elasticsearch_dbapi-0.3.0.dist-info/METADATA").decode()
        )
        assert metadata["Version"] == "0.3.0"
        assert metadata["Requires-Python"] == ">=3.10"
        entry_points = configparser.ConfigParser()
        entry_points.read_string(
            archive.read(
                "elasticsearch_dbapi-0.3.0.dist-info/entry_points.txt"
            ).decode()
        )
        assert set(entry_points["sqlalchemy.dialects"]) == {
            "elasticsearch",
            "elasticsearch.http",
            "elasticsearch.https",
            "odelasticsearch",
            "odelasticsearch.http",
            "odelasticsearch.https",
        }
        assert "es/tests/fixtures/flights.json" in wheel_names
    for name in sdist_names + wheel_names:
        assert "__pycache__" not in name
        assert not name.endswith((".pyc", ".pyo"))
