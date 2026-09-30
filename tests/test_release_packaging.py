"""Build a contaminated source tree and inspect both release artifacts."""

import ast
import configparser
from email.parser import Parser
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def release_version() -> str:
    """The ``VERSION`` assigned in setup.py, without executing setup()."""
    tree = ast.parse((ROOT / "setup.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and [
            getattr(target, "id", None) for target in node.targets
        ] == ["VERSION"]:
            return ast.literal_eval(node.value)
    raise AssertionError("setup.py does not assign VERSION")


def test_changelog_documents_release_version():
    changelog = (ROOT / "CHANGELOG.md").read_text().splitlines()
    assert f"### {release_version()}" in changelog


def test_release_artifacts_exclude_bytecode(tmp_path):
    root = ROOT
    version = release_version()
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
    wheel = source / f"dist/elasticsearch_dbapi-{version}-py3-none-any.whl"
    sdists = list((source / "dist").glob("*.tar.gz"))
    assert [sdist.name for sdist in sdists] == [f"elasticsearch_dbapi-{version}.tar.gz"]
    with tarfile.open(sdists[0]) as archive:
        sdist_names = archive.getnames()
        metadata = (
            archive.extractfile(next(n for n in sdist_names if n.endswith("/PKG-INFO")))
            .read()
            .decode()
        )
        assert Parser().parsestr(metadata)["Version"] == version
    with zipfile.ZipFile(wheel) as archive:
        wheel_names = archive.namelist()
        metadata = Parser().parsestr(
            archive.read(f"elasticsearch_dbapi-{version}.dist-info/METADATA").decode()
        )
        assert metadata["Version"] == version
        assert metadata["Requires-Python"] == ">=3.10"
        entry_points = configparser.ConfigParser()
        entry_points.read_string(
            archive.read(
                f"elasticsearch_dbapi-{version}.dist-info/entry_points.txt"
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
