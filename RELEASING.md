# Releasing

- Create a release PR and update `VERSION` in `setup.py` and `CHANGELOG.md`
  (a `### <version>` heading). Record one-off release decisions, such as
  superseded PRs, in the changelog or the release PR description, not here.
- Require green CI and review approval, then merge the release PR.
- Tag the reviewed, merged commit with the version (e.g. `0.3.0`) and push
  that tag. Do not tag an unreviewed local tree or publish anything from an
  existing build directory.

## Build and inspect the tagged source

Use a fresh detached worktree, not the developer's working directory. Run
these commands from a clone of the repository with the reviewed tag fetched:

```bash
VERSION=0.3.0  # the tag being released
release_dir=$(mktemp -d)
git worktree add --detach "$release_dir" "$VERSION"
cd "$release_dir"
test "$(git describe --tags --exact-match)" = "$VERSION"
test -z "$(git status --porcelain)"
grep -qx "VERSION = \"$VERSION\"" setup.py
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip build twine
rm -rf dist build
python -m build
python -m twine check "dist/elasticsearch_dbapi-$VERSION.tar.gz" "dist/elasticsearch_dbapi-$VERSION-py3-none-any.whl"
```

Inspect the sdist and wheel: the tagged version, Python >=3.10, all six
SQLAlchemy dialect entry points, and no bytecode or `__pycache__` directories.
CI's `tests/test_release_packaging.py` checks these properties for the version
in `setup.py`, even when the source tree contains test bytecode. Test the built
wheel before publishing.

## Publish only the inspected artifacts

Never use `dist/*`: a stale artifact of another version could be published
(for example a 0.2.x build, which Superset's `<0.3.0` pin would install
automatically). Upload only these two filenames from the clean tagged build:

```bash
python -m twine upload "dist/elasticsearch_dbapi-$VERSION.tar.gz" "dist/elasticsearch_dbapi-$VERSION-py3-none-any.whl"
```

Create the GitHub release for the same tag.
