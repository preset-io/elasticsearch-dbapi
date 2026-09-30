# Releasing

- Create a release PR and update `setup.py` and `CHANGELOG.md`.
- Require green CI and review approval, then merge the release PR.
- For 0.3.0, #123 is superseded by #124, which preserves Evan Rusackas's
  pagination contribution. #123 is closed as superseded; do not merge its
  older implementation separately.
- Tag the reviewed, merged commit as `0.3.0` and push that tag. Do not tag
  an unreviewed local tree or publish anything from the old `release/0.2.14`
  build directory.

## Build and inspect the tagged source

Use a fresh detached worktree, not the developer's working directory. Run
these commands from a clone of the repository with the reviewed tag fetched:

```bash
release_dir=$(mktemp -d)
git worktree add --detach "$release_dir" 0.3.0
cd "$release_dir"
test "$(git describe --tags --exact-match)" = 0.3.0
test -z "$(git status --porcelain)"
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip build twine
rm -rf dist build
python -m build
python -m twine check dist/elasticsearch_dbapi-0.3.0.tar.gz dist/elasticsearch_dbapi-0.3.0-py3-none-any.whl
```

Inspect the sdist and wheel: version 0.3.0, Python >=3.10, all six SQLAlchemy
dialect entry points, and no bytecode or `__pycache__` directories. CI's
`tests/test_release_packaging.py` checks these properties even when the source
tree contains test bytecode. Test the built wheel before publishing.

## Publish only the inspected artifacts

Never use `dist/*`: a stale 0.2.14 artifact would bypass Superset's <0.3.0
compatibility pin. Upload only these two filenames from the clean tagged build:

```bash
python -m twine upload dist/elasticsearch_dbapi-0.3.0.tar.gz dist/elasticsearch_dbapi-0.3.0-py3-none-any.whl
```

Create the GitHub release for the same `0.3.0` tag.
