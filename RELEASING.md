
- Create new branch and PR named: `release/X.Y.Z`.
- Update `setup.py` and CHANGELOG.md.
- Let CI run and be green, then merge.
- Release to Pypi

``` bash
python setup.py sdist bdist_wheel
twine upload dist/*
```
- tag X.Y.Z
- Create github release
