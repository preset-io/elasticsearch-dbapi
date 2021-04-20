
- Create new branch and PR named: `release/X.Y.Z`
- Update `setup.py` and CHANGELOG.md

## Release to Pypi

``` bash
python setup.py sdist bdist_wheel
twine upload dist/*
```
