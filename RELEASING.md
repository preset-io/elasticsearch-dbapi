# Releasing to Pypi instructions


## Test release on test.pypi.org

``` bash
python setup.py sdist bdist_wheel
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

Testing the release

```bash
pip install --index-url https://test.pypi.org/simple/ elasticsearch-dbapi
```

## Release to Pypi

``` bash
python setup.py sdist bdist_wheel
twine upload dist/*
```
