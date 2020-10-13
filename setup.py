import io
import os

from setuptools import find_packages, setup

VERSION = "0.1.3"
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

with io.open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="elasticsearch-dbapi",
    description=("A DBAPI and SQLAlchemy dialect for Elasticsearch"),
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=VERSION,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": [
            "elasticsearch = es.elastic.sqlalchemy:ESHTTPDialect",
            "elasticsearch.http = es.elastic.sqlalchemy:ESHTTPDialect",
            "elasticsearch.https = es.elastic.sqlalchemy:ESHTTPSDialect",
            "odelasticsearch = es.opendistro.sqlalchemy:ESHTTPDialect",
            "odelasticsearch.http = es.opendistro.sqlalchemy:ESHTTPDialect",
            "odelasticsearch.https = es.opendistro.sqlalchemy:ESHTTPSDialect",
        ]
    },
    install_requires=["elasticsearch>7", "sqlalchemy"],
    extras_require={"opendistro": ["requests_aws4auth"]},
    author="Preset Inc.",
    author_email="daniel@preset.io",
    url="http://preset.io",
    download_url="https://github.com/preset-io/elasticsearch-dbapi/releases/tag/"
    + VERSION,
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    tests_require=["nose>=1.0"],
    test_suite="nose.collector",
)
