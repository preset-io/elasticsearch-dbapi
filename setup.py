import os

from setuptools import find_packages, setup

VERSION = '0.1.0'
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

setup(
    name='elasticsearch-dbapi',
    description=('DBAPI for Elasticsearch'),
    long_description_content_type='text/markdown',
    version=VERSION,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'sqlalchemy.dialects': [
            'es = es.sqlalchemy:ESHTTPDialect',
            'es.http = es.sqlalchemy:ESHTTPDialect',
            'es.https = es.sqlalchemy:ESHTTPSDialect',
        ],
    },
    install_requires=[
        "sqlalchemy",
        "requests",
    ],
    author='Preset Inc.',
    author_email='daniel@preset.io',
    url='http://preset.io',
    download_url=(
        'https://github.com/preset-io/elasticsearch-dbapi/' + VERSION,
    ),
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    tests_require=[
        'nose>=1.0',
    ],
    test_suite='nose.collector',
)
