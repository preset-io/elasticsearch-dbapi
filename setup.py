import os

from setuptools import find_packages, setup

VERSION = '0.1.0'
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

setup(
    name='es-dbapi',
    description=('DBAPI for Elasticsearch'),
    long_description_content_type='text/markdown',
    version=VERSION,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'sqlalchemy.dialects': [
            'es = es.elastic.sqlalchemy:ESHTTPDialect',
            'es.http = es.elastic.sqlalchemy:ESHTTPDialect',
            'es.https = es.elastic.sqlalchemy:ESHTTPSDialect',
            'esaws = es.aws.sqlalchemy:ESHTTPDialect',
            'esaws.http = es.aws.sqlalchemy:ESHTTPDialect',
            'esaws.https = es.aws.sqlalchemy:ESHTTPSDialect',
        ],
    },
    install_requires=[
        "elasticsearch>7",
        "sqlalchemy",
    ],
    extras_require={
        "aws": ["requests_aws4auth"],
    },
    author='Preset Inc.',
    author_email='danielvazgaspar@gmail.com',
    url='http://preset.io',
    download_url=(
        'https://github.com/preset-io/es-dbapi/' + VERSION,
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
