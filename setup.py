from setuptools import setup, find_packages

setup(
    name='ElasticImporters',
    author='Team Narwhal',
    version='1.0.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'valuestore', 'psycopg2-binary', 'elasticsearch', 'zeep',
        'python-dateutil', 'flashtext'
    ],
    package_data={'': ['**/resources/*']},
    entry_points={
        'console_scripts': [
            'import-platsannonser = importers.platsannons.main:start',
            'import-taxonomy = importers.taxonomy.main:start',
            'import-auranest = importers.auranest.main:start',
            'set-platsannons-read-alias = '
            'importers.indexmaint.main:set_platsannons_read_alias',
            'set-platsannons-write-alias = '
            'importers.indexmaint.main:set_platsannons_write_alias',
            'set-auranest-read-alias = '
            'importers.indexmaint.main:set_auranest_read_alias',
            'set-auranest-write-alias = '
            'importers.indexmaint.main:set_auranest_write_alias',
        ],
    },
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)
