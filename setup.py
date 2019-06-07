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
            'import-platsannonser-daily = importers.platsannons.main:start_daily_index',
            'import-taxonomy = importers.taxonomy.main:start',
            'import-auranest = importers.auranest.main:start',
            'glitchfix-auranest = importers.auranest.main:glitchfix',
            'set-read-alias-platsannons = '
            'importers.indexmaint.main:set_platsannons_read_alias',
            'set-write-alias-platsannons = '
            'importers.indexmaint.main:set_platsannons_write_alias',
            'set-read-alias-auranest = '
            'importers.indexmaint.main:set_auranest_read_alias',
            'set-write-alias-auranest = '
            'importers.indexmaint.main:set_auranest_write_alias',
            'create-platsannons-index = '
            'importers.indexmaint.main:create_platsannons_index'
        ],
    },
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)
