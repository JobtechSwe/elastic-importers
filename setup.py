from setuptools import setup, find_packages
from importers import settings

setup(
    name='ElasticImporters',
    author='Team Narwhal',
    version=settings.VERSION,
    packages=find_packages(),
    include_package_data=True,
    install_requires=['valuestore', 'jobtech-common', 'elasticsearch', 'zeep', 'python-dateutil'],
    package_data={'': ['**/resources/*']},
    entry_points={
        'console_scripts': [
            'import-platsannonser = importers.platsannons.main:start',
            'import-platsannonser-daily = importers.platsannons.main:start_daily_index',
            'import-taxonomy = importers.taxonomy.main:start',
            'set-read-alias-platsannons  = importers.indexmaint.main:set_platsannons_read_alias',
            'set-write-alias-platsannons = importers.indexmaint.main:set_platsannons_write_alias',
            'create-platsannons-index = importers.indexmaint.main:create_platsannons_index'
        ],
    },
    tests_require=["pytest"]
)
