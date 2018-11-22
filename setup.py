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
        ],
    },
    setup_requires=["pytest-runner"],
    tests_require=["pytest"]
)
