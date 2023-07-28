from setuptools import setup, find_packages

with open("./requirements.txt") as requirement_file:
    requirements = requirement_file.read().split()

setup(
    name="data_pipelines",
    description="etl pipelines, various sources",
    version="1.0.1",
    author="dn757657",
    author_email="dn757657@dal.ca",
    install_requires=requirements,
    packages=find_packages(include=['dn757657_crypto_num_sources',
                                    'dn757657_data_endpoints'],
                           exclude=['dn757657_fin_news_sources']),  # package = any folder with an __init__.py file
)
