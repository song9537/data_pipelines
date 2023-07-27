from setuptools import setup, find_packages

with open("./requirements.txt") as requirement_file:
    requirements = requirement_file.read().split()

setup(
    name="data_pipelines",
    description="etl pipelines, various sources",
    version="1.0.0",
    author="dn757657",
    author_email="dn757657@dal.ca",
    install_requires=requirements,
    packages=find_packages(include=['src']),  # package = any folder with an __init__.py file
)
