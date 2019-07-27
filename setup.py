from setuptools import setup, find_packages

NAME = "knb"
VERSION = '0.1'
INSTALL_REQUIRES = [
    'azure-kusto-data[pandas]'
]

setup(
    name=NAME,
    version=VERSION,
    author="zachariah.cox@gmail.com",
    author_email="zachariah.cox@gmail.com",
    description="utilities for working with kusto from notebooks",
    url="https://github.com/zachariahcox/azure-kusto-notebooks",
    install_requires=INSTALL_REQUIRES,
    packages=find_packages(exclude=['tests'])
)
