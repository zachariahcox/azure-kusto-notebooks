from setuptools import setup, find_packages

PACKAGE_NAME = "azure-kusto-notebooks"
VERSION = '0.1.5'
INSTALL_REQUIRES = [
    'azure-kusto-data[pandas]'
]

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    author="zachariah.cox@gmail.com",
    author_email="zachariah.cox@gmail.com",
    description="utilities for working with kusto from notebooks",
    url="https://github.com/zachariahcox/azure-kusto-notebooks",
    install_requires=INSTALL_REQUIRES,
    namespace_packages=["azure"],
    packages=find_packages(exclude=['azure', 'tests']),
    extras_require={":python_version<'3.0'": ["azure-nspkg"]}
)
