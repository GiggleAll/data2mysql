from setuptools import setup

PACKAGE = "data2mysql"
NAME = "data2mysql"
DESCRIPTION = "This script is designed to efficiently import data into MySQL."
AUTHOR = "zhichaozhang3@gmail.com"
VERSION = '0.1'

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    license="MIT",
    packages=[PACKAGE],
    install_requires=[
        'click==6.7',
        'MySQL-python==1.2.5',
        'SQLAlchemy==1.2.4',
    ],
    zip_safe=False,
)
