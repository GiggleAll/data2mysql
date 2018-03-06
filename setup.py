from setuptools import setup

PACKAGE = "email-push"
NAME = "email-push"
DESCRIPTION = "send email by calling sendcloud web api"
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
        'requests==2.18.4',
        'voluptuous==0.11.1',
    ],
    zip_safe=False,
)
