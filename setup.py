from setuptools import find_packages, setup

import os
import re


def get_version():
    with open(os.path.join(os.path.dirname(__file__), 'seeker', '__init__.py')) as fp:
        return re.match(r".*__version__ = '(.*?)'", fp.read(), re.S).group(1)


setup(
    name='django-seeker',
    version=get_version(),
    description='A python package for mapping and querying Django models in Elasticsearch.',
    author='Dan Watson',
    author_email='watsond@imsweb.com',
    url='https://github.com/imsweb/django-seeker',
    license='BSD',
    packages=find_packages(),
    install_requires=[
        'elasticsearch-dsl>=7.0.0,<8.0.0',
        'snowballstemmer',
        'bleach',
    ],
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Utilities',
    ]
)
