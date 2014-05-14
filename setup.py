from setuptools import setup, find_packages
import seeker

setup(
    name='seeker',
    version=seeker.__version__,
    description='A python package for mapping and querying Django models in Elasticsearch.',
    author='Dan Watson',
    author_email='watsond@imsweb.com',
    url='http://imsweb.com',
    license='BSD',
    packages=find_packages(),
    install_requires=[
        'elasticsearch>=1.0',
        'tqdm',
    ],
    include_package_data=True,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Utilities',
    ]
)
