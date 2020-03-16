import sys

from setuptools import find_packages, setup
import versioneer

with open('../README.md', encoding='utf-8') as f:
    long_description = f.read()

install_requirements = [
    'boto3',
    'libgeohash',
    'shapely',
]

setup(
    name='dynamodb-geo',
    version=versioneer.get_version(),
    url='https://github.com/tom-mi/python-dynamodb-geo/',
    license='MIT',
    author='Thomas Reifenberger',
    install_requires=install_requirements,
    author_email='tom-mi at rfnbrgr.de',
    description='Geo queries for dynamodb based on geohashes',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    platforms='any',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8',
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: GIS',
    ],
    cmdclass=versioneer.get_cmdclass(),
)
