# encoding: utf-8
import codecs
from setuptools import setup, find_packages


with codecs.open('README.rst', encoding='utf-8') as f:
    long_description = f.read()

install_requires = [
    'aiozmq',
    'async_generator',
    'janus',
    'pyzmq',
]

setup(
    name='pipekit',
    version='0.0.1',
    description='Tools for flow-based programming',
    long_description=long_description,
    url='https://github.com/ncadou/pipekit',
    author='Nicolas Cadou',
    author_email='ncadou@cadou.ca',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Office/Business :: Scheduling',
        'License :: MIT',
        'Programming Language :: Python :: 3.5'
    ],
    keywords='pipe pipeline queue topology flow based programming',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'pipekit=pipekit:main',
        ],
    },
)
