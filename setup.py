# encoding: utf-8
import codecs
from setuptools import setup, find_packages

with codecs.open('README.rst', encoding='utf-8') as f:
    long_description = f.read()

install_requires = [
    'python-box',
    'janus',
    'strictyaml',
]

setup(
    name='pipekit',
    version='0.3.2',
    description='Tools for flow-based programming',
    long_description=long_description,
    url='https://github.com/ncadou/pipekit',
    author='Nicolas Cadou',
    author_email='ncadou@cadou.ca',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords='etl workflow flow-based pipe pipeline queue data processing',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'pipekit=pipekit:main',
        ],
    },
)
