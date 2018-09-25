from setuptools import find_packages, setup

import log_parser

setup(
    name='log-parser',
    version=log_parser.__version__,
    author_email='asiforis@gmail.com',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'log-parser = log_parser.main:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Environment :: Console',
    ],
)
