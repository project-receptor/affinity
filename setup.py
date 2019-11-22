#!/usr/bin/env python
"""A setuptools-based script for installing Affinity."""
from setuptools import find_packages, setup

with open('README.md') as handle:
    LONG_DESCRIPTION = handle.read()

setup(
    name='receptor-affinity',
    author='Red Hat',
    version='0.1.0',
    packages=find_packages(include=['affinity']),
    install_requires=['click'],
    # See https://pypi.org/classifiers/
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Software Development :: Testing',
    ],
    description=(
        "Set of tools to check receptor's binding affinity."
    ),
    entry_points={
        'console_scripts': [
            'affinity = affinity.cli:cli'
        ]
    },
    license='Apache 2.0',
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    python_requires=">=3.6",
    url='https://github.com/project-receptor/affinity',
)
