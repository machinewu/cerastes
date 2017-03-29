#!/usr/bin/env python

import os
import sys
import re
from setuptools import find_packages, setup

sys.path.insert(0, os.path.abspath('src'))

def _get_version():
  """Extract version from package."""
  with open('cerastes/__init__.py') as reader:
    match = re.search(
      r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
      reader.read(),
      re.MULTILINE
    )
    if match:
      return match.group(1)
    else:
      raise RuntimeError('Unable to extract version.')

def _get_long_description():
  """Get README contents."""
  with open('README.md') as reader:
    return reader.read()

setup(
  name='cerastes',
  version=_get_version(),
  description=__doc__,
  long_description=_get_long_description(),
  author='Yassine Azzouz',
  author_email='yassine.azzouz@agmail.com',
  url='https://github.com/yassineazzouz/cerastes',
  license='GPLv3',
  packages=find_packages(),
  package_data={'': ['*.proto','*.json']},
  classifiers=[
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
    'Operating System :: POSIX',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
  ],
  install_requires=[
    'docopt',
    'protobuf>2.4.1',
    'jsonschema>2.0',
    'python-krbV',
    'sasl',
    'six',
  ],
  entry_points={'console_scripts': 
     [ 'cerastes = cerastes.cmdtool:main' ]
  },
)
