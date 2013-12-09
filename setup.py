import os.path
from setuptools import setup, find_packages

HERE = os.path.abspath(os.path.dirname(__file__))

README_PATH = os.path.join(HERE, 'README.md')
try:
    README = open(README_PATH).read()
except IOError:
    README = ''


setup(
  name='splicer_aws',

  version='0.0.1',
  description='AWS adapter for Splicer',
  long_description=README,
  author='Scott Robertson',
  author_email='scott@triv.io',
  url='http://github.com/trivio/splicer_aws',
  classifiers=[
      "Programming Language :: Python",
      "License :: OSI Approved :: MIT License",
      "Operating System :: OS Independent",
      "Development Status :: 3 - Alpha",
      "Intended Audience :: Developers",
      "Topic :: Software Development",
  ],

  packages = find_packages(),

  install_requires = [
    'splicer',
    'boto'
  ],
 
  setup_requires=[
    'nose',
    'coverage'
  ],
  test_suite = 'nose.collector'
)
