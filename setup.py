#https://www.digitalocean.com/community/tutorials/how-to-package-and-distribute-python-applications

from setuptools import setup

setup(name='postgrez',
      version='0.1.0',
      description='A wrapper for common psycopg2 routines',
      author='Ian Whitestone',
      author_email='ianwhitestone@hotmail.com',
      url='https://github.com/ian-whitestone/postgrez',
      install_requires = ['psycopg2==2.7.3.1', 'PyYAML==3.12'],
      packages = ['postgrez']
)
