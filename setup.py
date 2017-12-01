#https://www.digitalocean.com/community/tutorials/how-to-package-and-distribute-python-applications

from setuptools import setup

with open('requirements.txt', 'r') as f:
    requirements_txt = f.read()

requirements = requirements_txt.strip().replace(' ','').split('\n')


setup(name='postgrez',
      version='0.1.0',
      description='A wrapper for common psycopg2 routines',
      author='Ian Whitestone',
      author_email='ianwhitestone@hotmail.com',
      url='https://github.com/ian-whitestone/postgrez',
      install_requires = requirements,
      packages = ['postgrez']
)
