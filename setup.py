from setuptools import setup, find_packages

import mayfly

setup(
	name='mayfly', 
	version=mayfly.__version__, 
	url='https://github.com/mayflyr/mayfly', 
	author='mayflyr', 
	packages=find_packages()
)
