from setuptools import setup, find_packages

requirements = [] # from requirements.txt later

setup(
    name="src",
    version="0.0.1",
    description="Package for development",
    packages=find_packages(), # finds utils and pipeline
    install_requires=requirements
)
