from setuptools import setup, find_packages

setup(
    name="myrestdatasource",
    version="0.3.0",
    description="Spark Data Source for reading from a REST API",
    author="Alessio",
    author_email="tugnolialessio@gmail.com",
    packages=find_packages(),
    install_requires=[
        "requests>=2.0"
    ]
)
