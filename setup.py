#!/usr/bin/env python
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
        name='libpredweb',
        version='1.00',
        scripts=['src/clean_cached_result.py', 'src/clean_server_file.sh'] ,
        author="Nanjiang Shu",
        author_email="nanjiang.shu@gmail.com",
        description="A library for the prediction protein web-server",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/nanjiang/lib-predweb",
        packages=setuptools.find_packages(),
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            ],
        )
