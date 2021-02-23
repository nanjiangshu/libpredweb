#!/usr/bin/env python
from setuptools import setup, find_packages
import glob

with open("README.md", "r") as fh:
    long_description = fh.read()

plotting_scripts = glob.glob('src/plot*.sh')

setup(
        name='libpredweb',
        version='1.0.0',
        scripts=['src/clean_cached_result.py', 'src/clean_server_file.sh',
            'src/nanjianglib.pl', 'src/check_web_server.pl',
            'src/check_jobqueuestatus.pl', 'src/my_ip2country.py',
            'src/show_jobqueuestatus.py']+plotting_scripts ,
        author="Nanjiang Shu",
        author_email="nanjiang.shu@gmail.com",
        description="A library for the prediction protein web-server",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/nanjiang/libpredweb",
        zip_safe=False,
        packages=find_packages('.'),
        include_package_data=True,
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            ],
        )
