#!/usr/bin/env python
"""setup script for libpredweb"""
import glob
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

PLOTTING_SCRIPTS = glob.glob('src/plot*.sh')

setup(
        name='libpredweb',
        version='1.0.1',
        scripts=[
            'src/clean_cached_result.py',
            'src/clean_server_file.sh',
            'src/nanjianglib.pl',
            'src/check_web_server.pl',
            'src/check_jobqueuestatus.pl',
            'src/my_ip2country.py',
            'src/stat_usage_web_server.sh',
            'src/show_jobqueuestatus.py',
            'src/job_final_process.py',
            'src/run_server_statistics.py',
            'src/restart_qd_fe.cgi'
            ] + PLOTTING_SCRIPTS,
        author="Nanjiang Shu",
        author_email="nanjiang.shu@gmail.com",
        description="A library for the prediction protein web-server",
        long_description=LONG_DESCRIPTION,
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
