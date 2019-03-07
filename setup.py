import setuptools
import pypyrus_etl as etl

with open('README.md', 'r') as fh:
    long_description = fh.read()

install_requires = [
    'pypyrus-logbook>=0.0.2', 'sqlalchemy', 'cx_Oracle', 'paramiko']

author = etl.__author__
email = etl.__email__
version = etl.__version__
description = etl.__doc__
license = etl.__license__

setuptools.setup(
    name='pypyrus-etl',
    version=version,
    author=author,
    author_email=email,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    license=license,
    url='https://github.com/t3eHawk/etl',
    install_requires=install_requires,
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
