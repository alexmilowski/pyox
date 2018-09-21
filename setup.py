"""A client library for interacting with Apache Knox for Hadoop REST services

See:

https://github.com/alexmilowski/python-hadoop-rest-api
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages

long_description = """
A client library for interacting with Apache Knox for Hadoop REST services

See:

https://github.com/alexmilowski/python-hadoop-rest-api
"""

import re
vdir = __file__[0:__file__.rfind('/')]+'/' if __file__.rfind('/')>=0 else ''
with open(vdir+'pyox/__init__.py', 'rt') as vfile:
   verstrline = vfile.read()
   VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
   mo = re.search(VSRE, verstrline, re.M)
   if mo:
      version_info = mo.group(1)
   else:
      raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

setup(
    name='pyox',

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=version_info,

    description='A client library for Apache Knox',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/alexmilowski/python-hadoop-rest-api',

    # Author details
    author='Alex Miłowski',
    author_email='alex@milowski.com',

    # Choose your license
    license='Apache 2.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    # What does your project relate to?
    keywords='',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=['pyox','pyox.apps.monitor', 'pyox.apps.tracker'],
    #find_packages(exclude=['contrib', 'docs', 'tests', 'virtualenv']),

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #   py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=['requests','flask','redis','cryptography'],

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={
    },

    include_package_data=True,

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    package_data={
    },

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files # noqa
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    data_files=[],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
    },
)
