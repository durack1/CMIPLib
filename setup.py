from distutils.core import setup
Version="0.0.1"
setup (name = "CMIPlib",
       author="Paul J. Durack (durack1@llnl.gov), Stephen Po-Chedley (pochedley1@llnl.gov)",
       version=Version,
       description = "Python utilities for CMIP data indexing and manipulation",
       url = "http://github.com/durack1/CMIPLib",
       packages = ['CMIPLib'],
       package_dir = {'CMIPLib': 'lib'},
      )

