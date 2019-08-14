import sys
from setuptools import setup

if sys.version_info < (3,0):
    sys.exit('Sorry, python 3 minimum. It''s finally happening, buddy.')

setup (
    name='connspy',
    version='1.0',
    author='Nicholas Ursa',
    author_email='nick.ursa@gmail.com',
    packages=['connspy'],
    install_requires=['bloom-filter'],
    entry_points = {
        'console_scripts': ['connspy=connspy.connspy:main',
            'connspy-stream=connspy.connspy-stream:main']
    }
)

# refered to: https://python-packaging.readthedocs.io/en/latest/index.html
