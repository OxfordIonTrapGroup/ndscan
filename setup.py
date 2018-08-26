from setuptools import find_packages, setup

setup(
    name='ndscan',
    version='0.1.0',
    url='https://github.com/klickverbot/ndscan',
    author='David P. Nadlinger',
    packages=find_packages(),
    package_data={'ndscan': ['icons/*.png', 'icons/*.svg']},
    entry_points={},
)
