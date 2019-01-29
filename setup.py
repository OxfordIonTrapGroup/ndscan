from setuptools import find_packages, setup

setup(
    name="ndscan",
    version="0.1.0",
    url="https://github.com/klickverbot/ndscan",
    description="Composable experiment fragments and multidimensional scans for ARTIQ",
    license="LGPLv3+",
    author="David Nadlinger",
    packages=find_packages(),
    package_data={"ndscan": ["icons/*.png", "icons/*.svg"]},
    install_requires=[
        "artiq"
    ]
)
