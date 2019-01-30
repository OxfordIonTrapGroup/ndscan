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
    # KLUDGE: ARTIQ dependency is not explicitly listed for now to avoid
    # problems with the ion trap group's Conda setup.
    # install_requires=["artiq"]
)
