from setuptools import find_packages, setup

setup(
    name="ndscan",
    version="0.2.0",
    url="https://github.com/klickverbot/ndscan",
    description="Composable experiment fragments and multidimensional scans for ARTIQ",
    license="LGPLv3+",
    author="David Nadlinger",
    packages=find_packages(),
    entry_points={"gui_scripts": ["ndscan_show = ndscan.show:main"]},
    # KLUDGE: ARTIQ dependency is not explicitly listed for now to avoid
    # problems with the ion trap group's Conda setup.
    # install_requires=["artiq"],

    # Include icons. We currently rely on direct filesystem access, so we can't
    # tolerate installation as a zipped egg.
    package_data={"ndscan.dashboard": ["icons/*.png", "icons/*.svg"]},
    zip_safe=False,
)
