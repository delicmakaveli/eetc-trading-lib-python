import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='eetc-algo-trading-lib',
    version='0.2.1',
    author='Stefan Delic',
    author_email='eastempiretradingcompany2019@gmail.com',
    description='Algorithmic Trading Library by East Empire Trading Company.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/delicmakaveli/eetc-trading-lib-python',
    license='GPLv2',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
