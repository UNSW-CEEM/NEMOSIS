import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="nemosis",
    version="3.7.0",
    author="Nicholas Gorman, Abhijith Prakash",
    author_email="n.gorman305@gmail.com",
    description="A tool for accessing AEMO data.",
    long_description="A tool for accessing AEMO data.",
    long_description_content_type="text/markdown",
    url="https://github.com/UNSW-CEEM/NEMOSIS",
    packages=setuptools.find_packages(),
    install_requires=[
        "requests",
        "joblib",
        "pyarrow",
        "feather-format",
        "pandas",
        "xlrd",
        "beautifulsoup4",
        "openpyxl",
        "parameterized"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: OS Independent",
    ],
)
