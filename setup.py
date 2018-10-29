import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="osdan",
    version="0.0.5",
    author="Nicholas Gorman",
    author_email="n.gorman305@gmail.com",
    description="A tool for accessing AEMO data.",
    long_description="A tool for accessing AEMO data.",
    long_description_content_type="text/markdown",
    url="https://github.com/UNSW-CEEM/osdan",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: OS Independent",
    ],
)