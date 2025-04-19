from setuptools import setup, find_packages
import os

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Get the list of all files in the package
package_data = []
for root, dirs, files in os.walk('vepi'):
    for file in files:
        if file.endswith('.py'):
            continue
        package_data.append(os.path.join(root, file))

setup(
    name="vepi",
    version="0.1.2",
    author="Alexander Viljoen",
    author_email="Alexander.Viljoen@gmail.com",
    description="A Python package for interacting with Vena's ETL API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Alexveeuk/Vepi",
    packages=find_packages(),
    package_data={
        'vepi': package_data,
    },
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "requests>=2.31.0",
        "pandas>=2.0.0",
    ],
    project_urls={
        "Bug Reports": "https://github.com/Alexveeuk/Vepi/issues",
        "Source": "https://github.com/Alexveeuk/Vepi",
    },
) 