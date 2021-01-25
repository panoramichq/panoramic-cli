import re

from setuptools import find_namespace_packages, setup

with open("src/panoramic/cli/__version__.py", encoding="utf8") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)  # type: ignore

TEST_REQUIRES = [
    "pytest>=5.3.5",
    "responses>=0.10.14",
    "freezegun>=0.3.15",
    "typing_extensions>=3.7.4",
]
DEV_REQUIRES = ["mypy>=0.790", "flake8>=3.8.3", "black==20.8b0", "pre-commit>=2.1.1"]

setup(
    name="pano",
    description="Panoramic Command Line Tool",
    url="https://github.com/panoramichq/panoramic-cli",
    project_urls={"Source Code": "https://github.com/panoramichq/panoramic-cli"},
    author="Panoramic",
    maintainer="Panoramic",
    version=version,
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_namespace_packages(where='src', include=["panoramic.*"]),
    package_dir={"": "src"},
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=[
        'click>=7.1.2',
        'colorama>=0.4.3',
        'PyYAML==5.3.1',
        'packaging>=20.4',
        'tqdm==4.47.0',
        'python-dotenv>=0.14.0',
        'jsonschema>=3.0',
        'requests>=2.18.0',
        "importlib_resources ; python_version<'3.7'",
        "analytics-python==1.2.9",
        "sqlalchemy>=1.3.19",
        "snowflake-sqlalchemy==1.2.3",
        "pybigquery==0.4.15",
        "schematics==2.1.0",
        "pydash==4.7.4",
        "Unidecode==1.0.23",
        "networkx==2.3",
        "xxhash==1.3.0",
        "antlr4-python3-runtime==4.8",
        "pydantic==1.5.1",
        "docstring_parser==0.7.3",
    ],
    extras_require={"tests": TEST_REQUIRES, "dev": TEST_REQUIRES + DEV_REQUIRES},
    include_package_data=True,
    entry_points={"console_scripts": ["pano=panoramic.cli:cli"]},
)
