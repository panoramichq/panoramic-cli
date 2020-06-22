from setuptools import find_namespace_packages, setup


TEST_REQUIRES = ["pytest>=5.3.5", "responses>=0.10.14", "freezegun>=0.3.15"]
DEV_REQUIRES = ["mypy>=0.780", "flake8>=3.8.3", "black>=19.10b0", "pre-commit>=2.1.1"]


VERSION = "0.1.0"
setup(
    name="panoramic-cli",
    description="Panoramic CLI",
    url="https://github.com/panoramichq/panoctl",
    project_urls={"Source Code": "https://github.com/panoramichq/panoctl"},
    author="Panoramic",
    maintainer="Panoramic",
    keywords=["TODO"],
    version=VERSION,
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_namespace_packages(where='src', include=["panoramic.*"]),
    package_dir={"": "src"},
    python_requires=">=3.6",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=['panoramic-auth>=0.1.0', 'requests>=2.24.0', 'click>=7.1.2', 'PyYAML==5.3.1'],
    extras_require={"tests": TEST_REQUIRES, "dev": TEST_REQUIRES + DEV_REQUIRES},
    include_package_data=True,
    entry_points={"console_scripts": ["panoctl=panoramic.cli:cli"]},
)
