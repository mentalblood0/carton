import pathlib

import setuptools

if __name__ == "__main__":
    setuptools.setup(
        name="carton",
        version="0.3.0",
        python_requires=">=3.7",
        keywords=["data-processing"],
        url="https://github.com/MentalBlood/carton",
        description="Core for log driven data processing applications",
        long_description=(pathlib.Path(__file__).parent / "README.md").read_text(),
        long_description_content_type="text/markdown",
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Developers",
            "Topic :: Database",
            "Topic :: Software Development :: Libraries",
            "Typing :: Typed",
            "Topic :: System :: Logging",
            "Operating System :: OS Independent",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "License :: OSI Approved :: BSD License",
        ],
        author="mentalblood",
        author_email="neceporenkostepan@gmail.com",
        maintainer="mentalblood",
        maintainer_email="neceporenkostepan@gmail.com",
        packages=setuptools.find_packages(exclude=["tests"]),
    )
