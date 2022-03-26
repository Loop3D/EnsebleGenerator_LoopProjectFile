  
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Perturb",
    version="0.0.01",
    author="Kavitha Madaiah",
    author_email="kavitha.madaiah@uwa.edu.au",
    description="A package to perturb interface ,orientation and drillhole data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Loop3D/EnsembleGenerator_LoopprojectFile",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
