# *EnsembleGenerator_LoopProjectFile* <br> 
***EnsembleGenerator_LoopProjectFile*** is a python library that perturbes the input data in LoopProjectFile(netcdf format) from map2l.It provides Functions for generating and analysing ensembles of geological models.Perturbed files are to the number of perturbation specified in .ini file.The output are available in csv and loop3d format.

**Mark Jessell**(Mark.Jessel@uwa.edu.au) contributed the original idea, The code development is led by **Kavitha Madaiah** (kavitha.madaiah@uwa.edu.au). **Evren Pakyuz-Charrier1** has provided significant inputs from his phd work .**Mark Lindsay** has made significant contributions to code developed using csv files as input.


# What is a Loop Project File?
A Loop Project File encapsulates all the data and models used or created in a Loop workflow.  This includes the ability to create meta-data which provides provenance of the data and the methodology and history of how models are used and produced.  The Loop Project File is based on netCDF and has APIs for C/C++, Python and Fortran.
- [Loop Project File - C/C++](https://github.com/Loop3D/LoopProjectFile-cpp)
- [Loop Project File - Python](https://github.com/Loop3D/LoopProjectFile)

# Why use a project file?
There are several benefits to using a single project file.  These include encapsulating the information in a single place, server storage and retrieval enabling batching of model creation over multiple servers, easily shared workflows, and storage efficiency among many more.

# Why use netCDF?
netCDF enables multi-dimensional data structure storage in a highly compressed format as well as data attribution for meta-data all within a single data file.

## Ensemble_Generator which uses csv files as input(by mark lindsay):
https://github.com/Loop3D/ensemble_generator



## workflow of EnsembleGenerator_LoopProjectFile:
Source of model and data: map2loop
1.input files in Netcdf_map2l in the following link.
	http://localhost:8888/tree/Netcdf_map2l
2.Perturb_config.ini which has all the inputs required for perturbation in the following 
	http://localhost:8888/tree/notebook
3.run the jupyter notebook in the following.
	http://localhost:8888/tree/notebook
	http://localhost:8888/notebooks/notebook/Perturb_interface_orientation_drillhole.ipynb
4.Output file both csv and loop3d formats available in the following link
	http://localhost:8888/tree/egen_runs
	
## Where to start for EnsembleGenerator_LoopProjectFile :

1. Install the dependencies:
- LoopProjectFile (https://github.com/Loop3D/LoopProjectFile) <br>
`Add LoopProjectFile code` <br>
- pandas (https://pandas.pydata.org/)  <br>
`pip install pandas` or `conda install pandas`<br>
- numpy (https://github.com/numpy/numpy)  <br>
`pip install numpy` or `conda install numpy` <br>
https://github.com/Loop3D/map2loop-2  <br>
`Add map2loop code` <br>
- netcdf4 (https://pypi.org/project/netCDF4/)
`pip install netcdf4` or `conda install netcdf4` <br>
-- Geopandas (https://pypi.org/project/netCDF4/)
`conda install geopandas` or `conda install -c conda-forge geopandas`


2. Clone the repository: <br>
`$ git clone https://github.com/Loop3D/EnsebleGenerator_LoopProjectFile.git`


3. Install from your local drive <br>
`cd \local_drive\` <br>
`python setup.py install --user --force`


## More information on the Monte Carlo simulation for uncertainty estimation.
https://www.researchgate.net/publication/324262019_Monte_Carlo_simulation_for_uncertainty_estimation_on_structural_data_in_implicit_3-D_geological_modeling_a_guide_for_disturbance_distribution_selection_and_parameterization

## Problems
For any bugs/feature requests/comments, please create a new https://github.com/Loop3D/EnsebleGenerator_LoopProjectFile/issues







