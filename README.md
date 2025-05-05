# MsPASS Launchers
This repository contains a set of "launcher" that simplify launching MsPASS on different platforms.  MsPASS is normally run packaged in a docker container.  That has major advantages in consistency of results as it reduces platform dependencies in the code base.  The disadvantage it create is that working with the framework is different from most user experience.  We have learned it best to think of the framework as defined by four services run in containers:  
1.  A MongoDB database service
2.  A parallel scheduler (dask or pyspark)
3.  One or more parallel worker instances
4.  A "frontend" that will run jupyter notebooks or python scripts directly.

The original bare-metal mspass repository has only some awkward shell script methods to launch MsPASS on HPC systems and a CLI docker incantation to run the package on Desktops.  The purpose of this repository is to provide simpler to use and maintain launchers for different platforms.  The current repository has support for two setups:
1.  A main program called `mspass_desktop` launches a simple graphical user interface to launch the package.   A section of the mspass User's manual is under construction to provide simple instructions for using this (python) program.
2.  In python/launchers is a module called `HPCLaucher.py`.   It can be used to simplify running MsPASS on HPC systems in batch mode.  It is driven by a configuration file that needs to be customized for a given site and processing set to controll how many workers a job will need to run.  Again, a section in the User's Manual is under construction to describe how to use HPCLauncher.

In addition, we expect to eventually add a launcher to simply running MsPASS on cloud systems like Amazon Web Services where Earthscope is in the process of moving their seismic data archives.   That will not appear, however, until Earthscope has a stable environment for interaction with the archive.  
