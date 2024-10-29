---
title: "Managing Dask Software Environments with Conda"
date: "2021-11-24"
categories: 
  - "dask"
---

# Managing Dask Software Environments with Conda

This post shows you how to set up conda on your machine and explains why it’s the best way to manage software environments for Dask projects.

 
This post will cover the following topics:

1. Install Miniconda
2. Install dependencies in base environment
3. Create separate software environments
4. Useful conda commands
5. Difference between conda and conda-forge

Most data scientists have a hard time managing software environments and debugging issues.  They absolutely hate the trial and error process that’s required to get a local environment properly set up.

Pay attention to this post closely so you can better understand the process and train yourself how to effectively debug environment issues.

## Install Miniconda

Go to [the conda page](https://docs.conda.io/en/latest/miniconda.html) to download the installer that suits your machine.  There are a plethora of options on the page.  It’s easiest to pick the Latest Miniconda Installer Link for your operating system.

I am using a Mac, so I use the Miniconda3 MaxOSX 64-bit pkg link.

Open the downloaded package file and it’ll walk you through the installation process.

![](https://lh6.googleusercontent.com/XoaUG8eGKBCvkNG938fdau6GFkC57diKKkzalEBz6sbm6INmt7XnJOGU5OANEjfrDGQoe9Yz_Y_s3M1KVYplgf2xWUsUr3W3Qaw0r7nw93OqEuDNlE2HI6fWUBhWF94nTpFGbh2b)

Close out your Terminal window, reopen it, and you should be ready to run conda commands.  Make sure the `conda version` runs in your Terminal to verify the installation completed successfully.

## Install dependencies in base

The default conda environment is called “base”.

You can run `conda list` to see the libraries that are installed in base.

```
# packages in environment at /Users/powers/opt/miniconda3:
#
# Name                    Version                   Build  Channel
brotlipy                  0.7.0           py39h9ed2024_1003  
ca-certificates           2021.7.5             hecd8cb5_1  
certifi                   2021.5.30        py39hecd8cb5_0  
cffi                      1.14.6           py39h2125817_0  
chardet                   4.0.0           py39hecd8cb5_1003  
conda                     4.10.3           py39hecd8cb5_0  
conda-package-handling    1.7.3            py39h9ed2024_1  
cryptography              3.4.7            py39h2fd3fbb_0  
idna                      2.10               pyhd3eb1b0_0  
libcxx                    10.0.0                        1  
libffi                    3.3                  hb1e8313_2  
ncurses                   6.2                  h0a44026_1  
openssl                   1.1.1k               h9ed2024_0  
pip                       21.1.3           py39hecd8cb5_0  
pycosat                   0.6.3            py39h9ed2024_0  
pycparser                 2.20                       py_2  
pyopenssl                 20.0.1             pyhd3eb1b0_1  
pysocks                   1.7.1            py39hecd8cb5_0  
python                    3.9.5                h88f2d9e_3  
python.app                3                py39h9ed2024_0  
readline                  8.1                  h9ed2024_0  
requests                  2.25.1             pyhd3eb1b0_0  
ruamel_yaml               0.15.100         py39h9ed2024_0  
setuptools                52.0.0           py39hecd8cb5_0  
six                       1.16.0             pyhd3eb1b0_0  
sqlite                    3.36.0               hce871da_0  
tk                        8.6.10               hb0a8c7a_0  
tqdm                      4.61.2             pyhd3eb1b0_1  
tzdata                    2021a                h52ac0ba_0  
urllib3                   1.26.6             pyhd3eb1b0_1  
wheel                     0.36.2             pyhd3eb1b0_0  
xz                        5.2.5                h1de35cc_0  
yaml                      0.2.5                haf1e3a3_0  
zlib                      1.2.11               h1de35cc_3
```

Run `conda install -c conda-forge dask` to install Dask in the base environment.

This will install Dask and all of its transitive dependencies.

Run `conda list` again and you’ll see a ton of new dependencies in the environment, including pandas and Dask.

Dask depends on other libraries, so when you install Dask conda will install both the Dask source code and the source code of all the libraries that Dask depends on (aka transitive dependencies).

## Difference between conda and conda-forge

Conda hosts 720+ official packages.  Community contributed packages are stored in conda-forge.

conda-forge is referred to as a “channel”.

Let’s inspect the Dask installation command we ran earlier: `conda install -c conda-forge dask`

The `-c conda-forge` part of the command is instructing conda to fetch the Dask dependency from the conda-forge channel.

## Create separate software environments

You can specify a list of dependencies in a YAML file and run a command to create a software environment with all of those dependencies (and their transitive dependencies).

This workflow is more complicated, but easier to maintain in the long run and more reliable.  It also allows your teammates to easily recreate your environment, which is key for collaboration.

You’re likely to have different projects with different sets of dependencies on your computer.  Multiple environments allow you to switch the dependencies for the different projects you’re working on.

Take a look at the following YAML file with conda dependencies:

```
name: standard-coiled
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.9
  - pandas
  - dask[complete]
  - pyarrow
  - jupyter
  - ipykernel
  - s3fs
  - coiled
  - python-blosc
  - lz4
  - nb_conda
  - jupyterlab
  - dask-labextension
```

We can run a single command to create the standard-coiled environment specified in the YAML file.  Clone the [coiled-resource repository](https://github.com/coiled/coiled-resources) and change into the project root directory to run this command on your machine.

`conda env create -f envs/standard-coiled.yml`

You can activate this environment and use all the software you just installed by running `conda activate standard-coiled`.

  
This is a completely different environment than the `base` environment.  You can switch back and forth between the two environments to easily switch between the different sets of dependencies.

## Useful conda commands

You can run conda env list to see all the environments on your machine.

```
# conda environments:
#
base                     /Users/powers/opt/miniconda3
standard-coiled       *  /Users/powers/opt/miniconda3/envs/standard-coiled
```

The star next to `standard-coiled` means it’s the active environment.

Change back to the base environment with `conda activate base`.

Delete the standard-coiled environment with `conda env remove --name standard-coiled`.

If an environment gets in a weird state, you can easily delete it and recreate it from the YAML file. Easily recreating environments is a big advantage of creating environments from YAML files. YAML files can also be referred to in the future as a reminder of how the environment was originally created.

## Dependency hell

It takes a while for conda to create an environment because it needs to perform dependency resolution and download the source code for the right libraries on your machine.

Dependency resolution is when conda figures out the set of dependency versions that’ll satisfy the version requirement of each dependency / transitive dependency for the environment.

Dependency hell is an uncomfortable situation when the dependencies cannot be resolved. Luckily conda is a mature technology and is good at resolving the dependencies whenever possible, thus saving you from dependency hell.

There are times when conda won’t be able to resolve a build. That’s when you should try relaxing version constraints or installing all packages at the same time. Conda has a harder time correctly working out the dependencies when they’re installed one-by-one on the command line. It’s best to put all the dependencies in a YAML file and install them all at once, so conda can perform the full dependency resolution process.

## Apple M1 Chip gotcha

If you are using an Apple computer with a M1 chip, you may want to try mambaforge instead of Miniconda. See this blog post on using Conda with Mac M1 machines for more details.

  
Type in `conda info` and look at the results.

```
active environment : base
    active env location : /Users/powers/opt/miniconda3
            shell level : 1
       user config file : /Users/powers/.condarc
 populated config files : 
          conda version : 4.10.3
    conda-build version : not installed
         python version : 3.9.5.final.0
       virtual packages : __osx=10.16=0
                          __unix=0=0
                          __archspec=1=x86_64
       base environment : /Users/powers/opt/miniconda3  (writable)
      conda av data dir : /Users/powers/opt/miniconda3/etc/conda
  conda av metadata url : None
           channel URLs : https://repo.anaconda.com/pkgs/main/osx-64
                          https://repo.anaconda.com/pkgs/main/noarch
                          https://repo.anaconda.com/pkgs/r/osx-64
                          https://repo.anaconda.com/pkgs/r/noarch
          package cache : /Users/powers/opt/miniconda3/pkgs
                          /Users/powers/.conda/pkgs
       envs directories : /Users/powers/opt/miniconda3/envs
                          /Users/powers/.conda/envs
               platform : osx-64
             user-agent : conda/4.10.3 requests/2.25.1 CPython/3.9.5 Darwin/20.3.0 OSX/10.16
                UID:GID : 501:20
             netrc file : None
           offline mode : False
```

The platform is `osx-64`, which is not optimized for Mac M1 chips. The libraries downloaded with this setup are run through an emulator, which can cause a performance drag.

## Conclusion

Conda is a great package manager for Python data science because it can handle the dependencies that are difficult to install like xgboost and cuda.

Data science libraries like xgboost contain a lot of C++ code and that’s why they’re hard for package managers to handle properly.

Python has other package managers, like Poetry, that work fine for simple Python builds that rely on pure Python libraries. Data scientists don’t have the luxury of working with pure Python libraries, so Poetry isn’t a good option for data scientists. Conda is the best option for Python data workflows.

This post taught you how to install conda, run basic commands, and manage multiple software environments. Keep practicing till you’ve memorized all the commands in this post and conda comes naturally for you. Installing software is a common data science pain point and it’s worth investing time studying the basics, so you’re able to debug complex installation issues.

Now is a good time to read the post on the [Dask JupyterLab workflow](https://coiled.io/blog/dask-jupyterlab-workflow/) that’ll teach you a great conda-powered Dask development workflow.
