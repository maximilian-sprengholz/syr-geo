# SYR Survey Geographical Context Data


## Project organization

```
.
├── .gitignore
├── environment.yml
├── LICENSE.md
├── Makefile
├── README.md
├── docs                    <- Documents
└── src                     <- Source code

```
*RO* = read-only, *HW* = human-writeable, *PG* = project-generated. Repository organization implemented with [cookiecutter](https://github.com/cookiecutter/cookiecutter) using an adapted version of the [good-enough-project template](https://github.com/bvreede/good-enough-project) by Barbara Vreede. The fork is available [here](https://github.com/maximilian-sprengholz/good-enough-project).

## Usage

To replicate the data processing and docs, you need to have [conda](https://docs.conda.io/en/latest/miniconda.html) installed and `conda` available via shell:

```bash
# create and activate conda environment (initialized as subdirectory ./env)
cd /path/to/syr-geo
conda env create --prefix ./env --file environment.yml
conda activate ./env
# run
make checksetup
make manuscript
```

## License

This project is licensed under the terms of the [MIT License](/LICENSE.md)