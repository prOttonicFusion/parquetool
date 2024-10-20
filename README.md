# parquetool
![Python version](https://img.shields.io/badge/python-v3.10+-blue)


A command-line utility for previewing & analyzing [Apache Parquet](https://parquet.apache.org/) files

## Installation 
```sh
git clone git@github.com:prOttonicFusion/parquetool.git
cd parquetool
pip3 install -r requirements.txt
```

## Usage

```sh
python parquetool.py --help
```

## Development

Install the development dependencies using
```sh
make dep-dev
```
after which you can test, lint and typecheck the code with
```sh
make test
make lint
make typecheck
```