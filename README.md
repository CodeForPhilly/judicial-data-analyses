# jat-data-analyses

Data analyses for judge docket data.
See the [pm0kjp/pa-judicial-data](https://github.com/pm0kjp/pa-judicial-data) for an explanation of the docket data, and context on the problem as a whole.

## Accessing data

`machow` on the [`#sjh-data-committee` channel on Code for Philly slack](https://codeforphilly.org/chat?channel=sjh-data-committee) can share a copy of the data.
He can also give you access to a cloud bucket with the data, where you can also store the
results of your analyses.

## Adding to this repository

This repository has analyses in the following structure:

* Each analysis is in its own folder. It's okay for an analysis to produce multiple reports.
* Its okay to make a new folder, even if your analysis is similar to someone else's.

## Getting started

* See [_to-parquet](./_to-parquet) for the script that converted the raw data to parquet.
* See [jake-example](./jake-example) for an example analysis.
