<h1 align="center">🚬 carton</h1>

<h3 align="center">Simplest log-driven document-oriented interface to RDBMS</h3>

<p align="center">
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/lint.yml"><img alt="Lint Status" src="https://github.com/MentalBlood/carton/actions/workflows/lint.yml/badge.svg"></a>
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/typing.yml"><img alt="Typing Status" src="https://github.com/MentalBlood/carton/actions/workflows/typing.yml/badge.svg"></a>
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/complexity.yml"><img alt="Complexity Status" src="https://github.com/MentalBlood/carton/actions/workflows/complexity.yml/badge.svg"></a>
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/tests.yml"><img alt="Tests Status" src="https://github.com/MentalBlood/carton/actions/workflows/tests.yml/badge.svg"></a>
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/coverage.yml"><img alt="Coverage Status" src="https://github.com/MentalBlood/carton/actions/workflows/coverage.yml/badge.svg"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href="https://www.python.org/"><img alt="Python version: >=3.7" src="https://img.shields.io/badge/Python-3.7%20|%203.8%20|%203.9%20|%203.10%20|%203.11%20|%203.12-blue"></a>
</p>

## Basic concepts

It uses two tables:

Main one:

- `id` is just autoincrementing primary key
- `time` is date and time when row was added
- `package` is identifier of package
- `key`
- `value`
- `actual` is boolean indicator of value actuality

with indexes on

- `time`
- `key`, `value` where `actual` is `true`
- `package`, `key`, `value`

Additional one used for keys dynamic enum implementation:

- `id` is just autoincrementing primary key
- `key` is explicit key representation

with index on `key`

So `select` operation implemented as taking `key` and `value` and returning all corresponding packages actual properties

## Some implementation details

`select` method is implemented as generator avoiding excessive memory usage and thick queries

`insert` method uses `N + 2` `execute` related calls where `N` is number of packages with no package identifier provided

There is no need to generate package identifier `package` as it taken to be `COALESCE((SELECT MAX(package) FROM carton), -1) + 1` by default

`key` enum related transformations implemented using non-invalidating cache

## Compared to [conveyor](https://github.com/MentalBlood/conveyor)

|                                          |                           carton |                     conveyor |
| ---------------------------------------- | -------------------------------: | ---------------------------: |
| core classes amount                      |                                3 |                           27 |
| storage type                             |                              RDB | RDB or files or user-defined |
| workers concept                          |                    semi-immanent |                          yes |
| automatic migrations                     |                       not needed |                          yes |
| update operation                         | for boolean column `actual` only |          for all stored data |
| delete operation                         |                               no |                          yes |
| entity properties limit (when using RDB) |       maximum integer type value |       maximum columns amount |
| reserving                                |                               no |                          yes |
| logging                                  |                         immanent |          secondary, optional |
| current time obtaining side              |                         database |                      library |
| identifiers generation side              |                         database |                      library |
| cross-repository transactions            |                       not needed |                          yes |
