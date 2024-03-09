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

`sentences`:

- `id` is just autoincrementing primary key
- `time` is date and time when row was added
- `subject` is subject identifier
- `predicate` is predicate applied to subject at given time
- `actual` is boolean indicator of predicate actuality

with indexes on

- `time`
- `predicate` where `actual` is `true`
- `subject`, `actual`

`predicates`:

- `id` is just autoincrementing primary key
- `predicate` is explicit predicate representation

with index on `predicate`

Predicates are of form `key[=value]`

So `select` operation implemented as taking `predicate` and returning all corresponding subjects actual properties

`select` method is implemented as generator avoiding excessive memory usage and thick queries

There is no need to generate package identifier `package` as it taken to be `COALESCE((SELECT MAX(package) FROM carton), -1) + 1` by default

## Concrete RDB types

### SQLite

#### `sentences`

| name      | type                                       |
| --------- | ------------------------------------------ |
| id        | integer primary key autoincrement          |
| time      | datetime default(datetime('now')) not null |
| subject   | integer not null                           |
| predicate | integer not null                           |
| actual    | integer default(1) not null                |

#### `predicates`

| name      | types                             |
| --------- | --------------------------------- |
| id        | integer primary key autoincrement |
| predicate | text unique                       |

### PostgreSQL

#### `sentences`

| name      | type                                                 |
| --------- | ---------------------------------------------------- |
| id        | bigserial primary key                                |
| time      | timestamp default(now() at time zone 'utc') not null |
| subject   | bigint not null                                      |
| predicate | bigint not null                                      |
| actual    | boolean default(true) not null                       |

#### `predicates`

| name      | types                 |
| --------- | --------------------- |
| id        | bigserial primary key |
| predicate | text unique           |
