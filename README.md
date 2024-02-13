<h1 align="center">🚬 carton</h1>

<h3 align="center">Core for log driven data processing applications</h3>

<p align="center">
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/lint.yml"><img alt="Lint Status" src="https://github.com/MentalBlood/carton/actions/workflows/lint.yml/badge.svg"></a>
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/typing.yml"><img alt="Typing Status" src="https://github.com/MentalBlood/carton/actions/workflows/typing.yml/badge.svg"></a>
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/complexity.yml"><img alt="Complexity Status" src="https://github.com/MentalBlood/carton/actions/workflows/complexity.yml/badge.svg"></a>
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/tests.yml"><img alt="Tests Status" src="https://github.com/MentalBlood/carton/actions/workflows/tests.yml/badge.svg"></a>
<a href="https://github.com/MentalBlood/carton/blob/master/.github/workflows/coverage.yml"><img alt="Coverage Status" src="https://github.com/MentalBlood/carton/actions/workflows/coverage.yml/badge.svg"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

Simple and reliable approach for data processing system storage

It uses two tables:

Main one:

- `id` is just autoincrementing primary key
- `time` is date and time when row was added
- `package` is identifier of package
- `key`
- `value`

with indexes on

- `time`
- `package`
- `value`
- `id`, `key`

Additional one used for keys dynamic enum implementation:

- `id` is just autoincrementing primary key
- `key` is explicit key representation

with index on `key`

So `select` operation implemented as taking

- `present` - dictionary of key/value pairs presented in packages
- `absent` - dictionary of key/value pairs not presented in packages
- `get` - set of key/value pairs to get

and generating dictionaries each of `package` and `get` contained keys
