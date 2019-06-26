# db-csv-load

Command to connect to an Oracle Database and quickly import data from CSV format into a Oracle table

## Prerequisites

Oracle Instant Client must already be installed

[Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html)

Note - Oracle Instant Client must be configured per your environment (please follow the instructions provided by Oracle).

## Table of Contents

- [db-csv-load](#db-csv-load)
  - [Prerequisites](#Prerequisites)
  - [Table of Contents](#Table-of-Contents)
  - [Installation](#Installation)
  - [Building](#Building)
  - [Usage](#Usage)
  - [Support](#Support)
  - [Contributing](#Contributing)

## Installation

1) Clone this repository into a local directory, copy the db-csv-load executable into your $PATH

```bash
$ git clone https://github.com/apexevangelists/db-csv-load
```

## Building

Pre-requisite - install Go

Compile the program -

```bash
$ go build
```

## Usage

```bash-3.2$ ./db-csv-load -h
Usage of ./db-csv-load:
  -configFile string
    	Configuration file for general parameters (default "config")
  -connection string
    	Configuration file for connection
  -db string
    	Database Connection, e.g. user/password@host:port/sid
  -debug
    	Debug mode (default=false)
  -delimiter string
    	Delimiter between fields (default ",")
  -enclosedBy string
    	Fields enclosed by (default "\"")
  -i string
    	Input Filename
  -input string
    	Input Filename
  -t string
    	Table to import into
  -table string
    	Table to import into

bash-3.2$
```

## Support

Please [open an issue](https://github.com/apexevangelists/db-csv-load/issues/new) for support.

## Contributing

Please contribute using [Github Flow](https://guides.github.com/introduction/flow/). Create a branch, add commits, and [open a pull request](https://github.com/apexevangelists/db-csv-load/compare).