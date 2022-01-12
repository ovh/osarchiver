
# OSArchiver: OpenStack databases archiver

OSArchiver is a python package that aims to archive and remove soft deleted data from OpenStack databases.
The package is shiped with a main script called osarchiver that reads a configuration file and run the archivers.

# Philosophy

* OSArchiver doesn't have any knowledge of Openstack business objects
* OSArchiver purely relies on the common way of how OpenStack marks data as deleted by setting the column 'deleted_at' to a datetime.
It means that a row is archivable/removable if the 'deleted_at' column is not NULL

# Limitations

* Support Mysql/MariaDB as db backend.
* python >= 3.5

# Design

OSArchiver reads an INI configuration file in which you can define:

* archivers: a section that hold one source and a non mandatory list of destinations
* sources: a section that define a source of where the data should be read (basically the OS DB)
* destinations: a section that define where the data should be archived

# How does it works:

                                       .----------.
            .--------------------------| Archiver |-----------------------------.
            |                          '----------'                             |
            |                                                                   |
            |                                                                   |
            |                                                                   |
            v                        _______________                            v
       .--------.                    \              \                    .-------------.
       | Source |-------------------->) ARCHIVE DATA )------------------>| Desinations |
       '--------'                    /______________/                    '-------------'
            |                                |                                  |
            |                                |                                  |
            |                                |                                  |
            |                                |                                  |
            |                                |                                  |
            |                                v                                  |
            |                  .--------------------------.                     |
            v                 ( No error and delete_data=1 )                    |
                               '--------------------------'                     |
        _.-----._                            |                    _.-----._     |
      .-         -.                          |                  .-         -.   |   ___   
      |-_       _-|                          |                  |-_       _-|   |  |   |\ 
      |  ~-----~  |                          |                  |  ~-----~  |<--'->|   ' ___   
      |           |                          |                  |           |      | SQL|   |\ 
      `._       _.'                          |                  `._       _.'      |____|   '-|---.
         "-----"                             |                     "-----"              | CSV |   |
      OpenStack DB                           v                  Archiving DB            |_____|   |
            ^                        _______________                                              v
            |                        \              \                                 .-----------------------.
            '-------------------------) DELETE DATA  )                               ( remote_store configured )
                                     /______________/                                 '-----------------------'
                                                                                                  |
                                                                                                  v
                                                                                             __________ 
                                                                                            [_|||||||_°]
                                                                                            [_|||||||_°]
                                                                                            [_|||||||_°]

                                                                                     Remote Storage (Swift, ...)

# Installation

```
git clone https://github.com/ovh/osarchiver.git
cd osarchiver
pip install -r requirements.txt
pip setup.py install
```

# osarchiver script

```
# osarchiver --help
usage: osarchiver [-h] --config CONFIG [--log-file LOG_FILE]
                  [--log-level {info,warn,error,debug}] [--debug] [--dry-run]

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG       Configuration file to read
  --log-file LOG_FILE   Append log to the specified file
  --log-level {info,warn,error,debug}
                        Set log level
  --debug               Enable debug mode
  --dry-run             Display what would be done without really deleting or
                        writing data
```

# Configuration
The configuation is an INI file containing several sections. You configure your
differents archivers in this configuration file. An example is available at the
root of the repository.

## DEFAULT section:
* Drescription: default section that define default/fallback value for options
* Format **[DEFAULT]**
* configuration parameters: all the parameters of archiver, source, destination
  and backend section can be added in this section, those will be the fallback
  value if the value is not set in a section.

## Archiver section:

* Description: defines where to read data and where to archive them and/or delete.
* Format **[archiver:*name*]**
* configuration parameters:
    * **src**: name of the src section
    * **dst**: comma separated list of destination section names
    * **enable**: 1 or 0, if set to 0 the archiver is ignored and not run

Example:
```properties
[archiver:My_Archiver]
src: os_prod
dst: file, db

[src:os_prod]
...

[dst:file]
...

[dst:db]
....
```

## Source section:

* Description: defines where the OpenStack database are. It supports for now
  one backend (db) but it may be easily extended
* Format **[src:*name*]**
* configuration parameters:
    * **backend**\*: the name of backend to use, only `db` is supported
    * **retention**: 12 MONTH
    * **archive_data**: 0 or 1 if set to 1 expect a dest to archive the data else
      won't run the archiving step just the delete step.
    * **delete_data**: 0 or 1 if set to 1 will run the delete step. If the
      archive step fails the delete step is not run to prevent loose of data.
    * *backend specific options*


## Destination section:

* Description: defines where the data should be written. It supports for now
  two backends (db for datatabase and file [csv, sql]) and may be extended
* Format **[dst:*name*]**
* configuration parameters:
    * **backend**\*: the name of backend to use, `db` or `file`
    * *backend specific options*


## Backends options:

### db
* Description: is the database (mysql/mariadb) backend
* options:
    * **host**: DB host to connect to
    * **port**: port of MariaDB server is running on
    * **user**: login of MariaDB server to connect with
    * **password**: password of user
    * **delete_limit**: apply a LIMIT to DELETE statement
    * **select_limit**: apply a LIMIT to SELECT statement
    * **bulk_insert**: data are inserted in DB every builk_insert rows
    * **deleted_column**: name of column that holds the date of soft delete, is
      also used to filter table to archive, it means that the table must have
      the deleted_column to be archived
    * **where**: the literal SQL where applied to the select statement
    Ex: where=${deleted_column} <= SUBDATE(NOW(), INTERVAL ${retention})
    * **retention**: how long time of data to keep in database (SQL format: 12
      MONTH, 1 DAY, etc..)
    * **excluded_databases**: comma, cariage return or semicolon separated
      regexp of DB to exclude when specfiying '*' as database. The following DB
      are akways ignored:  'mysql', 'performance_schema', 'information_schema'
    * **excluded_tables**: comma, cariage return or semicolon separated regexp
      of DB to exclude when specifying '*' as table. Ex: shadow_.*,.*_archived
    * **db_suffix**: a non mendatory suffix to apply to the archiving DB. The
    default suffix '_archive' is applied if you archive on same host than
    source without setting a db_suffix or table_suffix (avoid reading and
    writing on the same db.table)
    * **table_suffix**: apply a suffix to the archinving table if specified

### file
* Description: is the file archiving destination type, it writes SQL data in a
  file using one or several formats (supported: SQL, CSV)
    * **directory**: the directory path where to archive data. You may use the
      {date} keyword to append automaticaly the date to the directory path.
      (/backup/archive_{date})
    * **formats**: a comma, semicolon or cariage return separated list that
      define the format in witch archive the data (csv, sql)

You've developed a new cool feature ? Fixed an annoying bug ? We'd be happy

to hear from you !

Have a look in [CONTRIBUTING.md](https://github.com/ovh/osarchiver/blob/master/CONTRIBUTING.md)

# Related links

 * Contribute: https://github.com/ovh/osarchiver/blob/master/CONTRIBUTING.md
 * Report bugs: https://github.com/ovh/osarchiver/issues

# License

See https://github.com/ovh/osarchiver/blob/master/LICENSE
