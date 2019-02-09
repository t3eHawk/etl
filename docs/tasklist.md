# Project Requirements and Task List.
Here are collected all project requirements.

## Object - Database
### Must Have
- [x] Provide simple connection to any DB of any vendor.
- [x] Connect via the set of parameters (username, password, host, port, instance).
- [x] Connect via the credentials string.
- [x] Connect via the configuration INI file.
- [x] Give common interface to interact with the DB.
- [x] Give API to create table using arguments with the table name and definition.
- [x] Read the table definition from Python dict or JSON file.
- [ ] Configure the tables with the parameters:
  - [ ] Name.
  - [x] Compression.
  - [x] Primary key.
  - [x] Foreign key.
  - [ ] Partitions.
- [ ] Configure the columns with the parameters:
  - [x] Name.
  - [x] Data type.
  - [x] Default value.
  - [x] Unique.
  - [x] Not null.
  - [x] Auto increment.
  - [x] Primary key.
  - [x] Foreign key.
  - [ ] Index.

## Object - LinkLoader
### Must Have
- [x] Give access to the DB though the Database object.
- [x] Reflect the name of the source DB.
- [x] Reflect the name of the link for source DB.
- [x] Reflect the name of the loading table.
- [x] Read the configuration from Python dict or JSON file.
- [x] Support objects:
  - [x] Table *DS* - DS_{source DB name}_{table name} - main table with loaded
  data on target DB.
  - [x] Table *TEMP* - TEMP_{source DB name}_{table name} - temporary table raw
  data before it inserted to *DS*.
  - [x] Table *LOG* - LOG_{source DB name}_{table name} - table with the history
  of loading procedures reflecting the time, status and other metrics of each
  load.
- [ ] Give built-in ETL methods:
  - [x] Method *extract()* to extract data from original source to *TEMP*.
  - [ ] Method *transform()* to transform data in *TEMP* before it loaded to *DS*.
  - [x] Method *load()* to load finally data from *TEMP* to *DS*.
- [x] Define and update during the ETL procedure *LOG* metrics: load id, load
status, load start timestamp, load end timestamp, records found, records loaded,
last status.
- [ ] Status must reflect last loading condition and can be one of four:
  * 0 - started.
  * 1 - exported.
  * 2 - transformed.
  * 3 - loaded.
- [ ] There must be a way to choose between regular insert, full table refresh,
table merge/update.
- [x] There must be a way to choose between full table extract or filter it.
- [ ] Provide special exceptions for error cases.

### Should Have
- [x] Add basic bind parameters that may be used during ETL.
- [ ] Add configuration of status.
- [ ] Provide method or decorator for simple status update in *LOG*.
- [ ] Provide base classes for extraction, transformation and loading.
- [ ] Method *transform* should get functions that somehow transform the data.
- [ ] There should be a way to check loading data on duplicates.
due to some reason.

### Could Have
- [ ] Do not declare *Database* object separately but inside the *LinkLoader*
by common INI or JSON.
- [ ] Log Python actions to the file. Nice to have opportunity to choose
between the own or the existing logger.
- [ ] Provide the most popular transformation methods.
- [ ] There could be an error handler for records that was not loaded to *DS*.

### Won't Have
- [ ] Resume load from certain stage.
