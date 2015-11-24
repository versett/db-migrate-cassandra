# db-migrate-cassandra
Cassanda migration using CQLSH  for node DB migration

This module is based on node-db-migrate to support migration for Cassandra database, using node cassadnra-driver. This has to be installed as a dependency for db-migrate.

## Installation
```
npm install db-migrate
npm install db-migrate-cassandra
```

## Usage
Set up your database.json as mentoned in `database.json.example`

## Supported Migrations
* Create Table
  ```js
  exports.up = function (db, callback) {
    db.createTable('users', {
    'name': 'varchar',
    'age': 'int'
    }, {
    'primary_key': 'name'
    }, callback);
 };
 ```
 
  Supports multiple parimary keys

  ```js
  'primary_key': '(name, age)'
  ```

* Drop Table
  ```js
  exports.up = function (db, callback) {
    db.dropTable('users', callback);
  };
  ```
  
* Add new column
  ```js
  exports.up = function (db, callback) {
    db.addColumn('users', 'age', 'int', callback);
  };
  ```
  
* Drop existing column
  ```js
  exports.up = function (db, callback) {
    db.removeColumn('users', 'age', callback);
  };
  ```
  
* Rename a column
  ```js
  exports.up = function (db, callback) {
    db.renameColumn('users', 'age', 'age2', callback);
  };
  ```

* Change column type
  ```js
  exports.up = function (db, callback) {
    db.changeColumn('users', 'age', 'blob', callback);
  };
  ```
  
## TODOs
* This module is built using `cassandra-driver` need to add support for creating new keyspace.

## Contribution
* Fork the repository
* Build the feature
* Add tests
* Raise a pull request
