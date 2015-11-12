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
  db.createTable('users', {
  'name': 'varchar',
  'age': 'id'
  }, {
  'primary_key': 'name'
 });
 ```
 
  Supports multiple parimary keys

  ```js
  'primary_key': '(name, age)'
  ```

* Drop Table
  ```js
  db.dropTable('users');
  ```
  
* Add new column
  ```js
  db.addColumn('users', 'age', 'int');
  ```
  
* Drop existing column
  ```js
  db.removeColumn('users', 'age');
  ```
  
* Rename a column
  ```js
  db.renameColumn('users', 'age', 'age2');
  ```

* Change column type
  ```js
  db.changeColumn('users', 'age', 'blob');
  ```
  
## TODOs
* This module is built using `cassandra-driver` need to add support for creating new keyspace.

## Contribution
* Fork the repository
* Build the feature
* Add tests
* Raise a pull request
