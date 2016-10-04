var assert = require('chai').assert;
var expect = require('chai').expect;
var sinon = require('sinon');
var driver = require('./index');
var testHostname = 'localhost';
var config =  {
    "driver": "cassandra",
    "host": [testHostname],
    "database": "dbmigration"
};
var configWithHostListAsString =  {
    "driver": "cassandra",
    "host": testHostname + "," + testHostname,
    "database": "dbmigration"
};
var configWithSingleHostAsString =  {
    "driver": "cassandra",
    "host": testHostname,
    "database": "dbmigration"
};
var configWithoutHost = {
    "driver": "cassandra",
    "database": "dbmigration"
}

var internals = {};
internals.mod = {};

internals.migrationTable = 'migrations';
describe('Cassandra Migration', function() {
    driver.connect(config, internals, function(err, db) {        
        // Stub connection execute
        var executeSpy = sinon.spy();
        db.connection.execute = function () {};
        sinon.stub(db.connection, 'execute', executeSpy);

        describe('Create Table', function() {
            it('A simple table with one Primary key', function() {
                db.createTable('users', {
                    'name': 'varchar',
                    'age': 'id'
                }, {
                    'primary_key': 'name'
                });
                var expectedQuery = ['CREATE TABLE IF NOT EXISTS users',
                    '( name varchar, age id, PRIMARY KEY (name) )'];
                sinon.assert.calledOnce(executeSpy);
                assert.equal(executeSpy.args[0][0], expectedQuery.join(' '));
                executeSpy.reset();
            });

            it('A simple table with multiple Primary keys', function() {
                db.createTable('users', {
                    'name': 'varchar',
                    'age': 'id'
                }, {
                    'primary_key': '(name, age)'
                });
                var expectedQuery = ['CREATE TABLE IF NOT EXISTS users',
                    '( name varchar, age id, PRIMARY KEY (name, age) )'];
                sinon.assert.calledOnce(executeSpy);
                assert.equal(executeSpy.args[0][0], expectedQuery.join(' '));
                executeSpy.reset();
            });

            it('A simple table with compression', function() {
                db.createTable('users', {
                    'name': 'varchar'
                }, {
                    'primary_key': 'name',
                    'compression': "{'sstable_compression': 'DeflateCompressor', 'chunk_length_kb': 1024}"
                });
                var expectedQuery = ['CREATE TABLE IF NOT EXISTS users',
                    "( name varchar, PRIMARY KEY (name) )",
                    "WITH compression = {'sstable_compression': 'DeflateCompressor', 'chunk_length_kb': 1024}"];
                sinon.assert.calledOnce(executeSpy);
                assert.equal(executeSpy.args[0][0], expectedQuery.join(' '));
                executeSpy.reset();
            });
        });
        describe('Drop table', function() {
            it('Should build a valid SQL', function() {
                db.dropTable('users');
                var expectedQuery = ['DROP TABLE users'];
                sinon.assert.calledOnce(executeSpy);
                assert.equal(executeSpy.args[0][0], expectedQuery.join(' '));
                executeSpy.reset();
            })
        });
        describe('Alter Table', function() {
            it('Add a new column', function() {
                db.addColumn('users', 'age', 'int');
                var expectedQuery = ['ALTER TABLE users ADD age int'];
                sinon.assert.calledOnce(executeSpy);
                assert.equal(executeSpy.args[0][0], expectedQuery.join(' '));
                executeSpy.reset();
            });
            it('Drop a existing column', function() {
                db.removeColumn('users', 'age');
                var expectedQuery = ['ALTER TABLE users DROP age'];
                sinon.assert.calledOnce(executeSpy);
                assert.equal(executeSpy.args[0][0], expectedQuery.join(' '));
                executeSpy.reset();
            });
            it('Rename an existing column', function() {
                db.renameColumn('users', 'age', 'age2');
                var expectedQuery = ['ALTER TABLE users RENAME age TO age2'];
                sinon.assert.calledOnce(executeSpy);
                assert.equal(executeSpy.args[0][0], expectedQuery.join(' '));
                executeSpy.reset();
            });
            it('Change an existing column type', function() {
                db.changeColumn('users', 'age', 'blob');
                var expectedQuery = ['ALTER TABLE users ALTER age TYPE blob'];
                sinon.assert.calledOnce(executeSpy);
                assert.equal(executeSpy.args[0][0], expectedQuery.join(' '));
                executeSpy.reset();
            });
        });
    });

    describe('Driver contactPoints format', function() {
        var checkHostname = function(db, name) {
            name = name || testHostname;
            var hostname = db.connectionParam.hostname;
            for (var i = 0; i < db.connectionParam.length; i++) {
                assert.equal(hostname[i], name);
            }
        }

        driver.connect(config, internals, function(err, db) {            
            it('Host as array configuration', function() {
                checkHostname(db);
            });
        });

        driver.connect(
                configWithHostListAsString, internals, function(err, db) {            
            it('Host as comma-list configuration', function() {
                checkHostname(db);
            });
        });

        driver.connect(
                configWithSingleHostAsString, internals, function(err, db) {            
            it('Host as single string configuration', function() {
                checkHostname(db);
            });
        });
        
        driver.connect(configWithoutHost, internals, function(err, db) {
            it('Host missing', function() {
                checkHostname(db, 'localhost');
            });
        });
    });
});
