var assert = require('chai').assert;
var expect = require('chai').expect;
var sinon = require('sinon');
var driver = require('./index');
var config =  { "driver": "cassandra",
                "host": ["localhost"],
                "database": "dbmigration"
            };

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
})