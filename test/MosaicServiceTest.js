var expect = require('expect.js');
var PgConnector = require('../lib/PgConnector').PgConnector;
var MosaicService = require('../lib/MosaicService');
var config = require('./config');

describe('MosaicService', function() {

    test('should ...', function(service) {
        console.log('I AM HERE!');
    });

});

function test(msg, action) {
    it(msg, function(done) {
        var initConnector = new PgConnector(config);
        var testDbName = config.testDbName;
        return initConnector.writeTransaction(function(w) {
            return drop().then(create).then(function() {
                var service = new MosaicService({
                    db : {
                        "host" : config.db.host,
                        "port" : config.db.port,
                        "database" : testDbName,
                        "user" : config.db.user,
                        "password" : config.db.password,
                    }
                });
                return action(service);
            }).then(drop, drop);
            function create() {
                return Promise.resolve().then(function() {
                    return w.execSql('CREATE DATABASE ' + testDbName);
                });
            }
            function drop() {
                return Promise.resolve().then(function() {
                    return w.execSql('DROP DATABASE IF EXISTS ' + testDbName);
                });
            }
        }).then(function() {
            done();
        }, done);
    });

}
