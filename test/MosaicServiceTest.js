var expect = require('expect.js');
var PgConnector = require('../lib/PgConnector').PgConnector;
var MosaicService = require('../lib/MosaicService');
var config = require('./config');
var data = require('./data.json');

describe('MosaicService', function() {
    this.timeout(5000);
    var collection = 'toto';
    var lang = 'french';
    var params = {
        collection : collection,
        lang : lang
    };
    runTest('should be able to create, check and drop collections', function(
            service) {
        return service.getCollectionInfo({
            params : params
        }).then(function(res) {
            expect(res.code).to.be(404);
            return service.createCollection({
                params : params
            }).then(function(res) {
                expect(res.code).to.be(200);
                expect(res.data.collection).to.eql(collection);
                expect(res.data.lang).to.eql(lang);
            });
        }).then(function() {
            return service.getCollectionInfo({
                params : params
            }).then(function(res) {
                expect(res.code).to.be(200);
            });
        }).then(function() {
            return service.deleteCollection({
                params : params
            }).then(function(res) {
                expect(res.code).to.be(200);
                expect(res.data.collection).to.eql(collection);
                expect(res.data.lang).to.eql(lang);
            });
        }).then(function() {
            return service.getCollectionInfo({
                params : params
            }).then(function(res) {
                expect(res.code).to.be(404);
            });
        });
    });
    runTest('should be able to store data in a collection', function(service) {
        return service.createCollection({
            params : params
        }).then(function(res) {
            return service.setCollectionData({
                params : params,
                data : data.features
            }).then(function(res) {
                expect(res.code).to.be(200);
                expect(res.data.collection).to.eql(collection);
                expect(res.data.lang).to.eql(lang);
                expect(res.data.size).to.eql(data.features.length);
            });
        });
    });

    runTest('should be able to create collections index', function(service) {
        var params = {
            collection : collection,
            lang : lang,
            index : 'q'
        };
        return service.createCollection({
            params : params
        }).then(function() {
            return service.getCollectionIndexInfo({
                params : params
            }).then(function(res) {
                expect(res.code).to.eql(404);
            })
        }).then(function() {
            return service.createCollectionIndex({
                params : params,
                query : {
                    fields : {
                        name : 1,
                        description : 1
                    }
                }
            }).then(function(res) {
                expect(res.code).to.eql(200);

            });
        }).then(function() {
            return service.getCollectionIndexInfo({
                params : params
            }).then(function(res) {
                expect(res.code).to.eql(200);
            });
        }).then(function() {
            return service.createCollectionIndex({
                params : params
            }).then(function(res) {
                // 409 - Conflict; Index already exists
                expect(res.code).to.eql(409);
            });
        });
    });

    runTest('should be able to index entities', function(service) {
        var params = {
            collection : collection,
            lang : lang,
            index : 'q'
        };
        var query = {
            query : {
                q : 'science:*'
            },
            limit : 1000
        };
        return service.createCollection({
            params : params
        }).then(function() {
            return service.createCollectionIndex({
                params : params,
                query : {
                    fields : {
                        name : 1,
                        description : 1
                    }
                }
            });
        }).then(function() {
            // Set data
            return service.setCollectionData({
                params : params,
                data : data.features
            })
        }).then(function(res) {
            // Search in the index. The index should be empty.
            // It has be explicitly updated to take into account new data.
            return service.searchCollectionIndex({
                params : params,
                query : query
            }).then(function(res) {
                expect(res.code).to.be(200);
                expect(res.data.collection).to.eql(collection);
                expect(res.data.lang).to.eql(lang);
                expect(res.data.size).to.eql(0);
            });
        }).then(function() {
            // Update index
            return service.updateCollectionIndex({
                params : params
            }).then(function(res) {
                expect(res.code).to.eql(200);
            })
        }).then(function() {
            // Search again
            return service.searchCollectionIndex({
                params : params,
                query : query
            }).then(function(res) {
                // Now index contains data.
                expect(res.code).to.be(200);
                expect(res.data.collection).to.eql(collection);
                expect(res.data.lang).to.eql(lang);
                expect(res.data.size).to.eql(4);
                expect(res.data.features.map(function(f) {
                    return f.properties.id;
                })).to.eql([ 'palais-de-la-decouverte',//
                'cite-des-sciences-et-de-lindustrie', //
                'bibliotheque-de-sciences-humaines' + // 
                '-et-sociales-paris-descartes-cnrs',//
                'musee-curie' ]);
            });
        });
    });

    function sequence(actions) {
        var promise = Promise.resolve();
        actions.forEach(function(action) {
            promise = promise.then(function() {
                return action();
            });
        })
        return promise;
    }

    runTest('should be able to search in multiple indexes', function(service) {
        var params = {
            collection : collection,
            lang : lang,
            index : 'q'
        };
        return sequence([ function() {
            // Create collection
            return service.createCollection({
                params : params
            });
        }, function() {
            // Set data
            return service.setCollectionData({
                params : params,
                data : data.features
            })
        }, function() {
            // Create index by name and description
            return service.createCollectionIndex({
                params : {
                    collection : collection,
                    lang : lang,
                    index : 'q'
                },
                query : { // http query
                    fields : {
                        'name' : 1,
                        'description' : 1
                    }
                }
            })
        }, function() {
            // Create index by category
            return service.createCollectionIndex({
                params : {
                    collection : collection,
                    lang : lang,
                    index : 'category'
                },
                query : { // http query
                    fields : {
                        'category' : 1
                    }
                }
            })
        } ]).then(function() {
            // Search in the index by one criterion
            return service.searchCollectionIndex({
                params : params,
                query : { // http query
                    query : { // logical query for the DB
                        q : 'national:*',
                    },
                    limit : 1000
                }
            }).then(function(res) {
                // Now index contains data.
                expect(res.code).to.be(200);
                expect(res.data.collection).to.eql(collection);
                expect(res.data.lang).to.eql(lang);
                expect(res.data.size).to.eql(25);
                expect(res.data.features.map(function(f) {
                    return f.properties.id;
                })).to.eql([//
                'musee-national-des-arts-et-traditions-populaires',//
                'maison-de-lhistoire-de-france', //
                'musee-dennery', //
                'musee-de-larmee-paris', //
                'musee-national-de-la-marine', //
                'cite-des-sciences-et-de-lindustrie', //
                'musee-des-arts-et-metiers', //
                'musee-de-cluny', //
                'musee-hebert-paris', //
                'cite-nationale-de-lhistoire-de-limmigration', //
                'musee-de-lhomme', //
                'musee-national-jean-jacques-henner', //
                'musee-gustave-moreau', //
                'bibliotheque-nationale-de-france', //
                'musee-de-la-legion-dhonneur', //
                'musee-national-eugene-delacroix', //
                'musee-des-archives-nationales', //
                'musee-national-dart-moderne', //
                'musee-dorsay', //
                'musee-national-des-arts-asiatiques' + //
                '---guimet', //
                'bibliotheque-de-sciences-humaines-' + //
                'et-sociales-paris-descartes-cnrs', //
                'musee-national-du-sport', //
                'musee-picasso-paris', //
                'centre-national-dart-et-de-culture-georges-pompidou', //
                'parc-oceanique-cousteau' //
                ]);
            });
        }).then(function() {
            // Search in the index by two criteria
            // (in two indexes)
            return service.searchCollectionIndex({
                params : params,
                query : { // http query
                    query : { // logical query for the DB
                        q : 'national:*',
                        category : 'Peinture'
                    },
                    limit : 1000
                }
            }).then(function(res) {
                // Now index contains data.
                expect(res.code).to.be(200);
                expect(res.data.collection).to.eql(collection);
                expect(res.data.lang).to.eql(lang);
                expect(res.data.size).to.eql(7);
                expect(res.data.features.map(function(f) {
                    return f.properties.id;
                })).to.eql([//
                'musee-national-de-la-marine', //
                'musee-hebert-paris', //
                'musee-national-jean-jacques-henner', //
                'musee-gustave-moreau', //
                'musee-national-eugene-delacroix', //
                'musee-dorsay', //
                'musee-picasso-paris'//
                ]);
            });
        });
    });
});

function test(msg, action) {
    it(msg, function(done) {
        return Promise.resolve().then(function() {
            return action();
        }).then(function() {
            done();
        }, done);
    });
}

function runTest(msg, action) {
    it(msg, function(done) {
        var initConnector = new PgConnector(config);
        var testDbName = config.testDbName;
        return withConnector(initConnector, function() {
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
                    return withConnector(service, function() {
                        return action(service);
                    });
                }).then(drop, function(err) {
                    return drop().then(function() {
                        throw err;
                    });
                });
                function create() {
                    return Promise.resolve().then(function() {
                        return w.execSql('CREATE DATABASE ' //
                                + testDbName);
                    });
                }
                function drop() {
                    return Promise.resolve().then(function() {
                        return w.execSql('DROP DATABASE IF EXISTS ' //
                                + testDbName);
                    });
                }
            });
        }).then(function() {
            done();
        }, done);
    })

    function withConnector(connector, action) {
        return Promise.resolve().then(function() {
            return action(connector);
        }).then(function() {
            return connector.close();
        }, function(err) {
            return Promise.resolve().then(function() {
                return connector.close();
            }).then(function() {
                throw err;
            });
        });
    }

}
