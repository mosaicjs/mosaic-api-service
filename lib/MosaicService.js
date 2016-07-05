var mm = require('mosaic-mapnik');
var TilesGenerator = mm.tiles.TilesGenerator;
var path = require('path');
var pg = require('pg.js')
var TilesRenderer = require('./TilesRenderer');
var MosaicConfig = require('./MosaicConfig');
var extend = MosaicConfig.extend;
var templates = require('./templates');
var rest = require('./rest');

function loadTemplate(path) {
    return templates.load(__dirname, path);
}

var SQL = {
    createCollection : loadTemplate('./MosaicService_createCollection.sql'),
    createCollectionIndex : loadTemplate('./MosaicService_createCollectionIndex.sql'),
    dropCollection : loadTemplate('./MosaicService_dropCollection.sql'),
    saveGeoJsonInCollection : loadTemplate('./MosaicService_saveGeoJsonInCollection.sql'),
    searchCollectionIndex : loadTemplate('./MosaicService_searchCollectionIndex.sql'),
    searchCollectionIndexIds : loadTemplate('./MosaicService_searchCollectionIndexIds.sql'),
    getGeoJsonFromIndex : loadTemplate('./MosaicService_getGeoJsonFromIndex.sql'),
    getIdsFromIndex : loadTemplate('./MosaicService_getIdsFromIndex.sql'),
    searchPositionInCollectionIndex : loadTemplate('./MosaicService_searchPositionInCollectionIndex.sql'),
    searchPositionInCollection : loadTemplate('./MosaicService_searchPositionInCollection.sql'),
    selectGeoJsonFromCollection : loadTemplate('./MosaicService_selectGeoJsonFromCollection.sql'),
    selectGeoJsonFromCollectionByIds : loadTemplate('./MosaicService_selectGeoJsonFromCollectionByIds.sql'),
    selectIdsFromCollection : loadTemplate('./MosaicService_selectIdsFromCollection.sql'),
    tableExists : loadTemplate('./MosaicService_tableExists.sql'),
    tableSize : loadTemplate('./MosaicService_tableSize.sql'),
    updateCollectionIndex : loadTemplate('./MosaicService_updateCollectionIndex.sql'),
    viewExists : loadTemplate('./MosaicService_viewExists.sql'),
}

function MosaicService(options) {
    this.initialize(options);
}
extend(MosaicService.prototype, MosaicConfig.prototype);
extend(
        MosaicService.prototype,
        {

            initialize : function(options) {
                this.options = options || {};
            },

            // ---------------------------------------------------------------
            // Collection

            createCollection : rest({
                path : '/:collection',
                method : 'put',
                description : 'Creates a new collection with the specified name. '
                        + 'Returns the "409 Conflict" error if the resource already exists.',
                action : function(options) {
                    var that = this;
                    return that._writeTransaction(function() {
                        return that._checkCollection(options, false, //
                        function() {
                            var collection = that._getCollection(options);
                            var lang = that._getLanguage(options);
                            return that._execSql(SQL.createCollection({
                                collection : collection
                            })).then(function() {
                                var message = '' + //
                                'Collection was sucessfully created';
                                return that._ok({
                                    message : message,
                                    collection : collection
                                });
                            });
                        })
                    });
                }
            }),

            deleteCollection : rest({
                path : '/:collection',
                method : 'delete',
                description : 'Deletes the specified collection. '
                        + 'Returns a "404 Not Found" error if this collection does not exist.',
                action : function(options) {
                    var that = this;
                    return that._writeTransaction(function() {
                        return that._checkCollection(options, true, function() {
                            var collection = that._getCollection(options);
                            var lang = that._getLanguage(options);
                            return that._execSql(SQL.dropCollection({
                                collection : collection
                            })).then(function(res) {
                                var message = '' + //
                                'Collection was sucessfully removed';
                                return that._ok({
                                    message : message,
                                    collection : collection
                                });
                            });
                        });
                    });
                }
            }),

            getCollectionInfo : rest({
                path : '/:collection',
                method : 'get',
                description : 'Get information about the specified collection',
                action : function(options) {
                    var that = this;
                    return that._readTransaction(function() {
                        return that._checkCollection(options, true, function() {
                            var collection = that._getCollection(options);
                            var lang = that._getLanguage(options);
                            return that._execSql(SQL.tableSize({
                                table : collection + '_json'
                            })).then(function(res) {
                                var size = res[0].size;
                                return that._ok({
                                    message : 'Collection exists',
                                    collection : options.params.collection,
                                    size : size
                                });
                            });
                        });
                    });
                }
            }),

            // ---------------------------------------------------------------
            // Collection data

            getCollectionData : rest({
                path : '/:collection/data',
                method : 'get',
                description : 'Returns all data from the specified collection. '
                        + 'Returns the "404 Not found" error if the resource was not found',
                action : function(options) {
                    var that = this;
                    var sqlTemplate = SQL.selectGeoJsonFromCollection;
                    return that._getCollectionData(sqlTemplate, options)//
                    .then(function(res) {
                        return that._toFeatureCollection(res, options);
                    });
                }
            }),

            getCollectionDataByIds : rest({
                path : '/:collection/data/:ids',
                method : 'get',
                description : 'Returns full information for entities with the specified ids. '
                        + 'Returns the "404 Not found" error if the resource was not found',
                action : function(options) {
                    var that = this;
                    var sqlTemplate = SQL.selectGeoJsonFromCollectionByIds;
                    return that._getCollectionData(sqlTemplate, options)//
                    .then(function(res) {
                        return that._toFeatureCollection(res, options);
                    });
                }
            }),

            getCollectionIds : rest({
                path : '/:collection/ids',
                method : 'get',
                description : 'Returns a list of identifiers of all data ' //
                        + 'from the specified collection. '
                        + 'Returns the "404 Not found" error if the resource was not found',
                action : function(options) {
                    var that = this;
                    var sqlTemplate = SQL.selectIdsFromCollection;
                    return that._getCollectionData(sqlTemplate, options)//
                    .then(function(res) {
                        return that._toIdList(res, options);
                    });
                }
            }),

            setCollectionData : rest({
                path : '/:collection/data',
                method : 'put',
                description : 'Saves data in the collection',
                action : function(options) {
                    var that = this;
                    return that._writeTransaction(function() {
                        return that._checkCollection(options, true, function() {
                            var collection = that._getCollection(options);
                            var lang = that._getLanguage(options);
                            var array = that._toArray(options.data);
                            var chunkSize = 100;
                            var chunk = [];
                            var promises = [];
                            for (var i = 0; i < array.length; i++) {
                                chunk.push(array[i]);
                                if (chunk.length % chunkSize === 0) {
                                    promises.push(//
                                    that._saveChunk(collection, chunk));
                                    chunk = [];
                                }
                            }
                            if (chunk.length) {
                                promises.push(that
                                        ._saveChunk(collection, chunk));
                            }
                            return Promise.all(promises)//
                            .then(function(chunksInfo) {
                                var size = 0;
                                chunksInfo.forEach(function(info) {
                                    size += info.length;
                                });
                                var msg = //
                                'Data were successfully stored';
                                return that._ok({
                                    message : msg,
                                    collection : collection,
                                    size : size
                                });
                            });
                        });
                    });
                }
            }),

            // ---------------------------------------------------------------
            // Collection indexes

            createCollectionIndex : rest({
                path : '/:collection/index/:index',
                method : 'put',
                description : 'Creates a new collection index. '
                        + 'Returns the "409 Conflict" error '
                        + 'if the resource already exists.',
                action : function(options) {
                    var that = this;
                    return that._writeTransaction(function() {
                        return that._checkIndex(options, false, function() {
                            var collection = that._getCollection(options);
                            var index = that._getIndex(options);
                            var lang = that._getLanguage(options);

                            var fields = options.query.fields || options.data
                                    || {};
                            if (typeof fields === 'string') {
                                fields = JSON.parse(fields);
                            }
                            return that._execSql(SQL.createCollectionIndex({
                                collection : collection,
                                index : index,
                                lang : lang,
                                fields : JSON.stringify(fields)
                            })).then(function() {
                                return that._ok({
                                    message : 'Collection index ' + //
                                    'was sucessfully created',
                                    collection : collection,
                                    index : index
                                });
                            });
                        });
                    });
                }
            }),

            updateCollectionIndex : rest({
                path : '/:collection/index/:index',
                method : 'post',
                description : 'Refreshes a search index '
                        + 'with the specified name. '
                        + 'This method synchronizes the index with data - '
                        + 'it updates the corresponding materialized view '
                        + 'in the DB.' + 'Returns the "404 Not found" error '
                        + 'if the index was not found',
                action : function(options) {
                    var that = this;
                    return that._writeTransaction(function() {
                        return that._checkIndex(options, true, function() {
                            var collection = that._getCollection(options);
                            var index = that._getIndex(options);
                            var lang = that._getLanguage(options);
                            return that._execSql(SQL.updateCollectionIndex({
                                collection : collection,
                                index : index,
                                lang : lang
                            })).then(function() {
                                return that._ok({
                                    message : 'Collection index ' + //
                                    'was sucessfully updated',
                                    collection : collection,
                                    index : index,
                                    language : lang
                                });
                            });
                        });
                    });
                }
            }),

            deleteCollectionIndex : rest({
                path : '/:collection/index/:index',
                method : 'delete',
                description : 'Deletes the specified index.'
                        + 'Returns the "404 Not found" error if '
                        + 'the resource was not found',
                action : function(options) {
                    var that = this;
                    return that._writeTransaction(function() {
                        return that._checkIndex(options, true, function() {
                            var collection = that._getCollection(options);
                            var index = that._getIndex(options);
                            var lang = that._getLanguage(options);
                            return that._execSql(SQL.deleteCollectionIndex({
                                collection : collection,
                                index : index,
                                lang : lang
                            })).then(function() {
                                return that._ok({
                                    message : 'Collection index ' + //
                                    'was sucessfully removed',
                                    collection : collection,
                                    index : index,
                                    language : lang
                                });
                            });
                        });
                    });
                }
            }),

            getCollectionIndexInfo : rest({
                path : '/:collection/index/:index',
                method : 'get',
                description : 'Returns information about the specified index',
                action : function(options) {
                    var that = this;
                    return that._readTransaction(function() {
                        return that._checkIndex(options, true, function() {
                            var collection = that._getCollection(options);
                            var index = that._getIndex(options);
                            var lang = that._getLanguage(options);
                            // TODO: add more information about this
                            // index (size, etc)
                            return that._ok({
                                message : 'Index exists.',
                                collection : collection,
                                index : index,
                                language : lang
                            });
                        });
                    });
                }
            }),

            searchCollectionIndex : rest({
                path : '/:collection/index/:index/search',
                method : 'get',
                description : 'Performs full text search in the specified index '
                        + 'and returns found entries.',
                action : function(options) {
                    var that = this;
                    var searchSql = SQL.searchCollectionIndex;
                    var allDataSql = SQL.getGeoJsonFromIndex;
                    return that._searchCollectionIndex(searchSql, allDataSql,
                            options)//
                    .then(function(res) {
                        return that._toFeatureCollection(res, options);
                    });
                }
            }),

            searchCollectionIndexIds : rest({
                path : '/:collection/index/:index/searchIds',
                method : 'get',
                description : 'Performs full text search in the specified index '
                        + 'and returns identifiers of found entries.',
                action : function(options) {
                    var that = this;
                    var searchSql = SQL.searchCollectionIndexIds;
                    var allDataSql = SQL.getIdsFromIndex;
                    return that._searchCollectionIndex(searchSql, allDataSql,
                            options)//
                    .then(function(res) {
                        return that._toIdList(res, options);
                    });
                }
            }),

            searchPositionInCollectionIndex : rest({
                path : '/:collection/index/:index/position/:ids',
                method : 'get',
                description : 'Performs full text search in the specified index '
                        + 'and returns result positions of entities with the '
                        + 'specified identifiers.',
                action : function(options) {
                    var that = this;
                    var searchSql = SQL.searchPositionInCollectionIndex;
                    var allDataSql = SQL.searchPositionInCollection;
                    return that._searchCollectionIndex(searchSql, allDataSql,
                            options)//
                    .then(function(res) {
                        var result = {};
                        res.forEach(function(f) {
                            result[f.feature.id] = f.feature.pos;
                        })
                        return that._data(result);
                    });
                }
            }),

            // ---------------------------------------------------------------
            // Index search - visualization on tiles

            loadStaticTile : rest({
                path : '/tiles/:style/:z/:x/:y/tile.:format',
                method : 'get',
                description : 'This method produce web tiles from static sources '
                        + '(like Shape or GeoJSON files)',
                action : function(options) {
                    var that = this;
                    return that._getStaticTilesGenerator(options)//
                    .then(function(tilesGenerator) {
                        var params = options.params;
                        params.z = +params.z;
                        params.x = +params.x;
                        params.y = +params.y;
                        params.format = params.format || params.type || 'png';
                        return tilesGenerator.loadTile(options)//
                        .then(function(result) {
                            result.data = result.tile;
                            return result;
                        });
                        return service;
                    })
                }
            }),

            _getStaticTilesGenerator : function(options) {
                var that = this;
                return Promise.resolve().then(function() {
                    that._staticGenerators = that._staticGenerators || {};
                    var style = that._getStyle(options);
                    var promise = that._staticGenerators[style];
                    if (!promise) {
                        promise = Promise.resolve().then(function() {
                            return that._newStyleGenerator(options, style);
                        });
                        that._staticGenerators[style] = promise;
                    }
                    return promise;
                });
            },

            _newStyleGenerator : function(options) {
                var baseDir;
                var that = this;
                return Promise.resolve().then(function() {
                    baseDir = that._getTilesBaseDir(options);
                    return require(baseDir);
                }).then(function(config) {
                    config = extend({}, that.options, {
                        baseDir : baseDir
                    }, config, options);
                    delete config.data;
                    var tilesGenerator = new TilesGenerator(config);
                    return tilesGenerator;
                });
            },

            loadTile : rest({
                path : '/:collection/index/:index/:style/:z/:x/:y/tile.:format',
                method : 'get',
                description : 'This method renders and returns search results as tiles.',
                action : function(options) {
                    var that = this;
                    return that._run(function() {
                        return that._checkIndex(options, true, function() {
                            var collection = that._getCollection(options);
                            var index = that._getIndex(options);
                            var lang = that._getLanguage(options);
                            var query = that._getQuery(options);

                            var params = options.params || {};
                            params.z = +params.z;
                            params.x = +params.x;
                            params.y = +params.y;
                            params.format = params.format || params.type
                                    || 'png';
                            return Promise.resolve().then(function() {
                                return that._getTilesGenerator(options);
                            }).then(function(tilesGenerator) {
                                return tilesGenerator.loadTile(options);
                            }).then(function(results) {
                                results.data = results.tile || results.data;
                                delete results.tile;
                                return results;
                            });
                        });
                    });
                }
            }),

            _getTilesGenerator : function(options) {
                var that = this;
                return Promise.resolve().then(function() {
                    if (!that._tilesRenderer) {
                        that._tilesRenderer = new TilesRenderer(that.options);
                    }
                    return that._tilesRenderer;
                });
            },

            // ---------------------------------------------------------------
            // Errors and return results

            _data : function(data) {
                return {
                    code : 200,
                    data : data
                }
            },

            _ok : function(options) {
                var code = 200;
                var data = {
                    result : 'ok',
                    code : code,
                };
                for ( var key in options) {
                    data[key] = options[key];
                }
                return {
                    code : code,
                    data : data,
                }
            },

            _error : function(code, msg, options) {
                var data = {
                    result : 'error',
                    code : code,
                    message : msg,
                };
                for ( var key in options) {
                    data[key] = options[key];
                }
                return {
                    code : code,
                    data : data,
                }
            },

            /** Returns the "404 Not found" error if the resource was not found */
            _errorResourceNotFound : function(options) {
                return this._error(404, 'Not found', options);
            },

            /** Returns the "409 Conflict" error if the resource already exists. */
            _errorResourceAlreadyExists : function(options) {
                return this._error(409, 'Conflict', options);
            },

            // ---------------------------------------------------------------
            // Internal methods

            _writeTransaction : function(action) {
                return this._run(action);
            },

            _readTransaction : function(action) {
                return this._run(action);
            },

            _run : function(action) {
                var that = this;
                return Promise.resolve().then(function() {
                    return action.call(that);
                })
            },

            /** Executes the specified query and returns an array with results */
            _execSql : function(sql, options) {
                var that = this;
                return Promise.resolve().then(function() {
                    var url = that._getDbUrl(options);
                    return that._connect(url, function(client) {
                        return new Promise(function(resolve, reject) {
                            sql = that._prepareSql(sql, options);
                            return client.query(sql, {}, function(err, res) {
                                if (err) {
                                    return reject(err);
                                } else {
                                    return resolve(res.rows);
                                }
                            });
                        });
                    });
                });
            },

            /** Prepares the specified query based on the given parameters */
            _prepareSql : function(query, options) {
                options = options || {};
                var limit = +options.limit;
                var offset = +options.offset;
                if (this.options.log) {
                    this.options.log('SQL: ' + query);
                }
                var offsetSuffix = '';
                if (!isNaN(offset) && offset > 0) {
                    offsetSuffix = ' offset ' + offset;
                }
                var limitSuffix = '';
                if (!isNaN(limit) && limit >= 0) {
                    limitSuffix = ' limit ' + limit;
                }
                if (!!offsetSuffix || !!limitSuffix) {
                    query = 'select * from (' + query + ') as data ' + //
                    offsetSuffix + limitSuffix;
                }
                return query;
            },

            /**
             * Opens connection to the Db and calls the specified action with
             * the connection
             */
            _connect : function(url, action) {
                var that = this;
                return new Promise(function(resolve, reject) {
                    pg.connect(url, function(err, client, done) {
                        return Promise.resolve().then(function() {
                            if (err) {
                                return reject(err);
                            }
                            return action(client);
                        }).then(function(result) {
                            resolve(result);
                        }, function(err) {
                            reject(err);
                        }).then(done);
                    })
                });
            },

            _getDbUrl : function(options) {
                var conf = this.options.db;
                var cred = conf.user;
                if (conf.password) {
                    cred += ':' + conf.password;
                }
                if (cred && cred !== '') {
                    cred += '@';
                }
                var dbUrl = 'postgres://' + cred + conf.host + ':' + conf.port
                        + '/' + conf.dbname;
                return dbUrl;
            },

            // ---------------------------------------------------------------

            _collectionExists : function(collection) {
                // TODO: if the requested collection exists - add it an internal
                // index
                // to accelerate search.
                return this._tableExists(collection + '_json');
            },

            /** Returns an error message if index already exists */
            _checkCollection : function(options, shouldExist, action) {
                var that = this;
                return Promise.resolve().then(function() {
                    var collection = that._getCollection(options);
                    var lang = that._getLanguage(options);
                    return that._collectionExists(collection, lang) //
                    .then(function(exists) {
                        if (exists === shouldExist) {
                            return action.call(that);
                        }
                        if (shouldExist) {
                            return that._errorResourceNotFound({
                                message : 'Collection was not found',
                                collection : collection,
                                language : lang
                            });
                        } else {
                            return that._errorResourceAlreadyExists({
                                message : 'Collection already exists',
                                collection : collection,
                                language : lang
                            });
                        }
                    })

                })
            },

            _indexExists : function(collection, index, lang) {
                // TODO: if the requested collection exists - add it an internal
                // index
                // to accelerate search.
                return this._viewExists(collection + '_view_' + index + '_'
                        + lang);
            },

            /** Returns an error message if index already exists */
            _checkIndex : function(options, shouldExist, action) {
                var that = this;
                return Promise.resolve().then(function() {
                    var collection = that._getCollection(options);
                    var index = that._getIndex(options);
                    var lang = that._getLanguage(options);
                    return that._indexExists(collection, index, lang) //
                    .then(function(exists) {
                        if (exists === shouldExist) {
                            return action.call(that);
                        }
                        if (shouldExist) {
                            return that._errorResourceNotFound({
                                message : 'Collection index was not found',
                                collection : collection,
                                index : index,
                                language : lang
                            });
                        } else {
                            return that._errorResourceAlreadyExists({
                                message : 'Collection index already exists',
                                collection : collection,
                                index : index,
                                language : lang
                            });
                        }
                    })

                })
            },

            _tableExists : function(table) {
                return this._exists(SQL.tableExists({
                    table : table
                }));
            },

            _viewExists : function(view) {
                return this._exists(SQL.viewExists({
                    view : view
                }));
            },

            _exists : function(sql) {
                var that = this;
                return Promise.resolve().then(function() {
                    return that._execSql(sql).then(function(res) {
                        return !!res[0].exists;
                    });
                });
            },

            _saveChunk : function(collection, chunk) {
                var that = this;
                return Promise.resolve().then(function() {
                    var str = JSON.stringify(chunk);
                    str = str.replace(/[']/gim, "''");
                    return that._execSql(SQL.saveGeoJsonInCollection({
                        collection : collection,
                        data : str
                    })).then(function(result) {
                        return {
                            length : chunk.length
                        }
                    });
                });
            },

            _searchCollectionIndex : function(searchSql, allDataSql, options) {
                var that = this;
                return that._readTransaction(function() {
                    return that._checkIndex(options, true, function() {
                        var collection = that._getCollection(options);
                        var index = that._getIndex(options);
                        var lang = that._getLanguage(options);
                        var query = that._getQuery(options);
                        var lang = that._getLanguage(options);
                        var ids = that._getIdsAsSqlString(options);
                        var sqlTemplate = !!query ? searchSql : allDataSql;
                        var sql = sqlTemplate({
                            collection : collection,
                            index : index,
                            lang : lang,
                            query : query,
                            ids : ids
                        });
                        var limit = that._getSearchLimit(options);
                        var offset = that._getSearchOffset(options);
                        return that._execSql(sql, {
                            limit : limit,
                            offset : offset
                        });
                    });
                });
            },

            _getCollectionData : function(sqlTemplate, options) {
                var that = this;
                return that._readTransaction(function() {
                    return that._checkCollection(options, true, function() {
                        var collection = that._getCollection(options);
                        var lang = that._getLanguage(options);
                        var limit = that._getSearchLimit(options);
                        var offset = that._getSearchOffset(options);
                        var ids = that._getIdsAsSqlString(options);
                        return that._execSql(sqlTemplate({
                            collection : collection,
                            ids : ids
                        }), {
                            limit : limit,
                            offset : offset
                        });
                    })
                });
            },

            _toFeatureCollection : function(res, options) {
                var limit = this._getSearchLimit(options);
                var offset = this._getSearchOffset(options);
                return this._data({
                    type : 'FeatureCollection',
                    offset : offset,
                    limit : limit,
                    features : res.map(function(f) {
                        return f.feature;
                    })
                });
            },

            _toIdList : function(res, options) {
                var limit = this._getSearchLimit(options);
                var offset = this._getSearchOffset(options);
                return this._data({
                    offset : offset,
                    limit : limit,
                    ids : res.map(function(f) {
                        return f.feature.id;
                    })
                });
            }

        });

module.exports = MosaicService;