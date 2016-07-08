var mm = require('mosaic-mapnik');
var TilesGenerator = mm.tiles.TilesGenerator;
var path = require('path');

var TilesRenderer = require('./TilesRenderer');

var PgConnector = require('./PgConnector').PgConnector;
var PgConnectionWrapper = require('./PgConnector').PgConnectionWrapper;

var MosaicConfig = require('./MosaicConfig');
var extend = MosaicConfig.extend;
var templates = require('./templates');
var rest = require('./rest');

function loadTemplate(fileName) {
    var dir = path.join(__dirname, 'sql/runtime/');
    return templates.load(dir, fileName);
}

var SQL = {
    createCollection : loadTemplate('./MosaicService_createCollection.sql'),
    createCollectionIndex : loadTemplate('./MosaicService_createCollectionIndex.sql'),
    dropCollectionIndex : loadTemplate('./MosaicService_dropCollectionIndex.sql'),
    dropCollection : loadTemplate('./MosaicService_dropCollection.sql'),
    saveGeoJsonInCollection : loadTemplate('./MosaicService_saveGeoJsonInCollection.sql'),
    searchCollectionIndex : loadTemplate('./MosaicService_searchCollectionIndex.sql'),
    searchCollectionIndexIds : loadTemplate('./MosaicService_searchCollectionIndexIds.sql'),
    getGeoJsonFromIndex : loadTemplate('./MosaicService_getGeoJsonFromIndex.sql'),
    getIdsFromIndex : loadTemplate('./MosaicService_getIdsFromIndex.sql'),
    searchPositionInCollectionIndex : loadTemplate('./MosaicService_searchPositionInCollectionIndex.sql'),
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
var MosaicServiceMethods = {

    initialize : function(options) {
        PgConnector.call(this, options);
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
            return that.writeTransaction(function(w) {
                return that._checkCollection(w, options, false, //
                function() {
                    var collection = that._getCollection(options);
                    var lang = that._getLanguage(options);
                    return w.execSql(SQL.createCollection({
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
            return that.writeTransaction(function(w) {
                return that._checkCollection(w, options, true, function() {
                    var collection = that._getCollection(options);
                    var lang = that._getLanguage(options);
                    return w.execSql(SQL.dropCollection({
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
            return that.readTransaction(function(w) {
                return that._checkCollection(w, options, true,//
                function() {
                    var collection = that._getCollection(options);
                    var lang = that._getLanguage(options);
                    return w.execSql(SQL.tableSize({
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
            return that.writeTransaction(function(w) {
                return that._checkCollection(w, options, true,//
                function() {
                    var collection = that._getCollection(options);
                    var lang = that._getLanguage(options);
                    var array = that._toArray(options.data);
                    var chunkSize = 100;
                    var chunk = [];
                    var promises = [];
                    for (var i = 0; i < array.length; i++) {
                        chunk.push(array[i]);
                        if (chunk.length % chunkSize === 0) {
                            promises.push(that._saveChunk(w, //
                            collection, chunk));
                            chunk = [];
                        }
                    }
                    if (chunk.length) {
                        promises.push(that._saveChunk(w, //
                        collection, chunk));
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
            return that.writeTransaction(function(w) {
                return that._checkIndex(w, options, false, //
                function() {
                    var collection = that._getCollection(options);
                    var index = that._getIndex(options);
                    var lang = that._getLanguage(options);

                    var fields = options.query.fields || options.data || {};
                    if (typeof fields === 'string') {
                        fields = JSON.parse(fields);
                    }
                    return w.execSql(SQL.createCollectionIndex({
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
        description : 'Refreshes a search index ' + 'with the specified name. '
                + 'This method synchronizes the index with data - '
                + 'it updates the corresponding materialized view '
                + 'in the DB.' + 'Returns the "404 Not found" error '
                + 'if the index was not found',
        action : function(options) {
            var that = this;
            return that.writeTransaction(function(w) {
                return that._checkIndex(w, options, true, function() {
                    var collection = that._getCollection(options);
                    var index = that._getIndex(options);
                    var lang = that._getLanguage(options);
                    return w.execSql(SQL.updateCollectionIndex({
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
            return that.writeTransaction(function(w) {
                return that._checkIndex(w, options, true, function() {
                    var collection = that._getCollection(options);
                    var index = that._getIndex(options);
                    var lang = that._getLanguage(options);
                    return w.execSql(SQL.dropCollectionIndex({
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
            return that.readTransaction(function(w) {
                return that._checkIndex(w, options, true, function() {
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
        path : '/:collection/search',
        method : 'get',
        description : 'Performs full text search in the specified index '
                + 'and returns found entries.',
        action : function(options) {
            var that = this;
            var searchSql = SQL.searchCollectionIndex;
            return that._searchCollectionIndex(searchSql, options,
                    function(res) {
                        return that._toFeatureCollection(res, options);
                    });
        }
    }),

    searchCollectionIndexIds : rest({
        path : '/:collection/searchIds',
        method : 'get',
        description : 'Performs full text search in the specified index '
                + 'and returns identifiers of found entries.',
        action : function(options) {
            var that = this;
            var searchSql = SQL.searchCollectionIndexIds;
            return that._searchCollectionIndex(searchSql, options,
                    function(res) {
                        return that._toIdList(res, options);
                    });
        }
    }),

    searchPositionInCollectionIndex : rest({
        path : '/:collection/search/position/:ids',
        method : 'get',
        description : 'Performs full text search in the specified index '
                + 'and returns result positions of entities with the '
                + 'specified identifiers.',
        action : function(options) {
            var that = this;
            var allDataSql = SQL.searchPositionInCollection;
            var searchSql = SQL.searchPositionInCollectionIndex;
            return that._searchCollectionIndex(searchSql, options,
                    function(res) {
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
        path : '/:collection/:style/:z/:x/:y/tile.:format',
        method : 'get',
        description : 'This method renders and returns search results as tiles.',
        action : function(options) {
            var that = this;
            return that.readTransaction(function(w) {
                return that._checkIndexForSearch(w, options, function() {
                    var params = options.params || {};
                    params.z = +params.z;
                    params.x = +params.x;
                    params.y = +params.y;
                    params.format = params.format || params.type || 'png';
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

    _collectionExists : function(w, collection) {
        // TODO: if the requested collection exists - add it an internal
        // index to accelerate search.
        return this._tableExists(w, collection + '_json');
    },

    /** Returns an error message if index already exists */
    _checkCollection : function(w, options, shouldExist, action) {
        var that = this;
        return Promise.resolve().then(function() {
            var collection = that._getCollection(options);
            var lang = that._getLanguage(options);
            return that._collectionExists(w, collection, lang) //
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

    _indexExists : function(w, collection, index, lang) {
        // TODO: if the requested collection exists - add it an internal
        // index
        // to accelerate search.
        return this._viewExists(w, collection + '_view_' + index + '_' + lang);
    },

    _checkIndexForSearch : function(w, options, action) {
        var that = this;
        return Promise.resolve().then(function() {
            var collection = that._getCollection(options);
            var query = that._getQuery(options);
            var lang = that._getLanguage(options);
            var indexNames = Object.keys(query);
            if (!indexNames.length) {
                return that._errorResourceNotFound({
                    message : 'Search index was not defined or query is empty',
                    collection : collection,
                    language : lang
                });
            }
            var promises = indexNames.map(function(indexName) {
                return that._indexExists(w, collection, indexName, lang);
            });
            return Promise.all(promises).then(function(array) {
                var missingIndexes = [];
                var result = array.length > 0;
                array.forEach(function(r, i) {
                    if (!r) {
                        missingIndexes.push(indexNames[i]);
                    }
                });
                if (missingIndexes.length) {
                    return that._errorResourceNotFound({
                        message : 'Collection indexes are missing: ' + //
                        missingIndexes.join(','),
                        collection : collection,
                        indexes : missingIndexes,
                        language : lang
                    });
                } else {
                    return action.call(that);
                }
            });
        })
    },

    /** Returns an error message if index already exists */
    _checkIndex : function(w, options, shouldExist, action) {
        var that = this;
        return Promise.resolve().then(function() {
            var collection = that._getCollection(options);
            var index = that._getIndex(options);
            var lang = that._getLanguage(options);
            return that._indexExists(w, collection, index, lang) //
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

    _tableExists : function(w, table) {
        return this._exists(w, SQL.tableExists({
            table : table
        }));
    },

    _viewExists : function(w, view) {
        return this._exists(w, SQL.viewExists({
            view : view
        }));
    },

    _exists : function(w, sql) {
        var that = this;
        return Promise.resolve().then(function() {
            return w.execSql(sql).then(function(res) {
                return !!res[0].exists;
            });
        });
    },

    _saveChunk : function(w, collection, chunk) {
        var that = this;
        return Promise.resolve().then(function() {
            var str = JSON.stringify(chunk);
            str = str.replace(/[']/gim, "''");
            return w.execSql(SQL.saveGeoJsonInCollection({
                collection : collection,
                data : str
            })).then(function(result) {
                return {
                    length : chunk.length
                }
            });
        });
    },

    _searchCollectionIndex : function(sqlTemplate, options, action) {
        var that = this;
        return that.readTransaction(function(w) {
            return that._checkIndexForSearch(w, options, function() {
                var collection = that._getCollection(options);
                var lang = that._getLanguage(options);
                var query = that._getQueryAsSqlString(options);
                var lang = that._getLanguage(options);
                var ids = that._getIdsAsSqlString(options);
                var intersection = that._getIntersection(options);
                var sql = sqlTemplate({
                    collection : collection,
                    intersection : intersection,
                    lang : lang,
                    query : query,
                    ids : ids
                });
                var limit = that._getSearchLimit(options);
                var offset = that._getSearchOffset(options);
                return w.execSql(sql, {
                    limit : limit,
                    offset : offset
                }).then(action);
            });
        });
    },

    _getCollectionData : function(sqlTemplate, options) {
        var that = this;
        return that.readTransaction(function(w) {
            return that._checkCollection(w, options, true, function() {
                var collection = that._getCollection(options);
                var lang = that._getLanguage(options);
                var limit = that._getSearchLimit(options);
                var offset = that._getSearchOffset(options);
                var ids = that._getIdsAsSqlString(options);
                return w.execSql(sqlTemplate({
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

};

extend(MosaicService.prototype, PgConnector.prototype, MosaicConfig.prototype,
        MosaicService.prototype, MosaicServiceMethods);
module.exports = MosaicService;