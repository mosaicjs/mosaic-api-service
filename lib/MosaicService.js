var mm = require('mosaic-mapnik');
var TilesGenerator = mm.tiles.TilesGenerator;
var path = require('path');
var fs = require('fs');
var crypto = require('crypto');
var uuid = require('uuid');

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

function loadInitializationScripts() {
    var dir = path.join(__dirname, 'sql/initialize/');
    var list = fs.readdirSync(dir);
    list.sort();
    var result = [];
    list.forEach(function(fileName) {
        result.push(templates.load(dir, fileName));
    });
    return result;
}

var SQL = {
    createCollection : loadTemplate('./MosaicService_createCollection.sql'),
    createCollectionIndex : loadTemplate('./MosaicService_createCollectionIndex.sql'),
    getCollectionStats : loadTemplate('./MosaicService_getCollectionStats.sql'),
    getSearchResultsStats : loadTemplate('./MosaicService_getSearchResultsStats.sql'),
    dropCollection : loadTemplate('./MosaicService_dropCollection.sql'),
    dropCollectionIndex : loadTemplate('./MosaicService_dropCollectionIndex.sql'),

    saveGeoJsonInCollection : loadTemplate('./MosaicService_saveGeoJsonInCollection.sql'),
    searchCollectionIndex : loadTemplate('./MosaicService_searchCollectionIndex.sql'),
    searchCollectionIndexIds : loadTemplate('./MosaicService_searchCollectionIndexIds.sql'),
    searchPositionInCollectionIndex : loadTemplate('./MosaicService_searchPositionInCollectionIndex.sql'),
    searchResultsSize : loadTemplate('./MosaicService_searchResultsSize.sql'),
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
        path : '/:lang/:collection',
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
                        collection : collection,
                        lang : lang
                    })).then(function() {
                        var message = '' + //
                        'Collection was sucessfully created';
                        return that._ok({
                            message : message,
                            collection : collection,
                            lang : lang
                        });
                    });
                })
            });
        }
    }),

    deleteCollection : rest({
        path : '/:lang/:collection',
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
                        collection : collection,
                        lang : lang
                    })).then(function(res) {
                        var message = '' + //
                        'Collection was sucessfully removed';
                        return that._ok({
                            message : message,
                            collection : collection,
                            lang : lang
                        });
                    });
                });
            });
        }
    }),

    getCollectionInfo : rest({
        path : '/:lang/:collection',
        method : 'get',
        description : 'Get information about the specified collection',
        action : function(options) {
            var that = this;
            return that.readTransaction(function(w) {
                return that._checkCollection(w, options, true,//
                function() {
                    return that._getCollectionSize(w, options) //
                    .then(function(size) {
                        var collection = that._getCollection(options);
                        var lang = that._getLanguage(options);
                        return that._ok({
                            message : 'Collection exists',
                            collection : collection,
                            lang : lang,
                            size : size
                        });
                    });
                });
            });
        }
    }),

    // ---------------------------------------------------------------
    // Collection data

    getCollectionStats : rest({
        path : '/:lang/:collection/stats',
        method : 'get',
        description : 'Returns statistics about entries of the specified collection',
        action : function(options) {
            var that = this;
            return that.readTransaction(function(w) {
                return that._checkCollection(w, options, true,//
                function() {
                    var collection = that._getCollection(options);
                    var lang = that._getLanguage(options);
                    var fields = that._getFieldsAsString(options);
                    return w.execSql(SQL.getCollectionStats({
                        collection : collection,
                        lang : lang,
                        fields : fields
                    })).then(function(res) {
                        return res[0].stats;
                    }).then(function(stats) {
                        return that._ok({
                            message : 'Collection statistics.',
                            collection : collection,
                            lang : lang,
                            stats : stats
                        });
                    });
                });
            });
        }
    }),

    getCollectionData : rest({
        path : '/:lang/:collection/data',
        method : 'get',
        description : 'Returns all data from the specified collection. '
                + 'Returns the "404 Not found" error if the resource was not found',
        action : function(options) {
            var that = this;
            return that.readTransaction(function(w) {
                var sqlTemplate = SQL.selectGeoJsonFromCollection;
                return that._getCollectionData(w, sqlTemplate, options)//
                .then(function(res) {
                    return that._getCollectionSize(w, options) //
                    .then(function(size) {
                        return that._toFeatureCollection(res, options, size);
                    });
                });
            });
        }
    }),

    getCollectionDataByIds : rest({
        path : '/:lang/:collection/data/:ids',
        method : 'get',
        description : 'Returns full information for entities with the specified ids. '
                + 'Returns the "404 Not found" error if the resource was not found',
        action : function(options) {
            var that = this;
            return that.readTransaction(function(w) {
                var sqlTemplate = SQL.selectGeoJsonFromCollectionByIds;
                return that._getCollectionData(w, sqlTemplate, options)//
                .then(function(res) {
                    return that._getCollectionSize(w, options) //
                    .then(function(size) {
                        return that._toFeatureCollection(res, options, size);
                    });
                });
            });
        }
    }),

    getCollectionIds : rest({
        path : '/:lang/:collection/ids',
        method : 'get',
        description : 'Returns a list of identifiers of all data ' //
                + 'from the specified collection. '
                + 'Returns the "404 Not found" error if the resource was not found',
        action : function(options) {
            var that = this;
            return that.readTransaction(function(w) {
                var sqlTemplate = SQL.selectIdsFromCollection;
                return that._getCollectionData(w, sqlTemplate, options)//
                .then(function(res) {
                    return that._toIdList(res, options);
                });
            });
        }
    }),

    setCollectionData : rest({
        path : '/:lang/:collection/data',
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
                    var index = {};
                    array.forEach(function(entry) {
                        var id = that._checkEntryId(entry);
                        index[id] = entry;
                    });

                    var chunkSize = 100;
                    var chunk = [];
                    var promises = [];
                    Object.keys(index).forEach(function(id, i) {
                        var entry = index[id];
                        chunk.push(entry);
                        if (chunk.length % chunkSize === 0) {
                            promises.push(that._saveChunk(w, //
                            collection, lang, chunk));
                            chunk = [];
                        }
                    })
                    if (chunk.length) {
                        promises.push(that._saveChunk(w, //
                        collection, lang, chunk));
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
                            lang : lang,
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
        path : '/:lang/:collection/index/:index',
        method : 'put',
        description : 'Creates a new collection index. '
                + 'Returns the "409 Conflict" error '
                + 'if the resource already exists.',
        action : function(options) {
            var that = this;
            return that.writeTransaction(function(w) {
                return that._checkIndex(w, options, false, function() {
                    return that._checkCollection(w, options, true, function() {
                        var collection = that._getCollection(options);
                        var index = that._getIndex(options);
                        var lang = that._getLanguage(options);
                        var fields = that._getFieldsAsString(options);
                        return w.execSql(SQL.createCollectionIndex({
                            collection : collection,
                            index : index,
                            lang : lang,
                            fields : fields
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
            });
        }
    }),

    updateCollectionIndex : rest({
        path : '/:lang/:collection/index/:index',
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
                            lang : lang
                        });
                    });
                });
            });
        }
    }),

    deleteCollectionIndex : rest({
        path : '/:lang/:collection/index/:index',
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
                            lang : lang
                        });
                    });
                });
            });
        }
    }),

    getCollectionIndexInfo : rest({
        path : '/:lang/:collection/index/:index',
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
                        lang : lang
                    });
                });
            });
        }
    }),

    searchCollectionIndex : rest({
        path : '/:lang/:collection/search',
        method : 'get',
        description : 'Performs full text search in the specified index '
                + 'and returns found entries.',
        action : function(options) {
            var that = this;
            var searchSql = SQL.searchCollectionIndex;
            return that._searchCollectionIndex(searchSql, options,//
            function(res) {
                return that._getSearchResultsSize(options) //
                .then(function(size) {
                    return that._toFeatureCollection(res, options, size);
                });
            });
        }
    }),

    searchCollectionIndexIds : rest({
        path : '/:lang/:collection/searchIds',
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
        path : '/:lang/:collection/search/position/:ids',
        method : 'get',
        description : 'Performs full text search in the specified index '
                + 'and returns result positions of entities with the '
                + 'specified identifiers.',
        action : function(options) {
            var that = this;
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

    getCollectionSearchStats : rest({
        path : '/:lang/:collection/search/stats',
        method : 'get',
        description : 'Returns statistics about entries '
                + 'corresponding to the specified search query',
        action : function(options) {
            var that = this;
            return that.readTransaction(function(w) {
                return that._checkCollection(w, options, true, function() {
                    var collection = that._getCollection(options);
                    var lang = that._getLanguage(options);
                    var fields = that._getFieldsAsString(options);
                    var query = that._getQueryAsSqlString(options);
                    return w.execSql(SQL.getSearchResultsStats({
                        collection : collection,
                        lang : lang,
                        fields : fields,
                        query : query
                    })).then(function(res) {
                        return res[0].stats;
                    }).then(function(stats) {
                        return that._ok({
                            message : 'Search results statistics.',
                            collection : collection,
                            lang : lang,
                            query : that._getQuery(options),
                            stats : stats
                        });
                    });
                });
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
                params.format = that._getTilesFormat(options);
                return tilesGenerator.loadTile(options)//
                .then(function(result) {
                    result.data = result.tile;
                    return result;
                });
                return service;
            })
        }
    }),

    _getSha1 : function(str) {
        var shasum = crypto.createHash('sha1');
        shasum.update(str);
        return shasum.digest('hex');
    },

    _uuid5 : function(str) {
        var hash = this._getSha1(str);
        var val = parseInt(hash.substring(16, 18), 16);
        val = val & 0x3f | 0xa0; // set variant
        return '' + hash.substring(0, 8) + '-' + //
        hash.substring(8, 12) + '-' + //
        '5' + // set version
        hash.substring(13, 16) + '-' + //
        val.toString(16) + hash.substring(18, 20) + '-' + //
        hash.substring(20, 32);
    },

    _uuid4 : function() {
        return uuid.v4();
    },

    _checkEntryId : function(obj) {
        var uuid = obj.id || '';
        var properties = obj.properties || {};
        if (!uuid
                .match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/)) {
            if (properties.id) {
                uuid = this._uuid5(properties.id);
            } else {
                uuid = this._uuid4();
            }
        }
        obj.id = uuid;
        return uuid;
    },

    _checkInitialized : function(w) {
        var that = this;
        return Promise.resolve().then(function() {
            return that._exists(w, SQL.tableExists({
                table : 'collections'
            })).then(function(exists) {
                if (!exists) {
                    var list = loadInitializationScripts();
                    var options = {};
                    var promise = Promise.resolve();
                    list.forEach(function(sqlBuilder) {
                        promise = promise.then(function() {
                            var sql = sqlBuilder(options);
                            return w.execSql(sql);
                        });
                    })
                    return promise.then(function() {
                        var createTableSql = //
                        'CREATE TABLE collections (name varchar(255) UNIQUE)';
                        return w.execSql(createTableSql);
                    });
                }
            }).then(function() {
                return true;
            });
        });
    },

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
        path : '/:lang/:collection/:style/:z/:x/:y/tile.:format',
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
                    params.format = that._getTilesFormat(options);
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

    _getCollectionTableName : function(collection, lang) {
        return 'collection_' + collection + '_' + lang + '_json';
    },

    _getCollectionSize : function(w, options) {
        var that = this;
        return Promise.resolve().then(function() {
            var collection = that._getCollection(options);
            var lang = that._getLanguage(options);
            var name = that._getCollectionTableName(collection, lang);
            return w.execSql(SQL.tableSize({
                table : name
            })).then(function(res) {
                var size = res[0].size;
                return size;
            });
        });
    },

    _collectionExists : function(w, collection, lang) {
        // TODO: if the requested collection exists - add it an internal
        // index to accelerate search.

        var name = this._getCollectionTableName(collection, lang);
        return this._tableExists(w, name);
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
                        lang : lang
                    });
                } else {
                    return that._errorResourceAlreadyExists({
                        message : 'Collection already exists',
                        collection : collection,
                        lang : lang
                    });
                }
            })

        })
    },

    _indexExists : function(w, collection, index, lang) {
        // TODO: if the requested collection exists - add it an internal
        // index
        // to accelerate search.
        return this._viewExists(w, 'fts_' + collection + '_' + lang + '_'
                + index);
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
                    lang : lang
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
                        lang : lang
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
                        lang : lang
                    });
                } else {
                    return that._errorResourceAlreadyExists({
                        message : 'Collection index already exists',
                        collection : collection,
                        index : index,
                        lang : lang
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

    _saveChunk : function(w, collection, lang, chunk) {
        var that = this;
        return Promise.resolve().then(function() {
            var str = JSON.stringify(chunk);
            str = str.replace(/[']/gim, "''");
            return w.execSql(SQL.saveGeoJsonInCollection({
                collection : collection,
                lang : lang,
                data : str
            })).then(function(result) {
                return {
                    length : chunk.length
                }
            });
        });
    },

    _getSearchResultsSize : function(options) {
        var that = this;
        return that.readTransaction(function(w) {
            var collection = that._getCollection(options);
            var lang = that._getLanguage(options);
            var query = that._getQueryAsSqlString(options);
            var sql = SQL.searchResultsSize({
                collection : collection,
                lang : lang,
                query : query
            });
            return w.execSql(sql).then(function(res) {
                return res[0].size;
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
                var ids = that._getIdsAsSqlString(options);
                var sql = sqlTemplate({
                    collection : collection,
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

    _getCollectionData : function(w, sqlTemplate, options) {
        var that = this;
        return that._checkCollection(w, options, true, function() {
            var collection = that._getCollection(options);
            var lang = that._getLanguage(options);
            var limit = that._getSearchLimit(options);
            var offset = that._getSearchOffset(options);
            var ids = that._getIdsAsSqlString(options);
            return w.execSql(sqlTemplate({
                collection : collection,
                lang : lang,
                ids : ids
            }), {
                limit : limit,
                offset : offset
            });
        })
    },

    _toFeatureCollection : function(res, options, size) {
        var collection = this._getCollection(options);
        var lang = this._getLanguage(options);
        var limit = this._getSearchLimit(options);
        var offset = this._getSearchOffset(options);
        return this._data({
            type : 'FeatureCollection',
            collection : collection,
            lang : lang,
            offset : offset,
            limit : limit,
            size : size,
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