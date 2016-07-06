var mm = require('mosaic-mapnik');
var TilesGenerator = mm.tiles.TilesGenerator;
var path = require('path');
var DynamicTilesProvider = require('./DynamicTilesProvider');
var MosaicConfig = require('./MosaicConfig');
var extend = MosaicConfig.extend;

function TilesRenderer(options) {
    this.options = options || {};
    this._generators = {};
    this._configs = {};
}
extend(TilesRenderer.prototype, MosaicConfig.prototype, {

    loadTile : function(options) {
        var that = this;
        return Promise.resolve().then(function() {
            return that._getTilesGenerator(options);
        }).then(function(generator) {
            return generator.loadTile(options);
        });
    },

    _getTilesGenerator : function(options) {
        var that = this;
        return Promise.resolve().then(function() {
            var style = that._getStyle(options);
            that._tilesGenerators = that._tilesGenerators || {};
            var tilesGenerator = that._tilesGenerators[style];
            if (tilesGenerator)
                return tilesGenerator;
            return that._loadStyleConfig(style, options).then(function(config) {
                var dynamicProvider = that._getTilesProvider(config, options);
                tilesGenerator = new TilesGenerator({
                    baseDir : that._getTilesBaseDir(options),
                    ttl : that.options.ttl || 60 * 60 * 24 * 100,
                    cacheDir : that.options.cacheDir || '.cache',//
                    cacheId : function(options) {
                        var params = options.params || {};
                        var collection = params.collection || '';
                        var index = params.index || 'q';
                        var query = that._getQuery(options);
                        var locale = that._getLocale(options);
                        var format = params.format || 'png';
                        var cacheId = '[' + [ style, collection,//
                        index, locale, query, //
                        format ].join('][') + ']';
                        return cacheId;
                    },
                    provider : dynamicProvider
                });
                that._tilesGenerators[style] = tilesGenerator;
                return tilesGenerator;
            });
        });
    },

    _getTilesProvider : function(config, options) {
        var that = this;
        var orderBy = that._getOrderBy(config);
        var projection = that._getProjection(config);
        var providerOptions = extend({}, config, that.options, {
            baseDir : that._getTilesBaseDir(options),
            queryParams : function(options) {
                var collection = that._getCollection(options);
                var index = that._getIndex(options);
                var query = that._getQuery(options);
                var language = that._getLanguage(options);
                return {
                    query : query,
                    collection : collection,
                    index : index,
                    locale : language,
                    projection : projection,
                    order : orderBy
                }
            }
        });
        var dynamicProvider = new DynamicTilesProvider(providerOptions);
        return dynamicProvider;

    },

    _loadStyleConfig : function(style, options) {
        var that = this;
        return Promise.resolve().then(function() {
            var config = that._configs[style];
            if (config) {
                return config;
            }
            var baseDir = that.options.baseDir;
            var dir = path.join(baseDir, style);
            return Promise.resolve().then(function() {
                return require(dir);
            }).then(function(config) {
                that._configs[style] = config;
                return config;
            });
        }).then(function(config) {
            if (typeof config === 'function') {
                config = config(options);
            }
            return config;
        });
    },

    _getOrderBy : function(config) {
        var orderBy = config.orderBy || [];
        if (orderBy.length) {
            orderBy = orderBy.join(',');
            if (orderBy.length) {
                orderBy += ','
            }
        } else {
            orderBy = '';
        }
        return orderBy;
    },

    _getProjection : function(config) {
        var fields = config.fields || {};
        var projection = ['id::text AS id, '];
        Object.keys(fields).forEach(function(field) {
            var info = fields[field];
            if (typeof info === 'string') {
                info = {
                    field : info
                }
            }
            var array = info.field.split('.');
            var f = array[0];
            if (array.length > 1) {
                f = f + '->' + array.slice(1).map(function(n) {
                    return "'" + n + "'";
                }).join('->');
            }
            if (info.type === 'boolean') {
                f = 'to_boolean((' + f + ')::jsonb)';
            } else if (info.array) {
                f = "to_string((" + f + ")::jsonb, ',')";
            } else {
                f = "to_string((" + f + ")::jsonb)";
            }
            if (info['default']) {
                // f = 'coalesce(' + f + ", '" + info['default'] +
                // "')";
            }
            f += ' AS ' + field + ',';
            projection.push(f);
        });

        return projection.join('');
    }
});

module.exports = TilesRenderer;