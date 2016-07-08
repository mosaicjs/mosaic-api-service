var mm = require('mosaic-mapnik');
var MapnikRendererPool = mm.renderer.MapnikRendererPool;
var TilesProvider = mm.tiles.TilesProvider;
var VectorTilesGenerator = mm.tiles.VectorTilesGenerator;
var util = require('util');
var path = require('path');
var fs = require('fs');

var templates = require('./templates');
function loadTemplate(fileName) {
    var dir = path.join(__dirname, 'sql/runtime/');
    return templates.load(dir, fileName);
}
var CONFIG_TEMPLATE = loadTemplate('./DynamicTilesProvider_MapnikConfig.xml');
var SQL = {
    fullTextSearch : loadTemplate('./DynamicTilesProvider_fullTextSearch.sql'),
    allEntries : loadTemplate('./DynamicTilesProvider_allEntries.sql'),
}

function DynamicTilesProvider(options) {
    var that = this;
    that.options = options || {};
    that.provider = new VectorTilesGenerator(that.options);
    var pool = that.pool = new MapnikRendererPool(options);
    pool._readProjectConfig = function(key, options) {
        return Promise.resolve().then(function() {
            var sql = that._buildSqlQuery(options);
            sql = '(' + sql + ') as data';
            var collection = options.params.collection;
            var queryConfig = {
                sql : sql,
                layer : collection,
                geometry : 'geometry',
                "srs" : "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
                bufferSize : that.options.bufferSize || 128
            };
            var xml = CONFIG_TEMPLATE(function(key) {
                return that.options.db[key] || queryConfig[key] || '';
            });
            return {
                xml : xml
            }
        });
    };
}
util.inherits(DynamicTilesProvider, TilesProvider);

DynamicTilesProvider.prototype.loadTile = function(options) {
    var query = options.query || {};
    var key = JSON.stringify(query);
    var that = this;
    return this.pool.withRenderer(key, function(source) {
        return source.buildVectorTile(options.params) //
        .then(function(info) {
            var result = {};
            for ( var key in options) {
                result[key] = options[key];
            }
            result.tile = info.vtile;
            result.headers = {
                'Content-Type' : 'application/x-mapnik-vector-tile'
            };
            return result;
        });
    }, options);
}

DynamicTilesProvider.prototype._buildSqlQuery = function(options) {
    if (typeof this.options.buildSql === 'function') {
        return this.options.buildSql.call(this, options);
    }
    var queryParams = this.options.queryParams || function(options) {
        return '';
    };
    function getParams(key) {
        var params = queryParams(options) || {};
        return params[key] || '';
    }
    var query = getParams('query');
    var sql = query ? SQL.fullTextSearch(getParams) : SQL.allEntries(getParams);
    return sql;

}

module.exports = DynamicTilesProvider;
