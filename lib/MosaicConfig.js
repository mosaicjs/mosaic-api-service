var path = require('path');

function MosaicConfig() {
}
MosaicConfig.extend = function(to) {
    to = to || {};
    for (var i = 1; i < arguments.length; i++) {
        var from = arguments[i];
        if (!from)
            continue;
        for ( var key in from) {
            if (from.hasOwnProperty(key)) {
                to[key] = from[key];
            }
        }
    }
    return to;
}

MosaicConfig.extend(MosaicConfig.prototype, {

    _getCollection : function(options) {
        return options.params.collection;
    },

    _getQuery : function(options) {
        if (!options._searchQuery) {
            var q = options.query.query;
            if (typeof q === 'string') {
                try {
                    q = JSON.parse(q);
                } catch (err) {
                }
            }
            options._searchQuery = q || {};
        }
        return options._searchQuery;
    },

    _getQueryAsSqlString : function(options) {
        var query = this._getQuery(options);
        return JSON.stringify(query);
    },

    _getLanguage : function(options) {
        var query = options.query || {};
        var lang = query.lang || this.options.lang || 'french';
        return lang;
    },

    _getLocale : function(options) {
        var lang = this._getLanguage(options);
        return lang.substring(0, 2);
    },

    _getIds : function(options) {
        return (options.params.ids || '').split(',');
    },

    _getIdsAsSqlString : function(options) {
        return this._getIds(options).map(function(id) {
            return "'" + id + "'";
        }).join(',');
    },

    _getIndex : function(options) {
        return options.params.index;
    },

    /** Returns <code>true</code> if query results should be intersection */
    _getIntersection : function(options) {
        var query = options.query || {};
        return !!query.intersection || true;
    },

    _getSearchLimit : function(options) {
        return +options.query.limit || 100;
    },

    _getSearchOffset : function(options) {
        return +options.query.offset || 0;
    },

    _getStyle : function(options) {
        return options.params.style;
    },

    _getTilesBaseDir : function(options) {
        var style = this._getStyle(options);
        var baseDir = path.resolve(this.options.baseDir, style);
        return baseDir;
    },

    _getTilesFormat : function(options) {
        var params = options.params || {};
        return params.format || params.type || 'png'
    },

    _toArray : function(data) {
        var array;
        if (Array.isArray(data)) {
            array = data;
        } else if (Array.isArray(data.features)) {
            array = data.features;
        } else if (!!data) {
            array = [ data ];
        } else {
            array = [];
        }
        return array;
    },

});
module.exports = MosaicConfig;