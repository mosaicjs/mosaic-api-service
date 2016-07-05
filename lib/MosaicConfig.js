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

    _getStyle : function(options) {
        return that.params.style;
    },

    _getQuery : function(options) {
        var index = this._getIndex(options);
        var query = options.query[index] || options.query['q'];
        return query;
    },

    _getLanguage : function(options) {
        return 'french';
        return 'english';
    },

    _getLocale : function(options) {
        var lang = this._getLanguage(options);
        return lang.substring(0, 2);
    },

    _getCollection : function(options) {
        return options.params.collection;
    },

    _getIndex : function(options) {
        return options.params.index;
    },

    _getStyle : function(options) {
        return options.params.style;
    },

    _getSearchLimit : function(options) {
        return +options.query.limit || 100;
    },

    _getSearchOffset : function(options) {
        return +options.query.offset || 0;
    },

    _getIds : function(options) {
        return (options.params.ids || '').split(',');
    },

    _getIdsAsSqlString : function(options) {
        return this._getIds(options).map(function(id) {
            return "'" + id + "'";
        }).join(',');
    },

    _getTilesBaseDir : function(options) {
        var style = this._getStyle(options);
        var baseDir = path.resolve(this.options.baseDir, style);
        return baseDir;
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