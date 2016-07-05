var fs = require('fs');
var path = require('path');

function load(dir, file) {
    var sql = fs.readFileSync(path.resolve(dir, file), 'UTF8');
    return function(f) {
        return replace(sql, f);
    }
}

function replace(str, f) {
    var m;
    if (typeof f === 'object') {
        m = function(key) {
            return f[key];
        }
    } else if (typeof f === 'function') {
        m = f;
    } else {
        m = function() {
            return '';
        }
    }
    return (str || '').replace(/\$\{(.*?)\}/gim, function(full, val) {
        return m(val || '') || '';
    });
}

module.exports = {
    load : load,
    replace : replace
}
