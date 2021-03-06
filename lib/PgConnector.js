var pg = require('pg')

// ------------------------------------------------------------------------
/**
 * This class provides some utility methods allowing to query Postgres database
 * and return resulting JSON objects using promises.
 * 
 * @param options.url
 *            url of the Db connection
 */
function PgConnector(options) {
    this.options = options || {};
    this.pool = new pg.Pool(this.options.db);
}

PgConnector.prototype.writeTransaction = function(action) {
    return this._run(true, action);
}

PgConnector.prototype.readTransaction = function(action) {
    return this._run(false, action);
},

PgConnector.prototype._checkInitialized = function(wrapper) {
    return Promise.resolve(true);
}

/** Executes the specified query and returns a list of results - JSON objects */
PgConnector.prototype._run = function(write, action) {
    var that = this;
    return new Promise(function(resolve, reject) {
        that.pool.connect(function(err, client, done) {
            if (err) {
                return reject(err);
            }
            var wrapper;
            Promise.resolve().then(
                    function() {
                        wrapper = that._newWrapper(client);
                        if (!that._initializePromise) {
                            that._initializePromise = Promise.resolve().then(
                                    function() {
                                        return that._checkInitialized(wrapper);
                                    })
                        }
                        return that._initializePromise.then(function() {
                            return action(wrapper);
                        });
                    }).then(resolve, reject).then(close, close);
            function close() {
                if (wrapper) {
                    wrapper.close();
                }
                done();
            }
        });
    })
}

PgConnector.prototype.close = function() {
    var that = this;
    return Promise.resolve().then(function() {
        return that.pool.end();
    });
}

PgConnector.prototype._newWrapper = function(client) {
    return new PgConnectionWrapper(this, client);
}

// ------------------------------------------------------------------------

function PgConnectionWrapper(connector, client) {
    this.client = client;
    this.connector = connector;
}
PgConnectionWrapper.prototype.close = function() {
    delete this.client;
    delete this.connector;
}

PgConnectionWrapper.prototype.execSql = function(sql, options) {
    var that = this;
    return new Promise(function(resolve, reject) {
        try {
            sql = that._prepareSql(sql, options);
            // console.log('*', sql.substring(0, 2000));
            that.client.query(sql, {}, function(err, res) {
                if (err) {
                    return reject(err);
                } else {
                    return resolve(res.rows);
                }
            });
        } catch (err) {
            return reject(err);
        }
    });
}

/** Prepares the specified query based on the given parameters */
PgConnectionWrapper.prototype._prepareSql = function(query, options) {
    options = options || {};
    var limit = +options.limit;
    var offset = +options.offset;
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
}

// ------------------------------------------------------------------------

module.exports.PgConnector = PgConnector;
module.exports.PgConnectionWrapper = PgConnectionWrapper;
