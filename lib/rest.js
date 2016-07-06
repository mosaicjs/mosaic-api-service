module.exports = function rest(params) {
    var action = params.action;
    var result = function(options) {
        var that = this;
        return Promise.resolve().then(function() {
            return action.call(that, options);
        }).then(null, function(err) {
            var errorInfo = {
                code : err.code || 505,
                type : 'error',
                message : err.message || err,
                stack : (err.stack || '').split('\n'),
                method : params,
                options : options,

            }
            // FIXME: this information should be logged
            console.log(errorInfo);
            throw err;
        });
    };
    delete params.action;
    for ( var key in params) {
        result[key] = params[key];
    }
    return result;
}