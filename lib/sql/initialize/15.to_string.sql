CREATE OR REPLACE FUNCTION to_string(o jsonb, c text) RETURNS text AS $$
    var result = toString(o);
    return result.join(c);
    function toString(o, result){
        if (!result) result = [];
	    if (Array.isArray(o)) {
	       o.forEach(function(child){
	           toString(child, result);
	       });
	    } else if (typeof o === 'object') {
	       Object.keys(o).forEach(function(key){
	          var val = toString(o[key])
	          if (val){
                result.push(val);
	          }
	       })
	    } else if (o) {
	        result.push(o + '');
	    } else {
	        result.push('');
	    }
        return result;
    }
$$ LANGUAGE plv8 STRICT IMMUTABLE