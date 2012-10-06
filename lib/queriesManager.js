/**	Options
	*		queries - ready to work queryModule
	*		includeFields - seriously, who needs fields?
	*		log
	*/

module.exports = function(opts, pool){
	if(!pool){
		throw "queriesManager> Fail at init. I need a pool, buddy, that's the deal!";
	}
	var	util = require("util")
		,	addResult;

	opts = opts || {};
	if(opts.includeFields){
		addResults = function(resp, rows, fields){
			resp.push({
				rows: rows
			,	fields: fields
			});
		}
	}else{
		addResults = function(resp, rows){
			resp.push(rows);
		}
	}
	
	return function(queries, callback){
		if(util.isArray(queries)){
			queries.reverse();
		}
		callback = callbackWrapper(callback);
		
		pool.acquire(function(err, client){
			if(err){
				throw err;
			}else{
				manageQuery(client, queries, [], callback);
			}
		});
	};

	/**	Private functions
		*	executeQuery & manageQuery - these two functions are mutually recursive and responsible for sequentially handle all of the given queries
		*	callbackWrapper - release pool and check if there was an error
		* defaultCallback - self explanatory
		*/
	
	function executeQuery(client, query, rest, resp, callback){
		var correctQuery = true;

		while(typeof query == "object"){
			if(util.isArray(query)){
				rest = rest.concat(query.reverse());
				query = rest.pop();
			}else if(query.type){
				if(!(query.arguments == null || util.isArray(query.arguments))){
					query.arguments = [query.arguments];
				}
				if(opts.queries){
					query = opts.queries(query.type, query.arguments);
				}else{
					callback(client, "No queries module provided, unable to digest the given object: " + JSON.stringify(query), resp);
				}
			}else{
				correctQuery = false;
				break;
			}
		}
		if(!(query && correctQuery)){
			callback(client, "Invalid query: " + JSON.stringify(query), resp);
		}
		
		if(opts.log){
			console.log("execute> " + JSON.stringify(query));
		}

		client.query(query, function(err, rows, fields){
			if(err){
				callback(client, err, resp);
			}else{
				addResults(resp, rows, fields);
				manageQuery(client, rest, resp, callback);
			}
		});
	}
	
	function manageQuery(client, queries, resp, callback){
		if(opts.log){
			console.log("manage> " + JSON.stringify(queries));
		}
		var nextQuery;
		if(util.isArray(queries)){
			if(queries.length === 0){
				callback(client, null, resp);
			}else{
				nextQuery = queries.pop();
				executeQuery(client, nextQuery, queries, resp, callback);
			}
		}else{
			executeQuery(client, queries, [], resp, callback);
		}
	}
	
	function callbackWrapper(func){
		if(typeof func != "function"){
			func = defaultCallback;
		}
		return function(client, err, resp){
			pool.release(client);
			func(err, resp);
		};
	}
	
	function defaultCallback(err, resp){
		if(opts.log){
			console.log("Invalid callback");
			if(err){
				console.log("Error: " + err);
			}
			console.log("MySQL server response:\n\n" + resp);
		}
	};
};
