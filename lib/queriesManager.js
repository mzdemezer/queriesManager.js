/**	Options
	*		queries - ready to work queryModule
	*		includeFields - seriously, who needs fields?
	*		log
	*/

module.exports = function(opts, pool){
	if(!pool){
		throw "queriesManager> Fail at init. I need a pool, buddy, that's the deal!";
	}
	var util = require("util")
		,	addResult
		,	QM;

	opts = opts || {};
	if(opts.includeFields){
		addResults = function(resp, rows, fields){
			if(opts.log){
				console.log("addResults> " + rows);
			}
			resp.push({
				rows: rows
			,	fields: fields
			});
		}
	}else{
		addResults = function(resp, rows){
			if(opts.log){
				console.log("addResults> " + rows);
			}
			resp.push(rows);
		}
	}
	
	QM = function(qs, fF){
		prepareSerie(qs, fF, function(qs, fF){
			wrapCallback(qs, fF, "auto", function(queries, flowFuncs){
				pool.acquire(function(err, client){
					executeSerie(err, client, queries, flowFuncs);
				});
			});
		});
	};
	
	QM.createClient = function(callback){
		callback = callback || function(cl){
			cl.destroy();
		}
		pool.acquire(function(err, client){
			if(err){
				throw err;
			}else{
				callback(createClient(client));
			}
		});
	}
	
	function createClient(cli){
		var client = function(){
					var args = [].slice.apply(arguments)
						.concat([function(qs, fF){
							wrapCallback(qs, fF, "manual", function(queries, flowFuncs){
								executeSerie(null, cli, queries, flowFuncs);
							});
						}]);
					prepareSerie.apply(null, args);
				}
			,	okey = true;
		
		client.isOkey = function(){
			return okey;
		}
		
		client.fail = function(){
			okey = false;
		}
		
		client.destroy = function(){
			pool.release(cli);
			pool.destroy(cli);
		}
		return client;
	}
	
	QM.destroyClient = function(client){
		client.destroy();
	}
	
	return QM;

	/**	Private functions
		*	executeQuery & manageQuery - these two functions are mutually recursive and responsible for sequentially handle all of the given queries
		*	callbackWrapper - release pool and check if there was an error
		* defaultCallback - self explanatory
		*/
	
	function prepareSerie(queries, flowFuncs, callback){
		callback = callback || function(){
			console.log("Serie prepared, nothing to do with it!!");
		}
		if(util.isArray(queries)){
			queries.reverse();
		}
		if(typeof flowFuncs === "function"){
			flowFuncs = {
				callback: flowFuncs
			};
		}
		callback(queries, flowFuncs);
	}
	
	function wrapCallback(queries, flowFuncs, mode, callback){
		if(mode === "auto"){
			flowFuncs.callback = callbackAutoWrapper(flowFuncs.callback);
		}else{
			flowFuncs.callback = callbackManualWrapper(flowFuncs.callback);
		}
		callback(queries, flowFuncs);
	}
	
	function executeSerie(err, client, queries, flowFuncs){
		if(err){
			throw err;
		}else{
			if(flowFuncs.init){
				manageQuery(client, flowFuncs.init, [], {
					callback: function(err, resp){
						if(err){
							flowFuncs.callback(err, resp);
						}else{
							manageQuery(client, queries, [], flowFuncs);
						}
					}
				});
			}else{
				manageQuery(client, queries, [], flowFuncs);
			}
		}
	}
	
	function executeQuery(client, query, rest, resp, flowFuncs){
		var correctQuery = true;

		while(query && typeof query == "object"){
			if(util.isArray(query)){
				rest = rest.concat(query.reverse());
				query = rest.pop();
			}else if(query.type){
				if(!util.isArray(query.arguments)){
					query.arguments = [query.arguments];
				}
				if(opts.queries){
					query = opts.queries(query.type, query.arguments);
				}else{
					flowFuncs.callback(client, "No queries module provided, unable to digest the given object: " + JSON.stringify(query), resp);
				}
			}else{
				correctQuery = false;
				break;
			}
		}
		
		if(!correctQuery){
			flowFuncs.callback(client, "Invalid query: " + JSON.stringify(query), resp);
		}else{
			if(opts.log){
				console.log("execute> " + JSON.stringify(query));
			}
			if(query){
				client.query(query, function(err, rows, fields){
					
					if(err){
						flowFuncs.callback(client, err, resp);
					}else{
						addResults(resp, rows, fields);
						manageQuery(client, rest, resp, flowFuncs);
					}
				});
			}else{
				addResults(resp, null, null);
				manageQuery(client, rest, resp, flowFuncs);
			}
		}
	}
	
	function manageQuery(client, queries, resp, flowFuncs){
		if(opts.log){
			console.log("manage> " + JSON.stringify(queries));
		}
		var nextQuery;
		if(util.isArray(queries)){
			if(queries.length === 0){
				flowFuncs.callback(client, null, resp);
			}else{
				nextQuery = queries.pop();
				executeQuery(client, nextQuery, queries, resp, flowFuncs);
			}
		}else{
			executeQuery(client, queries, [], resp, flowFuncs);
		}
	}
	
	function callbackAutoWrapper(func){
		if(typeof func != "function"){
			func = defaultCallback;
		}
		return function(client, err, resp){
			pool.release(client);
			pool.destroy(client);
			func(err, resp);
		};
	}
	
	function callbackManualWrapper(func){
		if(typeof func != "function"){
			func = defaultCallback;
		}
		return function(client, err, resp){
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
