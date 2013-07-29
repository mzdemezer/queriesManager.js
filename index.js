module.exports = function(options){
	options = options || {};
	if(!(options.user && options.password)){
		throw "database> user or password unspecified, cannot configure module";
	}
	
	var opts = {
		host: "localhost"
	,	port: "3306"
	,	user: null
	,	log: false
	,	password: null
	,	database: null
	,	queries: null
	,	poolSize: 10
	,	includeFields: null
	}
	Object.keys(options).forEach(function(item, idx, arr){
		opts[item] = options[item] || opts[item];
	});
	
	if(options.initQueries != null){
		opts.initQueries = options.initQueries;
	}else{
		opts.initQueries = true;
	}
	
	if(options.cluster){
		options.minSize = ~~options.minSize || 10;
		opts.numCPUs = options.numCPUs || require("os").cpus().length;
		opts.poolSize = Math.ceil(opts.poolSize / opts.numCPUs);
		if(opts.poolSize < options.minSize){
			opts.poolSize = options.minSize;
		}
	}
	
	var mysql = require("mysql")
		,	poolModule = require("generic-pool")
		,	queriesManager = require("./lib/queriesManager")
		,	pool = poolModule.Pool({
				max: opts.poolSize
			,	create: function(callback){
					var client = mysql.createConnection({
								host: opts.host
							,	port: opts.port
							,	user: opts.user
							,	password: opts.password
							,	database: opts.database
							});
					
					client.connect();
					
					callback(null, client);
				}
			,	destroy: function(client){
// 					console.log("FINISHED");
					client.end();
				}
			});
	
	if(options.queriesModule){
		opts.queries = options.queriesModule(opts);
	}
	
	return function(cb){
		var init
			,	initPool
			,	query;
		cb = cb || function(){};
		if(opts.initQueries){
			query = { type: "init" };
		}else{
			query = "SHOW DATABASES;";
		}
		initPool = poolModule.Pool({
				max: opts.poolSize
			,	create: function(callback){
					var client = mysql.createConnection({
								host: opts.host
							,	port: opts.port
							,	user: opts.user
							,	password: opts.password
							});
					
					client.connect();
					
					callback(null, client);
				}
			,	destroy: function(client){
					client.end();
				}
		});
		init = queriesManager(opts, initPool);
		init(query, function(err, resp){
			cleanUp(init, initPool);
			if(opts.log){
				console.log("init database>\n" + JSON.stringify(resp));
			}
			if(err){
				if(err == "queries> Invalid query type: init"){
					console.log("queries> WARNING! There are no database init queries");
				}else{
					return cb(err, null);
				}
			}
			cb(null, queriesManager(opts, pool));
		});
	}
	
	function cleanUp(init, initPool){
		initPool.drain(function(){
			initPool.destroyAllNow();
			delete initPool;
			initPool = undefined;
			delete init;
			init = undefined;
		});
	}
};
