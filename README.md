This is a node module, that provides:
- linking generic-pool, and felixge-mysql
- sending sequences of queries
- usage of external queries module, that would accept { type, arguments } objects and return ready queries
- execution of single queries, mentioned objects, arrays of them, and arrays of arrays of a.........rrays of them :P

It is currently dependent on:
- generic-pool (https://github.com/coopernurse/node-pool)
- mysql (https://github.com/felixge/node-mysql)

It is planned to become independent of above.

Hope you like it.

<pre>
//	Usage:
/*	Options:
Necessary: 
-		user
-		password
Optional
-		database - better use it or you will have to use USE sth; as first query of each connection
-		queriesModule - module returning function that would return queries given (type, arguments); I strongly suggest to make use of it
-		host - default to localhost
-		port - default to 3306
-		poolSize - default to 10
-		cluster - default to false; if true then poolSize will be devided by numCPUs with minimal minSize || 10 value
-		numCPUs - default to require('os').cpus().length, but only if cluster is truthy
-		minSize - default to ten
-		initQueries - false/true, default true; if false, no initQueries will be executed on start
-		log
-		includeFields - if true every result will be array of[{ rows1, fields1 }, { rows2, fields2 }, ...] objects; otherwise it will be only [ rows1, rows2, rows3, ...]
*/
	var	qrMan = require("queriesManager")(options)
		,	database;
	qrMan(function(err, db){
		if(err){
			console.log("You are guilty of:\n" + err);
		}else{
			database = db;
		}
	});
	
//	e.g.
	database([
		[ { type: "getCakes", arguments: [ withCherries, withCockolate ] }
		,	{ type: "getCookies", arguments: [ withMarmolade, small, fresh ] }
		]
	,	"SELECT * FROM users;"
	], function(err, resp){
		if(err){
			console.log("You naughty one, you! Here's your error:\n" + err);
		}else{
			console.log("Server responded with sth like this:\n" + JSON.stringify(resp));
		}
	});
/*
	database( queries, callback )
		
		queries ::= "string being a ready to send query;"
		queries ::= {
				type: "query_function_name_from_queries_module"
			, arguments: [whatever_works_with_it]
			}
			arguments ::= whatever your queries module work with
				if you want to return another function inside, then you will of course have to route the arguments, but it's possible to for a function to return e.g. { type: "addUser", arguments: [arguments_from_outer_function] }, and all recursive processing will be done
		queries ::= [ array of those above, or even arrays of arrays of arrays of ... of arrays ... of those above ]
		
		callback ::= function(err, resp){ ... }
*/
</pre>

Distributed under MIT licence; see LICENCE for more
 
