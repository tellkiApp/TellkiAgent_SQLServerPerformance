/**
* This script was developed by Guberni and is part of Tellki's Monitoring Solution
*
* February, 2015
* 
* Version 1.0
*
* DEPENDENCIES:
*		mssql v1.3.0 (https://www.npmjs.com/package/mssql)
* 
* DESCRIPTION: Monitor MSSQL Performance utilization
*
* SYNTAX: node mssql_performance_monitor.js <HOST> <METRIC_STATE> <PORT> <USER_NAME> <PASS_WORD>
* 
* EXAMPLE: node mssql_performance_monitor.js "10.10.2.5" "1,1" "3306" "user" "pass"
*
* README:
*		<HOST> MSSQL ip address or hostname.
* 
*		<METRIC_STATE> is generated internally by Tellki and it's only used by Tellki default monitors.
*		1 - metric is on ; 0 - metric is off
*		
*		<PORT> MSSQL port
*		
*		<USER_NAME> MSSQL user to connect
*		
*		<PASS_WORD> MSSQL user password
**/


var fs = require('fs');

var tempDir = "/tmp";

var runs = 0;
/*
* METRICS IDS
* List with all metrics to be retrieved.
* Attribute "id" represents the metric id
* Attribute "value" used to store the metric value during retrieval
* Attribute "ratio" indicates if the metric value is absolute or a ratio (must be calculated)
* Attribute "query" represents the query to run to get the value
*/
var metrics =[];

metrics["BufferCacheHitRatio"] =  {id:"132:Buffer cache hit ratio:4",value:"",ratio:false,query:"SELECT (a.cntr_value * 1.0 / b.cntr_value) * 100.0 [BufferCacheHitRatio] FROM (SELECT * FROM sys.dm_os_performance_counters  WHERE counter_name = 'Buffer cache hit ratio' AND object_name LIKE '%Buffer Manager%') a  CROSS JOIN (SELECT * FROM sys.dm_os_performance_counters  WHERE counter_name = 'Buffer cache hit ratio base' and object_name LIKE '%Buffer Manager%') b "};
metrics["PageLifeExpectancy"] =  {id:"86:Page life expectancy:4",value:"",ratio:false,query:"SELECT cntr_value [PageLifeExpectancy] FROM sys.dm_os_performance_counters " + "WHERE counter_name = 'Page life expectancy' " + "AND object_name like '%Buffer Manager%'"};
metrics["BatchRequests"] =  {id:"151:Batch Requests/Sec:4",value:"",ratio:true,query:"SELECT cntr_value [BatchRequests] FROM sys.dm_os_performance_counters " + "WHERE counter_name = 'Batch Requests/sec' " + "AND object_name LIKE '%SQL Statistics%'"};
metrics["SqlCompilations"] =  {id:"48:SQL Compilations/Sec:4",value:"",ratio:true,query:"SELECT cntr_value [SqlCompilations] FROM sys.dm_os_performance_counters " + "WHERE counter_name = 'SQL Compilations/sec' " + "AND object_name LIKE '%SQL Statistics%'"};
metrics["ReCompilations"] =  {id:"65:SQL Re-Compilations/Sec:4",value:"",ratio:true,query:"SELECT cntr_value [ReCompilations] FROM sys.dm_os_performance_counters " + "WHERE counter_name = 'SQL Re-Compilations/sec' " + "AND object_name LIKE '%SQL Statistics%'"};	
metrics["UserConnections"] = {id:"126:User Connections:4",value:"",ratio:false,query:"SELECT cntr_value [UserConnections] FROM sys.dm_os_performance_counters " + "WHERE counter_name = 'User Connections' " + "AND object_name like '%General Statistics%'"};
metrics["LockWaits"] = {id:"9:Lock Waits/Sec:4",value:"",ratio:true,query:"SELECT cntr_value [LockWaits] FROM sys.dm_os_performance_counters " + "WHERE counter_name = 'Lock Waits/sec' " + "AND instance_name = '_Total' AND object_name LIKE '%Locks%'"};
metrics["PageSplits"] =  {id:"102:Page Splits/Sec:4",value:"",ratio:true,query:"SELECT cntr_value [PageSplits] FROM sys.dm_os_performance_counters " + "WHERE counter_name = 'Page Splits/sec' " + "AND object_name LIKE '%Access Methods%'"};
metrics["ProcessesBlocked"] =  {id:"196:Processes Block:4",value:"",ratio:false,query:"SELECT cntr_value [ProcessesBlocked] FROM sys.dm_os_performance_counters " + "WHERE counter_name = 'Processes blocked' " + "AND object_name like '%General Statistics%'"};
metrics["CheckpointPages"] =  {id:"115:Checkpoint Pages/Sec:4",value:"",ratio:true,query:"SELECT cntr_value [CheckpointPages] FROM sys.dm_os_performance_counters " + "WHERE counter_name = 'Checkpoint pages/sec' " + "AND object_name LIKE '%Buffer Manager%'"};

metricsLength = 10;
 

// ############# INPUT ###################################

//START
(function() {
	try
	{
		monitorInput(process.argv.slice(2));
	}
	catch(err)
	{	
		if(err instanceof InvalidParametersNumberError)
		{
			console.log(err.message);
			process.exit(err.code);
		}
		else if(err instanceof InvalidAuthenticationError)
		{
			console.log(err.message);
			process.exit(err.code);
		}
		else if(err instanceof DatabaseConnectionError)
		{
			console.log(err.message);
			process.exit(err.code);
		}
		else
		{
			console.log(err.message);
			process.exit(1);
		}
	}
}).call(this)


/*
* Verify number of passed arguments into the script.
*/
function monitorInput(args)
{
	if(args.length === 5)
	{
		monitorInputProcess(args);
	}
	else
	{
		throw new InvalidParametersNumberError()
	}
}

/*
* Process the passed arguments and send them to monitor execution (monitorDatabasePerformance)
* Receive: arguments to be processed
*/
function monitorInputProcess(args)
{
	//<HOST> 
	var hostname = args[0];
	
	//<METRIC_STATE> 
	var metricState = args[1].replace("\"", "");
	
	var tokens = metricState.split(",");

	var metricsExecution = new Array(metricsLength);

	for(var i in tokens)
	{
		metricsExecution[i] = (tokens[i] === "1")
	}
	
	//<PORT> 
	var port = args[2];
	
	
	// <USER_NAME> 
	var username = args[3];
	
	username = username.length === 0 ? "" : username;
	username = username === "\"\"" ? "" : username;
	if(username.length === 1 && username === "\"")
		username = "";
	
	// <PASS_WORD>
	var passwd = args[4];
	
	passwd = passwd.length === 0 ? "" : passwd;
	passwd = passwd === "\"\"" ? "" : passwd;
	if(passwd.length === 1 && passwd === "\"")
		passwd = "";
	
	//create request object to be executed
	var requests = []
	
	var request = new Object()
	request.hostname = hostname;
	request.metricsExecution = metricsExecution;
	request.port = port;
	request.username = username;
	request.passwd = passwd;
	
	requests.push(request)
	
	//call monitor
	monitorDatabasePerformance(requests);
	
}



// ################# DATABASE PERFORMANCE ###########################
/*
* Retrieve metrics information
* Receive: object request containing configuration
*/
function monitorDatabasePerformance(requests) 
{
	var mssql = require('mssql');
	
	for(var i in requests)
	{
		var request = requests[i];
		
		//set connection configuration
		var config = {
			user: request.username,
			password: request.passwd,
			server: request.hostname, // You can use 'localhost\\instance' to connect to named instance
			port: request.port
			//database: '',
		}
		
		
		//try connection
		var connection = new mssql.Connection(config, function(err) {
			// ... error checks
			if(err)
			{
				if(err.code === "ELOGIN")
				{
					errorHandler(new InvalidAuthenticationError());
				}
				else
				{
					var e = new DatabaseConnectionError(err.message);
					e.message = err.message;
					errorHandler(e);
				}
			}
			
			// create request 
			var req = connection.request();
			
			var metricsNames = Object.keys(metrics)
			
			//retrieve data
			for(var i in metricsNames)
			{
				var metric = metrics[metricsNames[i]];
				
				getDataValue(connection, req, metric, metricsNames[i], request);
			}
		});
	}
}



//################### OUTPUT METRICS ###########################
/*
* Send metrics to console
* Receive: metrics list to output
*/
function output(toOutput)
{
	for(var i in toOutput)
	{
		var out = "";
		
		out += toOutput[i].id + "|";
		out += toOutput[i].value + "|";
		
		console.log(out);
	}
}


//################### ERROR HANDLER #########################
/*
* Used to handle errors of async functions
* Receive: Error/Exception
*/
function errorHandler(err)
{
	if(err instanceof InvalidAuthenticationError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof DatabaseConnectionError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof CreateTmpDirError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof WriteOnTmpFileError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof MetricNotFoundError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else
	{
		console.log(err.message);
		process.exit(1);
	}
}



// ##################### UTILS #####################
/*
* Execute query and extract data from query results
* Receive:
* - mssql connection
* - connection request
* - metric containing the query to run
* - metric name
* - object request containing configuration
*/
function getDataValue(connection, connRequest, metric, metricName, request)
{
	connRequest.query(metric.query, function(err, recordset) {
	
		runs++;
		// ... error checks
		if(err)
		{
			var e = new DatabaseConnectionError(err.message);
			e.message = err.message;
			errorHandler(e);
		}
		
		if(recordset[0][metricName])
		{
			metric.value = parseFloat(recordset[0][metricName]); 
		}
		else
		{
			var e = new MetricNotFoundError();
			e.message = "Unable to collect metric "+ metrics[metricName].id;
			errorHandler(e);
		}
		
		if(runs === metricsLength)
		{
			connection.close();
			createJSONMetrics(request);
		}
	});

}


/*
* Create json structure to save 
* Receive: object request containing configuration 
*/
function createJSONMetrics(request)
{	
	var metricsName = Object.keys(metrics);
	
	var jsonString = "[";

	var dateTime = new Date().toISOString();

	for(var i in metricsName)
	{
		if(request.metricsExecution[i])
		{	
			jsonString += "{";
				
			jsonString += "\"variableName\":\""+metricsName[i]+"\",";
			jsonString += "\"metricUUID\":\""+metrics[metricsName[i]].id+"\",";
			jsonString += "\"timestamp\":\""+ dateTime +"\",";
			jsonString += "\"value\":\""+ metrics[metricsName[i]].value +"\"";
			
			jsonString += "},";
			
		}
	}

	if(jsonString.length > 1)
		jsonString = jsonString.slice(0, jsonString.length-1);

	jsonString += "]";
	
	processDeltas(request, jsonString);

}


/*
* Process performance results
* Receive: 
* - request object containing configuration
* - retrived results
*/
function processDeltas(request, results)
{
	var file = getFile(request.hostname, request.port);
	
	var toOutput = [];
	
	if(file)
	{		
		var previousData = JSON.parse(file);
		var newData = JSON.parse(results);
			
		for(var i = 0; i < newData.length; i++)
		{
			var endMetric = newData[i];
			var initMetric = null;
			
			for(var j = 0; j < previousData.length; j++)
			{
				if(previousData[j].metricUUID === newData[i].metricUUID)
				{
					initMetric = previousData[j];
					break;
				}
			}
			
			if (initMetric != null)
			{
				var deltaValue = getDelta(initMetric, endMetric, request);
				
				var rateMetric = new Object();
				rateMetric.id = endMetric.metricUUID;
				rateMetric.timestamp = endMetric.timestamp;
				rateMetric.value = deltaValue;
				
				toOutput.push(rateMetric);
			}
			else
			{	
				var rateMetric = new Object();
				rateMetric.id = endMetric.metricUUID;
				rateMetric.timestamp = endMetric.timestamp;
				rateMetric.value = 0;
				
				toOutput.push(rateMetric);
			}
		}
		
		setFile(request.hostname, request.port, results);

		for (var m = 0; m < toOutput.length; m++)
		{
			for (var z = 0; z < newData.length; z++)
			{
				var systemMetric = metrics[newData[z].variableName];
				
				if (systemMetric.ratio === false && newData[z].metricUUID === toOutput[m].id)
				{
					toOutput[m].value = newData[z].value;
					break;
				}
			}
		}

		output(toOutput)
		
	}
	else
	{
		setFile(request.hostname, request.port, results);
		process.exit(0);
	}
}



/*
* Calculate ratio metric's value
* Receive: 
* - previous value
* - current value
* - 
*/
function getDelta(initMetric, endMetric, request)
{
	var deltaValue = 0;

	var decimalPlaces = 2;

	var date = new Date().toISOString();
	
	if (parseFloat(endMetric.value) < parseFloat(initMetric.value))
	{	
		deltaValue = parseFloat(endMetric.value).toFixed(decimalPlaces);
	}
	else
	{	
		var elapsedTime = (new Date(endMetric.timestamp).getTime() - new Date(initMetric.timestamp).getTime()) / 1000;	
		deltaValue = ((parseFloat(endMetric.value) - parseFloat(initMetric.value))/elapsedTime).toFixed(decimalPlaces);
	}
	
	return deltaValue;
}



/*
* Get last results if any saved
* Receive: 
* - mssql hostname or ip address
* - mssql port
*/
function getFile(hostname, port)
{
		var dirPath =  __dirname +  tempDir + "/";
		var filePath = dirPath + ".mssql_"+ hostname +"_"+ port +".dat";
		
		try
		{
			fs.readdirSync(dirPath);
			
			var file = fs.readFileSync(filePath, 'utf8');
			
			if (file.toString('utf8').trim())
			{
				return file.toString('utf8').trim();
			}
			else
			{
				return null;
			}
		}
		catch(e)
		{
			return null;
		}
}


/*
* Save current metrics values to be used to calculate ratios on next runs
* Receive: 
* - mssql hostname or ip address
* - mssql port
* - retrieved result
*/
function setFile(hostname, port, json)
{
	var dirPath =  __dirname +  tempDir + "/";
	var filePath = dirPath + ".mssql_"+ hostname +"_"+ port +".dat";
		
	if (!fs.existsSync(dirPath)) 
	{
		try
		{
			fs.mkdirSync( __dirname+tempDir);
		}
		catch(e)
		{
			var ex = new CreateTmpDirError();
			ex.message = e.message;
			errorHandler(ex);
		}
	}

	try
	{
		fs.writeFileSync(filePath, json);
	}
	catch(err)
	{
		var ex = new WriteOnTmpFileError();
		ex.message = err.message;
		errorHandler(ex);
	}
}


//####################### EXCEPTIONS ################################

//All exceptions used in script

function InvalidParametersNumberError() {
    this.name = "InvalidParametersNumberError";
    this.message = "Wrong number of parameters.";
	this.code = 3;
}
InvalidParametersNumberError.prototype = Object.create(Error.prototype);
InvalidParametersNumberError.prototype.constructor = InvalidParametersNumberError;


function InvalidAuthenticationError() {
    this.name = "InvalidAuthenticationError";
    this.message = "Invalid authentication.";
	this.code = 2;
}
InvalidAuthenticationError.prototype = Object.create(Error.prototype);
InvalidAuthenticationError.prototype.constructor = InvalidAuthenticationError;

function DatabaseConnectionError() {
	this.name = "DatabaseConnectionError";
    this.message = "";
	this.code = 11;
}
DatabaseConnectionError.prototype = Object.create(Error.prototype);
DatabaseConnectionError.prototype.constructor = DatabaseConnectionError;

function CreateTmpDirError()
{
	this.name = "CreateTmpDirError";
    this.message = "";
	this.code = 21;
}
CreateTmpDirError.prototype = Object.create(Error.prototype);
CreateTmpDirError.prototype.constructor = CreateTmpDirError;


function WriteOnTmpFileError()
{
	this.name = "WriteOnTmpFileError"; 
    this.message = "";
	this.code = 22;
}
WriteOnTmpFileError.prototype = Object.create(Error.prototype);
WriteOnTmpFileError.prototype.constructor = WriteOnTmpFileError;

function MetricNotFoundError() {
    this.name = "MetricNotFoundError";
    this.message = "";
	this.code = 8;
}
MetricNotFoundError.prototype = Object.create(Error.prototype);
MetricNotFoundError.prototype.constructor = MetricNotFoundError;
