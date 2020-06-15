create or replace procedure SP_COMPACT(
    I_TGT_DB VARCHAR                 -- Name of the target (local) database
    ,I_TGT_SCHEMA VARCHAR            -- Name of the schema in the target (local) database
    ,I_MAX_SCHEMA_VERSIONS FLOAT     -- Maximum number of versions with different object structures
    ,I_MAX_VERSIONS_PER_SCHEMA FLOAT -- Maximum number of data snapshot versions in the same structural version
)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$
//note:  this proc returns an array, either success or fail right now
const tgt_db  = I_TGT_DB;
const tgt_schema  = I_TGT_SCHEMA;
const max_schema_versions = I_MAX_SCHEMA_VERSIONS;
const max_versions_per_schema = I_MAX_VERSIONS_PER_SCHEMA;

const internal = "INTERNAL_";
const meta_schema = internal + tgt_schema + "_METADATA";
const status_begin = "BEGIN";
const status_end = "END";
const status_failure = "FAILURE";
const version_initial = "000001";

var return_array = [];
var locked_schema_version = "";
var counter = 0;
var schema_name = "";
var status=status_end;
var schema_version="";
var data_version="";

var procName = Object.keys(this)[0];


function log ( msg ) {
   var d=new Date();
   var UTCTimeString=("00"+d.getUTCHours()).slice(-2)+":"+("00"+d.getUTCMinutes()).slice(-2)+":"+("00"+d.getUTCSeconds()).slice(-2);
   return_array.push(UTCTimeString+" "+msg);
}

function flush_log (status){
   var message="";
   var sqlquery="";
   for (i=0; i < return_array.length; i++) {
      message=message+String.fromCharCode(13)+return_array[i];
   }
   message=message.replace(/'/g,"");
      
   for (i=0; i<2; i++) {
      try {
   
         var sqlquery = "INSERT INTO \"" + tgt_db + "\"." + meta_schema + ".log (target_schema, version, status,message) values ";
         sqlquery = sqlquery + "('" + tgt_schema + "','" + "000000000000" + "','" + status + "','" + message + "');";
         snowflake.execute({sqlText: sqlquery});
         break;
      }
      catch (err) {
         sqlquery=`
            CREATE TABLE IF NOT EXISTS "`+ tgt_db + `".` + meta_schema + `.log (
               id integer AUTOINCREMENT (0,1)
               ,create_ts timestamp_ltz default current_timestamp
               ,target_schema varchar
               ,version varchar
               ,session_id number default to_number(current_session())
               ,status varchar
               ,message varchar)`;
         snowflake.execute({sqlText: sqlquery});      
      }
   }  
}

try {

    try {
       // a schema is locked in case there is a record in a table called parameter in the metadata schema
       // indicating the locked version. If a schema version is locked, then only new data versions will 
       // be promoted.
       sqlquery = `
          SELECT lpad(value:SchemaVersion::int,6,'0') 
          FROM  "` + tgt_db + `".` + meta_schema + `.parameter `
       var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
    
       if (ResultSet.next()) {
          locked_schema_version = ResultSet.getColumnValue(1);
          log("Schema locked on Version "+locked_schema_version)
       } else {
          locked_schema_version = "999999999999";    
          log("Schema not locked on Version ")
       }        
    }
    catch (err) {
          locked_schema_version = "999999999999";    
          log("Schema not locked on Version ");  
    }

    log("procName: "+procName+" "+status_begin);
    flush_log("START");

    // determine all schemas to be deleted, i.e. it's an obsolete schema version or and obsolete version 
    // an update to the dataset in a specific schema version

    var sqlquery=`
             select schema_name
             from (select schema_name, dense_rank() over (order by schema_version desc) schema_rank
                          , schema_version, data_version
                          , row_number() over (partition by schema_version order by schema_version desc, data_version desc) data_rank
                   from  (SELECT schema_name , substr(schema_name,-12,6) schema_version, substr(schema_name,-6) data_version  
                          FROM  "` + tgt_db + `".information_schema.schemata where rlike (schema_name,'` + tgt_schema + `_[0-9]{12}')))
             where (schema_rank > ` + max_schema_versions + ` or data_rank > ` + max_versions_per_schema + `)
               and schema_name < '` + tgt_schema + `_` + locked_schema_version + `'
             order by schema_name desc;`
     
    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
    
    while (ResultSet.next())  {
       counter = counter + 1;
       schema_name = ResultSet.getColumnValue(1);
       snowflake.execute({sqlText: "DROP /* # " + counter + " */ SCHEMA \"" + tgt_db + "\".\"" + schema_name + "\";"});
       log("drop: " + tgt_db + "." + schema_name);
    }

    log("procName: " + procName + " " + status_end)
    flush_log(status)

    return return_array
}
catch (err) {
    log("ERROR found - MAIN try command");
    log("err.code: " + err.code);
    log("err.state: " + err.state);
    log("err.message: " + err.message);
    log("err.stacktracetxt: " + err.stacktracetxt);
    log("procName: " + procName );

    flush_log(status_failure);
    
    return return_array;
}
$$;

