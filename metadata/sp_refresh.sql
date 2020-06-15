create or replace procedure SP_REFRESH(
    I_TGT_DB VARCHAR               -- Name of the replicated (secondary) database
    ,I_SVW_DB VARCHAR              -- Name of the new shared database
    ,I_SCHEMA VARCHAR              -- Name of schema in the replicated (secondary) database
    ,I_SCHEMA_VERSION VARCHAR      -- Target version ("LATEST" or specific Version)
    ,I_SHARE VARCHAR               -- Name of the Share to be created/used
)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$
//note:  this proc returns an array, either success or fail right now
const tgt_db  = I_TGT_DB;
const svw_db  = I_SVW_DB;
const tgt_schema  = I_SCHEMA;
const share = I_SHARE;
const tgt_schema_version = I_SCHEMA_VERSION

const internal = "INTERNAL_";
const tgt_meta_schema = tgt_schema + "_METADATA";
const meta_schema = internal + tgt_schema + "_METADATA";
const tgt_schema_version_latest = "LATEST";
const tgt_schema_new = internal + tgt_schema + "_NEW";

const status_begin = "BEGIN";
const status_end = "END";
const status_failure = "FAILURE";
const version_default = "000000";
const reference_type_direct = "DIRECT";
const reference_type_indirect = "INDIRECT";

var return_array = [];
var locked_schema_version = "";
var tgt_schema_curr = "";
var counter = 0;
var table_name = "";
var status=status_end;
var schema_version=version_default;
var data_version=version_default;
var curr_schema_version=version_default;
var curr_data_version=version_default; 

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
         var sqlquery = "INSERT INTO \"" + svw_db + "\"." + meta_schema + ".log (target_schema, version, status,message) values ";
         sqlquery = sqlquery + "('" + tgt_schema + "','" + curr_schema_version + curr_data_version + "','" + status + "','" + message + "');";
         snowflake.execute({sqlText: sqlquery});
         break;
      }
      catch (err) {
         sqlquery=`
            CREATE TABLE IF NOT EXISTS "`+ svw_db + `".` + meta_schema + `.log (
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

function grant_schema( i_src_schema_name, i_tgt_schema_name, i_reference_type) {
    var src_schema_name=i_src_schema_name;
    var tgt_schema_name=i_tgt_schema_name;
    var reference_type=i_reference_type;

    sqlquery = `
          SELECT TABLE_NAME
          FROM   "` + tgt_db + `".information_schema.tables
          WHERE  table_catalog = '` + tgt_db + `' and table_schema = '` + src_schema_name + `'`;

    var tableNameResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    while (tableNameResultSet.next())  {
          counter = counter + 1;
          table_name = tableNameResultSet.getColumnValue(1);

          // create a secure view to all tables in the the target schema and a grant select to the secure view to the share
          // since the share does not yet have the usage permission on the new share, these changes are not visible
          // to the consumer

          if (reference_type==reference_type_direct) {
             sqlquery = `
                GRANT SELECT ON /* # ` + counter + ` */ TABLE "` + tgt_db + `"."` + tgt_schema_name + `"."` + table_name + `" TO SHARE "` + share + `"`;
             snowflake.execute({sqlText: sqlquery});
             log("GRANT SELECT FOR TABLE " + table_name );
          } else {
             sqlquery = `
                CREATE OR REPLACE /* # ` + counter + ` */ SECURE VIEW "` + svw_db + `"."` + tgt_schema_name + `"."` + table_name + `" AS
                    SELECT * FROM "` + tgt_db + `"."` + src_schema_name + `"."` + table_name + `"`;
             snowflake.execute({sqlText: sqlquery});

             sqlquery = `
                GRANT SELECT ON /* # ` + counter + ` */ VIEW "` + svw_db + `"."` + tgt_schema_name + `"."` + table_name + `" TO SHARE "` + share + `"`;
             snowflake.execute({sqlText: sqlquery});
             log("CREATED SECURE VIEW FOR TABLE " + table_name );
          }
    }

    // Grant usage on the new schema to the share
    // Swap the current schema with the new schema
    // Revoke usage from the previously current (now old) schema

    sqlquery = `
          GRANT USAGE ON SCHEMA "` + svw_db + `"."` + tgt_schema_name + `" TO SHARE "` + share + `"`;
    snowflake.execute({sqlText: sqlquery});

}

try {

    snowflake.execute({sqlText: "CREATE SHARE IF NOT EXISTS \"" + share + "\""});
    snowflake.execute({sqlText: "CREATE DATABASE IF NOT EXISTS \"" + svw_db + "\""});
    snowflake.execute({sqlText: "CREATE SCHEMA IF NOT EXISTS \"" + svw_db + "\".\"" + meta_schema + "\""});
    snowflake.execute({sqlText: "CREATE SCHEMA IF NOT EXISTS \"" + svw_db + "\".\"" + tgt_meta_schema + "\""});
    snowflake.execute({sqlText: "CREATE SCHEMA IF NOT EXISTS \"" + svw_db + "\".\"" + tgt_schema + "\""});
    snowflake.execute({sqlText: "CREATE OR REPLACE SCHEMA \"" + svw_db + "\".\"" + tgt_schema_new + "\""});

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
          locked_schema_version = "[0-9]{12}";
          log("Schema not locked on Version ")
       }        
    }
    catch (err) {
          locked_schema_version = "[0-9]{12}";
          log("Schema not locked on Version ");  
    }

    // determine the latest data version in the latest (or locked) schema

    if (tgt_schema_version==tgt_schema_version_latest) {
       log("find latest version")
       sqlquery=`
          (SELECT schema_name , substr(schema_name,-12,6) schema_version, substr(schema_name,-6) data_version  
            FROM  "` + tgt_db + `".information_schema.schemata where rlike (schema_name,'` + tgt_schema + `_` + locked_schema_version + `'))
          ORDER BY schema_name DESC limit 1`;

       var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
       if (ResultSet.next()) {
          curr_schema_version=ResultSet.getColumnValue(2);
          curr_data_version=ResultSet.getColumnValue(3); 
          tgt_schema_curr=tgt_schema + "_" + curr_schema_version + curr_data_version ;
      
          log("procName: "+procName+" "+status_begin);
          log("CurrentVersion: " + tgt_schema_curr);
          flush_log(status_begin);
       } else {
          log ( "NO Schema found" );
          throw new Error('NO Schema found');
       }
    } else { 
       curr_schema_version=tgt_schema_version.substring(0,6);
       curr_data_version=tgt_schema_version.substring(6);       
       tgt_schema_curr=tgt_schema + "_" + curr_schema_version + curr_data_version ;
       log("CurrentVersion: " + tgt_schema_curr);
    }

    sqlquery = `
          GRANT USAGE ON DATABASE "` + svw_db + `" TO SHARE "` + share + `"`;
    snowflake.execute({sqlText: sqlquery});

    sqlquery = `
          GRANT REFERENCE_USAGE ON DATABASE "` + tgt_db + `" TO SHARE "` + share + `"`;
    snowflake.execute({sqlText: sqlquery});

    grant_schema( tgt_schema_curr, tgt_schema_new, reference_type_indirect);
    grant_schema( meta_schema, tgt_meta_schema, reference_type_indirect);

//       sqlquery = `
//          GRANT SELECT ON ALL VIEWS IN SCHEMA "` + svw_db + `"."` + tgt_schema_new + `" TO SHARE "` + share + `"`;
//       snowflake.execute({sqlText: sqlquery});

    sqlquery = `
          ALTER SCHEMA "` + svw_db + `"."` + tgt_schema + `" 
          SWAP WITH "` + svw_db + `"."` + tgt_schema_new + `"`;
    snowflake.execute({sqlText: sqlquery});
       
    sqlquery = `
          DROP SCHEMA "` + svw_db + `"."` + tgt_schema_new + `"`;
    snowflake.execute({sqlText: sqlquery});

    log("procName: " + procName + " END " + status)
    flush_log(status)

    return return_array
}
catch (err) {
    log("ERROR found - MAIN try command");
    log("err.code: " + err.code);
    log("err.state: " + err.state);
    log("err.message: " + err.message);
    log("err.stacktracetxt: " + err.stacktracetxt);
    log("procName: " + procName + " " + status_end)

    flush_log(status_failure);
    
    return return_array;
}
$$;

