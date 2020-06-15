create procedure METADATA.SP_COPY(I_SRC_DB VARCHAR, I_SRC_SCHEMA VARCHAR, I_TGT_DB VARCHAR, I_TGT_SCHEMA VARCHAR)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$
//note:  this proc returns an array, either success or fail right now

const src_db  = I_SRC_DB;
const tgt_db  = I_TGT_DB;
const src_schema  = I_SRC_SCHEMA;
const tgt_schema  = I_TGT_SCHEMA;

const internal = "INTERNAL_";
const tgt_meta_schema = tgt_schema + "_METADATA";
const meta_schema = internal + tgt_schema + "_METADATA";
const tgt_schema_streams = internal + tgt_schema + "_STREAMS";
const tgt_schema_tmp = internal + tgt_schema + "_TMP";
const table_execution_plan = "TABLE_EXECUTION_PLAN";

const type_source="SOURCE";
const type_target="TARGET";
const table_metadata_source = "TABLE_METADATA_" + type_source;
const table_metadata_source_post = "TABLE_METADATA_" + type_source + "_POST";
const table_metadata_target = "TABLE_METADATA_" + type_target;
const column_metadata_source = "COLUMN_METADATA_" + type_source;
const column_metadata_target = "COLUMN_METADATA_" + type_target;
const table_snapshot_metadata="TABLE_SNAPSHOT_METADATA";
const column_snapshot_metadata="COLUMN_SNAPSHOT_METADATA";
const stream_metadata_target="STREAM_METADATA_" + type_target;
const refresh_type_copy= "COPY";
const refresh_type_stream= "STREAM";
const mode_pre_copy="PRE_COPY"
const mode_post_copy="POST_COPY"

const change_data_name = "_CD";
const table_type_table = "TABLE";
const table_type_view = "VIEW";
const new_schema_version_true = "TRUE";
const new_schema_version_false = "FALSE";
const status_begin = "BEGIN";
const status_end = "END";
const status_warning = "WARNING";
const status_failure = "FAILURE";
const version_default = "000000";
const version_initial = "000001";
    
var return_array = [];
var counter = 0;
var action_code = 0;
var process_start_time_epoch=0;
var intra_version_update=0;
var table_name = "";
var stream_name ="";
var refresh_type = "";
var table_name_cd ="";
var table_type = "";
var op_name = "";
var column_name = "";
var column_list ="";
var sqlquery="";
var tgt_schema_curr = "";
var tgt_schema_next = "";
var status=status_end;
var curr_schema_version=version_default;
var next_schema_version=version_default;
var curr_data_version=version_default; 
var next_data_version=version_default;
var new_schema_version=new_schema_version_false;
var tgt_schema_new = internal + tgt_schema + "_NEW";

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
         sqlquery = sqlquery + "('" + tgt_schema + "','" + curr_schema_version + curr_data_version + "','" + status + "','" + message + "');";
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

function get_inventory_streams(mode){
   const max_batches=32
   const batch_size=128;
   const inventory_tmp="INVENTORY_TMP";
   const stream_metadata="STREAM_METADATA"
   const stream_cnt_max=max_batches*batch_size;

   var counter=0;
   var offset=0;
   var remaining_streams=0;
   var limit=batch_size;
   var stream_name="";
   var table_metadata_source_name="";
      
   log("get stream metadata for : "+src_db+"."+src_schema);
   
   if (mode == mode_pre_copy){
      log ("   Pre Copy Mode");
      table_metadata_source_name = table_metadata_source
      
      snowflake.execute({sqlText: "SHOW STREAMS IN SCHEMA \"" + tgt_db + "\".\"" + tgt_schema_streams + "\";"});

      sqlquery=`
         CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + stream_metadata + ` AS
            SELECT '`+src_db+`' database_name,'`+src_schema+`' schema_name,split_part("table_name",'.',3) table_name, "name" stream_name
               FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
               WHERE "stale"=false;`
       
      snowflake.execute({sqlText: sqlquery});
   } else {
      log ("   Post Copy Mode");
      table_metadata_source_name = table_metadata_source_post   
   }

   sqlquery=`
      CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_metadata_source_name + `" (
            refresh_type varchar(10)
            ,database_name varchar
            ,schema_name varchar
            ,table_name varchar
            ,stream_name varchar
            ,row_count_in_stream bigint
            ,fingerprint bigint
            ,last_change timestamp_ntz(6))`
   snowflake.execute({sqlText:sqlquery});  
   
   // we need to get the number of table because we have to batch reading the metadata info

   sqlquery=`
      SELECT database_name, schema_name, COUNT(distinct table_name) 
      FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + stream_metadata + `
      GROUP BY database_name, schema_name`;
   var ResultSet = (snowflake.createStatement({sqlText: sqlquery})).execute();

   if (ResultSet.next()) {
      remaining_streams=ResultSet.getColumnValue(3)
   } else {
      remaining_streams=0;
   }      

   if (remaining_streams>stream_cnt_max){
      log("Total Streams: "+remaining_streams+" above threshold of "+stream_cnt_max)
      remaining_streams=stream_cnt_max;
   }
   log("   Total Streams: "+remaining_streams)

   // As a precaution we have set the max number of objects to 16 * batchsize, i.e.
   counter=0;
   while (counter<max_batches && remaining_streams>0) {
      offset=batch_size*counter;

      if (limit>remaining_streams){
         limit=remaining_streams;
      }
      
      log("   Batch : "+counter+" Remaining Streams: "+remaining_streams);
      
      sqlquery=`
         CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + inventory_tmp + ` 
            LIKE "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_metadata_source_name + `"`
      snowflake.execute({sqlText:sqlquery});

      // the following statement constructs a statement to read the metadata for a batch of streams.
      // Since we would loose the stream data by inserting the results directly into a table, the statement below
      // creates SQL to create INSERT statements with the actual values. After that, the whole batch in copied
      // into the final table.
      var sqlquery=`
             SELECT  '('||listagg(stmt,' union ')||') ' 
             FROM (SELECT '( SELECT \\'`+refresh_type_stream+`\\'::varchar(10) refresh_type, \\''||database_name||'\\' database_name '||
                               ',\\''||schema_name||'\\' schema_name,\\''||table_name||'\\' table_name  ,\\''||stream_name||'\\' stream_name  '||
                               ',(SELECT count(*) FROM "`+tgt_db+`"."`+tgt_schema_streams+`"."'||stream_name||'")::bigint row_count_in_stream '||
                                  ',(SYSTEM$LAST_CHANGE_COMMIT_TIME('||
                                       '\\'"'||database_name||'"."'||schema_name||'"."'||table_name||'"\\'))::bigint fingerprint '||
                                  ',(fingerprint/1000)::timestamp_ntz(6) last_change '||
                            ')' stmt
                   FROM  "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + stream_metadata + `"
                   ORDER BY database_name, schema_name, table_name LIMIT ` + limit + ` OFFSET ` + offset + `);`
                
      var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
    
      if (ResultSet.next()) {
          sqlquery=`
             SELECT 'INSERT INTO "` + tgt_db + `"."` + tgt_schema_tmp + `".` + inventory_tmp + ` values '||
                  listagg('(\\''||refresh_type||'\\',\\''||database_name||'\\',\\''||schema_name||
                  '\\',\\''||table_name||'\\',\\''||stream_name||'\\','||row_count_in_stream||
                  ',null,\\''||last_change||'\\')',',') stmt
             FROM `+ ResultSet.getColumnValue(1);
          var ResultSet2 = (snowflake.createStatement({sqlText:sqlquery})).execute();
          
          if (ResultSet2.next()){
             snowflake.execute({sqlText: ResultSet2.getColumnValue(1)}); 
          }
      } else {
         throw new Error('Stream list for metadata not found');
      }

      sqlquery=`
         INSERT INTO "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source_name + ` 
            SELECT * FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + inventory_tmp ;
      snowflake.execute({sqlText: sqlquery});

      counter += 1;
      remaining_streams-=batch_size;
   }      
}

function get_inventory_tables_views(mode){
   const max_batches=32
   const batch_size=128;
   const inventory_tmp="INVENTORY_TMP";
   const tables_tmp="TABLES_TMP";
   const table_cnt_max=max_batches*batch_size;

   var counter=0;
   var offset=0;
   var remaining_tables=0;
   var limit=batch_size;
   var table_name="";
   var table_metadata="";
   var column_metadata="";
      
   log("get column metadata for : "+src_db+"."+src_schema);
   
   if (mode == mode_pre_copy){
      log ("   Pre Copy Mode");
      table_metadata_source_name = table_metadata_source
   
      snowflake.execute({sqlText: "SHOW COLUMNS IN SCHEMA \"" + src_db + "\".\"" + src_schema + "\";"});

      sqlquery=`
         CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + column_metadata_source + ` AS
            SELECT *, hash(table_name, column_name, data_type,kind,position) fingerprint
            FROM (SELECT row_number() over (partition by "table_name" order by "table_name","column_name") position
                      ,"database_name" database_name,"schema_name" schema_name,"table_name" table_name
                      ,"column_name" column_name,"data_type" data_type, "kind" kind 
                  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));`
       
      snowflake.execute({sqlText: sqlquery});
   
      sqlquery=`
         CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + tables_tmp + ` AS
             SELECT database_name, schema_name, table_name 
             FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + column_metadata_source + `
             WHERE table_name not in (
                SELECT table_name 
                FROM   "` + tgt_db + `".` + tgt_schema_tmp + `."` + table_metadata_source_name + `"
                WHERE refresh_type = '`+refresh_type_stream+`')
             GROUP BY database_name, schema_name, table_name
             ORDER BY database_name, schema_name, table_name`;   
      snowflake.execute({sqlText: sqlquery});
   } else {
      log ("   Post Copy Mode");
      table_metadata_source_name = table_metadata_source_post  
   }

   // we need to get the number of table because we have to batch reading the metadata info

   sqlquery=`
      SELECT database_name, schema_name, COUNT(distinct table_name) 
      FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + tables_tmp + `
      GROUP BY database_name, schema_name`;
      
   var ResultSet = (snowflake.createStatement({sqlText: sqlquery})).execute();

   if (ResultSet.next()){
      remaining_tables=ResultSet.getColumnValue(3)
   } else {
      remaining_tables=0;
   }

   if (remaining_tables>table_cnt_max){
      log("   Total Tables: "+remaining_tables+" above threshold of "+table_cnt_max)
      remaining_tables=table_cnt_max;
   }
   log("   Total Tables: "+remaining_tables)

   // As a precaution we have set the max number of objects to 16 * batchsize, i.e.
   counter=0;
   while (counter<max_batches && remaining_tables>0) {
      offset=batch_size*counter;

      if (limit>remaining_tables){
         limit=remaining_tables;
      }
      
      log("   Batch : "+counter+" Remaining Tables: "+remaining_tables);
      // the following statement constructs a statement to read the metadata for a batch of tables/views.
      // After the metadata for the batch has been put into a temp table it is copied into the
      //  final table.

      var sqlquery=`
          SELECT 'CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + inventory_tmp + ` 
           AS ('||listagg(stmt,' union ')||') order by last_change desc;' 
             FROM (SELECT '( SELECT \\'COPY\\'::varchar(10) refresh_type ,\\''||database_name||'\\' database_name '||
                                  ',\\''||schema_name||'\\' schema_name ,\\''||table_name||'\\' table_name , null::varchar stream_name '||
                                  ', null::bigint row_count_in_stream'||
                                  ',(SELECT hash_agg(*) FROM "'||database_name||'"."'||schema_name||'"."'||table_name||'")::bigint fingerprint '||
                                  ',((SYSTEM$LAST_CHANGE_COMMIT_TIME('||
                                    '\\'"'||database_name||'"."'||schema_name||'"."'||table_name||'"\\'))/1000)::timestamp_ntz(6) last_change '||
                           ')' stmt
                   FROM  "` + tgt_db + `"."` + tgt_schema_tmp + `".` + tables_tmp + `
                   ORDER BY database_name, schema_name, table_name LIMIT ` + limit + ` OFFSET ` + offset + `);`
                
      var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
    
      if (ResultSet.next()) {
         snowflake.execute({sqlText: ResultSet.getColumnValue(1)});
      } else {
         throw new Error('Table/View list for metadata not found');
      }
      sqlquery=`
         INSERT INTO "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source_name + ` 
            SELECT * FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + inventory_tmp ;
      snowflake.execute({sqlText: sqlquery});

      counter += 1;
      remaining_tables-=batch_size;
   }
   
   sqlquery=`
       CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + column_metadata_source + ` AS
         SELECT c.*
         FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source_name + ` t
         INNER JOIN "` + tgt_db + `"."` + tgt_schema_tmp + `".` + column_metadata_source + ` c
           ON c.table_name = t.table_name `;       
   snowflake.execute({sqlText: sqlquery});      
}

try {
    snowflake.execute({sqlText: "CREATE DATABASE IF NOT EXISTS \"" + tgt_db + "\";"});
    snowflake.execute({sqlText: "CREATE SCHEMA IF NOT EXISTS \"" + tgt_db + "\".\"" + tgt_schema_streams + "\";"});
    snowflake.execute({sqlText: "CREATE OR REPLACE TRANSIENT SCHEMA \"" + tgt_db + "\".\"" + tgt_schema_tmp + "\";"});
    snowflake.execute({sqlText: "CREATE SCHEMA IF NOT EXISTS \"" + tgt_db + "\"." + meta_schema + ";"});

    // get start time for copy process from the snowflake server
    
    var resultSet = (snowflake.createStatement({sqlText:"SELECT date_part(epoch_seconds,convert_timezone('UTC',current_timestamp))"})).execute();
    if (resultSet.next()) {
       process_start_time_epoch=resultSet.getColumnValue(1);
    }
    
    // get the version of the most recent snapshot
    
    sqlquery=`
         ((SELECT schema_name , substr(schema_name,-12,6) schema_version, substr(schema_name,-6) data_version  
           FROM  "` + tgt_db + `".information_schema.schemata where rlike (schema_name,'` + tgt_schema + `_[0-9]{12}')) UNION
          (SELECT '` + tgt_schema + `','` +version_default+ `','` +version_default+ `'))
         ORDER BY schema_name DESC limit 1`;
    
    var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
    if (ResultSet.next()) {
       curr_schema_version=ResultSet.getColumnValue(2);
       curr_data_version=ResultSet.getColumnValue(3); 
       next_schema_version=(version_default + (parseInt(curr_schema_version) + 1)).slice(-6);
       next_data_version=(version_default + (parseInt(curr_data_version) + 1)).slice(-6);
       tgt_schema_curr=tgt_schema + "_" + curr_schema_version + curr_data_version ;
    } 
    
    log("procName: "+procName+" "+status_begin);
    log("CurrentVersion: " + tgt_schema_curr);

    flush_log(status_begin);
    
    //=============================================
    // Metadata collection
    //=============================================

    // Get metadata for all streams on tables and views in source schema
    log("== Metadata Collection ==");
    
    get_inventory_streams(mode_pre_copy);

    // Get metadata for all tables (except tables with streams) & views in source schema 

    get_inventory_tables_views(mode_pre_copy);

    if (curr_schema_version==version_default) {
       log("no target metadata available");
       // this is the first time the process runs. There is no metadata for the target tables, i.e. we
       // create empty tables.
       sqlquery=`
          CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_target + `
             LIKE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source;
       snowflake.execute({sqlText: sqlquery});  
       
       sqlquery=`
          CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + column_metadata_target + `
             LIKE "` + tgt_db + `"."` + tgt_schema_tmp + `".` + column_metadata_source;
       snowflake.execute({sqlText: sqlquery});         
    } else {
       log("get target metadata from cache");
       // this is a subsequent run of the process and we get the metadata from the cache (previous run)
       sqlquery=`
             CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_metadata_target + `" AS 
                SELECT * 
                FROM "` + tgt_db + `".` + meta_schema + `."`  + table_snapshot_metadata + `"
                WHERE snapshot_id = '`+tgt_schema_curr+`'
                  AND intra_version_update = 0
                ORDER BY table_name`;
       snowflake.execute({sqlText:  sqlquery});

       sqlquery=`
            CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + column_metadata_target + `" AS
                SELECT * 
                FROM "` + tgt_db + `".` + meta_schema + `."` + column_snapshot_metadata + `"
                WHERE snapshot_id = '`+tgt_schema_curr+`'
                ORDER BY table_name, column_name`;
       snowflake.execute({sqlText: sqlquery});  
       
    }
    log("== Metadata Collection complete ==");

    //=============================================
    // Analyze Metadata and Optimize Execution Plan
    //=============================================

    log("== Analyze Metadata ==");
    // we analyze the source and target metadata to find differences. There are 4 distict situations
    // - Schema changes, i.e. tables/views have added/removed/renamed columns or tables have been added/removed
    // - A table has a stream and the stream has data
    // - A view has been updated since the last time the process ran (i.e. data has been modified
    // - A stream has been added/removed from a table
    sqlquery=`
       CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_execution_plan + `" AS
          SELECT table_name::varchar table_name, max(action_code)::integer max_action_code,
                 CASE max_action_code
                     WHEN 1001 then 'NO CHANGE'
                     WHEN 1002 then 'UPDATE VIA STREAM'
                     WHEN 1003 then 'UPDATE VIA COPY'
                     WHEN 1004 then 'ADD/REMOVE STREAM'
                     WHEN 1010 then 'NEW TABLE' 
                     WHEN 1100 then 'REMOVED TABLE' 
                     WHEN 2100 then 'REMOVED COLUMN'
                     WHEN 2010 then 'ADDED COLUMN'
                     WHEN 3010 then 'ADD/REMOVE COLUMN'
                     ELSE 'CHANGED SCHEMA' 
                 END::varchar action
          FROM (((
               SELECT table_name
                  ,row_number() OVER (PARTITION BY table_name ORDER BY curr_table_name, new_table_name) * 1000 
                    + CASE WHEN curr_table_name = new_table_name THEN 1 ELSE 0 END 
                    + CASE WHEN curr_table_name is null THEN 10 ELSE 0 END 
                    + CASE WHEN new_table_name is null then 100 ELSE 0 END AS action_code
               FROM (
                  SELECT coalesce(curr.table_name, new.table_name) table_name
                      ,curr.table_name curr_table_name
                      ,new.table_name new_table_name 
                  FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + column_metadata_target + ` curr
                     FULL OUTER JOIN (
                        SELECT cs.*
                        FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + column_metadata_source + ` cs
                        WHERE cs.table_name in (
                           (SELECT table_name
                            FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_target + `) union
                           (SELECT table_name
                            FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source + `)
                         )) new
                      ON new.table_name = curr.table_name and new.fingerprint = curr.fingerprint
                  GROUP BY curr.table_name, new.table_name
                  ORDER BY coalesce(curr.table_name,new.table_name))
               ORDER BY table_name, curr_table_name)
             ) UNION (
                SELECT table_name, 1002 action_code
                FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source + `
                WHERE refresh_type = '`+refresh_type_stream+`' AND row_count_in_stream>0
             ) UNION (
                SELECT table_name, 1003 action_code
                FROM ((SELECT table_name, fingerprint
                    FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source + `
                    WHERE refresh_type = '`+refresh_type_copy+`') MINUS
                   (SELECT table_name, fingerprint
                    FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_target + `))
             ) UNION (
                SELECT table_name, 1004 action_code
                FROM ((SELECT table_name, refresh_type
                    FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source + `  ) MINUS
                   (SELECT table_name, refresh_type
                    FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_target + `  ))))
          GROUP BY table_name
          ORDER BY max_action_code, table_name` ;
    snowflake.execute({sqlText: sqlquery});


    //=============================================
    // Run execution plan
    //=============================================

    // The Analysis created a action for each table, i.e. columns have been added/removed,
    // datatypes have been changed, data has been changed, ...
    // The action code determines the next steps. For that we compute the max of the action codes and follow the
    // mapping below
    //   No changes to any table between current source and previous run
    //       => all done
    //   All changes are applied via streams, i.e. no structural changes and no updates to views, no new streams
    //       => do not create a new version but update the tables directly
    //   Only data changes, i.e. via streams or data has changed in views, i.e. no structural changes to any table
    //   table/view and no new streams
    //       => create a new data version by cloning the previous snapshot
    //   At least one structural change, or one new or removed stream
    //       => create a new schema version by cloning the previous snapshot
    sqlquery=`
       SELECT max(max_action_code) action_code
       FROM "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_execution_plan + `"`;

    var tableNameResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

    if (tableNameResultSet.next()) {
       action_code = tableNameResultSet.getColumnValue(1)
    } else {
       throw new Error('Action Code not found');
    }

    if (action_code == 1001) {
       log("  no schema or data changes");
       log("  keep current version " + tgt_schema_curr);
    } else {
       log("== Analyze Metadata complete ==");

       //=============================================
       // Run execution plan
       //=============================================

       log("== Execution plan ==");
       if (action_code==1002) {
          log("  all changes applied via streams");
          log("  no new version created");
          tgt_schema_new=tgt_schema_curr;
          tgt_schema_next=tgt_schema_curr;
       } else {
          if (action_code == 1003) {
             tgt_schema_next=tgt_schema + "_" + curr_schema_version + next_data_version;
          } else {
             tgt_schema_next=tgt_schema + "_" + next_schema_version + version_initial;
          }
      
          try {
                sqlquery=`
                   CREATE OR REPLACE TRANSIENT SCHEMA "` + tgt_db + `"."` + tgt_schema_new + `"
                      CLONE "` + tgt_db + `"."` + tgt_schema_curr + `"
                      DATA_RETENTION_TIME_IN_DAYS = 0`;
                snowflake.execute({sqlText: sqlquery});
                log("clone schema " + tgt_schema_curr);
          }
          catch (err) {
                sqlquery=`
                   CREATE OR REPLACE TRANSIENT SCHEMA "` + tgt_db + `"."` + tgt_schema_new + `"
                      DATA_RETENTION_TIME_IN_DAYS = 0`;
                snowflake.execute({sqlText: sqlquery});
                log("First version; No schema to clone yet");
          }
       }

       sqlquery=`
          SELECT coalesce(m.table_name,e.table_name) table_name, e.max_action_code
                  , ifnull(m.refresh_type,'`+refresh_type_copy+`'), m.stream_name
          FROM "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_metadata_source + `" m
          FULL OUTER JOIN "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_execution_plan + `" e
                ON e.table_name = m.table_name
          WHERE max_action_code > 1001
          ORDER BY max_action_code, coalesce(m.table_name,e.table_name)`;

       var tableNameResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

       counter=0;
       while (tableNameResultSet.next()){
          counter+=1;
          table_name = tableNameResultSet.getColumnValue(1);
          action_code = tableNameResultSet.getColumnValue(2);
          refresh_type = tableNameResultSet.getColumnValue(3);
          stream_name = tableNameResultSet.getColumnValue(4);

          if (action_code == 1002) {
             log("   REFRESH "+table_name+" from stream " + stream_name);

             try {

                sqlquery = `
                    SELECT listagg('"'||column_name||'"',',') WITHIN GROUP (ORDER BY POSITION)
                    FROM "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + column_metadata_source + `"
                    WHERE database_name='` + src_db + `' and schema_name = '` + src_schema + `' and table_name = '` + table_name + `'`;

                var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

                if (ResultSet.next()) {
                   column_list=ResultSet.getColumnValue(1);
                } else {
                   throw new Error('Column List not found');
                }

                // We can not use a merge statement since we do not know the natural key.
                // For that reason we have to use a DELETE and INSERT method which requires the
                // data in the stream to be put into a temporary table

                sqlquery = `
                   CREATE OR REPLACE TABLE "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_name_cd + `" AS
                     SELECT *,hash(` + column_list + `) fingerprint
                     FROM "` + tgt_db + `"."` + tgt_schema_streams + `"."` + stream_name + `"`;

                snowflake.execute({sqlText: sqlquery });

                sqlquery = `
                   DELETE /* # ` + counter + ` */
                   FROM "` + tgt_db + `"."` + tgt_schema_new + `"."` + table_name + `"
                   WHERE fingerprint IN
                       (select fingerprint
                        from "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_name_cd + `"
                        where metadata$action='DELETE')`;
                snowflake.execute({sqlText: sqlquery});

                sqlquery = `
                   INSERT /* # ` + counter + ` */ INTO "` + tgt_db + `"."` + tgt_schema_new + `"."` + table_name + `" (` + column_list + `,fingerprint)
                     SELECT ` + column_list + `, fingerprint
                     FROM "` + tgt_db + `"."` + tgt_schema_tmp + `"."` + table_name_cd + `"
                     WHERE metadata$action='INSERT'`;
                snowflake.execute({sqlText: sqlquery});
             }
             catch (err) {
                log("   RESET TABLE" + table_name + " add fingerprint");

                // In case a table has to be reset completely (because the stream failed)
                // we indicate that a new schema version has to be created
                // Note that we compute a row hash for all rows to be able to find rows in case of subsequence deletes

                sqlquery = `
                   CREATE OR REPLACE /* # ` + counter + ` */ TABLE "` + tgt_db + `"."` + tgt_schema_new + `"."` + table_name + `" AS
                      SELECT *, hash(*) fingerprint
                      FROM "` + src_db + `"."` + src_schema + `"."` + table_name + `"`;
                snowflake.execute({sqlText: sqlquery});

                try {
                   sqlquery = `
                      CREATE OR REPLACE STREAM "` + tgt_db + `"."` + tgt_schema_streams + `"."` + stream_name + `"
                         ON TABLE "` + src_db + `"."` + src_schema + `"."` + table_name + `"`;
                   snowflake.execute({sqlText: sqlquery});
                   log("   RESET STREAM " + table_name)
                }
                catch (err) {
                   log("   " + status_warning + ": RESET STREAM " + stream_name + " FAILED")
                }
             }
          } else if (action_code == 1100) {
             // drop table/view
             log("   DROP " + table_name);

             sqlquery=`
                DROP /* # ` + counter + ` */
                TABLE "` + tgt_db + `"."` + tgt_schema_new + `"."` + table_name + `"`;
             snowflake.execute({sqlText: sqlquery});
          } else {
             // copy table/view
             if (refresh_type == refresh_type_copy) {
                log("   RESET TABLE " + table_name);

                sqlquery = `
                   CREATE OR REPLACE /* # ` + counter + ` */ TABLE "` + tgt_db + `"."` + tgt_schema_new + `"."` + table_name + `" AS
                      SELECT * FROM "` + src_db + `"."` + src_schema + `"."` + table_name + `"`;
                snowflake.execute({sqlText: sqlquery});
             } else {
                log("   RESET "+table_name+" with stream ");
                sqlquery = `
                   CREATE OR REPLACE /* # ` + counter + ` */ TABLE "` + tgt_db + `"."` + tgt_schema_new + `"."` + table_name + `" AS
                      SELECT *, hash(*) fingerprint
                      FROM "` + src_db + `"."` + src_schema + `"."` + table_name + `"`;
                snowflake.execute({sqlText: sqlquery });

                try {
                   log("   RESET STREAM " + stream_name);

                   sqlquery = `
                      CREATE OR REPLACE STREAM "` + tgt_db + `"."` + tgt_schema_streams + `"."` + stream_name + `"
                         ON TABLE "` + src_db + `"."` + src_schema + `"."` + table_name + `"`;
                   snowflake.execute({sqlText: sqlquery});
                }
                catch (err) {
                   log("   " + status_warning + ": RESET STREAM " + stream_name + " FAILED")
                }
             }
          }
       }
       log("==  Execution complete ==");

       //=============================================
       // Validate Snapshot
       //=============================================

       log("== Validate Snapshot ==");

       // Ideally we have a immutable snapshot where we copy from. Since we dont have that we check the metadata again
    
       get_inventory_streams(mode_post_copy);
       get_inventory_tables_views(mode_post_copy);
       
       // compare pre and post metadata images for data changes only.
       
       sqlquery=`
             SELECT table_name
             FROM ( (SELECT table_name, fingerprint
                     FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source + `) MINUS
                    (SELECT table_name, fingerprint
                     FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source_post + `) 
                  ) UNION
                  (SELECT table_name
                     FROM "` + tgt_db + `"."` + tgt_schema_tmp + `".` + table_metadata_source_post + `
                     WHERE refresh_type = '`+refresh_type_stream+`'
                       AND row_count_in_stream > 0)`;
                
       var tableNameResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();
 
       counter=0;
       while (tableNameResultSet.next()){
          counter += 1;
          table_name = tableNameResultSet.getColumnValue(1);
             
          log("   WARNING: Table "+table_name+" has changed since copy started")
       }
       log("== Validate Snapshot complete ==");

       if (tgt_schema_curr==tgt_schema_next) {
          log("current version "+tgt_schema_curr+" updated via streams");
          sqlquery = `
              SELECT max(intra_version_update)+1
              FROM "` + tgt_db + `".` + meta_schema + `.` + table_snapshot_metadata + `
              WHERE snapshot_id = '`+tgt_schema_curr+`'`;
          var ResultSet = (snowflake.createStatement({sqlText:sqlquery})).execute();

          if (ResultSet.next()) {
             intra_version_update=ResultSet.getColumnValue(1);
          } else {
             throw new Error('Intra Version Update not found');
          }
       } else {
          intra_version_update=0;

          try {
             sqlquery=`
                INSERT INTO "` + tgt_db + `".` + meta_schema + `.` + column_snapshot_metadata + `
                      SELECT '`+tgt_schema_next+`'::varchar(1024) snapshot_id,*
                      FROM "` + tgt_db + `".` + tgt_schema_tmp + `.` + column_metadata_source ;
             snowflake.execute({sqlText: sqlquery});
          }
          catch (err) {
             sqlquery=`
                CREATE OR REPLACE TABLE "` + tgt_db + `".` + meta_schema + `.` + column_snapshot_metadata + ` AS
                      SELECT '`+tgt_schema_next+`'::varchar(1024) snapshot_id,*
                      FROM "` + tgt_db + `".` + tgt_schema_tmp + `.` + column_metadata_source ;
             snowflake.execute({sqlText: sqlquery});
          }
       }

       log("NewVersion: " + tgt_schema_next + " Intra Version Update: " + intra_version_update);
    
       try {
            sqlquery=`
                INSERT INTO "` + tgt_db + `".` + meta_schema + `.` + table_snapshot_metadata + `
                      SELECT '`+tgt_schema_next+`'::varchar(1024) snapshot_id
                             ,`+intra_version_update+`::integer intra_version_update,m.*
                             ,i.row_count row_count, bytes total_bytes_in_table, e.action
                       FROM "` + tgt_db + `".` + tgt_schema_tmp + `.` + table_metadata_source +` m
                       INNER JOIN "` + tgt_db + `".` + tgt_schema_tmp + `.` + table_execution_plan +` e
                         ON m.table_name = e.table_name
                       INNER JOIN "` + tgt_db + `".information_schema.tables i
                         ON i.table_catalog='`+tgt_db+`' and i.table_schema='`+tgt_schema_new+`' and
                             i.table_name=m.table_name `;
            snowflake.execute({sqlText: sqlquery});
       }
       catch (err) {
            sqlquery=`
                CREATE OR REPLACE TABLE "` + tgt_db + `".` + meta_schema + `.` + table_snapshot_metadata + ` AS
                      SELECT '`+tgt_schema_next+`'::varchar(1024) snapshot_id
                              ,`+intra_version_update+`::integer intra_version_update,m.*
                             ,i.row_count row_count, bytes total_bytes_in_table, e.action
                       FROM "` + tgt_db + `".` + tgt_schema_tmp + `.` + table_metadata_source +` m
                       INNER JOIN "` + tgt_db + `".` + tgt_schema_tmp + `.` + table_execution_plan +` e
                         ON m.table_name = e.table_name
                       INNER JOIN "` + tgt_db + `".information_schema.tables i
                          ON i.table_catalog='`+tgt_db+`' and i.table_schema='`+tgt_schema_new+`' and
                             i.table_name=m.table_name `;
            snowflake.execute({sqlText: sqlquery});
       }

       if (intra_version_update == 0) {
          sqlquery=`
             ALTER SCHEMA "` + tgt_db + `"."` + tgt_schema_new + `"
                RENAME TO "` + tgt_db + `"."` + tgt_schema_next + `"` ;
          snowflake.execute({sqlText: sqlquery});
       }
    }

    log("procName: " + procName + " " + status_end);
    flush_log(status);

    return return_array;
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


