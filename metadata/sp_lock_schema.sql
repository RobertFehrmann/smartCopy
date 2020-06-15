create procedure METADATA.SP_LOCK_SCHEMA(I_TGT_DB VARCHAR, I_TGT_SCHEMA VARCHAR, I_TGT_VERSION VARCHAR)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$
const tgt_db  = I_TGT_DB;
const tgt_schema  = I_TGT_SCHEMA;
const tgt_version = I_TGT_VERSION;

const internal = "INTERNAL_";
const meta_schema = internal + tgt_schema + "_METADATA";
const parameter_table = "PARAMETER"
const status_begin = "BEGIN";
const status_end = "END";
const status_failure = "FAILURE";

   sqlquery = `
       CREATE OR REPLACE TABLE "` + tgt_db + `".` + meta_schema + `.` + parameter_table + ` (value variant)
          AS SELECT parse_json('{SchemaVersion:`+tgt_version+`}')`;
   snowflake.execute({sqlText: sqlquery});   

   return;

$$;


