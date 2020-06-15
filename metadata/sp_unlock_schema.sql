create procedure METADATA.SP_UNLOCK_SCHEMA(I_TGT_DB VARCHAR, I_TGT_SCHEMA VARCHAR)
    returns ARRAY
    language JAVASCRIPT
    execute as caller
as
$$
const tgt_db  = I_TGT_DB;
const tgt_schema  = I_TGT_SCHEMA;

const internal = "INTERNAL_";
const meta_schema = internal + tgt_schema + "_METADATA";
const parameter_table = "PARAMETER"

try {
   sqlquery = `
       DROP TABLE "` + tgt_db + `".` + meta_schema + `.` + parameter_table ;
   snowflake.execute({sqlText: sqlquery});   
}
catch (err) {
   return
}
$$;


