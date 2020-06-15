# smartCopy - Consuming Shared Data in a Virtual Private Snowflake Account 

## Overview

Snowflake has an amazing feature called [Secure Data Sharing](https://www.snowflake.com/use-cases/modern-data-sharing/). With Snowflake Secure Data Sharing, two account in the same cloud region with the same CSP (Cloud Service Provider) can share live data in an instant and secure way. Data Sharing is possible because of Snowflakes unique architecture, that separates storage and compute. Because of this architecture, a data provider can configure access to it's data by creating a share. Think of it as a collection of all the necessary metadata, for instance, names of shared objects, location of the data files, how to decrypt the files, and so on. However, data can only be shared between two Snowflake accounts the exist in the same Region and the same CSP. Sharing Data between accounts in different regions with the same CSP, or account with different CSPs (even in the same geographical region), require data to be replicated. Please review the [documentation](https://docs.snowflake.com/en/user-guide/secure-data-sharing-across-regions-plaforms.html) for more details. 

By design, a [Virtual Private Snowflake](https://docs.snowflake.com/en/user-guide/intro-editions.html#virtual-private-snowflake-vps) a VPS is considered its own region. For that reason, sharing data into a VPS Account requires the data from the provider side to be replicated into the VPS account. Then we can share the local copy of the dataset inside VPS by creating a local share.

### Copy Step

As mentioned in the documentation above, a database created from a share can not be used as a source for replication. Only a database that is "local" to the current account can be replicated. Therefore, if we want to consume shared data in a VPS account, we first have to create a local copy of the shared dataset and then we can replicate that local copy into the VPS account. On the surface, creating a local copy seems to be very straight forward. 

* create a local table via a CTAS (CREATE TABLE AS) statement into a new schema in a new database
* replicate that local database into the VPS account
* share the now replicated database to as many account inside the VPS as you desire
* run the whole process on a regular schedule

Though this will work, there are several challenges

* how do we handle the process when there are hundreds or thousands of objects (tables/views)?
* how do we handle bigger tables with 10th of GB of data and hundreds of millions or rows?
* how do we handle consistency since it takes time to copy the share object by object?
* how do we limit replication to the bare minimum since cross region / cross cloud data replication is costly?

SmartCopy is a set of Snowflake Procedures that handle all of the above challenges.

* Automate the copy process from source schema (share) to target schema (local)
* Collect metadata information like (list of objects copied and their state (data as well as structure)) 
* Analyze metadata information from previous step to limit data changes to a minimum and create execution plan
* Execute execution plan from previous step 
* Collect metadata information again and compare metadata sets for differences (potential consistency problems)
* Record metadata information (tables, performed actions, row counts, fingerprints) for auditibility

SmartCopy stores the data for the local copy in a schema with the name of the source schema appended by a version number. The version number consists of two 6 digit number that indicate the structural version (first 6 digits) and the data snapshot version (second 6 digits).   

### Replication Step

With the ability to create a local copy of a shared dataset, we can replicate the local copy into the VPS deployment via standard Snowflake replication. Details on how to setup replication can be found [here](https://docs.snowflake.com/en/user-guide/database-replication-config.html#). 

### Sharing Step 

The local copy of the shared dataset can now be shared to consumer account inside the VPS. For that we have to create a set of secure views pointing to the new local copies of the shared dataset. 

## Implementation Interface

The whole process of 
1. creating a local copy
1. sharing the replicated copy inside VPS

is supported via stored procedures Snowflake stored procedure

### SP_COPY

This procedure creates a local copy (target database & schema) of all tables/views inside a shared database (source database and schema). 
    
    create or replace procedure SP_COPY(
       I_SRC_DB VARCHAR       -- Name of the source (shared) database
       ,I_SRC_SCHEMA VARCHAR  -- Name of the schema in the source database
       ,I_TGT_DB VARCHAR      -- Name of the target (local) database
       ,I_TGT_SCHEMA VARCHAR  -- Name of the schema in the taget database
    )
    
### SP_REFRESH

This procedure creates a secure views based re-direction layer to the latest (or a specific) version of the replciated tables. 

    create or replace procedure SP_REFRESH(
       I_TGT_DB VARCHAR               -- Name of the replicated (secondary) database
       ,I_TGT_SCHEMA VARCHAR          -- Name of schema in the replicated (secondary) database
       ,I_SVW_DB VARCHAR              -- Name of the new shared database
       ,I_SVW_SCHEMA VARCHAR          -- Name of schema in the new shared database
       ,I_SHARE VARCHAR               -- Name of the Share to be created/used
       ,I_TGT_SCHEMA_VERSION VARCHAR  -- Target version ("LATEST" or specific Version)
    )
    
### SP_COMPACT

This procedure removes all previous version of the copied data leaving a maximum number of structural version and a maximum number of data snapshot versions.

    create or replace procedure SP_COMPACT(
       I_TGT_DB VARCHAR                 -- Name of the target (local) database
       ,I_TGT_SCHEMA VARCHAR            -- Name of the schema in the target (local) database
       ,I_MAX_SCHEMA_VERSIONS FLOAT     -- Maximum number of versions with different object structures
       ,I_MAX_VERSIONS_PER_SCHEMA FLOAT -- Maximum number of data snapshot versions in the same structural version
    )

## Setup

1. Clone the SmartCopy repo (use the command below or any other way to clone the repo)
    ```
    git clone https://github.com/RobertFehrmann/smartCopy.git
    ```   
1. Create database and role to host stored procedures. Both steps require the AccountAdmin role (unless your current role has the necessary permissions.
    ``` 
    use role AccountAdmin;
    drop role if exists smart_copy_rl;
    drop database if exists smart_copy_db;
    drop warehouse if exists smart_copy_vwh;
    create role smart_copy_rl;
    grant create share on account to role smart_copy_rl;
    create database smart_copy_db;
    grant all on database smart_copy_db to role smart_copy_rl;
    create warehouse smart_copy_vwh with 
       WAREHOUSE_SIZE = XSMALL 
       MAX_CLUSTER_COUNT = 1
       AUTO_SUSPEND = 1 
       AUTO_RESUME = TRUE;
    grant all on warehouse smart_copy_vwh to role smart_copy_rl;
    ``` 
1. Grant smart_copy_role to the appreopriate user (login). Replace `<user>` with the user you want to use for smart_copy. Generally speaking, this should be the user you are connected with right now. Note that you also could use the AccountAdmin role for all subsequent steps. That could be appropriate on a test or eval system but not for a production setup.
    ```
    grant role smart_copy_rl to user <user>;
    use role smart_copy_rl;
    create schema smart_copy_db.metadata; 
    ```
1. Create all procedures from the metadata directory inside the cloned repo by loading each file into a worksheet and then clicking `Run`. Note: if you are getting an error message, try to move the cursor to the end of the file and click `Run` again)

## Operations

The following steps need to be executed for every database 

1. Create the source database from the share and grant the necessary permission the role smart_copy_rl
    ```
    use role AccountAdmin;
    drop database if exists <source db>;
    create database <source db> from share <provider account>.<source db>;
    grant imported privileges on database <source db> to role smart_copy_rl;
    ```
1. Create the target (local) database, grant the necessary permission the role smart_copy_rl
    ```
    use role AccountAdmin;
    drop database if exists <local db>;
    create database <local database>;
    grant all on database <local db> to role smart_copy_rl;
    ```
1. Run the copy command 
    ```
    use role smart_copy_rl;
    call smart_copy_db.metadata.sp_copy(<shared db>,<shared schema>,<local db>,<local schema>);
    ```
1. Run the refresh command
    ```
    use role smart_copy_rl;
    call smart_copy_db.metadata.sp_refresh(<local db>,<local schema>,<new shared db>,<new shared schema>,<new share>,<target version>);
    ```


