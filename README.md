# smartCopy - Consuming Shared Data in a Virtual Private Snowflake Account 

## Overview

Snowflake has an amazing feature called [Secure Data Sharing](https://www.snowflake.com/use-cases/modern-data-sharing/). With Snowflake Secure Data Sharing, two accounts in the same Cloud Region can share live data in an instant and secure way. Data Sharing is possible because of Snowflakes unique architecture, which separates storage and compute. Because of this architecture, a data provider can configure access to it's data by creating a share. Think of it as a collection of all the necessary metadata, for instance, shared objects, location of the data files, how to decrypt the data files, and so on. However, data can only be shared between two Snowflake accounts that exist in the same Region at the same CSP (Cloud Service Provider). Sharing Data between accounts in different regions, or accounts with different CSPs (even in the same geographical region), require data to be replicated. You can find more information about replicating data between different regions [here](https://docs.snowflake.com/en/user-guide/secure-data-sharing-across-regions-plaforms.html). 

By design, a [Virtual Private Snowflake](https://docs.snowflake.com/en/user-guide/intro-editions.html#virtual-private-snowflake-vps) a VPS is considered its own region. For that reason, sharing data into a VPS account requires the data from the provider to be replicated into the VPS account (consumer). 

The complete process of making sharable data available in a VPS account consists of 3 Phases:
* Copy Phase: During the copy phase we create a local copy from from the shared data
* Replication Phase: During the repliation phase we replicate the local copy of the shared data into a VPS account
* Sharing Phase: During the sharing phase we create a new layer that expose the data in the replicated database through a set of secure views. 

### Copy Phase

As mentioned in the documentation above, a database created from a share can not be used as a source for replication. Only a database that is ***local*** can be replicated. Therefore, if we want to consume shared data in a VPS account, we first have to create a local copy of the shared dataset and then replicate that ***local*** copy into the VPS account. On the surface, creating a local copy seems to be straight forward. 

* create a local table via a `CTAS (CREATE TABLE AS)` statement into a new schema in a new database
* replicate that local database into the VPS account
* share the now replicated database to as many accounts inside the VPS as you desire
* run the whole process on a regular schedule

Though this will work, there are several challenges

* how do we handle a use case when there are hundreds or thousands of objects (tables/views)?
* how do we handle a use case with tables holding 10th of GB of data or billions of rows?
* how do we handle cross object consistency (since it takes time to copy a share object by object)?
* how do we limit replication of data to the bare minimum (since cross region / cross cloud data replication is costly)?

SmartCopy is a set of Snowflake Procedures that handle all of the above challenges.

* Automate the copy process from source schema (share) to target schema (local)
* Collect metadata information (list of objects to be copied and their state (data as well as structure)) 
* Analyze metadata information from previous step to limit data changes to a minimum and create an execution plan
* Execute execution plan from previous step 
* Collect metadata information again and compare metadata sets for differences (potential consistency problems)
* Record metadata information (tables, performed actions, row counts, fingerprints) for auditibility

SmartCopy stores the data for the local copy in a schema with the name of the source schema appended by a version number. The version number consists of two 6 digit number that indicate the structural version (first 6 digits) and the data snapshot version (second 6 digits). Every time the structure of any shared object changes, the structural version is incremented and the data snapshot version is reset to the initial version. Every time the data changes without any structural change, the data snapshot version is increased.    

### Replication Phase

With the ability to create a local copy of a shared dataset, we can replicate the local copy into the VPS deployment via standard Snowflake replication. Details on how to setup replication can be found [here](https://docs.snowflake.com/en/user-guide/database-replication-config.html#). 

### Sharing Phase 

The local copy of the shared dataset can now be shared to any consumer account inside the VPS. For that we have to create a set of secure views pointing to the new local copies of the shared dataset. 

## Implementation Interface

### SP_COPY

This procedure creates a local copy (target database & schema) of all tables/views inside a shared database (source database and schema). 
    
    create or replace procedure SP_COPY(
       I_SRC_DB VARCHAR  -- Name of the source (shared) database
       ,I_TGT_DB VARCHAR -- Name of the target (local) database
       ,I_SCHEMA VARCHAR -- Name of the schema
    )
    
### SP_REFRESH

This procedure creates a secure views based re-direction layer to the latest (or a specific) version of the replciated tables. 

    create or replace procedure SP_REFRESH(
       I_TGT_DB VARCHAR               -- Name of the replicated (secondary) database
       ,I_SVW_DB VARCHAR              -- Name of the new shared database
       ,I_SCHEMA VARCHAR              -- Name of schema in the replicated (secondary) database
       ,I_SCHEMA_VERSION VARCHAR      -- Target version ("LATEST" or specific Version)
       ,I_SHARE VARCHAR               -- Name of the Share to be created/used
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

SmartCopy is a set of Snowflake stored procedures that are being installed in a central repository database. Though all operations can be performed by using the AccountAdmin role, it considered best practice to follow the [principle of least privilege](https://en.wikipedia.org/wiki/Principle_of_least_privilege). Therefore, the instructions below show how to create a custom role `smart_copy_rl` and assign the necessary permissions to that role. Of course, we need a privileged role to create `smart_copy_rl` and assign the required permissions. From thereon forward, a privileged role will only be needed to create the source database (from share) and target database (local), or permissions to import a share and to create a database have been assigned to `smart_copy_rl` as well.


1. Clone the SmartCopy repo (use the command below or any other way to clone the repo)

    ```
    git clone https://github.com/RobertFehrmann/smartCopy.git
    ```   
    
1. SmartCopy requires permissions to create a share and read/write access to a database the hosts the SmartCopy code library. Though the AccountAdmin role provides the necessary permissions, its best practice to follow the principle of least permissions. For that, we create a custom role, called `smart_copy_rl`.

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
    
1. Grant additional permissions ***(optional)***

    ```
    grant create database on account to role smart_copy_rl;
    grant import share on account to role smart_copy_rl;
    ```
    
1. Grant smart_copy_rl to the appropriate user (login). Replace `<user>` with the user you want to use for smart_copy. Generally speaking, this should be the user you are connected with right now.

    ```
    grant role smart_copy_rl to user <user>;
    use role smart_copy_rl;
    create schema smart_copy_db.metadata; 
    ```
    
1. Create all procedures from the metadata directory inside the cloned github repo by loading each file into a worksheet and then executing the statement. 
***Note: If you are getting an error message (SQL compilation error: parse ...), move the cursor to the end of the worksheet (right after the semicolon), and click `Run`***

## Operations

Each shared database managed by SmartCopy needs to be configured. In case the optional permissions have been granted to role `smart_copy_rl`, all steps can be performed using role `smart_copy_rl`. Otherwise, source and target database need to be created by a privileged role and the necessary permissions have to be granted to role `smart_copy_rl`.

1. Create the source database from the share and grant the necessary permission to the role smart_copy_rl.

    ```
    use role AccountAdmin;
    drop database if exists <source db>;
    create database <source db> from share <provider account>.<source db>;
    grant imported privileges on database <source db> to role smart_copy_rl;
    ```
    
1. Create the target (local) database, and grant the necessary permission to role smart_copy_rl.

    ```
    use role AccountAdmin;
    drop database if exists <local db>;
    create database <local database>;
    grant all on database <local db> to role smart_copy_rl with grant option;
    ```
    
1. Run the copy command 

    ```
    use role smart_copy_rl;
    call smart_copy_db.metadata.sp_copy(<shared db>,<local db>,<schema>);
    ```
    
1. Run the refresh command to create the new share inside VPS.

    ```
    use role smart_copy_rl;
    call smart_copy_db.metadata.sp_refresh(<local db>,<new shared db>,<schema>,<schema version>,<new share>);
    ```


