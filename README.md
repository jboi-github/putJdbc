# putJdbc (putJdbcCsv, putJdbc)
putJdbc is a NIFI Processor for bulk loading data into a database. It is tested for extreme amouonts of data. With an approach that fits seemlessly into NIFI it has the ability to interact with the database to control ETL/ELT jobs as well as special parametrization of bulk-load-connections. It takes a FlowFile, then initializes a sequence of scripts and a load commands to load and transform data. With a number of options it is able to be configured using the best practice approaches for very large databases and high speed ingestion. See the usage page for the optimal configuration for your database. 

The processor comes in two flavours:
- **putJdbc** allows a parameterized insert statement to load into database. The input can be any record oriented data, that a RecordReader can parse.
- **putJdbcCsv** is especially designed to load extreme amounts of data into Teradata using the Fastload-CSV feature of Teradata's Jdbc driver. It takes a limited CSV Format and feeds IoT Platforms with up to 10 Million datapoints per second. 

For more details see the usage page of these processors in Nifi.

## Getting started
1. Link or clone this repository
2. use maven to make the Nifi archive
    ```bash
    mvn clean install
    ```
3. copy the NAR file to Nifi libs
    ```bash
    cp ./nifi-teradata-nar/target/nifi-teradata-nar-3.0.nar ${NIFI_HOME}/lib/
    ```
    where ${NIFI_HOME} is the directory of your NIFI installation
4. restart NIFI
    ```bash
    ${NIFI_HOME}/bin/nifi.sh restart
    ```
    Wait till NIFI is up and running.

Done!

Now you should be able to configure the first `putJdbc` processor.

## Configure your first putJdbc processor
yet to come...

## Notes and considerations
putJdbc and putJdbcCsv use one or two connections to a database and load bulk amounts of data into it. A number of scripts around the loading can be configured to setup and tear down special parameters for bulk loading as well as issuing ELT jobs against the loaded data. The following chapters are a collection of thoughts on how to configure for special use cases. Most of these thoughts are out of an IoT project where a NIFI Cluster of 28 nodes has feeded a Teradata Database of 32 Nodes, all running on AWS, with up to 10 Mio rows per second - 24x7. The AWS network had to transfer 12.5 Gbyte every second. At the same time the timeseries data was aggregated and queried a 150 times per second. PutJdbc and PutJdbcCsv was able to manage this. Can it manage yours?

### How it works
When NIFI triggers the processor, it loops over the FlowFiles till either the queue is empty or the user stops the processor or shutdown  NIFI. The sequence of steps as Pseudo Code:
1. Connect to database with primary connection
2. Run `connected-script`
3. For each FlowFile in queue:
3.1 Run `before-loading-script`
3.2 Do loading (see below)
3.3 Run `after-loading-script`
4. Run `before-disconnecting-script`
5. Disconnect

Each loading works in one of three possible ways:
- `putJdbcCsv`: The processor connects to the loading connection, connect the content of the FlowFile (must be CSV) to JDBC and executes the FlowFile as batch. After that the loading is committed.
- `putJdbc` with `Build Batch Strategy` set to `RecordReader as batch`: If an additional connection for loading is defined, the processor connects to the loading connection. Otherwise it uses he primary connection for loading. The `insert statement` is preapred and the FLowFile content parsed with the given `RecordReader`. Each field in each parsed record is bound on the corresponding parameter as defined by the properties `record-field-name.N`. It then executes the FlowFile as batch and the loading is committed.
- `putJdbc` with `Build Batch Strategy` set to `RecordReader as row-by-row`: If an additional connection for loading is defined, the processor connects to the loading connection. Otherwise it uses he primary connection for loading. The `insert statement` is preapred and the FLowFile content parsed with the given `RecordReader`. Each field in each parsed record is bound on the corresponding parameter as defined by the properties `record-field-name.N`. It executes the batch after each record. When all records in the FlowFile are processed the loading is committed.

- Connect - connected - before loading - loading - after loading - before loading - loading - after loading - … - disconnecting - disconnect
- Where each loading works in one of two possible ways:
    - One connection is defined: (use script connection) - prepare insert - build one batch - execute batch
    - If an extra connection for loading is defined: connect - prepare insert - build one batch - execute batch - commit - disconnect
- “build one batch” works in three different ways
    - “By RecordReader as batch” FlowFile content is read and parsed by a RecordReader. The record-fields to bind to the Prepared Statement are defined by dynamic properties.  build one batch: add row - add row -…- add row
    - “By RecordReader row-by-row” FlowFile content is read and parsed by a RecordReader. The record-fields to bind to the Prepared Statement are defined by dynamic properties.  build one batch: add row - execute batch - add row - execute batch -…- add row
    - “CSV” Take FlowFile as input and pass it to Fastload-CSV capabilities of Teradata’s Jdbc driver. This is only tested with Teradata. The load connection is mandatory as it must be especially defined and the RecordReader must be empty as it is ignored in this case.  Also additional dynamic properties are not allowed.
Special use cases and considerations:
- commit, rollback and exception handling
    - If only one connection is given, putJdbc does not explicitly commit the database. It assumes, that a database commit is done as part of the after loading script.
    - If an explicit loading connection is given, this connections is set to AutoCommit off at the beginning and then, after execute batch, a database commit is issued, if AutoCommit is actually off. Some databases do not understand the command.
    - The NIFI session is committed, when the after loading script has finished successfully. The idea is, that the data is accessible for the user at this point in time. Before that, the data might be loaded into a loading table, but is not visible to the user. On the other hand, if the loading runs directly into a user accessible table, the after loading script will do most likely only issue a database commit. In this case the NIFI commit happens directly after the database commit. 
    - Whenever a SQLException is thrown, it issues a rollback on all open database connections of the instance of putJdbcBulk and also a rollback on the NIFI session. This causes the current FlowFile to be pushed back into the queue for rework. The processor yields unless the SQLException indicates, to retry the command immediately.
    - All database Warnings and Exceptions are written to the log with the appropriate level.
    - Whenever an Exception is thrown during the parsing of the incoming FlowFile, it issues a rollback on all open database connections of the instance of putJdbcBulk. It then penalises and transfers the FlowFile to the failure relationship.
- %TABLE_ID% and %COMMIT_EPOCH%
    - All occurrences of this text in all scripts and the insert statement are replaced by:
        - TABLE_ID is a 10 characters long random value consisting of the 10 digits and 26 lower case characters. It is build once when the processor starts and before it connects to the database. This number can have up to 3.6e15 different values and is built every time the processor starts a thread and kept as long as there’re Flowfiles in the queue. If you would connect every minute with 100 threads in parallel it would take about 58 Min years to have a conflict on TABLE_ID with 50% probability. It’s seeded to the current time in nano seconds, a hash of the ip address and the current thread id. Therefore it very likely, that each thread on each server in a NIFI cluster has a different seed, producing a different TABLE_ID. Even with the extremely rare cases of conflicting TABLE_ID’s It is good practise to have some code in the connected- or the before loading script to fail, if the TABLE_ID was already used.
        - COMMIT_EPOCH is a count of the number of loadings happened within the current connection. For the first loading, this number is 0, the second loading get 1 and so on. COMMIT_EPOCH is always 0 for the connect script and contains the number of successful loadings in the disconnecting script.
    - Both values are available as text so they can be easily used as placeholders in an UpdateAttribute processor as described below. They’re also available as attributes in the FlowFile and set as attributes in the original FlowFile and the FlowFile transferred to the script relationship.
- Teradata CSV
    - Define two separated connections and set “build one batch” to “CSV”
    - The connection for loading must contain TYPE=FASTLOADCSV in the URL. It can optionally contain additional parameters, eg. for Sessions, charset, Ansi-Mode or delimiter of the CSV
    - Keep RecordReader empty and do not add any dynamic parameters
    - Make sure the incoming FLowFile contains CSV content, that is delimited by the delimiter defined in the URL, the fields are not quoted nor are delimiter escaped accordingly to the requirements of the Teradata JDBC driver.
    - In order to prepare the data into a large CSV file (with which Teradata-Fastload-CSV works best) you might want to setup a flow of:
        - Some ConvertRecord with a RecordReader that reads and parses your input and a FreeFormRecordSetWriter that writes your data with the right delimiter, might replace the delimiter character in string fields with any other character and does some optional other conversions. The data then are small packages of CSV records without header
        - Use a MergeContent with binary concatenation to collect into some reasonable large FlowFiles. Use Text and header field to add the column names in the loading table on top of the FlowFile
        - Pass the FlowFile into putJdbcBulk. Optionally use the strategy to fill different tables with one processor instance (see below) by adding an UpdateAttribute processor in front of putJdbcBulk.
- sqlite bulk load handles larger amounts of rows best, when using a row-by-row insert, that was prepared is only committed once per batch.
    - Define one connection and set “build one batch” to “By RecordReader row-by-row”
    - In connected-script: Optionally set some Pragmas
    - In before loading script: “begin transaction”
    - In after loading script: “commit” and optionally before or after the commit additional ELT.
- Limit number of simultaneously running bulk loads. If your database has a limit on simultaneously running bulk load jobs or you want to make sure, that your database does not get overloaded. But still want to run ELT jobs in parallel to loading.
    - Set number of threads of the processor to at least two, so that one processor can run ELT while another thread loads data.
    - Use an extra Connection Pool Service Controller for the loading connection
    - In this extra Connection Pool Service for the loading connection, limit the number of connections to 1 and set timeout as high as one loading might need to run. You might want to add some extra time to the timeout, just in case.
- Make sure FlowFiles contain a minimum number of rows as bulk loads usually profit from larger number of rows
    - Place a MergeContent or MergeRecord (Version >= 1.4) processor before putJdbcBulk to merge FLowFile content together.
- Fill different tables with one processor instance. If you have limited the number of concurrently running threads of this processor but have to fill different tables with different preparation, ELT, insert statement etc.
    - Place an UpdateAttributes processor before putJdbcBulk to set attributes for all scripts and for the insert statement
    - Use the attributes in the corresponding properties of putJdbcBulk
    - You can use %TABLE_ID% and %COMMIT_EPOCH% to name the loading table uniquely.
- AVRO schema of script relationship
    - The outgoing script contains timing information (Elapsed times) and TABLE_ID, COMMIT_EPOCH plus result sets and update counts returned by the scripts and insert statement. The information given back from SQL statements is taken as is.
    - It is created when the connecting script returns, after each loading sequence (after loading script finished) and right after the disconnecting script returns. If any of the scripts are empty, the FlowFile is still produced but will be empty.
Properties
- Primary Connection -> Mandatory
- Bulkload Connection -> Optional, if not set use Primary Connection for loading
- insert statement -> Mandatory. Might contain TABLE_ID and COMMIT_EPOCH. Contains ? as placeholder for values
- connected script -> Optional, if not set or empty will not call database
- before loading script -> Optional, if not set or empty will not call database
- after loaded script -> Optional, if not set or empty will not call database
- disconnecting script -> Optional, if not set or empty will not call database
- build batch -> Mandatory
    - By RecordReader as batch: RecordReader must be set. Dynamic properties define fields in records to bind to placeholder in insert statement.
    - By RecordReader row-by-row: RecordReader must be set. Dynamic properties define fields in records to bind to placeholder in insert statement.
    - CSV: RecordReader must be empty and no dynamic properties are allowed
- RecordReader -> Optional
- record-field-name.1 .. record-field-name.n: Contain name of record field to be bound to corresponding placeholder of insert statement. If exist mandatory.
Relationships
- Original: FlowFiles are passed through with additional attributes
- Failure: FlowFiles that failed to be parsed by given RecordReader. Note, that if a failure happens on the database, the NIFI session is rolled back an the FlowFile pushed back to the incoming queue.
- Script: After connected-script, disconnected-script and each after-loading-script a FlowFile with timings, result sets and update counts is written.
Written Attributes
- TABLE_ID and COMMIT_EPOCH as described above are written into FlowFiles of each relationship.
Pseudocode
onTrigger:
- connect to database with primary connection
- run connected script
- while isScheduled and FlowFile are available
    - get FlowFile
    - run before script
    - do bulk load
    - run after script
- run disconnecting script
- disconnect from database with primary connection 
do bulk load:
- If loading connection is set, connect
- prepare insert statement
- build batch
- if loading connection is set
    - commit loading connection
    - disconnect from loading connection
build batch:
- If “build batch” = CSV
    - connect FlowFile content to CSV reader of Fastload-CSV
    - execute batch
- else
    - Get RecordReader
    - For each Record
        - Get fields
        - Bind fields to row
        - add row to batch
        - if “build batch” = RecordReader row-by-row
            - execute batch
        - else
            - rows count ++
    - if rows count > 0
        - execute batch
create script-FlowFile:
- Parent is incoming FlowFile
- build JSON-Document from timings (before, loading, after / start, stop, elapsed), TABLE_ID, COMMIT_EPOCH, result sets and update counts
- content from JSON-Document 
before run script and before prepare insert statement 
- Replace TABLE_ID and COMMIT_EPOCH
get FlowFile
- get FlowFile
- set Attributes TABLE_ID and COMMIT_EPOCH
after connect to primary connection:
- Initialise TABLE_ID and COMMIT_EPOCH
after run after script:
- Increment COMMIT_EPOCH

