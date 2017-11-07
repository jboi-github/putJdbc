# putJdbc (putJdbcCsv, putJdbc)
putJdbc is a NIFI Processor for bulk loading data into a database. It is tested for extreme amount of data. With an approach that fits seamlessly into NIFI it has the ability to interact with the database to control ETL/ELT jobs as well as special parametrization of bulk-load-connections. It takes a FlowFile, then executes a sequence of scripts and a load commands to load and transform data. With a number of options it is able to be configured using the best practice approaches for very large databases and high speed ingestion. See the usage page for the optimal configuration for your database. 

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
    cp ./nifi-teradata-nar/target/nifi-teradata-nar-4.0.nar ${NIFI_HOME}/lib/
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
**putJdbc** and **putJdbcCsv** use one or two connections to a database and load bulk amount of data into it. A number of scripts around the loading can be configured to setup and tear down special parameters for bulk loading as well as issuing ETL/ELT jobs against the loaded data. The following chapter is a collection of thoughts on how to configure for special use cases. Most of these thoughts are out of an IoT project where a NIFI Cluster of 28 nodes has feeded a Teradata Database of 32 Nodes, all running on AWS, with up to 10 Mio rows per second - 24x7. The AWS network has to transfer 12.5 Gbyte every second. At the same time the timeseries data is aggregated and queried a 150 times per second. **PutJdbc** and **PutJdbcCsv** is able to manage this. Can it manage yours?

### How it works
When NIFI triggers the processor, it loops over the FlowFiles in the incoming queues till either all queues are empty or the user stops the processor or shutdown NIFI. The sequence of steps as Pseudo Code:
1. Connect to database with primary connection
2. Run `connected-script`
3. For each FlowFile in queues:
3.1 Run `before-loading-script`
3.2 Do loading (*see below*)
3.3 Run `after-loading-script`
4. Run `before-disconnecting-script`
5. Disconnect

Each loading works in one of three possible ways:
- `putJdbcCsv`: The processor connects to the loading connection, binds the content of the FlowFile (must be CSV) to JDBC and executes the FlowFile as batch. After that the loading is committed.
- `putJdbc` with `Build Batch Strategy` set to `RecordReader as batch`: If an additional connection for loading is defined, the processor connects to the loading connection. Otherwise it uses he primary connection for loading. The `insert statement` is preapred and the FLowFile content parsed with the given `RecordReader`. Each field in each parsed record is bound on the corresponding parameter as defined by the properties `record-field-name.N`. It then executes the FlowFile as batch and the loading is committed.
- `putJdbc` with `Build Batch Strategy` set to `RecordReader as row-by-row`: If an additional connection for loading is defined, the processor connects to the loading connection. Otherwise it uses he primary connection for loading. The `insert statement` is preapred and the FLowFile content parsed with the given `RecordReader`. Each field in each parsed record is bound on the corresponding parameter as defined by the properties `record-field-name.N`. It executes the batch after each record. When all records in the FlowFile are processed the loading is committed.

### Special use cases and considerations:
#### commit, rollback and exception handling
- If only one connection is given, **putJdbc** does not explicitly commit the database. It assumes, that a database commit is done as part of the after loading script.
- If an explicit loading connection is given, this connections is set to AutoCommit off at the beginning, silently ignoring if thsi si not supported by the database. At the end of loading a database commit is issued, if AutoCommit is actually off.
- The NIFI session is committed, when the after loading script has finished successfully. The idea is, that the data is accessible for the user at this point in time. Before that, the data might be loaded into a loading table, but is not visible to the user. On the other hand, if the loading runs directly into a user accessible table, the after loading script will do most likely only issue a database commit. In this case the NIFI commit happens directly after the database commit. 
- Whenever a SQLException is thrown, it issues a rollback on all open database connections of the instance of **putJdbc** and also a rollback on the NIFI session. This causes the current FlowFile to be pushed back into the queue for rework. The processor yields unless the SQLException indicates, to retry the command immediately.
- All database Warnings and Exceptions are written to the log with the appropriate level.
- Whenever an Exception is thrown during the parsing of the incoming FlowFile, it penalises and transfers the FlowFile to the failure relationship.

#### `%TABLE_ID%` and `%COMMIT_EPOCH%`
- All occurrences of this text in all scripts and the insert statement are replaced by:
  - `%TABLE_ID%` is a 10 characters long random value consisting of the 10 digits and 26 lower case characters. It is build once when the processor starts and before it connects to the database. This number can have up to 3.6e15 different values and is built every time the processor starts a thread and kept as long as there’re Flowfiles in the queue. If you would connect every minute with 100 threads in parallel it would take about 58 Mio years to have a conflict on `%TABLE_ID%` with 50% probability. It’s seeded to the current time in nano seconds, a hash of the ip address and the current thread id. Therefore it's very likely, that each thread on each server in a NIFI cluster has a different seed, producing a different `%TABLE_ID%`. Even with the extremely rare cases of conflicting `%TABLE_ID%`’s it is good practise to have some code in the connected- or the before loading script to fail, if the `%TABLE_ID%` is already in use.
  - `%COMMIT_EPOCH%` is a count of the number of loadings happened within the current connection. For the first loading, this number is 0, the second loading get 1 and so on. `%COMMIT_EPOCH%` is always 0 for the connect script and contains the number of successful loadings in the disconnecting script.
  - Both values are available as text so they can be easily used as placeholders in an UpdateAttribute processor as described below. They’re also available as attributes in the FlowFile and set as attributes in the original FlowFile and the FlowFile transferred to the script relationship.
     
#### **Teradata CSV**
- Define two separated connections and set “build one batch” to “CSV”
- The connection for loading must contain TYPE=FASTLOADCSV in the URL. It can optionally contain additional parameters, eg. for Sessions, Charset, Ansi-Mode or delimiter of the CSV
- Make sure the incoming FLowFile contains CSV content, that is delimited by the delimiter defined in the URL, the fields are not quoted nor are delimiter escaped accordingly to the requirements of the Teradata JDBC driver.
- For more information about the Teradata Fastload-CSV capability have a look [here / Teradata-JDBC Docs](http://developer.teradata.com/doc/connectivity/jdbc/reference/current/frameset.html) and [here / speed up JDBC connection](http://developer.teradata.com/connectivity/articles/speed-up-your-jdbcodbc-applications)
- In order to prepare the data into a large CSV file (with which Teradata-Fastload-CSV works best) you might want to setup a flow of:
  - Some `ConvertRecord` with a `RecordReader` that reads and parses your input and a `FreeFormRecordSetWriter` that writes your data with the right delimiter, might replace the delimiter character in string fields with any other character and does some optional other conversions. The data consists then of small packages of CSV records without header.
  - Use a `MergeContent` with binary concatenation to collect into some reasonable large FlowFiles. Use Text and header field to add the column names in the loading table on top of the FlowFile
  - Pass the FlowFile into **putJdbcCsv**. Optionally use the strategy to fill different tables with one processor instance (see below) by adding an `UpdateAttribute` processor in front of **putJdbcCsv**.

#### **sqlite bulk load** handles larger amounts of rows best, when using a row-by-row insert, that was prepared and is only committed once per batch.
- Define one connection and set `build batch Strategy` to `By RecordReader row-by-row`
- In connected-script: Optionally set some Pragmas
- In before loading script: `begin transaction`
- In after loading script: `commit` and optionally before or after the commit additional ELT.

#### Limit number of simultaneously running bulk loads. If your database has a limit on simultaneously running bulk load jobs or you want to make sure, that your database does not get overloaded. But still want to run ELT jobs in parallel to loading.
- Set number of threads of the processor to at least two, so that one processor can run ELT while another thread loads data.
- Use an extra Connection Pool Service Controller for the loading connection
- In this extra Connection Pool Service for the loading connection, limit the number of connections to 1 and set timeout as high as one loading might need to run. You might want to add some extra time to the timeout, just in case.

#### Make sure FlowFiles contain a minimum number of rows as bulk loads usually profit from larger number of rows
- Place a MergeContent or MergeRecord (Version >= 1.4) processor before putJdbc to merge FLowFile content together.

#### Fill different tables with one processor instance. If you have limited the number of concurrently running threads of this processor but have to fill different tables with different preparation, ELT, insert statement etc.
- Place an UpdateAttributes processor before putJdbc to set attributes for all scripts and for the insert statement
- Use the attributes in the corresponding properties of putJdbc
- You can use `%TABLE_ID%` and `%COMMIT_EPOCH%` to name the loading table uniquely.

#### AVRO schema of script relationship
- The outgoing script contains timing information (Elapsed times) and `%TABLE_ID%`, `%COMMIT_EPOCH%` plus result sets and update counts returned by the scripts and insert statement. The information given back from SQL statements is taken as is.
- It is created when the connecting script returns, after each loading sequence (after loading script finished) and right after the disconnecting script returns. If any of the scripts are empty, the FlowFile is still produced but will be empty.
