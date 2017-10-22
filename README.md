# putJdbc (putJdbcCsv, putJdbcFlex)
putJdbc is a NIFI Processor for bulk loading data into a database. It takes a FlowFile, then initializes a sequence of scripts and load commands to load and transform data. With a number of options it is able to be configured using the best practice approaches for very large databases and high speed ingestion. See the usage page for the optimal configuration for your database. 

The processor comes in two flavours:
- **putJdbcFlex** allows a parameterized insert statement to load into database. The input can be any record oriented data, that a RecordReader can parse.
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

Now you should be able to configure the first putJdbc processor.

## Configure your first putJdbc processor
