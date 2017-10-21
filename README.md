# putJdbc (putJdbcCsv, putJdbcFlex)
NIFI Processor for bulk loading data into a database. It takes a FlowFile, then initializes a sequence to load and transform the data using the best practice approach for very large databases.

The processor comes in two flavours:
- **putJdbcFlex** allows a parameterized insert statement to load into database. The input can be any record oriented data, that a RecordReader can parse.
- **putJdbcCsv** is especially designed to load extreme amounts of data into Teradata using the Fastload-CSV feature of Teradata's Jdbc driver.

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
    where ${NIFI_HOME} is the installation directory of your NIFI installation
4. restart NIFI

Done!

Now you should be able to configure the first putJdbc processor.

## Configure your first putJdbc processor
