# putJdbc (putJdbcCsv, putJdbcFlex)
NIFI Processor for bulk loading data into a database.
The processor comes in two flavours:
### putJdbcFlex allows allows a parameterized insert statement to load into data
base. The input can be any record oriented data, that any RecordReader can parse.
### putJdbcCsv i especially designed to load extreme amounts of data into Teradata using the Fastload-CSV feature of Teradata's Jdbc driver.
For more information see the usage page of these processors in Nifi.

## 
Load bulk amount of data into Teradata possibly using Fastload capabilities.
Define an insert statement for a table to land the data and have this processor start scripts for mini-batched ELT. 
The Loader uses a landing tables to load into. It first runs a script before it loads into the table. When the script 
has finished load of table starts. After a configurable time and amount of data this processor runs the ELT script 
and commits its work. When th Queue gets empty or the USer stops this processor, a final commit-sequence (database-commit, 
ELT Script, Nifi-commit) runs and the after script is started. Any exception that happens will roll back and yield the 
processor if it was not a transient error. The processor works best, if the before script ensures an empty and exclusive 
table to load the data. Thus, together with the commit and roll back for a session in Nifi ensures, that Flow Files 
are always loaded once and only once into the database.
