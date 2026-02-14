# Taxi Data ETL

A C# Console Application designed to extract, transform, and load (ETL) taxi trip data from a CSV file into a SQL Server database.

Assumptions Made
1.  DB: Created the database through SSMS, but added a creation script as an example.
2.	Timezones: The input CSV does not specify a timezone, but based on the requirements, it is assumed to be Eastern Standard Time (EST).
    The application explicitly converts these times to UTC before insertion.
3.	Duplicates: A record is considered a duplicate only if the exact combination of tpep_pickup_datetime, tpep_dropoff_datetime, and passenger_count matches an existing record.
    If passenger_count is missing, it is handled safely (treated as 0 for the hash key).
4.	Unsafe Data: To handle potentially unsafe sources, CsvHelper is used for strict, safe type parsing, and SqlBulkCopy is used for database insertion, which inherently protects against SQL injection.

Handling a 10GB CSV File
If the application were to process a 10GB CSV file, the current in-memory deduplication (HashSet) and list accumulation would cause an OutOfMemoryException. 
  To fix this, I would change the architecture to:
1.	Database-level Deduplication: Instead of a memory HashSet, I would use a SQL Staging Table.
    The program would read the CSV in chunks (e.g., using IAsyncEnumerable) and bulk insert raw data into the staging table.
    Then, I would use a SQL query (ROW_NUMBER() OVER(PARTITION BY...)) to filter duplicates and move the clean data to the final table.
2.	Batching: SqlBulkCopy would be executed in smaller batches (e.g., 50,000 rows at a time) directly from the CSV stream to keep RAM usage minimal.
