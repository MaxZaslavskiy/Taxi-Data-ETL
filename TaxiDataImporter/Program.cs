using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Reflection;
using TaxiDataImporter;

string csvFilePath = @"C:\Users\Lenovo\Downloads\db\sample-cab-data.csv";
string duplicatesFilePath = @"C:\Users\Lenovo\Downloads\db\duplicates.csv";
string connectionString = @"Server=localhost;Database=TaxiDb;Trusted_Connection=True;TrustServerCertificate=True;";

Console.WriteLine("Starting processing...");

var readConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
{
    HasHeaderRecord = true,
    TrimOptions = TrimOptions.Trim,
    MissingFieldFound = null
};

TimeZoneInfo estZone;
try { estZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time"); }
catch { estZone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); }

var uniqueKeys = new HashSet<(DateTime, DateTime, int)>();
var validRecords = new List<TripRecord>();

try
{
    using (var reader = new StreamReader(csvFilePath))
    using (var csv = new CsvReader(reader, readConfig))
    using (var writer = new StreamWriter(duplicatesFilePath))
    using (var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture))
    {
        csv.Context.RegisterClassMap<TripMap>();
        csv.Read();
        csv.ReadHeader();

        csvWriter.WriteHeader<TripRecord>();
        csvWriter.NextRecord();

        int totalRows = 0;
        int duplicateRows = 0;

        Console.WriteLine("Reading and transforming data...");

        while (csv.Read())
        {
            totalRows++;
            var record = csv.GetRecord<TripRecord>();

            var key = (record.PickupDatetime, record.DropoffDatetime, record.PassengerCount ?? 0);

            if (uniqueKeys.Contains(key))
            {
                csvWriter.WriteRecord(record);
                csvWriter.NextRecord();
                duplicateRows++;
                continue;
            }

            uniqueKeys.Add(key);

            if (record.StoreAndFwdFlag == "N") record.StoreAndFwdFlag = "No";
            else if (record.StoreAndFwdFlag == "Y") record.StoreAndFwdFlag = "Yes";

            record.PickupDatetime = TimeZoneInfo.ConvertTimeToUtc(record.PickupDatetime, estZone);
            record.DropoffDatetime = TimeZoneInfo.ConvertTimeToUtc(record.DropoffDatetime, estZone);

            validRecords.Add(record);
        }

        Console.WriteLine($"\nData preparation finished. Valid records: {validRecords.Count}. Inserting into DB...");

        BulkInsert(validRecords, connectionString);

        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            using (var command = new SqlCommand("SELECT COUNT(*) FROM Trips;", connection))
            {
                int rowCount = (int)command.ExecuteScalar();
                Console.WriteLine($"\n[SUCCESS] Total rows in 'Trips' table after execution: {rowCount}");
            }
        }

        Console.WriteLine("Done!");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Critical Error: {ex.Message}");
}


void BulkInsert(List<TripRecord> records, string connectionString)
{
    var dataTable = ToDataTable(records);

    using (var connection = new SqlConnection(connectionString))
    {
        connection.Open();

        using (var clearCmd = new SqlCommand("TRUNCATE TABLE Trips;", connection))
        {
            clearCmd.ExecuteNonQuery();
        }

        using (var bulkCopy = new SqlBulkCopy(connection))
        {
            bulkCopy.DestinationTableName = "Trips";

            bulkCopy.ColumnMappings.Add("PickupDatetime", "tpep_pickup_datetime");
            bulkCopy.ColumnMappings.Add("DropoffDatetime", "tpep_dropoff_datetime");
            bulkCopy.ColumnMappings.Add("PassengerCount", "passenger_count");
            bulkCopy.ColumnMappings.Add("TripDistance", "trip_distance");
            bulkCopy.ColumnMappings.Add("StoreAndFwdFlag", "store_and_fwd_flag");
            bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
            bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");
            bulkCopy.ColumnMappings.Add("FareAmount", "fare_amount");
            bulkCopy.ColumnMappings.Add("TipAmount", "tip_amount");

            try
            {
                bulkCopy.WriteToServer(dataTable);
                Console.WriteLine("Bulk insert completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"SQL Error: {ex.Message}");
            }
        }
    }
}

DataTable ToDataTable<T>(List<T> items)
{
    var dataTable = new DataTable(typeof(T).Name);
    PropertyInfo[] Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

    foreach (PropertyInfo prop in Props)
    {
        var type = (prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
            ? Nullable.GetUnderlyingType(prop.PropertyType)
            : prop.PropertyType;

        dataTable.Columns.Add(prop.Name, type);
    }

    foreach (T item in items)
    {
        var values = new object[Props.Length];
        for (int i = 0; i < Props.Length; i++)
        {
            values[i] = Props[i].GetValue(item, null) ?? DBNull.Value;
        }
        dataTable.Rows.Add(values);
    }
    return dataTable;
}