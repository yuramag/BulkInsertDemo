using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BulkInsertDemo.API;
using BulkInsertDemo.Helpers;
using BulkInsertDemo.Model;
using static System.Console;

namespace BulkInsertDemo
{
    class Program
    {
        private static void Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            var e = new ManualResetEventSlim();

            var task = Task.Run(async () => await RunDemoAsync(cts.Token));

            task.ContinueWith(t => WriteLine("EXCEPTION: {0}", t.Exception), TaskContinuationOptions.OnlyOnFaulted);
            task.ContinueWith(t => WriteLine("Task has been cancelled"), TaskContinuationOptions.OnlyOnCanceled);
            task.ContinueWith(t => e.Set());

            e.Wait();

            cts.Cancel();

            WriteLine("\nDONE! Press Enter to exit...");
            ReadLine();
        }

        private static async Task RunDemoAsync(CancellationToken cancellationToken)
        {
            var cs = ConfigurationManager.ConnectionStrings["DefaultCS"].ConnectionString;
            var recordsCount = int.Parse(ConfigurationManager.AppSettings["MaxRecordCount"]);

            WriteLine("Using Connection String: {0}", cs);
            WriteLine("Record Count: {0:#,0}", recordsCount);
            WriteLine();
            WriteLine("Press Enter to start Demo...");
            ReadLine();

            var csb = new SqlConnectionStringBuilder(cs)
            {
                MultipleActiveResultSets = true,
                ApplicationName = typeof (Program).Namespace,
                AsynchronousProcessing = true
            };

            using (var connection = new SqlConnection(csb.ConnectionString))
            {
                await connection.OpenAsync(cancellationToken);

                Write("Initializing the database... ");
                await InitializeDbAsync(connection);
                WriteLine("Success.");

                await RunDemoAsync("Static Dataset", () => RunStaticDatasetDemoAsync(connection, recordsCount, cancellationToken));
                await RunDemoAsync("Dynamic Dataset", () => RunDynamicDatasetDemoAsync(connection, recordsCount, cancellationToken));
                await RunDemoAsync("CSV Dataset", () => RunCsvDatasetDemoAsync(connection, recordsCount, cancellationToken));
            }
        }

        private static async Task RunDemoAsync(string description, Func<Task> demo)
        {
            WriteLine("\nPress Enter to run {0} Demo...", description);
            ReadLine();
            var sw = Stopwatch.StartNew();
            await demo();
            WriteLine("Elapsed: {0}", sw.Elapsed);
        }

        private static async Task InitializeDbAsync(DbConnection connection)
        {
            if (await TableSchemaProvider.TableExistsAsync(connection, "Contacts"))
                await TableSchemaProvider.DropTableAsync(connection, "Contacts");

            var sqlContacts = "create table Contacts(Id int primary key, FirstName varchar(255), LastName varchar(255), BirthDate smalldatetime)";
            await TableSchemaProvider.ExecuteScriptAsync(connection, sqlContacts);

            var intCols = string.Join(", ", Enumerable.Range(1, 10).Select(x => $"I_COL_{x:D2} int"));
            var stringCols = string.Join(", ", Enumerable.Range(1, 10).Select(x => $"S_COL_{x:D2} varchar(255)"));
            var dateCols = string.Join(", ", Enumerable.Range(1, 10).Select(x => $"D_COL_{x:D2} smalldatetime"));
            var guidCols = string.Join(", ", Enumerable.Range(1, 10).Select(x => $"G_COL_{x:D2} uniqueidentifier"));

            if (await TableSchemaProvider.TableExistsAsync(connection, "DynamicData"))
                await TableSchemaProvider.DropTableAsync(connection, "DynamicData");
            var sqlDynamicData = $"create table DynamicData(Id int primary key, {intCols}, {stringCols}, {dateCols}, {guidCols})";
            await TableSchemaProvider.ExecuteScriptAsync(connection, sqlDynamicData);

            if (await TableSchemaProvider.TableExistsAsync(connection, "CsvData"))
                await TableSchemaProvider.DropTableAsync(connection, "CsvData");
            var sqlCsvData = $"create table CsvData(Id int primary key, {intCols}, {stringCols}, {dateCols}, {guidCols})";
            await TableSchemaProvider.ExecuteScriptAsync(connection, sqlCsvData);
        }

        private static async Task RunStaticDatasetDemoAsync(SqlConnection connection, int count, CancellationToken cancellationToken)
        {
            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "Contacts";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = (int) TimeSpan.FromMinutes(10).TotalSeconds;

                bulkCopy.ColumnMappings.Add("Id", "Id");
                bulkCopy.ColumnMappings.Add("FirstName", "FirstName");
                bulkCopy.ColumnMappings.Add("LastName", "LastName");
                bulkCopy.ColumnMappings.Add("BirthDate", "BirthDate");

                using (var reader = new ObjectDataReader<Contact>(new RandomDataGenerator().GetContacts(count)))
                    await bulkCopy.WriteToServerAsync(reader, cancellationToken);
            }
        }

        private static async Task RunDynamicDatasetDemoAsync(SqlConnection connection, int count, CancellationToken cancellationToken)
        {
            var fields = await new TableSchemaProvider(connection, "DynamicData").GetFieldsAsync();

            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "DynamicData";
                bulkCopy.BatchSize = 1000;
                bulkCopy.BulkCopyTimeout = (int) TimeSpan.FromMinutes(10).TotalSeconds;

                foreach (var field in fields)
                    bulkCopy.ColumnMappings.Add(field.FieldName, field.FieldName);

                var data = new RandomDataGenerator().GetDynamicData(count);

                using (var reader = new DynamicDataReader<IDictionary<string, object>>(fields, data, (x, k) => x.GetValueOrDefault(k)))
                    await bulkCopy.WriteToServerAsync(reader, cancellationToken);
            }
        }

        private static async Task RunCsvDatasetDemoAsync(SqlConnection connection, int count, CancellationToken cancellationToken)
        {
            using (var csvReader = new StreamReader(@"Data\CsvData.csv"))
            {
                var csvData = CsvParser.ParseHeadAndTail(csvReader, ',', '"');

                var csvHeader = csvData.Item1
                    .Select((x, i) => new {Index = i, Field = x})
                    .ToDictionary(x => x.Field, x => x.Index);

                var csvLines = csvData.Item2;

                var fields = await new TableSchemaProvider(connection, "CsvData").GetFieldsAsync();

                using (var bulkCopy = new SqlBulkCopy(connection))
                {
                    bulkCopy.DestinationTableName = "CsvData";
                    bulkCopy.BatchSize = 1000;
                    bulkCopy.BulkCopyTimeout = (int) TimeSpan.FromMinutes(10).TotalSeconds;

                    foreach (var field in fields)
                        bulkCopy.ColumnMappings.Add(field.FieldName, field.FieldName);

                    using (var reader = new DynamicDataReader<IList<string>>(fields, csvLines.Take(count),
                        (x, k) => x.GetValueOrDefault(csvHeader.GetValueOrDefault(k, -1))))
                    {
                        await bulkCopy.WriteToServerAsync(reader, cancellationToken);
                    }
                }
            }
        }
    }
}