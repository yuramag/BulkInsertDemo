using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using BulkInsertDemo.API;

namespace BulkInsertDemo.Helpers
{
    public sealed class TableSchemaProvider
    {
        public TableSchemaProvider(DbConnection connection, string tableName)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            Connection = connection;
            TableName = tableName;
        }

        public DbConnection Connection { get; }
        public string TableName { get; }

        public async Task<List<SchemaFieldDef>> GetFieldsAsync(params string[] ignoreFields)
        {
            return await Task.Run(() =>
            {
                var schema = Connection.GetSchema("Columns", new[] { null, null, TableName });
                return (from row in schema.AsEnumerable()
                        let fieldName = row.Field<string>("COLUMN_NAME")
                        let size = row.Field<int?>("CHARACTER_MAXIMUM_LENGTH")
                        let dataType = SchemaFieldDef.StringToDataType(row.Field<string>("DATA_TYPE"))
                        where !ignoreFields.Contains(fieldName)
                        select new SchemaFieldDef { FieldName = fieldName, Size = size ?? 0, DataType = dataType }).ToList();
            });
        }

        public Task<bool> TableExistsAsync()
        {
            return TableExistsAsync(Connection, TableName);
        }

        public static async Task<bool> TableExistsAsync(DbConnection connection, string tableName)
        {
            return await Task.Run(() =>
            {
                var schema = connection.GetSchema("Tables", new[] { null, null, tableName });
                return schema.Rows.Count > 0;
            });
        }

        public Task TruncateTableAsync()
        {
            return TruncateTableAsync(Connection, TableName);
        }

        public static async Task TruncateTableAsync(DbConnection connection, string tableName)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"truncate table {tableName}";
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public Task DropTableAsync()
        {
            return DropTableAsync(Connection, TableName);
        }

        public static async Task DropTableAsync(DbConnection connection, string tableName)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"drop table {tableName}";
                await cmd.ExecuteNonQueryAsync();
            }
        }

        public Task ExecuteScriptAsync(string sql)
        {
            return ExecuteScriptAsync(Connection, sql);
        }

        public static async Task ExecuteScriptAsync(DbConnection connection, string sql)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}