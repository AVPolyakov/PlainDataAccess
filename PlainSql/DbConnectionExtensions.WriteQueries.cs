using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations.Schema;

namespace PlainSql
{
    public static partial class DbConnectionExtensions
    {
        public static async Task<TKey> InsertAsync<TKey>(this IDbConnection connection, object param)
        {
            var type = param.GetType();
            
            var table = GetTableName(type);
            
            var columnInfos = GetColumns(table, connection, type);
            
            var writeableColumns = columnInfos
                .Where(_ => !_.IsAutoIncrement && !_.IsReadOnly(type))
                .ToList();
            var columnsClause = string.Join(",", writeableColumns.Select(_ => _.ColumnName.EscapedName()));
            var autoIncrementColumn = columnInfos.SingleOrDefault(_ => _.IsAutoIncrement);
            if (autoIncrementColumn == null)
                throw new Exception("Auto increment column not found.");
            var outClause = autoIncrementColumn.ColumnName.EscapedName();
            var valuesClause = string.Join(",", writeableColumns.Select(_ => $"@{type.EntityColumnName(_.ColumnName)}"));
            
            var sqlText = ISqlAdapter.Current.InsertQueryText(table, columnsClause, valuesClause, outClause);
            
            var sql = new Sql(sqlText, param);
            
            return await sql.SingleAsync<TKey>(connection);
        }
        
        public static async Task<int> InsertAsync(this IDbConnection connection, object param)
        {
            var type = param.GetType();
            
            var table = GetTableName(type);
            
            var columnInfos = GetColumns(table, connection, type);
            
            var columns = columnInfos
                .Where(_ => !_.IsAutoIncrement && !_.IsReadOnly(type))
                .ToList();
            var columnsClause = string.Join(",", columns.Select(_ => _.ColumnName.EscapedName()));
            var valuesClause = string.Join(",", columns.Select(_ => $"@{type.EntityColumnName(_.ColumnName)}"));
            
            var sql = new Sql($@"
INSERT INTO {table} ({columnsClause}) 
VALUES ({valuesClause})", param);
            
            return await sql.ExecuteAsync(connection);
        }
        
        public static async Task<int> UpdateAsync(this IDbConnection connection, object param)
        {
            var type = param.GetType();
            
            var table = GetTableName(type);
            
            var columnInfos = GetColumns(table, connection, type);
            
            var setClause = string.Join(",",
                columnInfos
                    .Where(_ => !_.IsKey && !_.IsReadOnly(type))
                    .Select(_ => $"{_.ColumnName.EscapedName()}=@{type.EntityColumnName(_.ColumnName)}"));
            var whereClause = string.Join(" AND ", 
                columnInfos.Where(_ => _.IsKey).Select(_ => $"{_.ColumnName.EscapedName()}=@{type.EntityColumnName(_.ColumnName)}"));
            
            var sql = new Sql($@"
UPDATE {table}
SET {setClause}
WHERE {whereClause}", param);
            
            return await sql.ExecuteAsync(connection);
        }

        public static async Task<int> DeleteAsync<T>(this IDbConnection connection, object param)
        {
            var type = typeof(T);
            
            var tableName = GetTableName(type);
            
            var columnInfos = GetColumns(tableName, connection, type);
            var whereClause = string.Join(" AND ", 
                columnInfos.Where(_ => _.IsKey).Select(_ => $"{_.ColumnName.EscapedName()}=@{type.EntityColumnName(_.ColumnName)}"));
            
            var sql = new Sql($@"
DELETE FROM {tableName}
WHERE {whereClause}", param);
            
            return await sql.ExecuteAsync(connection);
        }
        
        public static async Task<T> GetByKeyAsync<T>(this IDbConnection connection, object param)
        {
            var type = typeof(T);
            
            var tableName = GetTableName(type);
            
            var columnInfos = GetColumns(tableName, connection, type);
            
            var selectClause = string.Join(",", 
                columnInfos.Select(_ => _.ColumnName.EscapedName()));
            var whereClause = string.Join(" AND ", 
                columnInfos.Where(_ => _.IsKey).Select(_ => $"{_.ColumnName.EscapedName()}=@{type.EntityColumnName(_.ColumnName)}"));
            
            var sql = new Sql($@"
SELECT {selectClause}
FROM {tableName}
WHERE {whereClause}", param);
            
            return await sql.SingleAsync<T>(connection);
        }

        private static string EscapedName(this string name) => ISqlAdapter.Current.EscapedName(name);
        
        private static bool IsReadOnly(this ColumnInfo columnInfo, Type type) => type.ColumnIsReadOnly(columnInfo.ColumnName);

        private static bool ColumnIsReadOnly(this Type type, string columnName)
        {
            var key = new IsReadOnlyKey(type, columnName);
            if (_isReadOnlyDictionary.TryGetValue(key, out var value))
                return value;

            bool Find()
            {
                var property = type.FindProperty(columnName);

                if (property == null)
                    return false;

                var attribute = property.GetCustomAttribute<DatabaseGeneratedAttribute>();
                
                if (attribute == null)
                    return false;

                return attribute.DatabaseGeneratedOption != DatabaseGeneratedOption.None;
            }

            var result = Find();

            _isReadOnlyDictionary.TryAdd(key, result);

            return result;
        }

        private static readonly ConcurrentDictionary<IsReadOnlyKey, bool> _isReadOnlyDictionary = new();

        private record IsReadOnlyKey(Type Type, string ColumnName)
        {
        }

        private static string GetTableName(Type type)
        {
            if (_tableNameDictionary.TryGetValue(type, out var value))
                return value;
            
            var tableAttributeName = GetTableAttributeName(type);
            var tableName = tableAttributeName ?? type.Name + "s";

            _tableNameDictionary.TryAdd(type, tableName);
            
            return tableName;
        }

        private static string? GetTableAttributeName(Type type)
        {
            var attribute = type.GetCustomAttribute<TableAttribute>();

            if (attribute == null)
                return null;

            if (string.IsNullOrWhiteSpace(attribute.Schema))
                return attribute.Name.EscapedName();
            
            return attribute.Schema.EscapedName() + "." + attribute.Name.EscapedName();
        }

        private static readonly ConcurrentDictionary<Type, string> _tableNameDictionary = new();
        
        private static readonly ConcurrentDictionary<TableKey, List<ColumnInfo>> _columnDictionary = new();
        
        private static List<ColumnInfo> GetColumns(string table, IDbConnection connection, Type type)
        {
            var tableKey = new TableKey(table, connection.ConnectionString);
            if (_columnDictionary.TryGetValue(tableKey, out var value)) 
                return value;
            
            var columnsFromDb = GetColumnsFromDb(table, connection, type);
            var result = columnsFromDb.Where(c => type.FindProperty(c.ColumnName) != null).ToList();
            
            _columnDictionary[tableKey] = result;
            
            return result;
        }

        private record TableKey(string Table, string ConnectionString)
        {
        }

        private static List<ColumnInfo> GetColumnsFromDb(string table, IDbConnection connection, Type type)
        {
            var wasClosed = connection.State == ConnectionState.Closed;
            try
            {
                if (wasClosed)
                    connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT * FROM {table}";

                    using (var reader = command.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.KeyInfo))
                    {
                        reader.CheckMapping(type);

                        var schemaTable = reader.GetSchemaTable();

                        return schemaTable!.Rows.Cast<DataRow>()
                            .Select(row => new ColumnInfo(
                                (string) row["ColumnName"],
                                true.Equals(row["IsKey"]),
                                true.Equals(row["IsAutoIncrement"])))
                            .ToList();
                    }
                }
            }
            finally
            {
                if (wasClosed)
                    connection.Close();
            }
        }

        private record ColumnInfo(string ColumnName, bool IsKey, bool IsAutoIncrement)
        {
        }
    }
}