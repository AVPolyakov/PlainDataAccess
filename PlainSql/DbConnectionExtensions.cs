using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;

namespace PlainSql
{
    public static partial class DbConnectionExtensions
    {
        public static Task<T> SingleAsync<T>(this Sql sql, IDbConnection connection) 
            => connection.QuerySingleAsync<T>(sql.Text, sql.Parameters);

        public static Task<T> SingleOrDefaultAsync<T>(this Sql sql, IDbConnection connection) 
            => connection.QuerySingleOrDefaultAsync<T>(sql.Text, sql.Parameters);

        public static Task<T> FirstAsync<T>(this Sql sql, IDbConnection connection) 
            => connection.QueryFirstAsync<T>(sql.Text, sql.Parameters);

        public static Task<T> FirstOrDefaultAsync<T>(this Sql sql, IDbConnection connection) 
            => connection.QueryFirstOrDefaultAsync<T>(sql.Text, sql.Parameters);

        public static async Task<List<T>> ToListAsync<T>(this Sql sql, IDbConnection connection)
        {
            var enumerable = await connection.QueryAsync<T>(sql.Text, sql.Parameters);
            return enumerable.AsList();
        }

        public static async Task<T[]> ToArrayAsync<T>(this Sql sql, IDbConnection connection)
        {
            var enumerable = await connection.QueryAsync<T>(sql.Text, sql.Parameters);
            return enumerable.ToArray();
        }

        public static Task<int> ExecuteAsync(this Sql sql, IDbConnection connection) => 
            connection.ExecuteAsync(sql.Text, sql.Parameters);
    }
}