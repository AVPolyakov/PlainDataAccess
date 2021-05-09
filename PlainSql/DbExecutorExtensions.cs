using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace PlainSql
{
    public static partial class DbExecutorExtensions
    {
        public static Task<T> SingleAsync<T>(this Sql sql, IDbExecutor<IDbConnection> executor)
            => executor.ExecuteAsync(sql.SingleAsync<T>);

        public static Task<T> SingleOrDefaultAsync<T>(this Sql sql, IDbExecutor<IDbConnection> executor)
            => executor.ExecuteAsync(sql.SingleOrDefaultAsync<T>);

        public static Task<T> FirstAsync<T>(this Sql sql, IDbExecutor<IDbConnection> executor)
            => executor.ExecuteAsync(sql.FirstAsync<T>);

        public static Task<T> FirstOrDefaultAsync<T>(this Sql sql, IDbExecutor<IDbConnection> executor)
            => executor.ExecuteAsync(sql.FirstOrDefaultAsync<T>);
        
        public static Task<List<T>> ToListAsync<T>(this Sql sql, IDbExecutor<IDbConnection> executor)
            => executor.ExecuteAsync(sql.ToListAsync<T>);

        public static Task<T[]> ToArrayAsync<T>(this Sql sql, IDbExecutor<IDbConnection> executor)
            => executor.ExecuteAsync(sql.ToArrayAsync<T>);

        public static Task<int> ExecuteAsync(this Sql sql, IDbExecutor<IDbConnection> executor)
            => executor.ExecuteAsync(sql.ExecuteAsync);
    }
}