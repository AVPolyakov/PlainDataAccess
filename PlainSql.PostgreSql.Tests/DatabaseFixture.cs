using System;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using DbUp;
using Npgsql;
using Xunit;

namespace PlainSql.PostgreSql.Tests
{
    public class DatabaseFixture: IAsyncLifetime
    {
        public IDbExecutor Db { get; }

        public DatabaseFixture()
        {
            Sql.MappingCheckEnabled = true;
            ISqlAdapter.Current = new PostgreSqlAdapter();
            DefaultTypeMap.MatchNamesWithUnderscores = true;
            
            Db = new DbExecutor(ConnectionString);
        }
        
        private static string ConnectionString => new NpgsqlConnectionStringBuilder(DefaultConnectionString) {Database = DatabaseName}.ConnectionString;
        
        private const string DefaultConnectionString = @"Server=127.0.0.1;Port=5432;Database=postgres;User Id=postgres;Password=qwe123456;";
        
        private const string DatabaseName = "plain_sql";

        public async Task InitializeAsync()
        {
            var db = new DbExecutor(DefaultConnectionString);

            var singleOrDefault = await new Sql(@$"
SELECT datname
FROM pg_catalog.pg_database 
WHERE datname = '{DatabaseName}'").SingleOrDefaultAsync<string?>(db);

            if (singleOrDefault != null)
                await new Sql($"DROP DATABASE {DatabaseName}").ExecuteAsync(db);

            await new Sql($"CREATE DATABASE {DatabaseName}").ExecuteAsync(db);

            var upgrader = DeployChanges.To
                .PostgresqlDatabase(ConnectionString)
                .WithScriptsEmbeddedInAssembly(Assembly.GetExecutingAssembly())
                .WithTransactionPerScript()
                .LogToConsole()
                .Build();

            var result = upgrader.PerformUpgrade();

            if (!result.Successful)
                throw new Exception("Database upgrade failed", result.Error);
        }
        
        public Task DisposeAsync() => Task.CompletedTask;
    }
}