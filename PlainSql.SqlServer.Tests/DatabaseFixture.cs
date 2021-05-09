using System;
using System.Data.SqlClient;
using System.Reflection;
using System.Threading.Tasks;
using DbUp;
using Xunit;

namespace PlainSql.SqlServer.Tests
{
    public class DatabaseFixture: IAsyncLifetime
    {
        public IDbExecutor Db { get; }

        public DatabaseFixture()
        {
            Sql.MappingCheckEnabled = true;
            ISqlAdapter.Current = new SqlServerAdapter(); 
            
            Db = new DbExecutor(ConnectionString);
        }

        private static string ConnectionString => new SqlConnectionStringBuilder(DefaultConnectionString) {InitialCatalog = DatabaseName}.ConnectionString;
        
        private const string DefaultConnectionString = @"Data Source=(local)\SQL2014;Initial Catalog=master;Integrated Security=True";
        
        private const string DatabaseName = "PlainSql";
        
        public async Task InitializeAsync()
        {
            var db = new DbExecutor(DefaultConnectionString);
            
            await new Sql(@$"
IF EXISTS ( SELECT * FROM sys.databases WHERE name = '{DatabaseName}' )
    DROP DATABASE [{DatabaseName}]

CREATE DATABASE [{DatabaseName}]
").ExecuteAsync(db);
            
            var upgrader = DeployChanges.To
                .SqlDatabase(ConnectionString)
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