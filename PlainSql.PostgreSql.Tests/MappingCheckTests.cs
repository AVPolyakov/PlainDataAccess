using System;
using System.Threading.Tasks;
using Xunit;

namespace PlainSql.PostgreSql.Tests
{
    [Collection(nameof(FixtureCollection))]
    public class MappingCheckTests
    {
        private readonly IDbExecutor _db;

        public MappingCheckTests(DatabaseFixture databaseFixture) => _db = databaseFixture.Db;

        [Fact]
        public async Task EmptyDestinationType_ExceptionThrown()
        {
            var sql = new Sql("SELECT p.post_id, p.text, p.creation_date FROM posts p");

            var exception = await Assert.ThrowsAsync<Exception>(
                () => sql.ToListAsync<PostInfo2>(_db));

            Assert.Equal(@"Property 'PostId' not found in destination type. You can copy list of properties to destination type PlainSql.PostgreSql.Tests.PostInfo2:
        public int? PostId { get; set; }
        public string Text { get; set; }
        public DateTime? CreationDate { get; set; }",
                exception.Message);
        }
        
        [Fact]
        public async Task FieldTypeMismatch_ExceptionThrown()
        {
            var sql = new Sql("SELECT p.post_id, p.text, p.creation_date FROM posts p");

            var exception = await Assert.ThrowsAsync<Exception>(
                () => sql.ToListAsync<PostInfo3>(_db));

            Assert.Equal(@"Type of field 'PostId' does not match. Field type is 'long' in destination and `int?` in query. You can copy list of properties to destination type PlainSql.PostgreSql.Tests.PostInfo3:
        public int? PostId { get; set; }
        public string Text { get; set; }
        public DateTime? CreationDate { get; set; }",
                exception.Message);
        }
        
        [Fact]
        public async Task IgnoredProperty_Success()
        {
            var sql = new Sql("SELECT p.post_id, p.text, p.creation_date FROM posts p");

            var list = await sql.ToListAsync<PostInfo4>(_db);
            
            Assert.NotNull(list);
        }
        
        [Fact]
        public async Task FieldTypeMismatch_Insert_ExceptionThrown()
        {
            var post = new Namaspace2.Post {CreationDate = new DateTime(2014, 1, 1)};

            var exception = await Assert.ThrowsAsync<Exception>(
                () => _db.InsertAsync<long>(post));

            Assert.Equal(@"Type of field 'PostId' does not match. Field type is 'long' in destination and `int` in query. You can copy list of properties to destination type PlainSql.PostgreSql.Tests.Namaspace2.Post:
        public int PostId { get; set; }
        public string Text { get; set; }
        public DateTime CreationDate { get; set; }",
                exception.Message);
        }        
    }
}