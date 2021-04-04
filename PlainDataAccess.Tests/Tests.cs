using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace PlainDataAccess.Tests
{
    public class Tests:  IClassFixture<DatabaseFixture>
    {
        private static ConnectionInfo Db => DatabaseFixture.Db;

        public Tests() => QueryExtensions.MappingCheckEnabled = true;

        [Fact]
        public async Task Posts_Success()
        {
            var date = new DateTime(2015, 1, 1);
            
            var query = Db.Query(@"
SELECT p.PostId, p.Text, p.CreationDate
FROM Post p
WHERE p.CreationDate >= @date
ORDER BY p.PostId", new {date});

            var postInfos = await query.ToList<PostInfo>();
            
            Assert.Equal(2, postInfos.Count);

            Assert.NotEqual(default, postInfos[0].PostId);
            Assert.NotEqual(default, postInfos[1].PostId);
            
            Assert.Equal("Test1", postInfos[0].Text);
            Assert.Null(postInfos[1].Text);
            
            Assert.Equal(new DateTime(2021, 01, 14), postInfos[0].CreationDate);
            Assert.Equal(new DateTime(2021, 02, 15), postInfos[1].CreationDate);
        }
        
        [Fact]
        public async Task Posts_DynamicSql_Success()
        {
            {
                var postInfos = await GetPosts(new DateTime(2015, 1, 1));
                Assert.Equal(2, postInfos.Count);
            }
            {
                var postInfos = await GetPosts(new DateTime(3015, 1, 1));
                Assert.Empty(postInfos);
            }
        }

        private static Task<List<PostInfo>> GetPosts(DateTime? date)
        {
            var query = Db.Query(@"
SELECT p.PostId, p.Text, p.CreationDate
FROM Post p");
            if (date.HasValue)
                query.Append(@"
WHERE p.CreationDate >= @date", new {date});

            return query.ToList<PostInfo>();
        }

        [Fact]
        public async Task ScalarType_Success()
        {
            var single = await Db.Query("SELECT @A1 AS A1",
                    new
                    {
                        A1 = "Test3"
                    })
                .Single<string>();
            
            Assert.Equal("Test3", single);
        }        
        
        [Fact]
        public async Task Enum_Success()
        {
            Enum1? a2 = Enum1.Item2;
            Enum1? a3 = null;
            Enum2? a5 = Enum2.Item2;
            Enum2? a6 = null;
            
            var record1 = await Db.Query(@"
SELECT 
    @A1 AS A1,
    @A2 AS A2,
    @A3 AS A3,
    @A4 AS A4,
    @A5 AS A5,
    @A6 AS A6
",
                    new
                    {
                        A1 = Enum1.Item2,
                        A2 = a2,
                        A3 = a3,
                        A4 = Enum2.Item2,
                        A5 = a5,
                        A6 = a6,
                    })
                .Single<Record1>();
            
            Assert.Equal(Enum1.Item2, record1.A1);
            Assert.Equal(a2, record1.A2);
            Assert.Equal(a3, record1.A3);
            Assert.Equal(Enum2.Item2, record1.A4);
            Assert.Equal(a5, record1.A5);
            Assert.Equal(a6, record1.A6);
        }
        
        [Fact]
        public async Task InsertUpdateDelete_Success()
        {
            int id;
            {
                var post = new Post {CreationDate = new DateTime(2014, 1, 1)};
                FillPost(post, new PostData {Text = "Test"});
                id = await Db.InsertWithInt32Identity(post);
                Assert.Equal("Test", await Db.Query("SELECT Text FROM Post WHERE PostId = @id", new {id}).Single<string>());
            }
            {
                var post = await Db.GetByKey<Post>(new {PostId = id});
                FillPost(post, new PostData {Text = "Test2"});
                await Db.Update(post);
                Assert.Equal("Test2", await Db.Query("SELECT Text FROM Post WHERE PostId = @id", new {id}).Single<string>());
            }
            {
                var rowCount = await Db.Delete<Post>(new {PostId = id});
                Assert.Equal(1, rowCount);
                Assert.Empty(await Db.Query("SELECT Text FROM Post WHERE PostId = @id", new {id}).ToList<string>());
            }
        }
        
        private static void FillPost(Post post, PostData postData)
        {
            post.Text = postData.Text;
        }
        
        [Fact]
        public async Task Subquery_Success()
        {
            var date = new DateTime(2015, 1, 1);

            var query = Db.Query();
            query.Append($@"
SELECT p.PostId, p.Text, p.CreationDate
FROM ({Post(query)}) p
WHERE p.CreationDate >= @date
ORDER BY p.PostId", new {date});

            var postInfos = await query.ToList<PostInfo>();
            
            Assert.Equal(2, postInfos.Count);
        }

        private static Query Post(Query query) => query.Query("SELECT * FROM Post");
    }
}