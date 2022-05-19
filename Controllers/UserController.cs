using Elasticsearch.Net;
using ElasticSearchDemo.Models;
using Microsoft.AspNetCore.Mvc;
using Nest;

namespace ElasticSearchDemo.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController : ControllerBase
    {

        private readonly IElasticClient elasticClient;

        public UserController(IElasticClient elasticClient)
        {
            this.elasticClient = elasticClient;
        }


        // POST: UserController/add
        [HttpPost("add")]
        public async Task<string> AddUser(User user)
        {
            var response = await elasticClient.IndexAsync<User>(user, x => x.Index("users"));
            return response.Id;
        }

        // GET: UserController/get
        [HttpGet("get")]
        public async Task<List<User>> GetUser()
        {
            var response = await elasticClient.SearchAsync<User>(s => s
            .Index("users")
            .Query(q => q.MatchAll()));

            return response.Documents.ToList();
        }

        // GET: UserController/get/name
        [HttpGet("get/{name}")]
        public async Task<User?> GetUserByID(string name)
        {
            var response = await elasticClient.SearchAsync<User>(s => s
            .Index("users")
            .Query(q => q.Match(m => m.Field(f => f.Name).Query(name))));

            return response.Documents.FirstOrDefault();
        }

        [HttpPut("update")]
        public async Task<long> UpdateUser(User user)
        {
            // x => x.Index("User").Query(q => q.Match(m => m.Field(f => f.Email).Query(user.Email)))
            // var response = await elasticClient.UpdateByQuery<User>(x => x.Index("users").Query(q => q.Match(m => m.Field(f => f.Email).Query(user.Email))));
            var response = await elasticClient.UpdateByQueryAsync<User>(u => u
                                .Index("users")
                                .Query(q => q.Match(m => m.Field(f => f.Email).Query(user.Email)))
                                .Script("ctx._source.name = '" + user.Name + "'; ctx._source.age = '" + user.Age + "'")
                                .Conflicts(Conflicts.Proceed)
                                .Refresh(true));

            return response.Updated;
        }


        // DELETE: UserController/delete/name
        [HttpDelete("delete/{name}")]
        public async Task<string> Delete(string name)
        {
            var response = await elasticClient.DeleteByQueryAsync<User>(x => x
            .Index("users").
            Query(q => q.Match(m => m.Field(f => f.Name).Query(name))));

            return response.Deleted == 1 ? "User deleted successfully" : "User not found by that name";
        }
    }
}
