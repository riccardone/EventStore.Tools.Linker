using System;
using System.Collections.Generic;
using Est.CrossClusterReplication.Model;
using Microsoft.AspNetCore.Mvc;

namespace Est.Web.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ReplicasController : ControllerBase
    {
        // GET: api/Replicas
        [HttpGet]
        public IEnumerable<CrossClusterReplica> Get()
        {
            return new List<CrossClusterReplica>();
        }

        // GET: api/Replicas/5
        [HttpGet("{name}", Name = "Get")]
        public CrossClusterReplica Get(string name)
        {
            throw new NotImplementedException();
        }

        // POST: api/Replicas
        [HttpPost]
        public void Post([FromBody] CrossClusterReplica value)
        {
        }

        // PUT: api/Replicas/5
        [HttpPut("{name}")]
        public void Put(string name, [FromBody] CrossClusterReplica value)
        {
            throw new NotImplementedException();
        }

        // DELETE: api/ApiWithActions/5
        [HttpDelete("{name}")]
        public void Delete(string name)
        {
        }
    }
}
