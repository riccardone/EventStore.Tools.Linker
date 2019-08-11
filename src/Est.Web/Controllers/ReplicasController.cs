using System;
using System.Collections.Generic;
using System.Linq;
using Est.CrossClusterReplication.Config.LiteDb;
using Est.CrossClusterReplication.Contracts;
using Est.CrossClusterReplication.Model;
using Microsoft.AspNetCore.Mvc;

namespace Est.Web.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ReplicasController : ControllerBase
    {
        private readonly IConfigReader _reader;
        private readonly IConfigWriter _writer;

        public ReplicasController()
        {
            // TODO configure DI
            _reader = new ConfigRepository();
            _writer = new ConfigRepository();
        }

        // GET: api/Replicas
        [HttpGet]
        public IEnumerable<CrossClusterReplica> Get()
        {
            return _reader.Get();
        }

        // GET: api/Replicas/5
        [HttpGet("{name}", Name = "Get")]
        public CrossClusterReplica Get(string name)
        {
            return _reader.Get(name);
        }

        // POST: api/Replicas
        [HttpPost]
        public void Post([FromBody] CrossClusterReplica value)
        {
            var alreadyExist = _reader.Get().Any(a => a.Name.Equals(value.Name));
            if (alreadyExist == false)
            {
                _writer.Set(value);
            }
            else
            {
                throw new Exception("Replica already exist");
            }
        }

        // PUT: api/Replicas/5
        [HttpPut("{name}")]
        public void Put(string name, [FromBody] CrossClusterReplica value)
        {
            var alreadyExist = _reader.Get().Any(a => a.Name.Equals(value.Name));
            if (alreadyExist == true)
            {
                _writer.Set(value);
            }
            else
            {
                throw new Exception("Replica does not exist");
            }
        }

        // DELETE: api/ApiWithActions/5
        [HttpDelete("{name}")]
        public void Delete(string name)
        {
            throw new NotImplementedException();
        }
    }
}
