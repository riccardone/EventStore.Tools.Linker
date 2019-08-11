using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Est.CrossClusterReplication.Contracts;
using Est.CrossClusterReplication.Model;
using LiteDB;

namespace Est.CrossClusterReplication.Config.LiteDb
{
    public class ConfigRepository : IConfigReader, IConfigWriter
    {
        private const string DbName = "CrossClusterReplication.db";
        public IList<CrossClusterReplica> Get()
        {
            // Re-use mapper from global instance
            var mapper = BsonMapper.Global;

            mapper.Entity<CrossClusterReplica>()
                .DbRef(x => x.Filters, "filters");

            using (var db = new LiteDatabase(DbName))
            {
                var replicas = db.GetCollection<CrossClusterReplica>("root");

                // When query Order, includes references
                var query = replicas
                    .Include(x => x.Filters).FindAll();

                var crossClusterReplicas = query as CrossClusterReplica[] ?? query.ToArray();
                return crossClusterReplicas.ToList();
            }
        }

        public CrossClusterReplica Get(string id)
        {
            // Re-use mapper from global instance
            var mapper = BsonMapper.Global;

            mapper.Entity<CrossClusterReplica>()
                .DbRef(x => x.Filters, "filters");

            using (var db = new LiteDatabase(DbName))
            {
                var replicas = db.GetCollection<CrossClusterReplica>("crossclusterreplicas");
              
                var query = replicas
                    .Include(x => x.Filters).FindOne(a => a.Name.Equals(id));
               
                return query;
            }
        }

        public void Set(CrossClusterReplica replica)
        {
            using (var db = new LiteRepository(DbName))
            {
                db.Insert(replica);
            }
        }
    }
}
