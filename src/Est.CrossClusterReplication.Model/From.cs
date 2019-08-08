using System;
using System.Collections.Generic;
using System.Text;

namespace Est.CrossClusterReplication.Model
{
    public class From
    {
        public Uri ConnectionString { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
    }
}
