using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.EventStore.Snapshot
{
    /// <summary>
    /// Class used for storing a Snapshot as BsonDocument
    /// </summary>
    public class SnapshotRepresentation
    {
        [JsonProperty("Id")]
        public string Id { get; set; }

        [JsonProperty("PersistenceId")]
        public string PersistenceId { get; set; }

        [JsonProperty("JornalVersion")]
        public long SequenceNr { get; set; }

        [JsonProperty("Timestamp")]
        public long Timestamp { get; set; }

        [JsonProperty("Snapshot")]
        public object Snapshot { get; set; }
    }
}
