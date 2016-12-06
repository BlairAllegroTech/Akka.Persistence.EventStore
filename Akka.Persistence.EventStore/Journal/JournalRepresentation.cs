namespace Akka.Persistence.EventStore.Journal
{
    using Newtonsoft.Json;
    /// <summary>
    /// Class used for storing intermediate result of the <see cref="IPersistentRepresentation"/>
    /// as BsonDocument into the MongoDB-Collection
    /// </summary>
    public class JournalRepresentation
    {

        //[JsonProperty("Id")]
        //public string Id { get; set; }

        [JsonProperty("PersistenceId")]
        public string PersistenceId { get; set; }

        [JsonProperty("SequenceNr"), JsonIgnore]
        public long SequenceNr { get; set; }

        //[JsonProperty("IsDeleted")]
        //public bool IsDeleted { get; set; }

        [JsonProperty("Payload")]
        public object Payload { get; set; }

        [JsonProperty("Manifest")]
        public string Manifest { get; set; }

        [JsonProperty("Tags", Required = Required.Default)]
        public string[] Tags { get; set; }
    }
}
