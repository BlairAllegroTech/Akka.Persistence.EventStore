using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Journal;
using EventStore.ClientAPI;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.EventStore.Journal
{
    public partial class EventStoreJournal : AsyncWriteJournal
    {
        private const string MetaDataPropertyName = "Metadata";
        private const string MetaPayloadType = "payloadtype";
        private const string JournalRepresentatioType = "messagetype";
        private int _batchSize = 500;
        private readonly Lazy<Task<IEventStoreConnection>> _connection;
        private readonly JsonSerializerSettings journalSerializerSettings;
        private readonly JsonSerializerSettings metaDataSerializerSettings;
        private ILoggingAdapter _log;
        private readonly EventStorePersistenceExtension _extension;

        public EventStoreJournal()
        {
            _log = Context.GetLogger();
            _extension = EventStorePersistence.Instance.Apply(Context.System);

            var journalConverters = new List<JsonConverter>();
            foreach(var converterString in _extension.EventStoreJournalSettings.TypeConverterClasses)
            {
                var converterType = Type.GetType(converterString, false);
                if (converterType == null)
                {

                }
                else
                {
                    // Only try to create type converters
                    if (typeof(JsonConverter).IsAssignableFrom(converterType))
                    {
                        var converter = Activator.CreateInstance(converterType) as JsonConverter;
                        if (converter != null)
                        {
                            journalConverters.Add(converter);
                        }
                    }
                }
            }

            journalConverters.Add(new ActorRefConverter(Context));

            journalSerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None, // TypeNameHandling.Objects,
                TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
                Formatting = Formatting.Indented,
                Converters = journalConverters,
                ContractResolver = JournalDataDataContractResolver.Instance,
                NullValueHandling = NullValueHandling.Ignore
            };

            metaDataSerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None,
                TypeNameAssemblyFormatHandling =  TypeNameAssemblyFormatHandling.Simple,
                Formatting = Formatting.Indented,
                Converters =
                {
                    new ActorRefConverter(Context)
                },
                //ContractResolver = MetaDataDataContractResolver.Instance,
                NullValueHandling = NullValueHandling.Ignore
            };

            _connection = new Lazy<Task<IEventStoreConnection>>(async () =>
            {
                try
                {
                    //var builder = ConnectionSettings.Create().Build();

                    var connection = EventStoreConnection.Create(
                        _extension.EventStoreJournalSettings.ConnectionString, 
                        _extension.EventStoreJournalSettings.ConnectionName
                        );
                    await connection.ConnectAsync();
                    return connection;
                }
                catch(Exception exc)
                {
                    _log.Error(exc.ToString());
                    return null;
                }
            });
        }

        private Task<IEventStoreConnection> GetConnection()
        {
            return _connection.Value;
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            try
            {
                var connection = await GetConnection();

                long sequence = 0L;
                var streamName = GetStreamName(persistenceId, _extension.TenantIdentifier);

                var lastEvent = await connection.ReadEventAsync(streamName, StreamPosition.End, false);

                if (lastEvent?.Event.HasValue ?? false)
                {
                    sequence = lastEvent.Event.Value.OriginalEventNumber + 1L;
                }

                return sequence;
            }
            catch (Exception e)
            {
                _log.Error(e, e.Message);
                throw;
            }
        }

        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            try
            {
                // see : https://geteventstore.com/blog/20130220/getting-started-part-2-implementing-the-commondomain-repository-interface/index.html
                if (toSequenceNr < fromSequenceNr || max == 0) return;
                if (fromSequenceNr == toSequenceNr) max = 1;
                if (toSequenceNr > fromSequenceNr && max == toSequenceNr)
                    max = toSequenceNr - fromSequenceNr + 1;

                var connection = await GetConnection();
                long count = 0;
                //var start = fromSequenceNr-1L;
                var start = fromSequenceNr;
                var localBatchSize = _batchSize;
                var streamName = GetStreamName(persistenceId, _extension.TenantIdentifier);
                StreamEventsSlice slice;
                do
                {
                    if (max == long.MaxValue && toSequenceNr > fromSequenceNr)
                    {
                        max = toSequenceNr - fromSequenceNr + 1;
                    }
                    if (max < localBatchSize)
                    {
                        localBatchSize = (int)max;
                    }
                    slice = await connection.ReadStreamEventsForwardAsync(streamName, start, localBatchSize, false);

                    //if (slice.Status == SliceReadStatus.StreamNotFound)
                    //    throw new AggregateNotFoundException(id, typeof(TAggregate));

                    //if (slice.Status == SliceReadStatus.StreamDeleted)
                    //    throw new AggregateDeletedException(id, typeof(TAggregate));

                    foreach (var @event in slice.Events)
                    {
                        IDictionary<string, object> metaDict = null;
                        if (@event.OriginalEvent.Metadata?.Length > 0)
                        {
                            var meta = Encoding.UTF8.GetString(@event.OriginalEvent.Metadata);
                            metaDict = JsonConvert.DeserializeObject<Dictionary<string, object>>(meta, metaDataSerializerSettings);
                        }
                        else
                        {
                            metaDict = new Dictionary<string, object>();
                        }

                        var json = Encoding.UTF8.GetString(@event.OriginalEvent.Data);
                        journalSerializerSettings.ContractResolver = new PayloadContractResolver( Type.GetType((string)metaDict["payloadtype"]));
                        var representation = JsonConvert.DeserializeObject<JournalRepresentation>(json, journalSerializerSettings);
                        representation.SequenceNr = @event.OriginalEvent.EventNumber + 1;

                        recoveryCallback(ToPersistenceRepresentation(representation, context.Sender, metaDict));
                        count++;
                        if (count == max) return;
                    }
                
                    start = slice.NextEventNumber;

                } while (!slice.IsEndOfStream);
            }
            catch (Exception e)
            {
                _log.Error(e, "Error replaying messages for: {0}", persistenceId);
                throw;
            }
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<Akka.Persistence.AtomicWrite> messages)
        {
            var writeTasks = messages.Select(async message =>
            {
                try
                {
                    // Journal entries grouped by aggregate
                    var journalEntries = ((IImmutableList<IPersistentRepresentation>)message.Payload)
                        .Select(ToJournalEntry)
                        .GroupBy(x => x.PersistenceId)
                        .ToList();


                    // TODO : if there are more than one gropuing then we should use a transaction to write
                    // to ensure messages writen atomically
                    var transactionRequired = journalEntries.Count() > 1;

                    if (transactionRequired)
                    {
                        // this is not supported by eventstore, transactions are per stream
                        // I think we should never get here..
                        _log.Warning("Multiple aggregates persisted in single write, transaction required!");
                    }

                    

                    foreach (var grouping in journalEntries)
                    {
                        var streamName = GetStreamName(grouping.Key, _extension.TenantIdentifier);

                        var representations = grouping.OrderBy(x => x.SequenceNr).ToArray();

                        // Eventstore sequences start at 0 Akka starts a 1.
                        // Expected version is one less than the version of the first item, minus one more to account for the 
                        // Zero based ndexing
                        var eventStoreVersion = (int)(representations.First().SequenceNr - 1L);

                        // If Version is 0 then then we are creating a new stream!!
                        var streamVersion = eventStoreVersion < 1 ? 
                            ExpectedVersion.NoStream : // this is a new stream
                            (eventStoreVersion - 1);   // Our event should have a version one larger than the current stream version

                        var events = representations.Select(x =>
                        {
                            var eventId = GuidUtility.Create(
                                GuidUtility.IsoOidNamespace, 
                                string.Concat(streamName, x.SequenceNr)
                                );

                            

                            var payload = x.Payload;
                            var payloadName = payload.GetType().Name;

                            IDictionary<string, object> metaData = null;
                            var property = payload.GetType().GetProperty(MetaDataPropertyName, BindingFlags.Instance | BindingFlags.NonPublic);
                            if (property != null)
                            {
                                metaData = property.GetValue(x.Payload) as IDictionary<string, object>;
                            }

                            if (metaData == null)
                                metaData = new Dictionary<string, object>();

                            // Store message and payload data types in metta data
                            metaData[MetaPayloadType] = string.Format($"{payload.GetType().FullName}, {payload.GetType().Assembly.GetName().Name}");
                            metaData[JournalRepresentatioType] = string.Format($"{typeof(JournalRepresentation).FullName}, {typeof(JournalRepresentation).Assembly.GetName().Name}");

                            byte[] meta = null, data = null;
                            try
                            {
                                var metaJson = JsonConvert.SerializeObject(
                                    metaData,
                                    metaData.GetType(),
                                    metaDataSerializerSettings);

                                // Converts message body using JSON Serializer
                                var json = JsonConvert.SerializeObject(x, journalSerializerSettings);

                                meta = Encoding.UTF8.GetBytes(metaJson);
                                data = Encoding.UTF8.GetBytes(json);
                            }
                            catch(Exception ex)
                            {
                                _log.Error(ex, "Failed to serialize Jornal Message");
                                throw;
                            }

                            if (streamVersion == ExpectedVersion.NoStream)
                                NotifyNewPersistenceIdAdded(grouping.Key);
                            else
                                NotifyPersistenceIdChange(grouping.Key);

                            return new EventData(eventId, payloadName, true, data, meta);
                        }).ToList();

                        var connection = await GetConnection();

                        // See : Write paging
                        // https://geteventstore.com/blog/20130220/getting-started-part-2-implementing-the-commondomain-repository-interface/index.html
                        await connection.AppendToStreamAsync(streamName, streamVersion, events);
                    }

                    OnAfterJournalEventsPersisted(persistenceIds: journalEntries.Select(e => e.Key));
                }
                catch (Exception e)
                {
                    _log.Error(e, "Error writing messages to store");
                    throw;
                }
            });

            var result = await Task<IImmutableList<Exception>>
                .Factory
                .ContinueWhenAll(
                    writeTasks.ToArray(), 
                    tasks => tasks?.Select(t => 
                    {
                        return t.IsFaulted ? TryUnwrapException(t.Exception) : null;
                    }).ToImmutableList()
                );

            return result;
        }

        /// <summary>
        /// Delete is not supported in Event Store
        /// </summary>
        /// <param name="persistenceId"></param>
        /// <param name="toSequenceNr"></param>
        /// <param name="isPermanent"></param>
        /// <returns></returns>
        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            var streamName = GetStreamName(persistenceId, _extension.TenantIdentifier);
            var connection = GetConnection().Result;
            return connection.DeleteStreamAsync(streamName, Convert.ToInt32(toSequenceNr));

            
           return Task.Delay(0);
        }

        private static JournalRepresentation ToJournalEntry(IPersistentRepresentation message)
        {
            var jornalEntry = new JournalRepresentation
            {
                //Id = message.PersistenceId + "_" + message.SequenceNr,
                //IsDeleted = message.IsDeleted,
                Payload = message.Payload,
                PersistenceId = message.PersistenceId,
                SequenceNr = message.SequenceNr,
                Manifest = message.Manifest,
                Tags = null
            };

            if (message.Payload is Tagged)
            {
                var tagged = (Tagged)message.Payload;

                //message.WithPayload(tagged.Payload);

                jornalEntry.Payload = tagged.Payload;
                if (tagged.Tags.Count != 0)
                {
                    jornalEntry.Tags = tagged.Tags.ToArray();
                }
            }

            return jornalEntry;
        }

        private Persistent ToPersistenceRepresentation(JournalRepresentation entry, IActorRef sender, IDictionary<string,object> metaData)
        {
            var property = entry.Payload.GetType().GetProperty(MetaDataPropertyName, BindingFlags.Instance | BindingFlags.NonPublic);
            if (property != null)
            {
                var meta = property.GetValue(entry.Payload) as IDictionary<string, object>;
                if (meta != null && metaData != null)
                {
                    // Copy meta data
                    foreach (var item in metaData)
                    {
                        meta[item.Key] = item.Value;
                    }
                }
            }

            return new Persistent(
                entry.Payload, 
                entry.SequenceNr, 
                entry.PersistenceId, 
                entry.Manifest, 
                false, //entry.IsDeleted, 
                sender);
        }

        /// <summary>
        /// In event store dash seperates categories
        /// http://docs.geteventstore.com/introduction/3.9.0/projections/
        /// </summary>
        /// <param name="persistenceId"></param>
        /// <param name="tenantId"></param>
        /// <returns></returns>
        private static string GetStreamName(string persistenceId, string tenantId)
        {
            // It would be good to make this method plugable.
            // Naming of streams is probably fairly important to most people


            // var id = persistenceId.Replace("-", "");
            // Persistence ids should use dashes spareingly, 
            // e.g. Event store uses them as a sperator, to catagorise stream names into related groups
            if (string.IsNullOrEmpty(tenantId))
            {
                return string.Format("journal-{0}", persistenceId);
            }
            else
            {
                return string.Format("{1}_journal-{0}", persistenceId, tenantId);
            }
        }


        #region JSon Serialization
        class ActorRefConverter : JsonConverter
        {
            private readonly IActorContext _context;

            public ActorRefConverter(IActorContext context)
            {
                _context = context;
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                writer.WriteValue(((IActorRef)value).Path.ToStringWithAddress());
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var value = reader?.Value?.ToString();

                if (string.IsNullOrEmpty(value))
                {
                    return null;
                }
                else
                {
                    ActorSelection selection = _context.ActorSelection(value);
                    return selection.Anchor;
                }
            }

            public override bool CanConvert(Type objectType)
            {
                return typeof (IActorRef).IsAssignableFrom(objectType);
            }
        }


        public class JournalDataDataContractResolver : DefaultContractResolver
        {
            public static readonly IContractResolver Instance = new JournalDataDataContractResolver();
            protected override IList<JsonProperty> CreateProperties(Type type, MemberSerialization memberSerialization)
            {
                return base.CreateProperties(type, memberSerialization)
                    .Where(p => p.PropertyName!=MetaDataPropertyName)
                    .ToList();
            }
        }

        public class PayloadContractResolver : JournalDataDataContractResolver
        {
            readonly Type PayloadType;

            public PayloadContractResolver(Type payloadType)
            {
                PayloadType = payloadType;
            }


            protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
            {
                var property = base.CreateProperty(member, memberSerialization);

                if (property.PropertyName.Equals("Payload", StringComparison.OrdinalIgnoreCase))
                {
                    property.PropertyType = PayloadType;
                }

                return property;
            }
        }


        [Obsolete]
        public class MetaDataDataContractResolver : DefaultContractResolver
        {
            public static readonly IContractResolver Instance = new MetaDataDataContractResolver();

            protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
            {
                var property = base.CreateProperty(member, memberSerialization);

                if (property.PropertyName.Equals("CorrelationId", StringComparison.OrdinalIgnoreCase))
                {
                    property.PropertyName = "$correlationid";
                }

                else if (property.PropertyName.Equals("CausationId", StringComparison.OrdinalIgnoreCase))
                {
                    property.PropertyName = "$causationid";
                }

                return property;
            }
        }
        #endregion Json Serialization
    }
}