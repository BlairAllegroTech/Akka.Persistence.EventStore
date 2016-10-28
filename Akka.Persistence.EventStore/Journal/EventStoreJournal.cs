using Akka.Actor;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.EventStore;
using Akka.Persistence.Journal;
using EventStore.ClientAPI;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.EventStore.Journal
{
    public partial class EventStoreJournal : AsyncWriteJournal
    {
        private int _batchSize = 500;
        private readonly Lazy<Task<IEventStoreConnection>> _connection;
        private readonly JsonSerializerSettings _serializerSettings;
        private ILoggingAdapter _log;
        private readonly EventStorePersistenceExtension _extension;

        public EventStoreJournal()
        {
            _log = Context.GetLogger();
            _extension = EventStorePersistence.Instance.Apply(Context.System);

            _serializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects,
                TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple,
                Formatting = Formatting.Indented,
                Converters =
                {
                    new ActorRefConverter(Context)
                }
            };

            _connection = new Lazy<Task<IEventStoreConnection>>(async () =>
            {
                try
                {
                    IEventStoreConnection connection = EventStoreConnection.Create(
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

                // read last entry from event journal
                var slice = await connection.ReadStreamEventsBackwardAsync(
                    GetStreamName(persistenceId, _extension.TenantIdentifier), 
                    StreamPosition.End, 
                    1, 
                    false);

                long sequence = 0;

                if (slice.Events.Any())
                    sequence = slice.Events.First().OriginalEventNumber + 1;

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
                if (toSequenceNr < fromSequenceNr || max == 0) return;
                if (fromSequenceNr == toSequenceNr) max = 1;
                if (toSequenceNr > fromSequenceNr && max == toSequenceNr)
                    max = toSequenceNr - fromSequenceNr + 1;

                var connection = await GetConnection();
                long count = 0;
                int start = ((int) fromSequenceNr-1);
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

                    foreach (var @event in slice.Events)
                    {
                        var json = Encoding.UTF8.GetString(@event.OriginalEvent.Data);
                        var representation = JsonConvert.DeserializeObject<JournalRepresentation>(json, _serializerSettings);
                        recoveryCallback(ToPersistenceRepresentation(representation, context.Sender));
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
                    var jornalEntries = ((IImmutableList<IPersistentRepresentation>)message.Payload)
                        .Select(ToJournalEntry)
                        .GroupBy(x => x.PersistenceId)
                        .ToList();


                    // TODO : if there are more than one gropuing then we should use a transaction to write
                    // to ensure messages writen atomically
                    var transactionRequired = jornalEntries.Count() > 1;

                    if (transactionRequired)
                    {
                        // this is not supported by eventstore, transactions are per stream
                        // I think we should never get here..
                        _log.Warning("Multiple aggregates persisted in single write, transaction required!");
                    }

                    

                    foreach (var grouping in jornalEntries)
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

                            // Converts using JSON Serializer
                            var json = JsonConvert.SerializeObject(x, _serializerSettings);
                            var data = Encoding.UTF8.GetBytes(json);

                            var meta = new byte[0];
                            var payload = x.Payload;
                            var payloadName = payload.GetType().Name;

                            if (payload.GetType().GetProperty("Metadata") != null)
                            {
                                var propType = payload.GetType().GetProperty("Metadata").PropertyType;
                                var metaJson = JsonConvert.SerializeObject(
                                    payload.GetType().GetProperty("Metadata").GetValue(x.Payload), 
                                    propType, 
                                    _serializerSettings);

                                meta = Encoding.UTF8.GetBytes(metaJson);
                            }

                            if (streamVersion == ExpectedVersion.NoStream)
                                NotifyNewPersistenceIdAdded(grouping.Key);
                            else
                                NotifyPersistenceIdChange(grouping.Key);

                            return new EventData(eventId, payloadName, true, data, meta);
                        }).ToList();

                        var connection = await GetConnection();
                        await connection.AppendToStreamAsync(streamName, streamVersion, events);
                    }

                    OnAfterJournalEventsPersisted(persistenceIds: jornalEntries.Select(e => e.Key));
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
                    tasks => tasks.Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null).ToImmutableList()
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
            return Task.Delay(0);
        }

        private static JournalRepresentation ToJournalEntry(IPersistentRepresentation message)
        {
            var jornalEntry = new JournalRepresentation
            {
                Id = message.PersistenceId + "_" + message.SequenceNr,
                IsDeleted = message.IsDeleted,
                Payload = message.Payload,
                PersistenceId = message.PersistenceId,
                SequenceNr = message.SequenceNr,
                Manifest = message.Manifest,
                Tags = null
            };

            if (message.Payload is Tagged)
            {
                var tagged = (Tagged)message.Payload;

                message.WithPayload(tagged.Payload);

                jornalEntry.Payload = tagged.Payload;
                if (tagged.Tags.Count != 0)
                {
                    jornalEntry.Tags = tagged.Tags.ToArray();
                }
            }

            return jornalEntry;
        }

        private Persistent ToPersistenceRepresentation(JournalRepresentation entry, IActorRef sender)
        {
            return new Persistent(
                entry.Payload, 
                entry.SequenceNr, 
                entry.PersistenceId, 
                entry.Manifest, 
                entry.IsDeleted, 
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
            var id = persistenceId.Replace("-", "");
            if (string.IsNullOrEmpty(tenantId))
            {
                return string.Format("journal-{0}", id);
            }
            else
            {
                return string.Format("{1}_journal-{0}", id, tenantId);
            }
        }

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


        
    }
}