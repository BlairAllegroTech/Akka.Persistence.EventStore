using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization.Formatters;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Journal;
using EventStore.ClientAPI;
using Newtonsoft.Json;
using Akka.Persistence.EventStore;
using System.Collections.Immutable;

namespace EventStore.Persistence
{
    public class EventStoreJournal : AsyncWriteJournal
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
                    IEventStoreConnection connection = EventStoreConnection.Create(_extension.EventStoreJournalSettings.ConnectionString, _extension.EventStoreJournalSettings.ConnectionName);
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

                var slice = await connection.ReadStreamEventsBackwardAsync(GetStreamName(persistenceId), StreamPosition.End, 1, false);

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
                var streamName = GetStreamName(persistenceId);
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
                        var representation = JsonConvert.DeserializeObject<IPersistentRepresentation>(json, _serializerSettings);
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

        //protected override async Task WriteMessagesAsync(IEnumerable<IPersistentRepresentation> messages)
        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<Akka.Persistence.AtomicWrite> messages)
        {
            var writeTasks = messages.Select(async message =>
            {
                var persistentMessages = ((IImmutableList<IPersistentRepresentation>)message.Payload).ToArray();

                try
                {
                    foreach (var grouping in persistentMessages.GroupBy(x => x.PersistenceId))
                    {
                        var streamName = GetStreamName(grouping.Key);

                        var representations = grouping.OrderBy(x => x.SequenceNr).ToArray();
                        var expectedVersion = (int)representations.First().SequenceNr - 2;

                        var streamVersion = expectedVersion < 0 ? ExpectedVersion.NoStream : expectedVersion;

                        var events = representations.Select(x =>
                        {
                            var eventId = GuidUtility.Create(GuidUtility.IsoOidNamespace, string.Concat(streamName, x.SequenceNr));
                            var json = JsonConvert.SerializeObject(x, _serializerSettings);
                            var data = Encoding.UTF8.GetBytes(json);
                            var meta = new byte[0];
                            var payload = x.Payload;
                            if (payload.GetType().GetProperty("Metadata") != null)
                            {
                                var propType = payload.GetType().GetProperty("Metadata").PropertyType;
                                var metaJson = JsonConvert.SerializeObject(payload.GetType().GetProperty("Metadata").GetValue(x.Payload), propType, _serializerSettings);
                                meta = Encoding.UTF8.GetBytes(metaJson);
                            }
                            return new EventData(eventId, x.GetType().FullName, true, data, meta);
                        }).ToList();


                        var connection = await GetConnection();
                        await connection.AppendToStreamAsync(streamName, streamVersion, events);
                        
                    }
                }
                catch (Exception e)
                {
                    _log.Error(e, "Error writing messages to store");
                    throw;
                }
            });

            var result = await Task<IImmutableList<Exception>>
                .Factory
                .ContinueWhenAll(writeTasks.ToArray(),
                    tasks => tasks.Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null).ToImmutableList());

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
            //return Task.FromResult<object>(null);
            return Task.Delay(0);
        }

        private static Persistent ToPersistenceRepresentation(IPersistentRepresentation entry, IActorRef sender)
        {
            return new Persistent(
                entry.Payload,
                entry.SequenceNr,
                entry.PersistenceId,
                entry.Manifest,
                entry.IsDeleted,
                sender);
        }

        private static string GetStreamName(string persistenceId)
        {
            return string.Format("{0}-journal", persistenceId);
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