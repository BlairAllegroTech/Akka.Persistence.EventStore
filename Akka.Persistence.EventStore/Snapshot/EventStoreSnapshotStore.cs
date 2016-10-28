using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Event;
using Akka.Persistence;
using Akka.Persistence.Serialization;
using Akka.Persistence.Snapshot;
using Akka.Serialization;
using EventStore.ClientAPI;
using Newtonsoft.Json;
using Akka.Persistence.EventStore;

namespace Akka.Persistence.EventStore.Snapshot
{
    public class EventStoreSnapshotStore : SnapshotStore
    {
        private readonly Lazy<Task<IEventStoreConnection>> _connection;

        private readonly Serializer _serializer;
        private ILoggingAdapter _log;
        private readonly EventStorePersistenceExtension _extension;

        public EventStoreSnapshotStore()
        {
            _log = Context.GetLogger();
            _extension = EventStorePersistence.Instance.Apply(Context.System);
            var serialization = Context.System.Serialization;
            _serializer = serialization.FindSerializerForType(typeof(SnapshotRepresentation));

            _connection = new Lazy<Task<IEventStoreConnection>>(async () =>
            {
                IEventStoreConnection connection = EventStoreConnection.Create(_extension.EventStoreSnapshotSettings.ConnectionString, _extension.EventStoreSnapshotSettings.ConnectionName);
                await connection.ConnectAsync();
                return connection;
            });
        }

        private Task<IEventStoreConnection> GetConnection()
        {
            return _connection.Value;
        }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var connection = await GetConnection();
            var streamName = GetStreamName(persistenceId, _extension.TenantIdentifier);
            var requestedSnapVersion = (int)criteria.MaxSequenceNr;
            //StreamEventsSlice slice = null;
            //if (criteria.MaxSequenceNr == long.MaxValue)
            //{
            //    requestedSnapVersion = StreamPosition.End;
            //    slice = await connection.ReadStreamEventsBackwardAsync(streamName, requestedSnapVersion, 1, false);
            //}
            //else
            //{
            //    slice = await connection.ReadStreamEventsBackwardAsync(streamName, StreamPosition.End, requestedSnapVersion, false);
            //}

            // Always just read last snapshot, theres no point trying to read old snapshots..
            // Also here 'criteria.MaxSequenceNr' has no relationship to the position in the event stream, this was a BUG!
            var slice = await connection.ReadStreamEventsBackwardAsync(streamName, StreamPosition.End, 1, false);
            if (slice.Status == SliceReadStatus.StreamNotFound)
            {
                // Why is this required ? We should never write to the event store when we try to read something that does not exist!!
                //await connection.SetStreamMetadataAsync(streamName, ExpectedVersion.Any, StreamMetadata.Data);
                return null;
            }

            if (slice.Events.Any())
            {
                _log.Debug("Found snapshot of {0}", persistenceId);
                //if (requestedSnapVersion == StreamPosition.End)
                //{
                //    var @event = slice.Events.First().OriginalEvent;
                //    return (SelectedSnapshot)_serializer.FromBinary(@event.Data, typeof(SelectedSnapshot));
                //}
                //else
                //{
                //    var @event = slice.Events.Where(t => t.OriginalEvent.EventNumber == requestedSnapVersion).First().OriginalEvent;
                //    return (SelectedSnapshot)_serializer.FromBinary(@event.Data, typeof(SelectedSnapshot));
                //}

                var @event = slice.Events.First().OriginalEvent;
                var representation = (SnapshotRepresentation)_serializer.FromBinary(@event.Data, typeof(SnapshotRepresentation));
                return ToSelectedSnapshot(representation);
            }
            else
            {
                // No snapshot found
                _log.Debug("No snapshot found for: {0}", persistenceId);
                return null;
            }
        }

        private static string GetStreamName(string persistenceId, string tenantId)
        {
            // It would be good to make this method plugable.
            // Naming of streams is probably fairly important to most people
            var id = persistenceId.Replace("-", "");
            if (string.IsNullOrEmpty(tenantId))
            {
                return string.Format("snapshot-{0}", id);
            }
            else
            {
                return string.Format("{1}_snapshot-{0}", id, tenantId);
            }
        }

        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            var connection = await GetConnection();
            var streamName = GetStreamName(metadata.PersistenceId, _extension.TenantIdentifier);
            
            // Serializes the snapshot using Akka default serializer, currently JSON but will soon default to Wire.
            // Setting IsJson false means it will be ignored by EventStore projections
            var data = _serializer.ToBinary(ToSnapshotEntry(metadata, snapshot));
            var eventData = new EventData(Guid.NewGuid(), typeof(SnapshotRepresentation).Name, false, data, new byte[0]);

            await connection.AppendToStreamAsync(streamName, ExpectedVersion.Any, eventData);
        }

        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            // We do not support deleting
            return Task.Delay(0);
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            // We do not support deleting
            return Task.Delay(0);
        }

        private static SnapshotRepresentation ToSnapshotEntry(SnapshotMetadata metadata, object snapshot)
        {
            return new SnapshotRepresentation
            {
                Id = metadata.PersistenceId + "_" + metadata.SequenceNr,
                PersistenceId = metadata.PersistenceId,
                SequenceNr = metadata.SequenceNr,
                Snapshot = snapshot,
                Timestamp = metadata.Timestamp.ToUniversalTime().Ticks
            };
        }

        private static SelectedSnapshot ToSelectedSnapshot(SnapshotRepresentation entry)
        {
            return new SelectedSnapshot(
                new SnapshotMetadata(entry.PersistenceId, entry.SequenceNr, new DateTime(entry.Timestamp, DateTimeKind.Utc)), 
                entry.Snapshot
                );
        }


    }
}