using Akka.Event;
using Akka.Persistence.Snapshot;
using Akka.Serialization;
using EventStore.ClientAPI;
using System;
using System.Threading.Tasks;

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
                IEventStoreConnection connection = EventStoreConnection.Create(
                    _extension.EventStoreSnapshotSettings.ConnectionString, 
                    _extension.EventStoreSnapshotSettings.ConnectionName);
                //connection.Settings.Log = Logger.EventStoreLogger.Create(_log);

                connection.ErrorOccurred += (object sender, ClientErrorEventArgs e) => 
                {
                    _log.Debug("Event Store Connected Error",  e.Exception.Message);
                };

                connection.Reconnecting += (object sender, ClientReconnectingEventArgs e) =>
                {
                    _log.Debug("Event Store Reconnecting");
                };
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

            try
            {
                if (SnapshotSelectionCriteria.None.Equals(criteria))
                {
                    return null;
                }

                var result = await FindSnapshotEvent(persistenceId, streamName, connection, criteria);
                return result?.snapshot;
            }
            catch (Exception ex)
            {
                _log.Error($"Failed to read Last Stream message from stream {streamName}", ex);
                throw;
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

            _log.Debug($"Saving Snap Shot {streamName}");
            await connection.AppendToStreamAsync(streamName, ExpectedVersion.Any, eventData);
            await Task.CompletedTask;
        }

        /// <summary>
        /// Delete a single snapshot identified by PersistenceId and Sequence Number
        /// NOTE : We can't do this with ES, do delete all snapshots prior
        /// </summary>
        /// <param name="metadata"></param>
        /// <returns>A task to be executed</returns>
        protected override async Task DeleteAsync(SnapshotMetadata metadata)
        {
            {
                var connection = await GetConnection();
                var persistenceId = metadata.PersistenceId;
                var streamName = GetStreamName(persistenceId, _extension.TenantIdentifier);

                try
                {
                    var currentPosition = (long)StreamPosition.End;
                    while (true)
                    {
                        var latestSnapshot = await connection.ReadEventAsync(streamName, currentPosition, false);

                        if (latestSnapshot.Status != EventReadStatus.Success)
                        {
                            _log.Debug("No snapshot found for: {0}", persistenceId);
                            break;
                        }
                        else
                        {
                            _log.Debug("Found snapshot of {0}", persistenceId);
                            var @event = latestSnapshot.Event.Value.Event;

                            var representation = (SnapshotRepresentation)_serializer.FromBinary(@event.Data, typeof(SnapshotRepresentation));
                            var result = ToSelectedSnapshot(representation);

                            if (result.Metadata.SequenceNr == metadata.SequenceNr)
                            {
                                // TODO : Delete
                                await TruncateStreamBeforeSequenceNumber(persistenceId, currentPosition);
                                break;
                            }

                            if (result.Metadata.SequenceNr < metadata.SequenceNr)
                            {
                                break;
                            }
                            else
                            {
                                // Read the next event
                                currentPosition = latestSnapshot.Event.Value.OriginalEventNumber - 1L;
                                if (currentPosition < 1)
                                    break;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _log.Error($"Failed to read Last Stream message from stream {streamName}", ex);
                    throw;
                }

            }
        }

        /// <summary>
        /// Deletes all snapshots in stream matching <paramref name="criteria"/>.
        /// </summary>
        /// <param name="persistenceId"></param>
        /// <param name="criteria"></param>
        /// <returns></returns>
        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            var connection = await GetConnection();
            var streamName = GetStreamName(persistenceId, _extension.TenantIdentifier);
            try
            {
                long streamOffset;
                var result = await FindSnapshotEvent(persistenceId, streamName, connection, criteria);
                if (result.HasValue)
                {
                    await TruncateStreamBeforeSequenceNumberAsync(persistenceId, result.Value.streamOffset);
                }
            }
            catch (Exception ex)
            {
                _log.Error($"DeleteAsync Failed stream:{streamName}", ex);
                throw;
            }
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

        struct SnapShotSearchSearch
        {
            public SelectedSnapshot snapshot;
            public long streamOffset;
        }
        private async Task<SnapShotSearchSearch?> FindSnapshotEvent(string persistenceId, string streamName, IEventStoreConnection connection, SnapshotSelectionCriteria criteria)
        {
            if (SnapshotSelectionCriteria.Latest.Equals(criteria))
            {
                // Always just read last snapshot, theres no point trying to read old snapshots..
                // Also here 'criteria.MaxSequenceNr' has no relationship to the position in the event stream, this was a BUG!
                var latestSnapshot = await connection.ReadEventAsync(streamName, StreamPosition.End, false);

                if (latestSnapshot.Status != EventReadStatus.Success)
                {
                    _log.Debug("No snapshot found for: {0}", persistenceId);
                    return null;
                }
                var @event = latestSnapshot.Event.Value.Event;
                var representation = (SnapshotRepresentation)_serializer.FromBinary(@event.Data, typeof(SnapshotRepresentation));

                return new SnapShotSearchSearch {
                    snapshot = ToSelectedSnapshot(representation),
                    streamOffset = latestSnapshot.Event.Value.OriginalEventNumber,
                };
            }
            else
            {

                var currentPosition = (long)StreamPosition.End;
                while (true)
                {
                    // Always just read last snapshot, theres no point trying to read old snapshots..
                    // Also here 'criteria.MaxSequenceNr' has no relationship to the position in the event stream, this was a BUG!
                    var latestSnapshot = await connection.ReadEventAsync(streamName, currentPosition, false);

                    if (latestSnapshot.Status != EventReadStatus.Success)
                    {
                        _log.Debug("No snapshot found for: {0}", persistenceId);
                        break;
                    }
                    else
                    {
                        _log.Debug("Found snapshot of {0}", persistenceId);
                        var @event = latestSnapshot.Event.Value.Event;

                        var representation = (SnapshotRepresentation)_serializer.FromBinary(@event.Data, typeof(SnapshotRepresentation));
                        var result = ToSelectedSnapshot(representation);

                        if (result.Metadata.SequenceNr < criteria.MinSequenceNr)
                            break;

                        if (criteria.MinTimestamp.HasValue)
                        {
                            if (result.Metadata.Timestamp < criteria.MinTimestamp.Value)
                            {
                                break;
                            }
                        }

                        if (result.Metadata.SequenceNr <= criteria.MaxSequenceNr && result.Metadata.Timestamp <= criteria.MaxTimeStamp)
                        {
                            return new SnapShotSearchSearch
                            {
                                snapshot = ToSelectedSnapshot(representation),
                                streamOffset = currentPosition,
                            };
                        }
                        else
                        {
                            // Read the next event
                            currentPosition = latestSnapshot.Event.Value.OriginalEventNumber - 1L;
                            if (currentPosition < 1L)
                                break;
                        }
                    }
                }

                return null;
            }
        }
        private async Task TruncateStreamBeforeSequenceNumber(string persistenceId, long maxSequenceNumber)
        {
            var streamName = GetStreamName(persistenceId, _extension.TenantIdentifier);

            await GetConnection().ContinueWith((Task<IEventStoreConnection> getConnection) =>
            {
                var connection = getConnection.Result;

                connection.GetStreamMetadataAsync(streamName)
                    .ContinueWith((Task<StreamMetadataResult> GetMetaDataTask) =>
                    {
                        var metaData = GetMetaDataTask.Result;
                        if (!metaData.StreamMetadata.TruncateBefore.HasValue || metaData.StreamMetadata.TruncateBefore < maxSequenceNumber)
                        {
                            var builder = metaData.StreamMetadata.Copy().SetTruncateBefore(maxSequenceNumber);

                            connection.SetStreamMetadataAsync(streamName, metaData.MetastreamVersion, builder)
                            .ContinueWith((Task<WriteResult> result) => 
                            {
                                
                            }, TaskContinuationOptions.None);
                        }
                        else
                        {

                        }
                    }, TaskContinuationOptions.None);

            }, TaskContinuationOptions.None);
        }

        private async Task<WriteResult> TruncateStreamBeforeSequenceNumberAsync(string persistenceId, long maxSequenceNumber)
        {
            try
            {
                var streamName = GetStreamName(persistenceId, _extension.TenantIdentifier);

                var connection = await GetConnection();
                var metaData = await connection.GetStreamMetadataAsync(streamName);
                if (!metaData.StreamMetadata.TruncateBefore.HasValue || metaData.StreamMetadata.TruncateBefore < maxSequenceNumber)
                {
                    var builder = metaData.StreamMetadata.Copy().SetTruncateBefore(maxSequenceNumber);

                    return await connection.SetStreamMetadataAsync(streamName, metaData.MetastreamVersion, builder);
                }

                return default(WriteResult);
            }
            catch(Exception ex)
            {
                _log.Error($"Failed To Delete Snapshot: {persistenceId}:{maxSequenceNumber}", ex);
                throw;
            }
        }

    }
}