namespace Akka.Persistence.EventStore.Tests
{
    using Configuration;
    using System.Configuration;
    using TCK.Snapshot;


    public partial class EventStoreSnapshotSpec : SnapshotStoreSpec
    {
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            akka {
                stdout-loglevel = DEBUG
                loglevel = DEBUG
                loggers = [""Akka.Logger.NLog.NLogLogger,Akka.Logger.NLog""]

                persistence {

                publish-plugin-commands = off
                snapshot-store {
                    plugin = ""akka.persistence.snapshot-store.event-store""
                    event-store {
                        class = ""Akka.Persistence.EventStore.Snapshot.EventStoreSnapshotStore, Akka.Persistence.EventStore""
                        plugin-dispatcher = ""akka.actor.default-dispatcher""
                        
                        # the event store connection string
                        connection-string = ""ConnectTo=tcp://admin:changeit@127.0.0.1:1113;""

                        # name of the connection
                        connection-name = ""akka.net-snapshot""
                    }
                }
            }
        }
        ");

        public EventStoreSnapshotSpec()
            : base(CustomConfig(), "EventStoreSnapshotSpec")
        {
            Initialize();
        }

        static Config CustomConfig()
        {
            var customEventStoreConnection = ConfigurationManager.AppSettings["es.connection-string"];
            if (string.IsNullOrEmpty(customEventStoreConnection))
            {
                return SpecConfig;
            }
            else
            {
                // Override connection string
                var config = ConfigurationFactory.ParseString(
                       string.Format(@"akka.persistence.snapshot-store.event-store.connection-string=""{0}"" ", customEventStoreConnection)
                       ).WithFallback(SpecConfig);

                return config;
            }
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            //cleanup
            StorageCleanup.Clean();
        }
    }

     
}
