namespace Akka.Persistence.EventStore.Tests
{
    using Configuration;
    using Configuration.Hocon;
    using System.Configuration;
    using TestKit.Journal;

    public partial class EventStoreJournalSpec : JournalSpec
    {
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            akka {
                stdout-loglevel = DEBUG
                loglevel = DEBUG
                loggers = [""Akka.Logger.NLog.NLogLogger,Akka.Logger.NLog""]

                persistence {

                publish-plugin-commands = off
                journal {
                    plugin = ""akka.persistence.journal.event-store""
                    event-store {
                        class = ""EventStore.Persistence.EventStoreJournal, Akka.Persistence.EventStore""
                        plugin-dispatcher = ""akka.actor.default-dispatcher""
                        
                        # the event store connection string
                        connection-string = ""ConnectTo=tcp://admin:changeit@127.0.0.1:1113;""

                        # name of the connection
                        connection-name = ""akka.net""
                    }
                }
            }
        }
        ");

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
                var config =  ConfigurationFactory.ParseString(
                       string.Format(@"akka.persistence.journal.event-store.connection-string=""{0}"" ", customEventStoreConnection)
                       ).WithFallback(SpecConfig);

                return config;
            }
        }

        public EventStoreJournalSpec()
            : base(CustomConfig(), "EventStoreJournalSpec")
        {
            Initialize();
        }

        

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            //cleanup
            StorageCleanup.Clean();
        }
    }
}
