namespace Akka.Persistence.EventStore.Tests
{
    using Configuration;
    using System;
    using System.Configuration;
    using System.Net.Http;
    using TCK.Journal;

    public partial class EventStoreJournalSpec : JournalSpec
    {
        readonly HttpClient client;
        private static readonly Config SpecConfig = ConfigurationFactory.ParseString(@"
            akka {
                stdout-loglevel = DEBUG
                loglevel = DEBUG
                loggers = [""Akka.Logger.NLog.NLogLogger,Akka.Logger.NLog""]

                persistence {
                    #tenant-identifier = ""Test""
                    #publish-plugin-commands = on
                    journal {
                        plugin = ""akka.persistence.journal.event-store""
                        event-store {
                            class = ""Akka.Persistence.EventStore.Journal.EventStoreJournal, Akka.Persistence.EventStore""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                        
                            # the event store connection string
                            #connection-string = ""ConnectTo=tcp://admin:changeit@127.0.0.1:1113;""

                            # name of the connection
                            connection-name = ""akka.net-journal""

                            json-type-converters = []
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
                var config = SpecConfig
                    .WithFallback( ConfigurationFactory.ParseString(
                       string.Format(@"akka.persistence.journal.event-store.connection-string=""{0}"" ", customEventStoreConnection)
                       ))
                    .WithFallback(ConfigurationFactory.ParseString(
                       string.Format(@"akka.persistence.tenant-identifier=""{0}"" ", Guid.NewGuid())
                       ));

                return config;
            }
        }

        protected override bool SupportsRejectingNonSerializableObjects { get { return false; } }

        public EventStoreJournalSpec()
            : base(CustomConfig(), "EventStoreJournalSpec")
        {
            var uriBuilder = new UriBuilder(ConfigurationManager.AppSettings["es.client"]);
            client = new HttpClient();
            client.BaseAddress = uriBuilder.Uri;
            client.DefaultRequestHeaders.Add("Accept", "application/json");

            Initialize();
        }

        protected override void PreparePersistenceId(string pid)
        {
            //var relativeUrl = $"/streams/Test_journal-{pid}";
            //var success = client.GetAsync(relativeUrl).Result;
            //if (success.StatusCode == System.Net.HttpStatusCode.OK)
            //{
            //    // var result = client.DeleteAsync(relativeUrl).Result;
            //}
            base.PreparePersistenceId(pid);
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            //cleanup
            StorageCleanup.Clean();
        }
    }
}
