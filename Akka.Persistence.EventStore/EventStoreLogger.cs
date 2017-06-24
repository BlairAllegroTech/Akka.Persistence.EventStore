using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Akka.Event;

namespace Akka.Persistence.EventStore.Logger
{
    public class EventStoreLogger : ILogger
    {
        public static ILogger Create(ILoggingAdapter logger)
        {
            return new EventStoreLogger(logger);
        }

        readonly ILoggingAdapter _logger;
        EventStoreLogger(ILoggingAdapter logger)
        {
            _logger = logger;
        }

        void ILogger.Debug(string format, params object[] args)
        {
            _logger.Debug(format, args);
        }

        void ILogger.Debug(Exception ex, string format, params object[] args)
        {
            _logger.Debug(format, args);
        }

        void ILogger.Error(string format, params object[] args)
        {
            _logger.Error(format, args);
        }

        void ILogger.Error(Exception ex, string format, params object[] args)
        {
            _logger.Error(ex, format, args);
        }

        void ILogger.Info(string format, params object[] args)
        {
            _logger.Info(format, args);
        }

        void ILogger.Info(Exception ex, string format, params object[] args)
        {
            _logger.Info(format, args);
        }
    }
}
