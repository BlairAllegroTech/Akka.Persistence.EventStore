namespace Akka.Persistence.EventStore.Journal
{
    using Akka.Actor;
    using Akka.Event;
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Threading;

    public partial class EventStoreJournal
    {
        bool ReadJournal_FeatureSwitch = false;

        #region Read Jornal
        [Serializable]
        public sealed class EventAppended : IDeadLetterSuppression
        {
            public readonly string PersistenceId;

            public EventAppended(string persistenceId)
            {
                PersistenceId = persistenceId;
            }
        }

        [Serializable]
        public sealed class PersistenceIdAdded : IDeadLetterSuppression
        {
            public readonly string PersistenceId;

            public PersistenceIdAdded(string persistenceId)
            {
                PersistenceId = persistenceId;
            }
        }

        [Serializable]
        public sealed class CurrentPersistenceIds : IDeadLetterSuppression
        {
            public readonly IEnumerable<string> AllPersistenceIds;

            public CurrentPersistenceIds(IEnumerable<string> allPersistenceIds)
            {
                AllPersistenceIds = allPersistenceIds.ToImmutableHashSet();
            }
        }

        private readonly Dictionary<string, ISet<IActorRef>> _persistenceIdSubscribers = new Dictionary<string, ISet<IActorRef>>();
        private readonly Dictionary<string, ISet<IActorRef>> _tagSubscribers = new Dictionary<string, ISet<IActorRef>>();
        private readonly HashSet<IActorRef> _allPersistenceIdSubscribers = new HashSet<IActorRef>();
        private readonly ReaderWriterLockSlim _allPersistenceIdsLock = new ReaderWriterLockSlim();
        private HashSet<string> _allPersistenceIds = new HashSet<string>();

        protected bool HasPersistenceIdSubscribers => _persistenceIdSubscribers.Count != 0;
        protected bool HasTagSubscribers => _tagSubscribers.Count != 0;
        protected bool HasAllPersistenceIdSubscribers => _allPersistenceIdSubscribers.Count != 0;

        private bool TryAddPersistenceId(string persistenceId)
        {
            try
            {
                _allPersistenceIdsLock.EnterUpgradeableReadLock();

                if (_allPersistenceIds.Contains(persistenceId)) return false;
                else
                {
                    try
                    {
                        _allPersistenceIdsLock.EnterWriteLock();
                        _allPersistenceIds.Add(persistenceId);
                        return true;
                    }
                    finally
                    {
                        _allPersistenceIdsLock.ExitWriteLock();
                    }
                }
            }
            finally
            {
                _allPersistenceIdsLock.ExitUpgradeableReadLock();
            }
        }

        /// <summary>
        /// Configure custom messages that can be handeled by this provider..
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        protected override bool ReceivePluginInternal(object message)
        {
            //return message.Match()
            //    .With<ReplayTaggedMessages>(replay =>
            //    {
            //        ReplayTaggedMessagesAsync(replay)
            //        .PipeTo(replay.ReplyTo, success: h => new RecoverySuccess(h), failure: e => new ReplayMessagesFailure(e));
            //    })
            //    .With<SubscribePersistenceId>(subscribe =>
            //    {
            //        AddPersistenceIdSubscriber(Sender, subscribe.PersistenceId);
            //        Context.Watch(Sender);
            //    })
            //    .With<SubscribeAllPersistenceIds>(subscribe =>
            //    {
            //        AddAllPersistenceIdSubscriber(Sender);
            //        Context.Watch(Sender);
            //    })
            //    .With<SubscribeTag>(subscribe =>
            //    {
            //        AddTagSubscriber(Sender, subscribe.Tag);
            //        Context.Watch(Sender);
            //    })
            //    .With<Terminated>(terminated => RemoveSubscriber(terminated.ActorRef))
            //    .With<Query>(query => HandleEventQuery(query))
            //    .WasHandled;


            return base.ReceivePluginInternal(message);

        }

        private void NotifyNewPersistenceIdAdded(string persistenceId)
        {
            if (ReadJournal_FeatureSwitch)
            {
                var isNew = TryAddPersistenceId(persistenceId);
                if (isNew && HasAllPersistenceIdSubscribers /*&& !IsTagId(persistenceId)*/)
                {
                    var added = new PersistenceIdAdded(persistenceId);
                    foreach (var subscriber in _allPersistenceIdSubscribers)
                        subscriber.Tell(added);
                }
            }
        }

        private void NotifyPersistenceIdChange(string persistenceId)
        {
            if (ReadJournal_FeatureSwitch)
            {
                ISet<IActorRef> subscribers;
                if (_persistenceIdSubscribers.TryGetValue(persistenceId, out subscribers))
                {
                    var changed = new EventAppended(persistenceId);
                    foreach (var subscriber in subscribers)
                        subscriber.Tell(changed);
                }
            }
        }

        public void AddPersistenceIdSubscriber(IActorRef subscriber, string persistenceId)
        {
            if (ReadJournal_FeatureSwitch)
            {
                ISet<IActorRef> subscriptions;
                if (!_persistenceIdSubscribers.TryGetValue(persistenceId, out subscriptions))
                {
                    subscriptions = new HashSet<IActorRef>();
                    _persistenceIdSubscribers.Add(persistenceId, subscriptions);
                }

                subscriptions.Add(subscriber); 
            }
        }

        public void AddAllPersistenceIdSubscriber(IActorRef subscriber)
        {
            if (ReadJournal_FeatureSwitch)
            {
                _allPersistenceIdSubscribers.Add(subscriber);
                //subscriber.Tell(new CurrentPersistenceIds(AllPersistenceIds)); 
            }
        }

        private void NotifyTagChange(string tag)
        {

        }

        private void OnAfterJournalEventsPersisted(IEnumerable<string> persistenceIds = null, IList<string> allTags = null)
        {

            if (ReadJournal_FeatureSwitch)
            {
                if (HasPersistenceIdSubscribers && persistenceIds != null)
                {
                    foreach (var persistenceId in persistenceIds)
                    {
                        NotifyPersistenceIdChange(persistenceId);
                    }
                }

                if (HasTagSubscribers && allTags != null && allTags.Count != 0)
                {
                    foreach (var tag in allTags)
                    {
                        NotifyTagChange(tag);
                    }
                } 
            }
        }
        #endregion
    }
}
