using System;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace TaskFunnels.Redis
{
    class RedisTaskFunnel : IRedisTaskFunnel
    {
        private readonly ConnectionMultiplexer _redis;
        private readonly ILogger<RedisTaskFunnel> _logger;

        public RedisTaskFunnel(ConnectionMultiplexer redis, ILogger<RedisTaskFunnel> logger)
        {
            _redis = redis;
            _logger = logger;
        }
        private static string CreateParentChildSetPath(string parentPipeName)
        {
            if (parentPipeName?.Contains(RedisTaskMultiplexorConstants.PathSeparator) == true)
            {
                throw new ApplicationException($"Pipenames must not contain '{RedisTaskMultiplexorConstants.PathSeparator}' but name was '{parentPipeName}'");
            }
            var childPipePath = $"{RedisTaskMultiplexorConstants.RedisTaskMultiplexorInfoPrefix}{parentPipeName}";
            return childPipePath;
        }

        public (bool sent, bool clients) TrySendMessage<T>(string parentPipeName, string childPipeName, T body, int maxListLength = int.MaxValue, TimeSpan? expiry = null)
        {
            if (body == null)
            {
                throw new ArgumentNullException(nameof(body));
            }
            //will throw ArgumentException is body is not a supported type
            var redisValue = RedisValue.Unbox(body);

            var db = _redis.GetDatabase();
            var parentInfoPath = CreateParentChildSetPath(parentPipeName);
            var childPipePath = PipeInfo.Create(parentPipeName, childPipeName);
            var trans = db.CreateTransaction();
            {
                if (maxListLength < int.MaxValue)
                {
                    trans.AddCondition(Condition.ListLengthLessThan(childPipePath.PipePath, maxListLength));
                }

                //ensure the name of the new pipe exists for the pipe monitor (before checking list length)
                db.SetAdd(RedisTaskMultiplexorConstants.PipeNameSetKey, parentPipeName);
                //add the child to the parents hash set (and update the expiry time on it)
                db.HashSet(parentInfoPath, childPipeName, ExpiryToTimeString(expiry ?? TimeSpan.FromDays(7)));

                //add the message to the list
                db.ListRightPush(childPipePath.PipePath, redisValue);
            }
            var executed = trans.Execute();
            if (!executed)
            {
                return (false, false);
            }

            var sub = _redis.GetSubscriber();
            var listeners = sub.Publish(childPipePath.BroadcastPath, $"{{\"type\":\"new\",\"parent\":\"{parentPipeName}\",\"parent\":\"{childPipeName}\"}}");
            return (true, listeners > 0);
        }

        public bool LockExtend<T>(RedisPipeValue<T> value, TimeSpan lockExpiry)
        {
            var db = _redis.GetDatabase();
            return db.LockExtend(value.PipeInfo.LockPath, value.LockValue, lockExpiry);
        }
        public bool LockRelease<T>(RedisPipeValue<T> value, bool success)
        {
            var lockRetained = LockExtend(value, TimeSpan.FromMinutes(30));
            if (!lockRetained)
            {
                throw new ApplicationException($"Could not retain lock for {value.PipeInfo.LockPath} whilst trying to release.");
            }
            //since we have the lock, we should safely be able to pop the item from the queue.
            var db = _redis.GetDatabase();
            var popped = false;
            
            if (success && value.Peaked)
            {
                var poppedValue = db.ListLeftPop(value.PipeInfo.PipePath);
                popped = poppedValue.HasValue;
                if (popped)
                {
                    var same = poppedValue.CompareTo(value.RedisValue) == 0;
                    if (!same)
                    {
                        db.ListLeftPush(value.PipeInfo.PipePath, poppedValue);
                        _logger.LogError($"Queue {value.PipeInfo.PipePath} was different when releasing the lock so we put it back!");
                    }
                }
            }
            var unlocked = db.LockRelease(value.PipeInfo.LockPath, value.LockValue);
            if (success && !popped)
            {
                _logger.LogError($"Queue {value.PipeInfo.LockPath} was entirely empty when releasing the lock!");
            }

            if (!unlocked)
            {
                _logger.LogError($"Failed to releaselock {value.PipeInfo.PipePath} at end of LockRelease!");
            }

            return unlocked;
        }

        static bool ArraysEqual(byte[] a1, byte[] a2)
        {
            if (a1.Length == a2.Length)
            {
                for (int i = 0; i < a1.Length; i++)
                {
                    if (a1[i] != a2[i])
                    {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        public RedisPipeValue<T> TryReadMessage<T>(bool peak, string parentPipeName, string childPipeName, TimeSpan lockExpiry)
        {
            var db = _redis.GetDatabase();
            var childPipePath = PipeInfo.Create(parentPipeName, childPipeName);
            var lockValue = $"{Environment.MachineName}:{Guid.NewGuid()}";
            var lockInfo = db.LockTake(childPipePath.LockPath, lockValue, lockExpiry);

            if (!lockInfo)
            {
                return null;
            }

            if (peak)
            {
                var message = db.ListGetByIndex(childPipePath.PipePath, 0);
                if (!message.HasValue)
                {
                    db.LockRelease(childPipePath.LockPath, lockValue);
                    lockValue = null;
                }
                return new RedisPipeValue<T>(childPipePath, message, lockValue, true);
            }
            else
            {
                var message = db.ListLeftPop(childPipePath.PipePath);
                if (!message.HasValue)
                {
                    db.LockRelease(childPipePath.LockPath, lockValue);
                    lockValue = null;
                }
                return new RedisPipeValue<T>(childPipePath, message, lockValue, false);
            }
        }

        private static string ExpiryToTimeString(TimeSpan expiry)
        {
            return DateTime.UtcNow.Add(expiry).ToString("s");
        }
    }
}