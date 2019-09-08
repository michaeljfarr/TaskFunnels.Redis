using System;
using StackExchange.Redis;

namespace TaskFunnels.Redis
{
    public class RedisPipeValue<T> 
    {
        private readonly RedisValue _redisValue;
        private readonly Lazy<T> _convertedValue;

        public RedisPipeValue(PipeInfo pipeInfo, RedisValue redisValue, string lockValue, bool peaked)
        {
            _redisValue = redisValue;
            _convertedValue = new Lazy<T>(() =>
            {
                var value = _redisValue.Box();
                var typedValue = (T)value;
                return typedValue;
            });
            PipeInfo = pipeInfo;
            LockValue = lockValue;
            Peaked = peaked;
        }
        internal RedisValue RedisValue => _redisValue;
        public T Value => _convertedValue.Value;
        public PipeInfo PipeInfo { get; }
        public string LockValue { get; private set; }
        public bool Peaked { get; }
    }
}