using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace TaskFunnels.Redis
{
    public static class ServiceCollectionExtensions
    {
        public static void ConfigureRedis(this IServiceCollection services, IConfiguration configuration)
        {
            var redisOptions = ConfigurationOptions.Parse(configuration.RequireConfig("RedisConnection"));
            services.AddSingleton(redisOptions);
        }

        public static void AddRedisFactory(this IServiceCollection services)
        {
            services.AddSingleton(sp=>ConnectionMultiplexer.Connect(sp.GetRequiredService<ConfigurationOptions>()));
        }

        public static void AddRedisMonitor(this IServiceCollection services)
        {
            services.AddSingleton<RedisMonitor>();
        }

        public static void AddRedisTaskFunnel(this IServiceCollection services)
        {
            services.AddSingleton<IRedisTaskFunnel, RedisTaskFunnel>();
        }

        private static string RequireConfig(this IConfiguration configuration, string key)
        {
            var val = configuration[key];
            if (string.IsNullOrWhiteSpace(val))
            {
                throw new ApplicationException($"Required config with key {key} had no value or wasn't present.");
            }

            return val;
        }
    }
}