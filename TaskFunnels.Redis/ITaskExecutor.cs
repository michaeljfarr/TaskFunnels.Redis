using System;
using System.Threading.Tasks;

namespace TaskFunnels.Redis
{
    public interface ITaskExecutor
    {
        TimeSpan MaxExecutionTime { get; }
        Task Execute(PipeInfo pipeInfo, RedisPipeValue<byte[]> value);
    }
}