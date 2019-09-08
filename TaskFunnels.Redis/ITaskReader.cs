using System.Collections.Generic;

namespace TaskFunnels.Redis
{
    public interface ITaskReader
    {
        /// <summary>
        /// Sets up the executors by connecting a subscriber for each message queue.  The redis implementation
        /// will analyze the existing 'pipes' and begin processing any existing messages.
        /// </summary>
        void Initialize(Dictionary<string, ITaskExecutor> taskExecutors, int maxThreads);
    }
}