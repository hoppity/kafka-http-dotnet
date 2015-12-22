using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaHttp.Net
{
    internal static class TaskExtensions
    {
        internal static Task TimeoutAfter(this Task task, int milliseconds, string timeoutMessage)
        {
            var tokenSource = new CancellationTokenSource();
            var delay = Task.Delay(10000, tokenSource.Token);
            return Task.Run(async () =>
            {
                var completed = await Task.WhenAny(task, delay);
                if (completed != task) throw new TimeoutException(timeoutMessage);

                tokenSource.Cancel();
            });
        }
    }
}