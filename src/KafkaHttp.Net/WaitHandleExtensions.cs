
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaHttp.Net
{
    internal static class WaitHandleExtensions
    {
        internal static Task ToTask(this WaitHandle waitHandle, string timeoutMessage = null, string successMessage = null)
        {
            var tcs = new TaskCompletionSource<object>();
            ThreadPool.RegisterWaitForSingleObject(
                waitObject: waitHandle,
                callBack: (o, timeout) =>
                {
                    if (timeout)
                    {
                        Trace.TraceError(timeoutMessage);
                        tcs.SetResult(new TimeoutException($"{timeoutMessage} Timeout occured waiting for response."));
                        return;
                    }

                    Trace.TraceInformation(successMessage);
                    tcs.SetResult(null);
                },
                state: null,
                timeout: TimeSpan.FromSeconds(10),
                executeOnlyOnce: true);
            return tcs.Task;
        }
    }
}
