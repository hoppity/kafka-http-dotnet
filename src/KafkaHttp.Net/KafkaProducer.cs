using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Web;
using Quobject.SocketIoClientDotNet.Client;

namespace KafkaHttp.Net
{
    public interface IKafkaProducer
    {
        Task CreateTopic(string name);
        Task Publish(params Message<string>[] payload);
    }

    public class KafkaProducer : IKafkaProducer
    {
        public const int CreateTopicTimeout = 10000;
        public const int PublishTimeout = 10000;

        private readonly Socket _socket;
        private readonly Json _json;

        public KafkaProducer(Socket socket)
        {
            _socket = socket;
            _json = new Json();
        }

        public Task CreateTopic(string name)
        {
            Trace.TraceInformation($"Creating topic {name}...");

            var tcs = new TaskCompletionSource<object>();
            _socket.Emit(
                "createTopic",
                (e, d) =>
                {
                    if (e != null)
                    {
                        Trace.TraceError(e.ToString());
                        tcs.SetException(new Exception($"An error occurred creating topic {name}: {e}"));
                        return;
                    }

                    Trace.TraceInformation($"Topic {name} created.");
                    tcs.SetResult(true);
                }
                , name);


            return tcs.Task.TimeoutAfter(CreateTopicTimeout, $"Timeout occured while creating topic {name}.");
        }

        public Task Publish(params Message<string>[] payload)
        {
            var tcs = new TaskCompletionSource<object>();
            _socket.Emit(
                "publish",
                (e, d) =>
                {
                    if (e != null)
                    {
                        Trace.TraceError(e.ToString());
                        tcs.SetException(new Exception(e.ToString()));
                        return;
                    }

                    tcs.SetResult(true);
                },
                _json.Serialize(payload));

            return tcs.Task.TimeoutAfter(PublishTimeout, "Timeout occured while publishing payload.");
        }
    }
}
