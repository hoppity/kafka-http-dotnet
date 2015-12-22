using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Quobject.SocketIoClientDotNet.Client;

namespace KafkaHttp.Net
{
    public interface IKafkaProducer
    {
        Task CreateTopic(string name);
        void Publish(params Message<string>[] payload);
    }

    public class KafkaProducer : IKafkaProducer
    {
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
                        tcs.SetException(new Exception(e.ToString()));
                        return;
                    }

                    Trace.TraceInformation($"Topic {name} created.");
                    tcs.SetResult(true);
                }
                , name);

            return tcs.Task;
        }

        public void Publish(params Message<string>[] payload)
        {
            _socket.Emit("publish", _json.Serialize(payload));
        }
    }
}
