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
            Trace.TraceInformation("Creating topic...");
            var waitHandle = new AutoResetEvent(false);

            _socket.On("topicCreated", o => waitHandle.Set());
            _socket.Emit("createTopic", name);

            return waitHandle.ToTask($"Failed to create topic {name}.", $"Created topic {name}.");
        }

        public void Publish(params Message<string>[] payload)
        {
            _socket.Emit("publish", _json.Serialize(payload));
        }
    }
}
