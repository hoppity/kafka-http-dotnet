using System;
using System.Threading;
using System.Threading.Tasks;
using Quobject.SocketIoClientDotNet.Client;

namespace KafkaHttp.Net
{
    public interface IKafkaConsumerStream : IDisposable
    {
        IKafkaConsumerStream Message(Action<Message<string>> action);
        IKafkaConsumerStream Error(Action<Exception> action);
        IKafkaConsumerStream Close(Action action);
        IKafkaConsumerStream Open();
        void Block();
        void Shutdown();
    }

    public class KafkaConsumerStream : IKafkaConsumerStream
    {
        private readonly string _group;
        private readonly string _topic;
        private readonly Socket _socket;
        private readonly IJson _json;
        private readonly EventWaitHandle _waitHandle;

        public KafkaConsumerStream(string baseUri, string group, string topic)
        {
            _group = @group;
            _topic = topic;
            _json = new Json();
            _socket = IO.Socket(baseUri);
            _waitHandle = new EventWaitHandle(false, EventResetMode.ManualReset);
        }

        public IKafkaConsumerStream Open()
        {
            Console.WriteLine("Openning conection...");
            _socket.On(Socket.EVENT_CONNECT, () =>
            {
                Task.Run(() =>
                {
                    Console.WriteLine($"Connected. Subscribing to '{_topic}' as '{_group}'.");
                    var args = _json.Serialize(new { group = _group, topic = _topic });
                    _socket.Emit("subscribe", args);
                });
            });

            return this;
        }

        public IKafkaConsumerStream Message(Action<Message<string>> action)
        {
            Console.WriteLine("Subscribing to 'message' event.");
            _socket.On("message", o =>
            {
                var text = o.ToString();
                var message = _json.Deserialize<Message<string>>(text);
                action(message);
            });
            return this;
        }

        public IKafkaConsumerStream Error(Action<Exception> action)
        {
            Console.WriteLine("Subscribing to error events.");
            Action<object> raise = o =>
            {
                Console.WriteLine("Received error event.");
                action(new Exception(o.ToString()));
            };
            _socket.On(Socket.EVENT_CONNECT_ERROR, raise);
            _socket.On(Socket.EVENT_ERROR, raise);
            _socket.On(Socket.EVENT_RECONNECT_ERROR, raise);
            return this;
        }

        public IKafkaConsumerStream Close(Action action)
        {
            Console.WriteLine("Subscribing to 'disconnect' event.");
            _socket.On(Socket.EVENT_DISCONNECT, action);
            return this;
        }

        public void Block()
        {
            _waitHandle.WaitOne();
        }

        public void Shutdown()
        {
            _socket.Close();
            _waitHandle.Set();
        }

        public void Dispose()
        {
            Shutdown();
            _waitHandle?.Dispose();
        }
    }
}
