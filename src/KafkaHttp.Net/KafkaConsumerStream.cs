using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Quobject.SocketIoClientDotNet.Client;

namespace KafkaHttp.Net
{
    public interface IKafkaConsumerStream : IDisposable
    {
        IKafkaConsumerStream OnMessage(Action<Message<string>> action);
        IKafkaConsumerStream OnError(Action<Exception> action);
        IKafkaConsumerStream OnClose(Action action);
        IKafkaConsumerStream OnOpen(Action action = null);
        IKafkaConsumerStream OnSubscribed(Action action);
        Task CreateTopic(string name);
        void Publish(params Message<string>[] payload);
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

        public IKafkaConsumerStream OnOpen(Action action = null)
        {
            Trace.TraceInformation("Openning conection...");
            _socket.On(Socket.EVENT_CONNECT, () =>
            {
                Task.Run(() =>
                {
                    action?.Invoke();

                    Trace.TraceInformation($"Connected. Subscribing to '{_topic}' as '{_group}'.");
                    var args = _json.Serialize(new { group = _group, topic = _topic });
                    _socket.Emit("subscribe", args);
                });
            });

            return this;
        }

        public IKafkaConsumerStream OnSubscribed(Action action)
        {
            _socket.On("subscribed", () =>
            {
                Trace.TraceInformation($"Subscribed to {_topic}.");
                action();
            });
            return this;
        }

        public IKafkaConsumerStream OnMessage(Action<Message<string>> action)
        {
            Trace.TraceInformation("Subscribing to 'message' event.");
            _socket.On("message", o =>
            {
                var text = o.ToString();
                var message = _json.Deserialize<Message<string>>(text);
                action(message);
            });
            return this;
        }

        public IKafkaConsumerStream OnError(Action<Exception> action)
        {
            Trace.TraceInformation("Subscribing to error events.");
            Action<object> raise = o =>
            {
                Trace.TraceWarning("Received error event.");
                action(new Exception(o.ToString()));
            };
            _socket.On(Socket.EVENT_CONNECT_ERROR, raise);
            _socket.On(Socket.EVENT_ERROR, raise);
            _socket.On(Socket.EVENT_RECONNECT_ERROR, raise);
            return this;
        }

        public IKafkaConsumerStream OnClose(Action action)
        {
            Trace.TraceInformation("Subscribing to 'disconnect' event.");
            _socket.On(Socket.EVENT_DISCONNECT, action);
            return this;
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
