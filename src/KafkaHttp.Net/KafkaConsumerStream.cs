using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Quobject.EngineIoClientDotNet.ComponentEmitter;
using Quobject.SocketIoClientDotNet.Client;

namespace KafkaHttp.Net
{
    public interface IKafkaConsumerStream : IDisposable
    {
        IKafkaConsumerStream OnMessage(Action<ReceivedMessage> action);
        IKafkaConsumerStream OnError(Action<Exception> action);
        IKafkaConsumerStream OnClose(Action action);
        IKafkaConsumerStream OnSubscribed(Action action);
        IKafkaConsumerStream Start();
        void Block();
        void Shutdown();
    }

    public class KafkaConsumerStream : IKafkaConsumerStream
    {
        private readonly string _group;
        private readonly string _topic;
        private readonly Socket _socket;
        private readonly IJson _json;
        private readonly EventWaitHandle _shutdownHandle;

        public KafkaConsumerStream(Socket socket, string group, string topic)
        {
            _group = group;
            _topic = topic;
            _socket = socket;
            _json = new Json();
            _shutdownHandle = new EventWaitHandle(false, EventResetMode.ManualReset);
        }

        public IKafkaConsumerStream Start()
        {
            if (_socket.Io().ReadyState == Manager.ReadyStateEnum.OPEN)
                Subscribe();
            else
                Trace.TraceInformation("Watiting for socket to enter open state...");

            _socket.On(Socket.EVENT_CONNECT, Subscribe);

            return this;
        }

        private void Subscribe()
        {
            // If this happens while the socket is receiving the connect event then a large
            // delay (~20s) is introduced due to a race condition. Instead, if we introduce
            // a slight delay to ensure it happens after the connect event has fully been
            // received we do not experience the large delay.
            Task.Delay(10)
                .ContinueWith(t =>
            {
                Trace.TraceInformation($"Connected. Subscribing to '{_topic}' as '{_group}'.");
                var args = _json.Serialize(new { group = _group, topic = _topic });
                _socket.Emit("subscribe", args);
            });
        }

        public IKafkaConsumerStream OnSubscribed(Action action)
        {
            _socket.On("subscribed", o =>
            {
                var topic = o as string;
                if (topic != _topic) return;

                Trace.TraceInformation($"Subscribed to {_topic}.");
                action();
            });
            return this;
        }

        public IKafkaConsumerStream OnMessage(Action<ReceivedMessage> action)
        {
            Trace.TraceInformation("Subscribing to 'message' event.");
            _socket.On("message", o =>
                {
                    var text = o.ToString();
                    var message = _json.Deserialize<ReceivedMessage>(text);
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

        public void Block()
        {
            _shutdownHandle.WaitOne();
        }

        public void Shutdown()
        {
            _socket.Close();
            _shutdownHandle.Set();
        }

        public void Dispose()
        {
            Shutdown();
            _shutdownHandle?.Dispose();
        }
    }
}
