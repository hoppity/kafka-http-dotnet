﻿using System;
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
        IKafkaConsumerStream Open(Action action = null);
        IKafkaConsumerStream Subscribed(Action action);
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

        public IKafkaConsumerStream Open(Action action = null)
        {
            Console.WriteLine("Openning conection...");
            _socket.On(Socket.EVENT_CONNECT, () =>
            {
                Task.Run(() =>
                {
                    action?.Invoke();

                    Console.WriteLine($"Connected. Subscribing to '{_topic}' as '{_group}'.");
                    var args = _json.Serialize(new { group = _group, topic = _topic });
                    _socket.Emit("subscribe", args);
                });
            });

            return this;
        }

        public IKafkaConsumerStream Subscribed(Action action)
        {
            _socket.On("subscribed", () =>
            {
                Console.WriteLine($"Subscribed to {_topic}.");
                action();
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

        public Task CreateTopic(string name)
        {
            Console.WriteLine("Creating topic...");
            var waitHandle = new AutoResetEvent(false);

            _socket.On("topicCreated", o => waitHandle.Set());
            _socket.Emit("createTopic", name);

            var tcs = new TaskCompletionSource<object>();
            ThreadPool.RegisterWaitForSingleObject(
                waitObject: waitHandle,
                callBack: (o, timeout) =>
                {
                    Console.WriteLine(timeout ? "Failed to create topic." : "Topic created successfully.");
                    tcs.SetResult(new TimeoutException("Timed out waiting for response."));
                },
                state: null,
                timeout: TimeSpan.FromSeconds(10),
                executeOnlyOnce: true);
            return tcs.Task;

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
