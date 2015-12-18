using System;

namespace KafkaHttp.Net
{
    public interface ISocketClient
    {

    }

    public class SocketClient : ISocketClient
    {
        private WebSocketWrapper _socket;

        public SocketClient(string uri)
        {
            _socket = WebSocketWrapper.Create(uri).Connect();
        }

        public void Connect(Action callback)
        {
            _socket.OnConnect(w => callback());
        }

        public void Emit<T>(string @event, T data)
        {
            _socket.SendMessage(@event);
        }

        public void On<T>(string @event, Action<T> callback)
        {
            _socket.OnMessage((s, w) =>
            {
                Console.WriteLine($"Received message: {s}");
                callback(default(T));
            });
        }
    }
}
