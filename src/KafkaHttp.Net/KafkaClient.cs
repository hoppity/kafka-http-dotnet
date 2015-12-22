using System;
using Quobject.SocketIoClientDotNet.Client;

namespace KafkaHttp.Net
{
    public interface IKafkaClient : IDisposable
    {
        IKafkaConsumerStream Consumer(string groupName, string topicName);
        IKafkaProducer Producer();
    }

    public class KafkaClient : IKafkaClient
    {
        private Socket _socket;

        public KafkaClient(string uri)
        {
            _socket = IO.Socket(uri);
        }

        public void OnOpen(Action callback)
        {
            _socket.On(Socket.EVENT_CONNECT, callback);
        }

        public IKafkaConsumerStream Consumer(string groupName, string topicName)
        {
            return new KafkaConsumerStream(_socket, groupName, topicName).Start();
        }

        public IKafkaProducer Producer()
        {
            return new KafkaProducer(_socket);
        }

        public void Dispose()
        {
            if (_socket == null) return;

            _socket.Disconnect();
            _socket = null;
        }
    }
}
