using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Quobject.SocketIoClientDotNet.Client;

namespace KafkaHttp.Net
{
    public interface IKafkaClient : IDisposable
    {
        IKafkaConsumerStream Consumer(string groupName, string topicName);
    }

    public class KafkaClient : IKafkaClient
    {
        private Socket _socket;

        public KafkaClient(string uri)
        {
            _socket = IO.Socket(uri);
        }

        public IKafkaConsumerStream Consumer(string groupName, string topicName)
        {
            return new KafkaConsumerStream(_socket, groupName, topicName).Start();
        }

        public void Dispose()
        {
            if (_socket == null) return;

            _socket.Disconnect();
            _socket = null;
        }
    }
}
