using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaHttp.Net
{
    public interface IKafkaConsumerInstance : IDisposable
    {
        string Id { get; }
        string BaseUri { get; }
        IKafkaConsumerStream Subscribe(string topicName, int? maxBytes = null);
        Task Commit();
        void Shutdown();
    }

    public class KafkaConsumerInstance : IKafkaConsumerInstance
    {
        //private readonly IRestClient _client;
        //private readonly ConsumerInstance _instance;
        private readonly List<IKafkaConsumerStream> _streams = new List<IKafkaConsumerStream>();
        private bool _active = true;
        //private readonly AsyncLock _lock = new AsyncLock();

        internal KafkaConsumerInstance()//IRestClient client, ConsumerInstance instance)
        {
            //_client = client;
            //_instance = instance;
        }

        public string Id
        {
            get { return null; } //_instance.InstanceId; }
        }

        public string BaseUri
        {
            get { return null; } //_instance.BaseUri; }
        }

        public IKafkaConsumerStream Subscribe(string topicName, int? maxBytes = null)
        {
            //var stream = new KafkaConsumerStream(_uri, topicName, maxBytes);
            //_streams.Add(stream);
            //return stream;
            return null;
        }

        public Task Commit()
        {
            return null;
            //return _client.Post<CommitOffsetsResponse[]>(string.Format("{0}/offsets", BaseUri));
        }

        public void Shutdown()
        {
            //using (await _lock.LockAsync())
            {
                if (!_active) return;
                _active = false;

                ShutdownStreams();
                ShutdownInstance();
            }
        }

        public void Dispose()
        {
            Shutdown();
            foreach (var stream in _streams)
            {
                stream.Dispose();
            }
            _streams.Clear();
        }

        private void ShutdownStreams()
        {
            _streams.ForEach(s => s.Shutdown());
        }

        private void ShutdownInstance()
        {
            //await _client.Delete(BaseUri);
        }
    }
}
