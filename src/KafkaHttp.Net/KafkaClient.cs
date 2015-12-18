using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KafkaHttp.Net
{
    public interface IKafkaClient : IDisposable
    {
        //IKafkaConsumer Consumer(string groupName);
        //IKafkaTopic Topic(string name);
        //Task<IReadOnlyList<string>> Topics();
    }

    public class KafkaClient : IKafkaClient
    {
        //private readonly IRestClient _restClient;

        //internal KafkaClient(IRestClient restClient)
        //{
        //    _restClient = restClient;
        //}

        //public KafkaClient(KafkaClientOptions clientOptions)
        //{
        //    _restClient = new RestClient(clientOptions);
        //}

        //public KafkaClient(string url) : this(new KafkaClientOptions { Uri = new Uri(url) })
        //{
        //}

        //public IKafkaConsumer Consumer(string groupName)
        //{
        //    return new KafkaConsumer(_restClient, groupName);
        //}

        //public IKafkaTopic Topic(string name)
        //{
        //    return new KafkaTopic(_restClient, name);
        //}

        //public async Task<IReadOnlyList<string>> Topics()
        //{
        //    var topics = await _restClient.Get<List<string>>("topics");
        //    return topics.AsReadOnly();
        //}

        public void Dispose()
        {
            //    if (_restClient != null) _restClient.Dispose();
        }
    }
}
