using System.Threading.Tasks;

namespace KafkaHttp.Net
{
    //public interface IKafkaConsumer
    //{
    //    Task<IKafkaConsumerInstance> Join(KafkaConsumerOptions options = null);
    //}

    //public class KafkaConsumer : IKafkaConsumer
    //{
    //    private readonly IRestClient _client;
    //    private readonly string _groupName;

    //    internal KafkaConsumer(IRestClient client, string groupName)
    //    {
    //        _client = client;
    //        _groupName = groupName;
    //    }

    //    public async Task<IKafkaConsumerInstance> Join(KafkaConsumerOptions options = null)
    //    {
    //        if (options == null) options = new KafkaConsumerOptions();

    //        var response = await _client.Post<ConsumerInstance>(
    //            "consumers/" + _groupName,
    //            new ConsumerOptions
    //            {
    //                Format = options.Format,
    //                AutoOffsetReset = options.AutoOffsetReset,
    //                AutoCommitEnable = options.AutoCommitEnable
    //            });

    //        return new KafkaConsumerInstance(_client, response);
    //    }
    //}
}
