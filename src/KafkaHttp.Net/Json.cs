using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;

namespace KafkaHttp.Net
{
    public interface IJson
    {
        string Serialize(object value);
        object Deserialize(string value);
        T Deserialize<T>(string value);
    }

    public class Json : IJson
    {
        private readonly JsonSerializerSettings _settings = new JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            Converters = new List<JsonConverter>
            {
                new StringEnumConverter {CamelCaseText = true}
            }
        };

        public string Serialize(object value)
        {
            return JsonConvert.SerializeObject(value, _settings);
        }

        public object Deserialize(string value)
        {
            return JsonConvert.DeserializeObject(value);
        }

        public T Deserialize<T>(string value)
        {
            return JsonConvert.DeserializeObject<T>(value);
        }
    }
}
