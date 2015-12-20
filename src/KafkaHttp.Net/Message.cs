namespace KafkaHttp.Net
{
    public class Message : Message<byte[]>
    {
    }

    public class Message<T>
    {
        public T Value { get; set; }
        public string Key { get; set; }
        public int? Partition { get; set; }
        public string Topic { get; set; }
    }
}
