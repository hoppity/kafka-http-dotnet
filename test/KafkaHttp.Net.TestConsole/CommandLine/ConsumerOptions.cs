using System;
using CommandLine;

namespace KafkaHttp.Net.TestConsole.CommandLine
{
    [Verb("consumer")]
    public class ConsumerOptions
    {
        [Option('a', "apiUrl", Required = true, HelpText = "Url for Kafka REST proxy.")]
        public Uri ApiUrl { get; set; }

        [Option('t', "topic", Required = true, HelpText = "Topic name to subscribe to.")]
        public string TopicName { get; set; }

        [Option('g', "consumerGroup", Required = false, HelpText = "Name of the consumer group that this consumer is a part of.", Default = "")]
        public string ConsumerGroup { get; set; }

        [Option('b', "beginning", Required = false, HelpText = "Start consuming messages from the beginning of the topic.", Default = false)]
        public bool FromBeginning { get; set; }
    }
}
