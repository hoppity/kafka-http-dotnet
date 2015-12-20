using System;
using CommandLine;

namespace KafkaHttp.Net.TestConsole.CommandLine
{
    [Verb("latency")]
    public class LatencyOptions
    {
        [Option('a', "apiUrl", Required = true, HelpText = "Url for Kafka HTTP proxy.")]
        public Uri ApiUrl { get; set; }
        [Option('b', "batchsize", Required = false, HelpText = "The number of messages to send in a batch.", Default = 1)]
        public int BatchSize { get; set; }
        [Option('m', "messages", Required = true, HelpText = "The total number of messages to send.")]
        public int Messages { get; set; }
    }
}