using CommandLine;
using KafkaHttp.Net.TestConsole.CommandLine;
using KafkaHttp.Net.TestConsole.Commands;

namespace KafkaHttp.Net.TestConsole
{
    class Program
    {
        static int Main(string[] args)
        {
            return Parser.Default
                .ParseArguments<ConsumerOptions, LatencyOptions>(args)
                .MapResult(
                    (ConsumerOptions opts) => -1,
                    (LatencyOptions opts) => new Latency().Start(opts),
                    errs => 1
                );
        }
    }
}
