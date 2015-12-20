using System;
using System.Threading;
using System.Threading.Tasks;
using KafkaHttp.Net.TestConsole.CommandLine;
using Metrics;

namespace KafkaHttp.Net.TestConsole.Commands
{
    public class Latency
    {
        Histogram _received = Metric.Histogram("Received Messages", Unit.Items);
        public int Start(LatencyOptions options)
        {
            Thread.Sleep(5000);
            var topicName = "perf-topic-" + DateTime.UtcNow.Ticks;
            var consumerGroupName = "perf-consumer-" + topicName;

            Console.WriteLine("{0}: Topic - {1}", DateTime.Now.ToLongTimeString(), topicName);
            Console.WriteLine("{0}: Consumer group - {1}", DateTime.Now.ToLongTimeString(), consumerGroupName);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            using (var stream = new KafkaConsumerStream(options.ApiUrl.ToString(), consumerGroupName, topicName))
            {
                var receivedCount = 0;
                stream
                    .Subscribed(() => SetupProducer(stream, topicName, options.BatchSize, options.Messages))
                    .Message(m =>
                    {
                        if (m.Value == null) return;

                        var ticks = DateTime.UtcNow.Ticks;
                        var t = (long)TimeSpan.FromTicks(ticks - long.Parse(m.Value)).TotalMilliseconds;
                        _received.Update(t);

                        receivedCount++;
                        if (receivedCount != options.Messages) return;
                            
                        Console.WriteLine($"Received {options.Messages} messages.");
                        stream.Shutdown();
                    })
                    .Error(e =>
                    {
                        Console.Error.WriteLine(e);
                    })
                    .Close(() => Console.WriteLine("Socket closed."))
                    .Open(() => stream.CreateTopic(topicName).Wait())
                    .Block();
            }

            Console.WriteLine("Waiting 5sec to show latest metrics.");
            Thread.Sleep(5000);

            return 0;
        }

        public void SetupProducer(IKafkaConsumerStream stream, string topic, int batchSize, int numMessages)
        {
            Console.WriteLine("Starting publisher...");
            Task.Run(async () =>
            {
                var published = 0;
                while (published < numMessages)
                {
                    await Task.Delay(10);

                    var payload = new Message<string>
                    {
                        Topic = topic,
                        Value = DateTime.UtcNow.Ticks.ToString()
                    };
                    stream.Publish(payload);
                    published++;
                }
                Console.WriteLine($"Finshed publishing {numMessages} messages in batches of {batchSize}.");
            });
        }
    }
}
