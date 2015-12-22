using System;
using System.Diagnostics;
using System.Threading;
using KafkaHttp.Net.TestConsole.CommandLine;
using Metrics;

namespace KafkaHttp.Net.TestConsole.Commands
{
    public class Latency
    {
        readonly Metrics.Timer _received = Metric.Timer("Received Messages", Unit.Requests);
        readonly Metrics.Timer _published = Metric.Timer("Published Messages", Unit.Requests);

        public int Start(LatencyOptions options)
        {
            Thread.Sleep(5000);
            if (options.Trace)
                Trace.Listeners.Add(new ConsoleTraceListener());

            var topicName = "perf-topic-" + DateTime.UtcNow.Ticks;
            var consumerGroupName = "perf-consumer-" + topicName;

            Console.WriteLine("{0}: Topic - {1}", DateTime.Now.ToLongTimeString(), topicName);
            Console.WriteLine("{0}: Consumer group - {1}", DateTime.Now.ToLongTimeString(), consumerGroupName);
            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            using (var client = new KafkaClient(options.ApiUrl.ToString()))
            using (var stream = client.Consumer(consumerGroupName, topicName))
            {
                var producer = client.Producer();
                client.OnOpen(async () =>
                {
                    await producer.CreateTopic(topicName);

                    var receivedCount = 0;
                    stream
                        .OnSubscribed(() => SetupProducer(producer, topicName, options.BatchSize, options.Messages))
                        .OnMessage(m =>
                        {
                            if (m.Value == null) return;

                            var ticks = DateTime.UtcNow.Ticks;
                            var t = (long)TimeSpan.FromTicks(ticks - long.Parse(m.Value)).TotalMilliseconds;
                            _received.Record(t, TimeUnit.Milliseconds);

                            receivedCount++;
                            if (receivedCount != options.Messages) return;

                            Console.WriteLine($"Received {options.Messages} messages.");
                            stream.Shutdown();
                        })
                        .OnError(e =>
                        {
                            Console.Error.WriteLine(e);
                        })
                        .OnClose(() => Console.WriteLine("Socket closed."))
                        .Start();
                });
                stream.Block();
            }

            Console.WriteLine("Waiting 5sec to show latest metrics.");
            Thread.Sleep(5000);

            return 0;
        }

        public Thread SetupProducer(IKafkaProducer producer, string topic, int batchSize, int numMessages)
        {
            Console.WriteLine("Starting publisher...");
            var t = new Thread(() =>
            {
                var published = 0;
                while (published < numMessages)
                {
                    Thread.Sleep(10);
                    var payload = new Message<string>
                    {
                        Topic = topic,
                        Value = DateTime.UtcNow.Ticks.ToString()
                    };
                    using (_published.NewContext())
                        producer.Publish(payload);
                    published++;
                }
                Console.WriteLine($"Finshed publishing {numMessages} messages in batches of {batchSize}.");
            });
            t.Start();
            return t;
        }
    }
}
