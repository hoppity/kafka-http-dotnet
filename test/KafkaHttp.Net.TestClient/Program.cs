using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaHttp.Net.TestClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Thread.Sleep(5000);
            var socket = new KafkaConsumerStream("http://192.168.33.1:8085", "test.group", "test.topic.4");

            socket.Message(m =>
            {
                if (m.Value == null) return;
                var ticks = DateTime.UtcNow.Ticks;
                var t = ticks - long.Parse(Encoding.UTF8.GetString(m.Value));
                Console.WriteLine($"Received: {t} ticks");
                Task.Delay(1).Wait();
            })
            .Error(e =>
            {
                Console.Error.WriteLine(e);
                socket.Shutdown();
            })
            .Close(() => Console.WriteLine("Socket closed."))
            .Open()
            .Block();

            Console.ReadLine();
        }
    }
}
