using System;
using System.Text;
using System.Threading.Tasks;
using RdKafka;

namespace SimpleProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = "127.0.0.1:9092"; // args[0];
            string topicName = "testtopic"; // args[1];

            var task = TestAsync(brokerList, topicName);

            task.Wait();
        }

        private static async Task TestAsync(string brokerList, string topicName)
        {
            var config = new Config();

            config["debug"] = "all";

            using (Producer producer = new Producer(config, brokerList))
            {
                producer.OnError += Producer_OnError;
                producer.OnStatistics += Producer_OnStatistics;

                using (Topic topic = producer.Topic(topicName))
                {
                    string text = "test";

                    var task = ProduceAndReportUsingContinueWithAsync(topic, text);

                    await task;
                }
            }
        }

        private static void Producer_OnStatistics(object sender, string e)
        {
            Console.WriteLine($"OnStatistics: {e}");
        }

        private static void Producer_OnError(object sender, Handle.ErrorArgs e)
        {
            Console.WriteLine($"OnError: {e.ErrorCode}: {e.Reason}");
        }

        private static Task ProduceAndReportUsingContinueWithAsync(Topic topic, string text)
        {
            byte[] data = Encoding.UTF8.GetBytes(text);
            Task<DeliveryReport> deliveryReport = topic.Produce(data);
            return deliveryReport.ContinueWith(task =>
            {
                Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
            });
        }
    }
}
