using System;
using System.Text;
using System.Threading;
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
                    Console.WriteLine($"Before: Thread id: {Thread.CurrentThread.ManagedThreadId} {Thread.CurrentThread.Name}");

                    string text = "test";

                    //var task = ProduceAndReportUsingContinueWithAsync(topic, text);
                    var task = ProduceAndReportUsingAwaitAsync(topic, text);

                    Console.WriteLine($"After: Thread id: {Thread.CurrentThread.ManagedThreadId} {Thread.CurrentThread.Name}");

                    await task;

                    Console.WriteLine($"After final await: Thread id: {Thread.CurrentThread.ManagedThreadId} {Thread.CurrentThread.Name}");
                }
            }
        }

        private static void Producer_OnStatistics(object sender, string e)
        {
            Console.WriteLine($"OnStatistics: Thread id: {Thread.CurrentThread.ManagedThreadId} {Thread.CurrentThread.Name}");

            Console.WriteLine($"OnStatistics: {e}");
        }

        private static void Producer_OnError(object sender, Handle.ErrorArgs e)
        {
            Console.WriteLine($"OnError: Thread id: {Thread.CurrentThread.ManagedThreadId} {Thread.CurrentThread.Name}");

            Console.WriteLine($"OnError: {e.ErrorCode}: {e.Reason}");
        }

        private static Task ProduceAndReportUsingContinueWithAsync(Topic topic, string text)
        {
            Console.WriteLine($"Before Topic.Produce: Thread id: {Thread.CurrentThread.ManagedThreadId} {Thread.CurrentThread.Name}");

            byte[] data = Encoding.UTF8.GetBytes(text);
            Task<DeliveryReport> deliveryReport = topic.Produce(data);
            return deliveryReport.ContinueWith(task =>
            {
                Console.WriteLine($"In ContinueWith: Thread id: {Thread.CurrentThread.ManagedThreadId} {Thread.CurrentThread.Name}");

                Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
            });
        }

        private static async Task ProduceAndReportUsingAwaitAsync(Topic topic, string text)
        {
            byte[] data = Encoding.UTF8.GetBytes(text);
            DeliveryReport deliveryReport = await topic.Produce(data);

            //await Task.Delay(TimeSpan.FromSeconds(1));
            await Task.Yield(); // Force the task not to continue synchronously (similar to ContinueWith)

            Console.WriteLine($"Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");
        }
    }
}
