// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka.Serialization;


namespace Confluent.Kafka.Examples.SimpleProducer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = "127.0.0.1:9092"; // args[0];
            string topicName = "testtopic"; // args[1];

            Test(brokerList, topicName);
        }

        private static void Test(string brokerList, string topicName)
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", brokerList },
                { "debug", "all" },
            };

            using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
            {
                producer.OnError += Producer_OnError;
                producer.OnStatistics += Producer_OnStatistics;

                Console.WriteLine($"{producer.Name} producing on {topicName}. q to exit.");

                string text = $"test: {DateTime.Now.ToString("o")}";

                var unusedTask = ProduceAndReportUsingContinueWithAsync(producer, topicName, text);

                // Tasks are not waited on synchronously (ContinueWith is not synchronous),
                // so it's possible they may still in progress here.
                producer.Flush();
            }
        }
        private static Task ProduceAndReportUsingContinueWithAsync(Producer<Null, string> producer, string topicName, string text)
        {
            var deliveryReportTask = producer.ProduceAsync(topicName, null, text);
            return deliveryReportTask.ContinueWith(task =>
            {
                Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
            });
        }

        private static void Producer_OnStatistics(object sender, string e)
        {
            Console.WriteLine($"OnStatistics: {e}");
        }

        private static void Producer_OnError(object sender, Error error)
        {
            Console.WriteLine($"OnError: {error.Code} ({error.Code.GetReason()}): {error.Reason}");
        }
    }
}
