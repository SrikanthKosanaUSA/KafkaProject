using Confluent.Kafka;
using System;
using System.IO;
using System.Threading.Tasks;

namespace KafkaProducer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string brokers = "127.0.0.1:9092";
            string topic = "test";

            var config = new ProducerConfig { BootstrapServers = brokers };

            using (var writer = new ProducerBuilder<string, string>(config).Build())
            {

                var cancelled = false;

                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; 
                    cancelled = true;
                };

                while (!cancelled)
                {
                    Console.Write("Type your message here...>>>");

                    string message;

                    message = Console.ReadLine();

                    if (message == null)
                    {
                        break;
                    }

                    
                    string value = message;

                    int index = message.IndexOf(" ");
                    if (index != -1)

                    {

                        value = message.Substring(index + 1);
                    }

                    var deliveryReport = await writer.ProduceAsync(
                                topic, new Message<string, string> { Value = value });

                    Console.WriteLine($"Message posted to your topic : {deliveryReport.Topic} and partion : {deliveryReport.TopicPartitionOffset}");


                }
            }
             
            // Console.WriteLine("Hello World!");
        }
    }
}
