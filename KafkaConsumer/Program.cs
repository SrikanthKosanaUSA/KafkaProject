using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = "ConsumerGroup",
                BootstrapServers = "localhost:9092",
                EnableAutoCommit = false,
                EnablePartitionEof = true,
                AutoOffsetReset = AutoOffsetReset.Earliest

            };

            using (var reader = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                reader.Subscribe("test");
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var resultset = reader.Consume(cts.Token);

                            if (resultset.IsPartitionEOF)
                            {
                                Console.WriteLine($"Hey!!!, you reached the edge of topic {resultset.Topic}, " +
                                                  $"partition {resultset.Partition}, " +
                                                  $"offset {resultset.Offset}."
                                                  );
                                continue;
                            }

                            Console.WriteLine($"The message is '{resultset.Value}' " +
                                              $"at offset number:::: '{resultset.Offset}' ");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    reader.Close();
                    //Console.WriteLine("Hello World!");
                }
            }
        }
    }

}