using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    class ServingConsumption : IHostedService, IDisposable
    {
        private Timer timer;

        public ServingConsumption()
        {
                
        }

        

        private void consumeJob(object state)
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

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("BackgroundService for consuming messages from Topic is starting.....>");

            timer = new Timer(consumeJob, null, TimeSpan.Zero, TimeSpan.FromSeconds(5));

            return Task.CompletedTask;


            //throw new NotImplementedException();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("Background Service is stopping......>");

            timer?.Change(Timeout.Infinite, 0);

            return Task.CompletedTask;
            
            //throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }

    
}
