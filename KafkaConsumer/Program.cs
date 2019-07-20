using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    class Program
    {
        static async Task Main(string[] args)
        {

            await new HostBuilder().ConfigureServices((HostExecutionContext, services) =>
            {
                services.AddHostedService<ServingConsumption>();
            })
                .RunConsoleAsync();

        }    
    }

}