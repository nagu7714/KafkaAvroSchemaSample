using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Ini;
namespace Produce
{
    public class Program
    {
        public static IConfiguration readConfig()
        {
            return new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                 .AddIniFile("client.properties", false)
                 .Build();

        }
        public static void Produce(string topic, IConfiguration config)
        {
            using (var producer = new ProducerBuilder<string, string>(config.AsEnumerable()).Build())
            {
                producer.Produce(topic, new Message<string, string> { Key = "key", Value = "value" },
       (deliveryReport) =>
       {
           if (deliveryReport.Error.Code != ErrorCode.NoError)
           {
               Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
           }
           else
           {
               Console.WriteLine($"Produced event to topic {topic}: key = {deliveryReport.Message.Key,-10} value = {deliveryReport.Message.Value}");
           }
       });
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }

        public static void Consume(string topic, IConfiguration config)
        {
            config["group.id"] = "csharp-group-1";
            config["auto.offset.reset"] = "earliest";

            // creates a new consumer instance
            using (var consumer = new ConsumerBuilder<string, string>(config.AsEnumerable()).Build())
            {
                consumer.Subscribe(topic);
                while (true)
                {
                    // consumes messages from the subscribed topic and prints them to the console
                    var cr = consumer.Consume();
                    Console.WriteLine($"Consumed event from topic {topic}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
                }              
            }
        }
        static void Main(string[] args)
        {
            // producer and consumer code here
            IConfiguration config = readConfig();
            const string topic = "orders";

            Produce(topic, config);
            Consume(topic, config);
        }


    }
}
