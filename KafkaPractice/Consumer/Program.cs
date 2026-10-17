using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Consumer;
using KafkaPractice.Contracts;
using MassTransit;

var builder = Host.CreateApplicationBuilder(args);

var kafkaBootstrapServers = "localhost:9092";
var schemaRegistryUrl = "http://localhost:8081";

var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
{
    Url = schemaRegistryUrl
});

builder.Services.AddMassTransit(x =>
{
    x.UsingInMemory((context, cfg) => cfg.ConfigureEndpoints(context));

    x.AddRider(rider =>
    {
        rider.AddProducer<string, OrderCreatedEvent>("order-created-dlq",
            context => context.MessageId.ToString(),
            (_, producer) =>
            {
                producer.SetKeySerializer(new AvroSerializer<string>(schemaRegistry).AsSyncOverAsync());

                producer.SetValueSerializer(new AvroSerializer<OrderCreatedEvent>(schemaRegistry).AsSyncOverAsync());
            });

        rider.AddConsumer<OrderCreatedConsumer>();

        rider.UsingKafka((context, cfg) =>
        {
            cfg.Host(kafkaBootstrapServers);
            cfg.TopicEndpoint<string, OrderCreatedEvent>("order-created", "order-service-group",
                e =>
                {
                    e.SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync());
                    e.SetValueDeserializer(new AvroDeserializer<OrderCreatedEvent>(schemaRegistry).AsSyncOverAsync());

                    e.AutoOffsetReset = AutoOffsetReset.Earliest;

                    e.UseMessageRetry(r => r.Interval(3, TimeSpan.FromSeconds(2)));

                    e.ConfigureConsumer<OrderCreatedConsumer>(context);
                });
        });
    });
});

var host = builder.Build();
host.Run();
