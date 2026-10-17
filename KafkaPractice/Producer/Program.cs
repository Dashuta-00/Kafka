using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaPractice.Contracts;
using MassTransit;

var builder = WebApplication.CreateBuilder(args);

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
        rider.AddProducer<string, OrderCreatedEvent>(
            "order-created",
            context => context.MessageId.ToString() ?? Guid.NewGuid().ToString(),
            (_, producer) =>
            {
                producer.SetKeySerializer(new AvroSerializer<string>(schemaRegistry).AsSyncOverAsync());

                producer.SetValueSerializer(new AvroSerializer<OrderCreatedEvent>(schemaRegistry).AsSyncOverAsync());
            });
        rider.UsingKafka((_, cfg) =>
        {
            cfg.Host(kafkaBootstrapServers);
        });
    });
});

var app = builder.Build();


app.MapPost("/orders", async (CreateOrderRequest req, ITopicProducer<string, OrderCreatedEvent> producer) =>
{
    var orderId = Guid.NewGuid().ToString();
    var evt = new OrderCreatedEvent
    {
        orderId = orderId,
        customerEmail = req.Email,
        total = req.Total,
        createdAt = DateTime.UtcNow.ToString("O")
    };

    await producer.Produce(orderId, evt);
    
    return Results.Ok(new {OrderId = orderId});
});

app.Run();

public record CreateOrderRequest(string Email, double Total);
