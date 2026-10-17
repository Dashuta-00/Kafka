using KafkaPractice.Contracts;
using MassTransit;

namespace Consumer;

public class OrderCreatedConsumer(
    ITopicProducer<string, OrderCreatedEvent> dlqProducer,
    ILogger<OrderCreatedConsumer> logger) : IConsumer<OrderCreatedEvent>
{
    public async Task Consume(ConsumeContext<OrderCreatedEvent> context)
    {
        try
        {
            await ProcessOrderAsync(context.Message);
            logger.LogInformation("Обработан заказ {OrderId}", context.Message.orderId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Ошибка обработки {OrderId}, отправка в DLQ", context.Message.orderId);
            
            await dlqProducer.Produce(context.Message.orderId, context.Message);
        }
    }

    private Task ProcessOrderAsync(OrderCreatedEvent order)
    {
        if (order.customerEmail.Contains("fail"))
        {
            throw new InvalidOperationException("Симуляция сбоя обработки заказа");
        }
        
        return Task.CompletedTask;
    }
}