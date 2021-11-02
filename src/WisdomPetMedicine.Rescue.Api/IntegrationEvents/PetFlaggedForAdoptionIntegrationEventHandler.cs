using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using WisdomPetMedicine.Pet.Api.IntegrationEvents;
using WisdomPetMedicine.Rescue.Api.Infrastructure;
using WisdomPetMedicine.Rescue.Domain.Entities;
using WisdomPetMedicine.Rescue.Domain.Repositories;
using WisdomPetMedicine.Rescue.Domain.ValueObjects;

namespace WisdomPetMedicine.Rescue.Api.IntegrationEvents
{
    public class PetFlaggedForAdoptionIntegrationEventHandler: BackgroundService
    {
        private readonly ILogger<PetFlaggedForAdoptionIntegrationEventHandler> _logger;
        private readonly IServiceScopeFactory _serviceScopeFactory;
        private readonly ServiceBusClient _client;
        private readonly ServiceBusProcessor _processor;

        public PetFlaggedForAdoptionIntegrationEventHandler(ILogger<PetFlaggedForAdoptionIntegrationEventHandler> logger, IConfiguration configuration, IServiceScopeFactory serviceScopeFactory)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _serviceScopeFactory = serviceScopeFactory;
            _client = new ServiceBusClient(configuration["ServiceBus:ConnectionString"]);
            _processor = _client.CreateProcessor(configuration["ServiceBus:TopicName"], configuration["ServiceBus:SubscriptionName"]);
            _processor.ProcessMessageAsync += Processor_ProcessMessageAsync;
            _processor.ProcessErrorAsync += Processor_ProcessErrorAsync;
        }

        private Task Processor_ProcessErrorAsync(ProcessErrorEventArgs args)
        {
            _logger.LogInformation(args.Exception.ToString());
            return Task.CompletedTask;
        }

        private async Task Processor_ProcessMessageAsync(ProcessMessageEventArgs args)
        {
            var body = args.Message.Body.ToString();
            var theEvent = JsonConvert.DeserializeObject<PetFlaggedForAdoptionIntegrationEvent>(body);
            await args.CompleteMessageAsync(args.Message);

            using var scope = _serviceScopeFactory.CreateScope();
            var repo = scope.ServiceProvider.GetRequiredService<IRescueRepository>();
            var dbContext = scope.ServiceProvider.GetRequiredService<RescueDbContext>();
            dbContext.RescuedAnimalMetadata.Add(theEvent);

            var rescuedAnimal = new RescuedAnimal(RescuedAnimalId.Create(theEvent.Id));
            await repo.AddRescuedAnimalAsync(rescuedAnimal);
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _processor.StartProcessingAsync(stoppingToken);
        }

        public async override Task StopAsync(CancellationToken stoppingToken)
        {
            await _processor.StopProcessingAsync(stoppingToken);
        }
    }
}
