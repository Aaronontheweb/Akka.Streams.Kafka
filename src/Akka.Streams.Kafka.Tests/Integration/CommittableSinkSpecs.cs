// -----------------------------------------------------------------------
//  <copyright file="CommittableSinkSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Streams.Kafka.Tests.Integration;

public class CommittableSinkSpec : KafkaIntegrationTests
{
    public CommittableSinkSpec(ITestOutputHelper output, KafkaFixture fixture) : base(nameof(CommittableSinkSpec), output, fixture)
    {
    }
    
    private static readonly string[] Numbers = Enumerable.Range(1, 200).Select(c => c.ToString()).ToArray();

    [Fact]
    public async Task ConsumerToProducerMustCommitAfterProducingToTargetTopic()
    {
        var messages = Numbers;
        var topic1 = CreateTopic(1);
        var targetTopic = CreateTopic(2);
        var group1 = CreateGroup(1);

        await ProduceStrings(topic1, messages, ProducerSettings);

        var consumerSettings = CreateConsumerSettings<Null, string>(group1);
        
        var copying 
            = KafkaConsumer.SourceWithOffsetContext(consumerSettings, Subscriptions.Topics(topic1))
                .Select(c => ProducerMessage.Single(new ProducerRecord<Null, string>(targetTopic, c.Message.Key, c.Message.Value)))
                .ToMaterialized(KafkaProducer.C)
    }
}