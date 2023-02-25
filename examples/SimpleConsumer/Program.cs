using System;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Actor.Dsl;
using Akka.Configuration;
using Akka.Event;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Settings;
using Akka.Util;
using Confluent.Kafka;
using Config = Akka.Configuration.Config;

namespace SimpleConsumer
{
    public sealed class AppActor : ReceiveActor
    {
        private readonly ILoggingAdapter _log = Context.GetLogger();
        
        public AppActor()
        {
            Receive<string>(message =>
            {
                IActorRef child = Context.ActorOf(Props.Create(() => new LoggerActor()));
                child.Forward(message);
            });
        }
    }

    public sealed class LoggerActor : ReceiveActor
    {
        private readonly ILoggingAdapter _log = Context.GetLogger();
        
        public LoggerActor()
        {
            ReceiveAsync<string>(async message =>
            {
                // simulate some variable-length I/O work
                await Task.Delay(TimeSpan.FromSeconds(ThreadLocalRandom.Current.Next(1, 15)));
                _log.Info(message);
                Context.Stop(Self); // termninate when finished
                Sender.Tell("complete");
            });
        }
    }

    public class Program
    {
        public static async Task Main(string[] args)
        {
            Config fallbackConfig = ConfigurationFactory.ParseString(@"
                    akka.suppress-json-serializer-warning=true
                    akka.loglevel = INFO
                ").WithFallback(ConfigurationFactory.FromResource<ConsumerSettings<object, object>>("Akka.Streams.Kafka.reference.conf"));

            var system = ActorSystem.Create("TestKafka", fallbackConfig);
            var actor = system.ActorOf(Props.Create(() => new AppActor()));
            var materializer = system.Materializer();

            var consumerSettings = ConsumerSettings<string, string>.Create(system, null, null)
                .WithBootstrapServers("localhost:29092")
                .WithGroupId("group1");

            var subscription = Subscriptions.Topics("akka100");

            KafkaConsumer.PlainSource(consumerSettings, subscription)
                .SelectAsyncUnordered(100, async result =>
                {
                    var r = await actor.Ask<string>(
                        $"Consumer: {result.Topic}/{result.Partition} {result.Offset}: {result.Message.Value}");
                    return r;
                })
                .RunWith(Sink.Ignore<string>(), materializer);

            await system.WhenTerminated;
        }
    }
}
