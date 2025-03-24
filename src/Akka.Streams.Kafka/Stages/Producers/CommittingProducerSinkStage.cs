// -----------------------------------------------------------------------
//  <copyright file="CommittingProducerSinkStage.cs" company="Akka.NET Project">
//      Copyright (C) 2025 - 2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Streams.Kafka.Stages.Consumers;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using Confluent.Kafka;
using Error = Confluent.Kafka.Error;

namespace Akka.Streams.Kafka.Stages.Producers;

internal sealed class
    CommittingProducerSinkStage<K, V, TIn> : GraphStageWithMaterializedValue<SinkShape<TIn>, Task<Done>>
    where TIn : IEnvelope<K, V, ICommittable>
{
    public CommittingProducerSinkStage(ProducerSettings<K, V> producerSettings,
        CommitterSettings committerSettings)
    {
        ProducerSettings = producerSettings;
        CommitterSettings = committerSettings;
        In = new Inlet<TIn>("messages");
        Shape = new SinkShape<TIn>(In);
    }
    
    public ProducerSettings<K,V> ProducerSettings { get; }
    
    public CommitterSettings CommitterSettings { get; }

    public Inlet<TIn> In { get; } 

    public override SinkShape<TIn> Shape { get; }

    public override ILogicAndMaterializedValue<Task<Done>> CreateLogicAndMaterializedValue(Attributes inheritedAttributes) => throw new NotImplementedException();

    private class CommittingProducerStageLogic : TimerGraphStageLogic
    {
        private const string CommitNow = "commit";
        private readonly CommittingProducerSinkStage<K, V, TIn> _stage;

        /// <summary>
        /// Completion source behind the materialized <see cref="Task"/>
        /// </summary>
        private readonly TaskCompletionSource<Done> _streamCompletion = new();

        private readonly Lazy<Decider> _decider; 

        public CommittingProducerStageLogic(CommittingProducerSinkStage<K, V, TIn> stage, Attributes inheritedAttributes) : base(stage.Shape)
        {
            _stage = stage;
            _decider = new Lazy<Decider>(() =>
                inheritedAttributes.GetAttribute<ActorAttributes.SupervisionStrategy>()?.Decider ??
                Deciders.StoppingDecider);
            _closeAndFailStageCallback = GetAsyncCallback<Error>(CloseAndFailStage);
            _commitObservationLogic = new CommitObservationLogic(_stage.CommitterSettings);
        }

        protected override object LogSource => _stage.GetType();

        private readonly Action<Error> _closeAndFailStageCallback;
        private readonly CommitObservationLogic _commitObservationLogic;
        
        public IProducer<K,V>? Producer { get; set; } 

        public override void PreStart()
        {
            base.PreStart();
            Producer = _stage.ProducerSettings.CreateKafkaProducer((producer, error) =>
                _closeAndFailStageCallback(error));
            
            TryPull(_stage.In);
            ScheduleCommit();
            Log.Debug("CommittingProducerSink initialized");
        }

        private void ScheduleCommit()
        {
            ScheduleOnce(CommitNow, _stage.CommitterSettings.MaxInterval);
        }

        private void CloseAndFailStage(Exception ex)
        {
            // TODO
        }

        protected override void OnTimer(object timerKey) => throw new NotImplementedException();
    }
}