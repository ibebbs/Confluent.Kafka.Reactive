using Confluent.Kafka.Reactive.Producer;
using FakeItEasy;
using Microsoft.Reactive.Testing;
using NUnit.Framework;
using System;

namespace Confluent.Kafka.Reactive.Tests.Producer
{
    [TestFixture]
    public class ShouldManageKafkaProducerLifetimeBy
    {
        [Test]
        public void ConstructingAdapterWhenConnectIsCalled()
        {
            var factory = A.Fake<Func<ProducerConfig, IAdapter<string, string>>>();

            var subject = new Instance<string, string>(new ProducerConfig(), new TestScheduler(), factory);

            A.CallTo(() => factory.Invoke(A<ProducerConfig>.Ignored)).MustNotHaveHappened();

            subject.Connect();

            A.CallTo(() => factory.Invoke(A<ProducerConfig>.Ignored)).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void DisposingAdapterWhenConnectDisposableIsDisposed()
        {
            var scheduler = new TestScheduler();
            var adapter = A.Fake<IAdapter<string, string>>();

            var subject = new Instance<string, string>(new ProducerConfig(), scheduler, config => adapter);

            var connection = subject.Connect();

            connection.Dispose();

            A.CallTo(() => adapter.Dispose()).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void DisposingAdapterWhenAllConnectDisposablesAreDisposed()
        {
            var scheduler = new TestScheduler();
            var adapter = A.Fake<IAdapter<string, string>>();

            var subject = new Instance<string, string>(new ProducerConfig(), scheduler, config => adapter);

            var connection1 = subject.Connect();
            var connection2 = subject.Connect();

            connection1.Dispose();
            A.CallTo(() => adapter.Dispose()).MustNotHaveHappened();
            connection2.Dispose();
            A.CallTo(() => adapter.Dispose()).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void DisposingAdapterWhenConsumerIsDisposed()
        {
            var scheduler = new TestScheduler();
            var adapter = A.Fake<IAdapter<string, string>>();

            var subject = new Instance<string, string>(new ProducerConfig(), scheduler, config => adapter);
            subject.Connect();
            subject.Dispose();

            A.CallTo(() => adapter.Dispose()).MustHaveHappenedOnceExactly();
        }
    }
}
