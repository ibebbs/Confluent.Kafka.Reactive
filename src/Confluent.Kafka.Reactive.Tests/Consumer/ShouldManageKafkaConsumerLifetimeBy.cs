﻿using Confluent.Kafka.Reactive.Consumer;
using FakeItEasy;
using Microsoft.Reactive.Testing;
using NUnit.Framework;
using System;
using System.Reactive.Disposables;

namespace Confluent.Kafka.Reactive.Tests.Consumer
{
    [TestFixture]
    public class ShouldManageKafkaConsumerLifetimeBy
    {
        [Test]
        public void ConstructingAdapterWhenConnectIsCalled()
        {
            var factory = A.Fake<Func<ConsumerConfig, IAdapter<string, string>>>();

            var subject = new Instance<string, string>(new ConsumerConfig(), new TestScheduler(), factory);

            A.CallTo(() => factory.Invoke(A<ConsumerConfig>.Ignored)).MustNotHaveHappened();

            subject.Connect();

            A.CallTo(() => factory.Invoke(A<ConsumerConfig>.Ignored)).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void DisposingAdapterWhenConnectDisposableIsDisposed()
        {
            var scheduler = new TestScheduler();
            var adapter = A.Fake<IAdapter<string, string>>();

            var subject = new Instance<string, string>(new ConsumerConfig(), scheduler, config => adapter);

            var connection = subject.Connect();

            connection.Dispose();

            A.CallTo(() => adapter.Dispose()).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void DisposingAdapterWhenAllConnectDisposablesAreDisposed()
        {
            var scheduler = new TestScheduler();
            var adapter = A.Fake<IAdapter<string, string>>();

            var subject = new Instance<string, string>(new ConsumerConfig(), scheduler, config => adapter);

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

            var subject = new Instance<string, string>(new ConsumerConfig(), scheduler, config => adapter);
            subject.Connect();
            subject.Dispose();

            A.CallTo(() => adapter.Dispose()).MustHaveHappenedOnceExactly();
        }
    }
}
