using Confluent.Kafka.Reactive.Consumer;
using FakeItEasy;
using Microsoft.Reactive.Testing;
using NUnit.Framework;
using System;
using System.Reactive.Disposables;

namespace Confluent.Kafka.Reactive.Tests.Consumer
{
    [TestFixture]
    public class Should
    {
        [Test]
        public void ConstructWrapperWhenConnectIsCalled()
        {
            var factory = A.Fake<Func<ConsumerConfig, IWrapper<string, string>>>();

            var subject = new Instance<string, string>(new ConsumerConfig(), new TestScheduler(), factory);

            A.CallTo(() => factory.Invoke(A<ConsumerConfig>.Ignored)).MustNotHaveHappened();

            subject.Connect();

            A.CallTo(() => factory.Invoke(A<ConsumerConfig>.Ignored)).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void DisposeWrapperWhenConnectDisposableIsDisposed()
        {
            var scheduler = new TestScheduler();
            var wrapper = A.Fake<IWrapper<string, string>>();

            var subject = new Instance<string, string>(new ConsumerConfig(), scheduler, config => wrapper);

            var connection = subject.Connect();

            connection.Dispose();

            A.CallTo(() => wrapper.Dispose()).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void DisposeWrapperWhenAllConnectDisposablesAreDisposed()
        {
            var scheduler = new TestScheduler();
            var wrapper = A.Fake<IWrapper<string, string>>();

            var subject = new Instance<string, string>(new ConsumerConfig(), scheduler, config => wrapper);

            var connection1 = subject.Connect();
            var connection2 = subject.Connect();

            connection1.Dispose();
            A.CallTo(() => wrapper.Dispose()).MustNotHaveHappened();
            connection2.Dispose();
            A.CallTo(() => wrapper.Dispose()).MustHaveHappenedOnceExactly();
        }

        [Test]
        public void DisposeWrapperWhenConsumerIsDisposed()
        {
            var scheduler = new TestScheduler();
            var wrapper = A.Fake<IWrapper<string, string>>();

            var subject = new Instance<string, string>(new ConsumerConfig(), scheduler, config => wrapper);
            subject.Connect();
            subject.Dispose();

            A.CallTo(() => wrapper.Dispose()).MustHaveHappenedOnceExactly();
        }
    }
}
