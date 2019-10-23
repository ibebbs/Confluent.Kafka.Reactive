using System;
using System.Reactive.Disposables;
using System.Threading;

namespace Confluent.Kafka.Reactive
{
    /// <summary>
    /// Represents a disposable resource that only disposes its underlying disposable resource when all <see cref="GetDisposable">dependent disposable objects</see> have been disposed.
    /// </summary>
    /// <remarks>
    /// Based on <see cref="RefCountDisposable"/> but doesn't require the the disposable to be disposed prior to disposing the inner disposable
    /// </remarks>
    public sealed class ImmediateRefCountDisposable : IDisposable
    {
        private IDisposable _disposable;

        /// <summary>
        /// Holds the number of active child disposables
        /// </summary>
        private int _count;

        /// <summary>
        /// Initializes a new instance of the <see cref="RefCountDisposable"/> class with the specified disposable.
        /// </summary>
        /// <param name="disposable">Underlying disposable.</param>
        /// <exception cref="ArgumentNullException"><paramref name="disposable"/> is null.</exception>
        public ImmediateRefCountDisposable(IDisposable disposable)
        {
            _disposable = disposable ?? throw new ArgumentNullException(nameof(disposable));
            _count = 0;
        }

        /// <summary>
        /// Returns a dependent disposable that when disposed decreases the refcount on the underlying disposable.
        /// </summary>
        /// <returns>A dependent disposable contributing to the reference count that manages the underlying disposable's lifetime.</returns>
        /// <exception cref="ObjectDisposedException">This instance has been disposed and is configured to throw in this case by <see cref="RefCountDisposable(IDisposable, bool)"/>.</exception>
        public IDisposable GetDisposable()
        {
            // the current state
            var cnt = Volatile.Read(ref _count);

            for (; ; )
            {
                // If bit 31 is set and the active count is zero, don't create an inner
                if (_disposable == null)
                {
                    throw new ObjectDisposedException("RefCountDisposable");
                }

                // Should not overflow the bits 0..30
                if (cnt == int.MaxValue)
                {
                    throw new OverflowException($"RefCountDisposable can't handle more than {int.MaxValue} disposables");
                }

                // Increment the active count by one, works because the increment
                // won't affect bit 31
                var u = Interlocked.CompareExchange(ref _count, cnt + 1, cnt);
                if (u == cnt)
                {
                    return new InnerDisposable(this);
                }
                cnt = u;
            }
        }

        /// <summary>
        /// Disposes the underlying disposable only when all dependent disposables have been disposed.
        /// </summary>
        public void Dispose()
        {
            var disposable = Interlocked.Exchange(ref _disposable, null);

            if (disposable != null)
            {
                disposable.Dispose();
            }
        }

        private void Release()
        {
            var cnt = Volatile.Read(ref _count);

            for (; ; )
            {
                var u = cnt - 1;

                var b = Interlocked.CompareExchange(ref _count, u, cnt);

                if (b == cnt)
                {
                    // if after the CAS there was zero active disposables and
                    // the main has been also marked for disposing,
                    // it is safe to dispose the underlying disposable
                    if (u == 0)
                    {
                        Dispose();
                    }
                    break;
                }

                cnt = b;
            }
        }

        private sealed class InnerDisposable : IDisposable
        {
            private ImmediateRefCountDisposable _parent;

            public InnerDisposable(ImmediateRefCountDisposable parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref _parent, null)?.Release();
            }
        }
    }
}
