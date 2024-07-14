// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT License.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

using Moq;

using Xunit;

namespace Tests
{
    public class MergeTest : MergeTestBase
    {
        protected override IAsyncEnumerable<T> Merge<T>(params IAsyncEnumerable<T>[] sources)
        {
            return AsyncEnumerableEx.Merge(sources);
        }
    }

    public class MergeFairTest : MergeTestBase
    {
        protected override IAsyncEnumerable<T> Merge<T>(params IAsyncEnumerable<T>[] sources)
        {
            return AsyncEnumerableEx.MergeFair(sources);
        }

        [Fact]
        public async Task CanMerge_SimpleSynchronousSequence_CorrectOrder()
        {
            var xs = Merge(AsyncEnumerable.Range(0,2), AsyncEnumerable.Range(10,2));

            await using var en = xs.GetAsyncEnumerator();
            await HasNextAsync(en, 10);
            await HasNextAsync(en, 0);
            await HasNextAsync(en, 11);
            await HasNextAsync(en, 1);
        }
    }

    public class MergeUnfairTest : MergeTestBase
    {
        protected override IAsyncEnumerable<T> Merge<T>(params IAsyncEnumerable<T>[] sources)
        {
            return AsyncEnumerableEx.MergeUnfair(sources);
        }

        [Fact]
        public async Task CanMerge_SimpleSynchronousSequence_CorrectOrder()
        {
            var xs = Merge(AsyncEnumerable.Range(0,2), AsyncEnumerable.Range(10,2));

            await using var en = xs.GetAsyncEnumerator();
            await HasNextAsync(en, 0);
            await HasNextAsync(en, 1);
            await HasNextAsync(en, 10);
            await HasNextAsync(en, 11);
            await NoNextAsync(en);
        }
    }


    public abstract class MergeTestBase : AsyncEnumerableExTests
    {
        protected abstract IAsyncEnumerable<T> Merge<T>(params IAsyncEnumerable<T>[] sources);

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public async Task CanMerge_SimpleSynchronousSequence(int nEnumerables)
        {
            var xs = Merge(Enumerable.Range(0, nEnumerables).Select(x => AsyncEnumerable.Range(x * 10, 2)).ToArray());

            // We will test all expected values are returned, but we won't test in which order
            var capturedValues = await xs.ToHashSetAsync();

            Assert.Equal(nEnumerables * 2, capturedValues.Count);
            for (var i = 0; i < nEnumerables; i++)
            {
                var idx = i * 10;
                Assert.Contains(idx, capturedValues);
                Assert.Contains(idx+1, capturedValues);
            }
        }

        [Fact]
        public async Task CanMerge_SimpleAsynchronousSequence()
        {
            var enumerator1 = new Mock<IAsyncEnumerator<int>>();
            var enumerable1 = new Mock<IAsyncEnumerable<int>>();
            enumerable1.Setup(x => x.GetAsyncEnumerator(It.IsAny<CancellationToken>())).Returns(enumerator1.Object);
            enumerator1.SetupSequence(x => x.MoveNextAsync()).Returns(new ValueTask<bool>(true)).Returns(AsyncContractVerifyingValueTaskSource.CreateValueTask(() => true)).Returns(new ValueTask<bool>(false));
            enumerator1.SetupSequence(x => x.Current).Returns(0).Returns(1).Throws(() => new InvalidOperationException("Only two results available"));
            enumerator1.Setup(x => x.DisposeAsync()).Returns(new ValueTask());

            var enumerator2 = new Mock<IAsyncEnumerator<int>>();
            var enumerable2 = new Mock<IAsyncEnumerable<int>>();
            enumerable2.Setup(x => x.GetAsyncEnumerator(It.IsAny<CancellationToken>())).Returns(enumerator2.Object);
            enumerator2.SetupSequence(x => x.MoveNextAsync()).Returns(new ValueTask<bool>(true)).Returns(AsyncContractVerifyingValueTaskSource.CreateValueTask(() => true)).Returns(new ValueTask<bool>(false));
            enumerator2.SetupSequence(x => x.Current).Returns(10).Returns(11).Throws(() => new InvalidOperationException("Only two results available"));
            enumerator2.Setup(x => x.DisposeAsync()).Returns(new ValueTask());

            var xs = Merge(enumerable1.Object, enumerable2.Object);

            // We will test all expected values are returned, but we won't test in which order
            var capturedValues = await xs.ToHashSetAsync();

            Assert.Equal(4, capturedValues.Count);
            Assert.Contains(0, capturedValues);
            Assert.Contains(1, capturedValues);
            Assert.Contains(10, capturedValues);
            Assert.Contains(11, capturedValues);
            enumerator1.Verify(mock => mock.DisposeAsync(), Times.Once());
            enumerator2.Verify(mock => mock.DisposeAsync(), Times.Once());
        }

        [Fact]
        public async Task CanMerge_WhenDisposingCompletedSourceThrows()
        {
            var enumerator1HasThrown = new TaskCompletionSource<bool>();

            var enumerator1 = new Mock<IAsyncEnumerator<int>>();
            var enumerable1 = new Mock<IAsyncEnumerable<int>>();
            enumerable1.Setup(x => x.GetAsyncEnumerator(It.IsAny<CancellationToken>())).Returns(enumerator1.Object);
            enumerator1.SetupSequence(x => x.MoveNextAsync()).Returns(new ValueTask<bool>(true)).Returns(new ValueTask<bool>(false));
            enumerator1.SetupSequence(x => x.Current).Returns(0).Throws(() => new InvalidOperationException("Only two results available"));
            enumerator1.Setup(x => x.DisposeAsync()).Callback(() => enumerator1HasThrown.TrySetResult(true)).Throws<UnitTestException>();

            var enumerator2 = new Mock<IAsyncEnumerator<int>>();
            var enumerable2 = new Mock<IAsyncEnumerable<int>>();
            enumerable2.Setup(x => x.GetAsyncEnumerator(It.IsAny<CancellationToken>())).Returns(enumerator2.Object);
            enumerator2.SetupSequence(x => x.MoveNextAsync()).Returns(new ValueTask<bool>(true)).Returns(async () =>
            {
                await enumerator1HasThrown.Task;
                return true;
            }).Returns(new ValueTask<bool>(false));
            enumerator2.SetupSequence(x => x.Current).Returns(10).Returns(11).Throws(() => new InvalidOperationException("Only two results available"));
            enumerator2.Setup(x => x.DisposeAsync()).Returns(new ValueTask());

            var xs = Merge(enumerable1.Object, enumerable2.Object);

            // We will test all expected values are returned, but we won't test in which order
            var exception = await Assert.ThrowsAnyAsync<Exception>(async () => await xs.ToHashSetAsync());

            // The existing Merge implementation will throw the original exception, unless it encounters any further exceptions - in which case it would swallow the first original exception, and throw
            // an aggregate exception containing only the following exceptions. That seems inconsistent, so new implementations will try to always throw an aggregate exception
            Assert.True(exception is UnitTestException || (exception is AggregateException agg && agg.InnerExceptions.Count == 1 && agg.InnerExceptions[0] is UnitTestException));

            // The source throwing on disposal should not prevent the second source from being disposed
            enumerator1.Verify(mock => mock.DisposeAsync(), Times.Once());
            enumerator2.Verify(mock => mock.DisposeAsync(), Times.Once());
        }


        [Fact]
        public async Task CanHandleException()
        {
            var enumerator1 = new Mock<IAsyncEnumerator<int>>();
            var enumerable1 = new Mock<IAsyncEnumerable<int>>();
            enumerable1.Setup(x => x.GetAsyncEnumerator(It.IsAny<CancellationToken>())).Returns(enumerator1.Object);
            enumerator1.SetupSequence(x => x.MoveNextAsync()).Returns(new ValueTask<bool>(true)).Returns(AsyncContractVerifyingValueTaskSource.CreateValueTask(() => throw new UnitTestException()));
            enumerator1.SetupSequence(x => x.Current).Returns(1).Throws(() => new InvalidOperationException("Only one result available"));
            enumerator1.Setup(x => x.DisposeAsync()).Returns(new ValueTask());

            var enumerator2 = new Mock<IAsyncEnumerator<int>>();
            var enumerable2 = new Mock<IAsyncEnumerable<int>>();
            enumerable2.Setup(x => x.GetAsyncEnumerator(It.IsAny<CancellationToken>())).Returns(enumerator2.Object);
            enumerator2.SetupSequence(x => x.MoveNextAsync()).Returns(new ValueTask<bool>(true)).Returns(new ValueTask<bool>(true)).Returns(new ValueTask<bool>(false));
            enumerator2.SetupSequence(x => x.Current).Returns(10).Returns(11).Throws(() => new InvalidOperationException("Only two results available"));
            enumerator2.Setup(x => x.DisposeAsync()).Returns(new ValueTask());

            var xs = Merge(enumerable1.Object, enumerable2.Object);

            var ex = await Assert.ThrowsAsync<AggregateException>(async () =>
            {
                await foreach (var _ in xs)
                    continue;
            });
            Assert.Single(ex.InnerExceptions);
            Assert.IsType<UnitTestException>(ex.InnerExceptions[0]);
            enumerator1.Verify(mock => mock.DisposeAsync(), Times.Once());
            enumerator2.Verify(mock => mock.DisposeAsync(), Times.Once());
        }


        private sealed class UnitTestException : Exception
        {
        }

         private sealed class AsyncContractVerifyingValueTaskSource : IValueTaskSource<bool>
        {
            private const short ExpectedToken = 1;

            private readonly Func<bool> _getResult;
            private readonly object _lock = new();
            private State _state = State.Initial;

            private AsyncContractVerifyingValueTaskSource(Func<bool> getResult)
            {
                _getResult = getResult;
            }

            public static ValueTask<bool> CreateValueTask(Func<bool> getResult)
            {
                return new ValueTask<bool>(new AsyncContractVerifyingValueTaskSource(getResult), ExpectedToken);
            }

            public ValueTaskSourceStatus GetStatus(short token)
            {
                if (token != ExpectedToken)
                    throw new InvalidOperationException("Token mismatch");

                lock (_lock)
                {
                    switch (_state)
                    {
                        case State.Initial:
                            return ValueTaskSourceStatus.Pending;
                        case State.Completed:
                            return ValueTaskSourceStatus.Faulted;
                        default:
                            throw new InvalidOperationException("It is not valid to call GetStatus on a ValueTask once the result has already been drained");
                    }
                }
            }

            public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            {
                lock (_lock)
                {
                    switch (_state)
                    {
                        case State.Initial:
                        {
                            _state = State.Completed;
                            Task.Factory.StartNew(continuation, state, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default);
                            break;
                        }
                        default:
                            throw new InvalidOperationException("Must not call OnCompleted twice, and must not call after result has been drained");
                    }
                }
            }

            public bool GetResult(short token)
            {
                lock (_lock)
                {
                    switch (_state)
                    {
                        case State.Completed:
                        {
                            _state = State.ResultDrained;
                            return _getResult();
                        }
                        default:
                            throw new InvalidOperationException("GetResult must not be called before the result is available, and/or after the result has already been drained");
                    }
                }
            }

            private enum State
            {
                Initial,
                Completed,
                ResultDrained
            }
        }
    }
}
