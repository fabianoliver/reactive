using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

using BenchmarkDotNet.Attributes;

using NotImplementedException = System.NotImplementedException;

#if CURRENT
namespace Benchmarks.System.Interactive;

[MemoryDiagnoser]
public class MergeBenchmark
{
    [Params(2,3,4,8)] public int NSourceEnumerables { get; set; }
    [Params(10_000, 1_000_000)] public int NElementsPerEnumerable { get; set; }

    [Params(0, 0.5, 1.0)] public double PercentElementsAsync { get; set; }


    private IAsyncEnumerable<int>[] _source;

    [IterationSetup]
    public void Setup()
    {
        var nAsync = (int)(NElementsPerEnumerable * PercentElementsAsync);
        var nSync = NElementsPerEnumerable - nAsync;

        _source = Enumerable.Range(0, NSourceEnumerables)
            .Select(_ => new BenchmarkAsyncEnumerable(nAsync, nSync))
            .ToArray();
    }

    [Benchmark(Baseline = true)]
    public async Task OldImplementation()
    {
        await AsyncEnumerableEx.Merge(_source).LastAsync();
    }

    [Benchmark]
    public async Task NewImplementationFair()
    {
        await AsyncEnumerableEx.MergeFair(_source).LastAsync();
    }

    [Benchmark]
    public async Task NewImplementationUnfair()
    {
        await AsyncEnumerableEx.MergeFair(_source).LastAsync();
    }

    private sealed class BenchmarkAsyncEnumerable(int NAsynchronous, int NSynchronous) : IAsyncEnumerable<int>
    {
        public IAsyncEnumerator<int> GetAsyncEnumerator(CancellationToken cancellationToken = new CancellationToken())
        {
            return new BenchmarkEnumerator(NAsynchronous, NSynchronous);
        }
    }

    private sealed class BenchmarkEnumerator(int NAsynchronous, int NSynchronous) : IAsyncEnumerator<int>
    {
        private int _current;
        private readonly CompleteAsynchronouslyAfterAwaitingValueTaskSource _source = new();

        public ValueTask<bool> MoveNextAsync()
        {
            if (_current++ < NAsynchronous)
            {
                return new ValueTask<bool>(_source, _source.Version);
            }

            if(_current <= (NAsynchronous + NSynchronous))
            {
                return new ValueTask<bool>(true);
            }

            return new ValueTask<bool>(false);
        }

        public int Current => _current;


        public ValueTask DisposeAsync()
        {
            return new();
        }

        private sealed class CompleteAsynchronouslyAfterAwaitingValueTaskSource : IValueTaskSource<bool>
        {
            private ManualResetValueTaskSourceCore<bool> _core = new();

            public short Version => _core.Version;

            public bool GetResult(short token)
            {
                try
                {
                    return _core.GetResult(token);
                }
                finally
                {
                    _core.Reset();
                }
            }

            public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

            public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
            {
                _core.OnCompleted(continuation, state, token, flags);
                ThreadPool.QueueUserWorkItem(o => ((CompleteAsynchronouslyAfterAwaitingValueTaskSource)o!)._core.SetResult(true), this);
            }
        }
    }

}
#endif
