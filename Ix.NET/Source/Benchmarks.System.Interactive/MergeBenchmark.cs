using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using BenchmarkDotNet.Attributes;

#if CURRENT
namespace Benchmarks.System.Interactive;

[MemoryDiagnoser]
public class MergeBenchmark
{
    [Params(3)] public int NSourceEnumerables { get; set; }
    [Params(1_000_000)] public int NElementsPerEnumerable { get; set; }

    private IAsyncEnumerable<int>[] _source;

    [IterationSetup]
    public void Setup()
    {
        _source = Enumerable.Range(0, NSourceEnumerables)
            .Select(_ => AsyncEnumerable.Range(0, NElementsPerEnumerable))
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
}
#endif
