using System.Linq;
using System.Threading.Tasks;

using Xunit;

namespace Tests;

public class MergeSourceCompletionStrategyUnfair
{
    [Fact]
    public async Task PrefersSourcesAtHead()
    {
        var xs = new MergeSourceCompletionListener<int, CompletionQueueUnfairManySources>(new ValueTask<int>[]
        {
            new ValueTask<int>(1),
            new ValueTask<int>(1000)
        }, new CompletionQueueUnfairManySources(2));
        xs.Start();

        Assert.Equal(0, await xs);
        xs.Replace(0, new ValueTask<int>(2));
        Assert.Equal(0, await xs);
        xs.Replace(0, new ValueTask<int>(3));
        Assert.Equal(0, await xs);
        Assert.Equal(1, await xs);
        xs.Replace(1, new ValueTask<int>(4));
        Assert.Equal(1, await xs);
        xs.Replace(1, new ValueTask<int>(5));
        xs.Replace(0, new ValueTask<int>(6));
        Assert.Equal(0, await xs);
        Assert.Equal(1, await xs);
    }
}
