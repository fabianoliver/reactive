// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT License.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;

namespace Benchmarks.System.Interactive
{
    internal class Program
    {
        internal static void Main(string[] args)
        {
            Console.WriteLine("Effective Ix-version: " + typeof(EnumerableEx).Assembly.GetName().Version);

            //Available compiler directives: IX3_1_1, IX3_2, CURRENT.

            var switcher = new BenchmarkSwitcher(new[] {
                typeof(BufferCountBenchmark),
                typeof(IgnoreElementsBenchmark),
                typeof(DeferBenchmark),
                typeof(RetryBenchmark)
#if !NET6_0_OR_GREATER
                ,
                typeof(MinMaxBenchmark)
#endif
#if CURRENT
                ,
                typeof(MergeBenchmark)
#endif
            });

            switcher.Run(args, DefaultConfig.Instance.With(Job.Default.WithCustomBuildConfiguration("Current")));
            Console.ReadLine();
        }
    }
}
