// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT License.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace System.Linq
{
    public static partial class AsyncEnumerableEx
    {
        /// <summary>
        /// Merges elements from all of the specified async-enumerable sequences into a single async-enumerable sequence.
        /// The resulting enumerable will prefer values originating from sources towards the head of the <paramref name="sources"/> array.
        /// Example: If all input sources were an infinite stream of synchronously available values, the resulting enumerable would only publish values from the first source.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source sequences.</typeparam>
        /// <param name="sources">Async-enumerable sequences.</param>
        /// <returns>The async-enumerable sequence that merges the elements of the async-enumerable sequences.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> == null.</exception>
        public static IAsyncEnumerable<TSource> MergeUnfair<TSource>(params IAsyncEnumerable<TSource>[] sources)
        {
            if(sources.Length <= 2)
                return MergeBase<TSource, CompletionQueueUnfairFewSources.Factory, CompletionQueueUnfairFewSources>(sources);
            return MergeBase<TSource, CompletionQueueUnfairManySources.Factory, CompletionQueueUnfairManySources>(sources);
        }

        /// <summary>
        /// Merges elements from all of the specified async-enumerable sequences into a single async-enumerable sequence.
        /// The resulting enumerable produces values in roughly the same order as they arrive from the individual sources; if multiple sources are available repeatedly synchronously, the resulting enumerable round robins between them.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source sequences.</typeparam>
        /// <param name="sources">Async-enumerable sequences.</param>
        /// <returns>The async-enumerable sequence that merges the elements of the async-enumerable sequences.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> == null.</exception>
        public static IAsyncEnumerable<TSource> MergeFair<TSource>(params IAsyncEnumerable<TSource>[] sources)
        {
            return MergeBase<TSource, CompletionQueueFair.Factory, CompletionQueueFair>(sources);
        }

        private static IAsyncEnumerable<TSource> MergeBase<TSource, TQueueStrategyFactory, TQueueStrategy>(params IAsyncEnumerable<TSource>[] sources)
            where TQueueStrategyFactory : struct, ICompletionQueueFactory<TQueueStrategy>
            where TQueueStrategy : struct, ICompletionQueue
        {
            if (sources == null)
                throw Error.ArgumentNull(nameof(sources));

            return Core(sources);

            static async IAsyncEnumerable<TSource> Core(IAsyncEnumerable<TSource>[] sources, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
            //
            // This new implementation of Merge differs from the original one in a few ways:
            //
            // - It's cheaper because:
            //   - no conversion from ValueTask<bool> to Task<bool> takes place using AsTask,
            //   - we don't instantiate Task.WhenAny tasks for each iteration.
            // - It's fairer because:
            //   - the MoveNextAsync tasks are awaited concurently, but completions are queued,
            //     instead of awaiting a new WhenAny task where "left" sources have preferential
            //     treatment over "right" sources.
            //
            {
                var count = sources.Length;
                var active = 0;

                var enumerators = new IAsyncEnumerator<TSource>[count];
                var moveNextTasks = new ValueTask<bool>[count];
                var errors = default(List<Exception>);
                var whenAny = default(MergeSourceCompletionListener<bool, TQueueStrategyFactory, TQueueStrategy>);

                try
                {
                    var i = 0;
                    try
                    {
                        for (; i < count; i++)
                        {
                            var enumerator = sources[i].GetAsyncEnumerator(cancellationToken);
                            enumerators[i] = enumerator;
                            try
                            {
                                moveNextTasks[i] = enumerator.MoveNextAsync();
                            }
                            catch (Exception)
                            {
                                // Populate so we can await all moveNextTasks up to including i when cleaning up
                                moveNextTasks[i] = new ValueTask<bool>(false);
                                throw;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        errors = new List<Exception>();
                        errors.Add(ex);

                        for (; i >= 0; i--)
                        {
                            try
                            {
                                // Since when any hasn't been created yet, we can await freely
                                await moveNextTasks[i].ConfigureAwait(false);
                            }
                            catch (Exception moveNextException)
                            {
                                errors.Add(moveNextException);
                            }

                            var enumerator = enumerators[i];

                            try
                            {
                                await enumerator.DisposeAsync().ConfigureAwait(false);
                            }
                            catch (Exception disposeException)
                            {
                                errors.Add(disposeException);
                            }
                        }

                        throw new AggregateException(errors);
                    }

                    active = count;
                    whenAny = new MergeSourceCompletionListener<bool, TQueueStrategyFactory, TQueueStrategy>(moveNextTasks);
                    whenAny.Start();

                    while (active > 0)
                    {
                        var index = await whenAny;
                        var enumerator = enumerators[index];
                        var moveNextTask = moveNextTasks[index];

                        // We can be sure this task is already completed, otherwise whenAny wouldn't have returned its index.
                        // The ValueTask is still safe to consume as we haven't drained its result yet.
                        bool hasNext;
                        try
                        {
                            var awaiter = moveNextTask.GetAwaiter();
                            Debug.Assert(awaiter.IsCompleted);
                            hasNext = awaiter.GetResult();
                        }
                        catch (Exception ex)
                        {
                            active--;
                            if (errors == null)
                                errors = new List<Exception>();
                            errors.Add(ex);

                            try
                            {
                                await enumerator.DisposeAsync().ConfigureAwait(false);
                            }
                            catch (Exception disposeEx)
                            {
                                errors.Add(disposeEx);
                            }

                            break;
                        }

                        if (!hasNext)
                        {
                            active--;

                            try
                            {
                                await enumerator.DisposeAsync().ConfigureAwait(false);
                            }
                            catch (Exception ex)
                            {
                                if (errors == null)
                                    errors = new List<Exception>();
                                errors.Add(ex);
                                break;
                            }
                        }
                        else
                        {
                            var item = enumerator.Current;
                            whenAny.Replace(index, enumerator.MoveNextAsync());
                            yield return item;
                        }
                    }
                }
                finally
                {
                    while (active > 0)
                    {
                        var index = await whenAny;
                        --active;

                        var enumerator = enumerators[index];
                        var moveNextTask = moveNextTasks[index];

                        try
                        {
                            var awaiter = moveNextTask.GetAwaiter();
                            Debug.Assert(awaiter.IsCompleted);
                            Debug.Assert(enumerator != null);
                            awaiter.GetResult();
                        }
                        catch (Exception ex)
                        {
                            // Add to errors, but don't re-throw so we can keep processing the rest
                            if (errors == null)
                                errors = new List<Exception>();
                            errors.Add(ex);
                        }

                        try
                        {
                            await enumerator.DisposeAsync().ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            // Add to errors, but don't re-throw so we can keep processing the rest
                            if (errors == null)
                                errors = new List<Exception>();
                            errors.Add(ex);
                        }
                    }
                }

                if (errors != null)
                {
                    throw new AggregateException(errors);
                }
            }
        }
    }
}
