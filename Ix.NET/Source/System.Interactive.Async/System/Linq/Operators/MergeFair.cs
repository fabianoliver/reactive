﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT License.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace System.Linq
{
    public static partial class AsyncEnumerableEx
    {
        /// <summary>
        /// Merges elements from all of the specified async-enumerable sequences into a single async-enumerable sequence.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements in the source sequences.</typeparam>
        /// <param name="sources">Async-enumerable sequences.</param>
        /// <returns>The async-enumerable sequence that merges the elements of the async-enumerable sequences.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> is null.</exception>
        public static IAsyncEnumerable<TSource> MergeFair<TSource>(params IAsyncEnumerable<TSource>[] sources)
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

                var enumerators = new IAsyncEnumerator<TSource>[count];
                var moveNextTasks = new ValueTask<bool>[count];

                try
                {
                    for (var i = 0; i < count; i++)
                    {
                        IAsyncEnumerator<TSource> enumerator = sources[i].GetAsyncEnumerator(cancellationToken);
                        enumerators[i] = enumerator;

                        // REVIEW: This follows the lead of the original implementation where we kick off MoveNextAsync
                        //         operations immediately. An alternative would be to do this in a separate stage, thus
                        //         preventing concurrency across MoveNextAsync and GetAsyncEnumerator calls and avoiding
                        //         any MoveNextAsync calls before all enumerators are acquired (or an exception has
                        //         occurred doing so).

                        moveNextTasks[i] = enumerator.MoveNextAsync();
                    }

                    var whenAny = ValueTaskExt.WhenAny(moveNextTasks);

                    int active = count;

                    while (active > 0)
                    {
                        int index = await whenAny;

                        IAsyncEnumerator<TSource> enumerator = enumerators[index];
                        ValueTask<bool> moveNextTask = moveNextTasks[index];

                        if (!await moveNextTask.ConfigureAwait(false))
                        {
                            //
                            // Replace the task in our array by a completed task to make finally logic easier. Note that
                            // the WhenAnyValueTask object has a reference to our array (i.e. no copy is made), so this
                            // gets rid of any resources the original task may have held onto. However, we *don't* call
                            // whenAny.Replace to set this value, because it'd attach an awaiter to the already completed
                            // task, causing spurious wake-ups when awaiting whenAny.
                            //

                            moveNextTasks[index] = new ValueTask<bool>();

                            // REVIEW: The original implementation did not dispose eagerly, which could lead to resource
                            //         leaks when merged with other long-running sequences.

                            enumerators[index] = null; // NB: Avoids attempt at double dispose in finally if disposing fails.
                            await enumerator.DisposeAsync().ConfigureAwait(false);

                            active--;
                        }
                        else
                        {
                            TSource item = enumerator.Current;

                            //
                            // Replace the task using whenAny.Replace, which will write it to the moveNextTasks array, and
                            // will start awaiting the task. Note we don't have to write to moveNextTasks ourselves because
                            // the whenAny object has a reference to it (i.e. no copy is made).
                            //

                            whenAny.Replace(index, enumerator.MoveNextAsync());

                            yield return item;
                        }
                    }
                }
                finally
                {
                    // REVIEW: The original implementation performs a concurrent dispose, which seems undesirable given the
                    //         additional uncontrollable source of concurrency and the sequential resource acquisition. In
                    //         this modern implementation, we release resources in opposite order as we acquired them, thus
                    //         guaranteeing determinism (and mimicking a series of nested `await using` statements).

                    // REVIEW: If we decide to phase GetAsyncEnumerator and the initial MoveNextAsync calls at the start of
                    //         the operator implementation, we should make this symmetric and first await all in flight
                    //         MoveNextAsync operations, prior to disposing the enumerators.

                    var errors = default(List<Exception>);

                    for (var i = count - 1; i >= 0; i--)
                    {
                        ValueTask<bool> moveNextTask = moveNextTasks[i];
                        IAsyncEnumerator<TSource> enumerator = enumerators[i];

                        try
                        {
                            try
                            {
                                //
                                // Await the task to ensure outstanding work is completed prior to performing a dispose
                                // operation. Note that we don't have to do anything special for tasks belonging to
                                // enumerators that have finished; we swapped in a placeholder completed task.
                                //

                                // REVIEW: This adds an additional continuation to all of the pending tasks (note that
                                //         whenAny also has registered one). The whenAny object will be collectible
                                //         after all of these complete. Alternatively, we could drain via whenAny, by
                                //         awaiting it until the active count drops to 0. This saves on attaching the
                                //         additional continuations, but we need to decide on order of dispose. Right
                                //         now, we dispose in opposite order of acquiring the enumerators, with the
                                //         exception of enumerators that were disposed eagerly upon early completion.
                                //         Should we care about the dispose order at all?

                                _ = await moveNextTask.ConfigureAwait(false);
                            }
                            finally
                            {
                                if (enumerator != null)
                                {
                                    await enumerator.DisposeAsync().ConfigureAwait(false);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (errors == null)
                            {
                                errors = new List<Exception>();
                            }

                            errors.Add(ex);
                        }
                    }

                    // NB: If we had any errors during cleaning (and awaiting pending operations), we throw these exceptions
                    //     instead of the original exception that may have led to running the finally block. This is similar
                    //     to throwing from any finally block (except that we catch all exceptions to ensure cleanup of all
                    //     concurrent sequences being merged).

                    if (errors != null)
                    {
                        throw new AggregateException(errors);
                    }
                }
            }
        }
    }
}
