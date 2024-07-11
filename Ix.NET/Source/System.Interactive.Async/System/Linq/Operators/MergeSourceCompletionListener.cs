using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace System.Linq;

internal interface ICompletionQueue
{
    bool IsCompleted { get; }
    int DrainCompletedIndex();
    void OnCompleted(int index);
}

internal interface ICompletionQueueFactory<out TQueueStrategy> where TQueueStrategy : ICompletionQueue
{
    TQueueStrategy Create(int size);
}

internal readonly struct CompletionQueueFair : ICompletionQueue
{
    private readonly Queue<int> _ready;

    public CompletionQueueFair(int size)
    {
        _ready = new Queue<int>(size); // NB: Should never exceed this length, so we won't see dynamic realloc.
    }

    public bool IsCompleted
    {
        get
        {
            return _ready.Count > 0;
        }
    }

    public int DrainCompletedIndex()
    {
        return _ready.Dequeue();
    }

    public void OnCompleted(int index)
    {
        _ready.Enqueue(index);
    }

    public struct Factory : ICompletionQueueFactory<CompletionQueueFair>
    {
        public CompletionQueueFair Create(int size)
        {
            return new CompletionQueueFair(size);
        }
    }
}


internal readonly struct CompletionQueueUnfairFewSources : ICompletionQueue
{
    private readonly bool[] _ready;

    public CompletionQueueUnfairFewSources(int size)
    {
        _ready = new bool[size];
    }

    public bool IsCompleted
    {
        get
        {
            for (var i = 0; i < _ready.Length; i++)
            {
                if (_ready[i])
                    return true;
            }

            return false;
        }
    }

    public int DrainCompletedIndex()
    {
        for (var i = 0; i < _ready.Length; i++)
        {
            if (_ready[i])
            {
                _ready[i] = false;
                return i;
            }
        }

        // We should never reach this point if used correctly
        throw new Exception("Tried draining results, but none were available");
    }

    public void OnCompleted(int index)
    {
        Debug.Assert(!_ready[index]);
        _ready[index] = true;
    }

    public struct Factory : ICompletionQueueFactory<CompletionQueueUnfairFewSources>
    {
        public CompletionQueueUnfairFewSources Create(int size)
        {
            return new CompletionQueueUnfairFewSources(size);
        }
    }
}

internal readonly struct CompletionQueueUnfairManySources : ICompletionQueue
{
    private readonly LinkedList<Entry> _readySorted;
    private readonly LinkedListNode<Entry>[] _ready;

    public bool IsCompleted
    {
        get
        {
            return _readySorted.First.Value.HasResult;
        }
    }

    public int DrainCompletedIndex()
    {
        var node = _readySorted.First;
        Debug.Assert(node.Value.HasResult);
#if NET5_0_OR_GREATER
                    ref var value = ref node.ValueRef;
                    value.HasResult = false;
#else
        node.Value.HasResult = false; // signal that this node has a result
#endif
        _readySorted.RemoveFirst();
        _readySorted.AddLast(node);
        return node.Value.TaskIndex;
    }

    private CompletionQueueUnfairManySources(int size)
    {
        _readySorted = new LinkedList<Entry>();
        _ready = new LinkedListNode<Entry>[size];

        for (var i = 0; i < size; i++)
        {
            _readySorted.AddLast(new Entry(i));
            _ready[i] = _readySorted.Last;
        }
    }

    public struct Factory : ICompletionQueueFactory<CompletionQueueUnfairManySources>
    {
        public CompletionQueueUnfairManySources Create(int size)
        {
            return new CompletionQueueUnfairManySources(size);
        }
    }

#if NET5_0_OR_GREATER
    private struct Entry
#else
    private sealed class Entry
#endif
    {
        public readonly int TaskIndex;
        public bool HasResult;

        public Entry(int taskIndex)
        {
            TaskIndex = taskIndex;
            HasResult = false;
        }
    }

    public void OnCompleted(int index)
    {
        var node = _ready[index];

#if NET5_0_OR_GREATER
        ref var value = ref node.ValueRef;
        value.HasResult = true;
#else
        node.Value.HasResult = true; // signal that this node has a result
#endif

        _readySorted.Remove(node);
        _readySorted.AddFirst(node);
    }
}

internal sealed class MergeSourceCompletionListener<T, TQueueStrategyFactory, TQueueStrategy>
    where TQueueStrategyFactory : struct, ICompletionQueueFactory<TQueueStrategy>
    where TQueueStrategy : struct, ICompletionQueue
{
    /// <summary>
    /// The tasks to await. Entries in this array may be replaced using <see cref="Replace"/>.
    /// </summary>
    private readonly ValueTask<T>[] _tasks;

    /// <summary>
    /// Array of cached delegates passed to awaiters on tasks. These delegates have a closure containing the task index.
    /// </summary>
    private readonly Action[] _onReady;

    private TQueueStrategy _queueStrategy;
    private readonly object _lock = new();

    /// <summary>
    /// Callback of the current awaiter, if any.
    /// </summary>
    /// <remarks>
    /// Protected for reads and writes by a lock on <see cref="_lock"/>.
    /// </remarks>
    private Action _onCompleted;

    /// <summary>
    /// Creates a when any task around the specified tasks.
    /// </summary>
    /// <param name="tasks">Initial set of tasks to await.</param>
    public MergeSourceCompletionListener(ValueTask<T>[] tasks)
    {
        _tasks = tasks;

        var n = tasks.Length;
        _queueStrategy = new TQueueStrategyFactory().Create(n);
        _onReady = new Action[n];

        for (var i = 0; i < n; i++)
        {
            //
            // Cache these delegates, for they have closures (over `this` and `index`), and we need them
            // for each replacement of a task, to hook up the continuation.
            //

            int index = i;
            _onReady[index] = () => OnReady(index);
        }
    }

    /// <summary>
    /// Start awaiting the tasks. This is done separately from the constructor to avoid complexity dealing
    /// with handling concurrent callbacks to the current instance while the constructor is running.
    /// </summary>
    public void Start()
    {
        for (var i = _tasks.Length - 1; i >= 0; i--)
        {
            // Register a callback with the task, which will enqueue the index of the completed task
            // for consumption by awaiters.
            Replace(i, _tasks[i]);
        }
    }

    /// <summary>
    /// Gets an awaiter to await completion of any of the awaited tasks, returning the index of the completed
    /// task. When sequentially awaiting the current instance, task indices are yielded with preference for completed tasks appearing aerly in the source array.
    /// If all tasks have completed and been observed by awaiting the current instance, the awaiter
    /// never returns on a subsequent attempt to await the completion of any task. The caller is responsible
    /// for bookkeeping that avoids awaiting this instance more often than the number of pending tasks.
    /// </summary>
    /// <returns>Awaiter to await completion of any of the awaited task.</returns>
    /// <remarks>This class only supports a single active awaiter at any point in time.</remarks>
    public Awaiter GetAwaiter() => new Awaiter(this);

    /// <summary>
    /// Replaces the (completed) task at the specified <paramref name="index"/> and starts awaiting it.
    /// </summary>
    /// <param name="index">The index of the parameter to replace.</param>
    /// <param name="task">The new task to store and await at the specified index.</param>
    public void Replace(int index, in ValueTask<T> task)
    {
        _tasks[index] = task;

        if (task.IsCompleted)
        {
            _onReady[index]();
        }
        else
        {
            task.ConfigureAwait(false).GetAwaiter().OnCompleted(_onReady[index]);
        }
    }

    /// <summary>
    /// Called when any task has completed (thus may run concurrently).
    /// </summary>
    /// <param name="index">The index of the completed task in <see cref="_tasks"/>.</param>
    private void OnReady(int index)
    {
        Action onCompleted = null;

        lock (_lock)
        {
            // Move node that is ready to the head of the list, and set its value to true to signify a result is available
            _queueStrategy.OnCompleted(index);

            // If there's a current awaiter, we'll steal its continuation action and invoke it. By setting
            // the continuation action to null, we avoid waking up the same awaiter more than once. Any
            // task completions that occur while no awaiter is active will end up being enqueued in _ready.
            if (_onCompleted != null)
            {
                onCompleted = _onCompleted;
                _onCompleted = null;
            }
        }

        onCompleted?.Invoke();
    }

    /// <summary>
    /// Invoked by awaiters to check if any task has completed, in order to short-circuit the await operation.
    /// </summary>
    /// <returns><c>true</c> if any task has completed; otherwise, <c>false</c>.</returns>
    private bool IsCompleted()
    {
        lock (_lock)
        {
            return _queueStrategy.IsCompleted;
        }
    }

    /// <summary>
    /// Gets the index of the earliest task that has completed, used by the awaiter. After stealing an index from
    /// the ready queue (by means of awaiting the current instance), the user may chose to replace the task at the
    /// returned index by a new task, using the <see cref="Replace"/> method.
    /// </summary>
    /// <returns>Index of the earliest task that has completed.</returns>
    private int GetResult()
    {
        lock (_lock)
        {
            Debug.Assert(_queueStrategy.IsCompleted);
            return _queueStrategy.DrainCompletedIndex();
        }
    }

    /// <summary>
    /// Register a continuation passed by an awaiter.
    /// </summary>
    /// <param name="action">The continuation action delegate to call when any task is ready.</param>
    private void OnCompleted(Action action)
    {
        bool shouldInvoke = false;

        lock (_lock)
        {
            //
            // Check if we have anything ready (which could happen in the short window between checking
            // for IsCompleted and calling OnCompleted). If so, we should invoke the action directly. Not
            // doing so would be a correctness issue where a task has completed, its index was enqueued,
            // but the continuation was never called (unless another task completes and calls the action
            // delegate, whose subsequent call to GetResult would pick up the lost index).
            //

            if (IsCompleted())
            {
                shouldInvoke = true;
            }
            else
            {
                Debug.Assert(_onCompleted == null, "Only a single awaiter is allowed.");

                _onCompleted = action;
            }
        }

        //
        // NB: We assume this case is rare enough (IsCompleted and OnCompleted happen right after one
        //     another, and an enqueue should have happened right in between to go from an empty to a
        //     non-empty queue), so we don't run the risk of triggering a stack overflow due to
        //     synchronous completion of the await operation (which may be in a loop that awaits the
        //     current instance again).
        //

        if (shouldInvoke)
        {
            action();
        }
    }

    /// <summary>
    /// Awaiter type used to await completion of any task.
    /// </summary>
    public struct Awaiter : INotifyCompletion
    {
        private readonly MergeSourceCompletionListener<T, TQueueStrategyFactory, TQueueStrategy> _parent;

        public Awaiter(MergeSourceCompletionListener<T, TQueueStrategyFactory, TQueueStrategy> parent) => _parent = parent;

        public bool IsCompleted => _parent.IsCompleted();
        public int GetResult() => _parent.GetResult();
        public void OnCompleted(Action action) => _parent.OnCompleted(action);
    }
}
