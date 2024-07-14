using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace System.Linq;

internal interface ICompletionQueue
{
    int Size { get; }
    bool IsCompleted { get; }
    int DrainCompletedIndex();
    // May be invoked concurrently to any other function or property of this interface. An invocation for a given index X will not overlap with any other invocation of the same index X
    void OnCompleted(int index);
}

// Fair completion strategy optimised for exactly two input sources.
// Compared to the more generic version, we can do without a ConcurrentQueue here, and instead represent a concurrent queue via a simple int property that represents all possible queue states
internal struct CompletionQueueFairTwoSources : ICompletionQueue
{
    private int _queueState = QueueState.Empty;

    public CompletionQueueFairTwoSources(int size)
    {
        if (size != 2)
            throw new ArgumentException(nameof(size));
    }

    public int Size => 2;

    public bool IsCompleted
    {
        get
        {
            return Volatile.Read(ref _queueState) != 0;
        }
    }

    public int DrainCompletedIndex()
    {
        int state;
        int newState;
        int result;
        do
        {
            state = Volatile.Read(ref _queueState);
            switch (state)
            {
                case QueueState.Zero:
                    // [0] => Drain single element
                    newState = QueueState.Empty;
                    result = 0;
                    break;
                case QueueState.One:
                    // [1] => Drain single element
                    newState = QueueState.Empty;
                    result = 1;
                    break;
                case QueueState.ZeroOne:
                    // [0,1] => Drain 0, 1 remains in the queue
                    newState = QueueState.One;
                    result = 0;
                    break;
                case QueueState.OneZero:
                    // [1,0] => Drain 1, 0 remains in the queue
                    newState = QueueState.Zero;
                    result = 1;
                    break;
                default:
                    throw new Exception();
            }
        } while (Interlocked.CompareExchange(ref _queueState, newState, state) != state);

        return result;
    }

    public void OnCompleted(int index)
    {
        switch (index)
        {
            case 0:
                OnCompleted0();
                break;
            case 1:
                OnCompleted1();
                break;
            default:
                throw new Exception();
        }
    }

    private void OnCompleted0()
    {
        int state;
        int newState;
        do
        {
            state = Volatile.Read(ref _queueState);
            switch (state)
            {
                case QueueState.Empty:
                    // Queue was empty, so new queue just contains our element [0]
                    newState = QueueState.Zero;
                    break;
                case QueueState.One:
                    // Queue was [1], so new queue should be [1,0]
                    newState = QueueState.OneZero;
                    break;
                default:
                    throw new Exception();
            }
        } while (Interlocked.CompareExchange(ref _queueState, newState, state) != state);
    }

    private void OnCompleted1()
    {
        int state;
        int newState;
        do
        {
            state = Volatile.Read(ref _queueState);
            switch (state)
            {
                case QueueState.Empty:
                    // Queue was empty, so new queue just contains our element [1]
                    newState = QueueState.One;
                    break;
                case QueueState.Zero:
                    // Queue was [0], so new queue should be [0,1]
                    newState = QueueState.ZeroOne;
                    break;
                default:
                    throw new Exception();
            }
        } while (Interlocked.CompareExchange(ref _queueState, newState, state) != state);
    }

    private static class QueueState
    {
        public const int Empty = 0;       // Represents an empty queue
        public const int Zero = 1;        // Represents a queue with one element: Index 0
        public const int One = 2;         // Represents a queue with one element: Index 1
        public const int ZeroOne = 3;     // Represents a queue with two elements: [0, 1]
        public const int OneZero = 4;     // Represents a queue with two elements: [1, 0]
    }
}

internal readonly struct CompletionQueueFair : ICompletionQueue
{
    private readonly ConcurrentQueue<int> _ready;

    public CompletionQueueFair(int size)
    {
        _ready = new ConcurrentQueue<int>();
        Size = size;
    }

    public int Size { get; }

    public bool IsCompleted
    {
        get
        {
            return !_ready.IsEmpty;
        }
    }

    public int DrainCompletedIndex()
    {
        if (!_ready.TryDequeue(out var result))
            throw new Exception();
        return result;
    }

    public void OnCompleted(int index)
    {
        _ready.Enqueue(index);
    }
}

internal readonly struct CompletionQueueUnfairFewSources : ICompletionQueue
{
    private readonly int[] _ready;

    public CompletionQueueUnfairFewSources(int size)
    {
        _ready = new int[size];
        Size = size;
    }

    public int Size { get; }

    public bool IsCompleted
    {
        get
        {
            for (var i = 0; i < _ready.Length; i++)
            {
                if (Volatile.Read(ref _ready[i]) == 1)
                    return true;
            }

            return false;
        }
    }

    public int DrainCompletedIndex()
    {
        for (var i = 0; i < _ready.Length; i++)
        {
            if (Interlocked.CompareExchange(ref _ready[i], 0, 1) == 1)
            {
                return i;
            }
        }

        // We should never reach this point if used correctly
        throw new Exception("Tried draining results, but none were available");
    }

    public void OnCompleted(int index)
    {
        Debug.Assert(Volatile.Read(ref _ready[index]) == 0);
        Volatile.Write(ref _ready[index], 1);
    }
}

internal readonly struct CompletionQueueUnfairManySources : ICompletionQueue
{
    private readonly object _lock = new();
    private readonly LinkedList<Entry> _readySorted;
    private readonly LinkedListNode<Entry>[] _ready;

    public CompletionQueueUnfairManySources(int size)
    {
        Size = size;
        _readySorted = new LinkedList<Entry>();
        _ready = new LinkedListNode<Entry>[size];

        for (var i = 0; i < size; i++)
        {
            _readySorted.AddLast(new Entry(i));
            _ready[i] = _readySorted.Last;
        }
    }

    public int Size { get; }

    public bool IsCompleted
    {
        get
        {
            lock (_lock)
            {
                return _readySorted.First.Value.HasResult;
            }
        }
    }

    public int DrainCompletedIndex()
    {
        lock (_lock)
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
    }

    public void OnCompleted(int index)
    {
        lock (_lock)
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
}

internal sealed class MergeSourceCompletionListener<T, TQueueStrategy>
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
    public MergeSourceCompletionListener(ValueTask<T>[] tasks, TQueueStrategy queueStrategy)
    {
        if (queueStrategy.Size != tasks.Length)
            throw new ArgumentException($"Queue strategy must be compatible for size {tasks.Length}", nameof(queueStrategy));

        _tasks = tasks;

        var n = tasks.Length;
        _queueStrategy = queueStrategy;
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
        _queueStrategy.OnCompleted(index);
        var onCompleted = Interlocked.Exchange(ref _onCompleted, null);
        onCompleted?.Invoke();
    }

    /// <summary>
    /// Invoked by awaiters to check if any task has completed, in order to short-circuit the await operation.
    /// </summary>
    /// <returns><c>true</c> if any task has completed; otherwise, <c>false</c>.</returns>
    private bool IsCompleted()
    {
        return _queueStrategy.IsCompleted;
    }

    /// <summary>
    /// Gets the index of the earliest task that has completed, used by the awaiter. After stealing an index from
    /// the ready queue (by means of awaiting the current instance), the user may chose to replace the task at the
    /// returned index by a new task, using the <see cref="Replace"/> method.
    /// </summary>
    /// <returns>Index of the earliest task that has completed.</returns>
    private int GetResult()
    {
        Debug.Assert(_queueStrategy.IsCompleted);
        return _queueStrategy.DrainCompletedIndex();
    }

    /// <summary>
    /// Register a continuation passed by an awaiter.
    /// </summary>
    /// <param name="action">The continuation action delegate to call when any task is ready.</param>
    private void OnCompleted(Action action)
    {
        if (IsCompleted())
        {
            action();
            return;
        }

        Volatile.Write(ref _onCompleted, action);
        if (IsCompleted())
        {
            var onCompleted = Interlocked.Exchange(ref _onCompleted, null);
            if (onCompleted != null)
            {
                Debug.Assert(IsCompleted());
                Debug.Assert(ReferenceEquals(onCompleted, action));
                onCompleted();
            }
        }
    }

    /// <summary>
    /// Awaiter type used to await completion of any task.
    /// </summary>
    public struct Awaiter : INotifyCompletion
    {
        private readonly MergeSourceCompletionListener<T, TQueueStrategy> _parent;

        public Awaiter(MergeSourceCompletionListener<T, TQueueStrategy> parent) => _parent = parent;

        public bool IsCompleted => _parent.IsCompleted();
        public int GetResult() => _parent.GetResult();
        public void OnCompleted(Action action) => _parent.OnCompleted(action);
    }
}
