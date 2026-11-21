using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace BroadcastLocal;

internal sealed class ReaderOwnerAwaitTool
{

    private static WaitCallback callback = static act => (act as Action)?.Invoke();
    private enum SignalSwitch : byte
    {
        Reader,
        Owner,
    }
    private static ManualResetEventSlim SignedSharedWaiter { get; } = new(true);
    private static Task SignedSharedTask { get; } = Task.CompletedTask;
    private static ReaderOwnerAwaitTool SignedShared { get; } = new()
    {
        readerWait = SignedSharedWaiter,
        readerContinuationUnion = SignedSharedTask,
        ownerWait = SignedSharedWaiter,
        ownerContinuationUnion = SignedSharedTask,
    };

    private static ReaderOwnerAwaitTool? pool;
    private ReaderOwnerAwaitTool? freeNode;

    private static ReaderOwnerAwaitTool Rent()
    {
        var top = Volatile.Read(ref pool);
        while (true)
        {
            if (top is null)
                return new();

            var next = Interlocked.Exchange(ref top.freeNode, top);
            if (object.ReferenceEquals(next, top))
            {
                top = Volatile.Read(ref pool);
                continue; 
            }

            if (object.ReferenceEquals(
                top,
                top = Interlocked.CompareExchange(ref pool, next, top)
            ))
                return top;
        }
    }

    private void Return()
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(object.ReferenceEquals(this, this.freeNode));
#endif
        var waiter = Interlocked.CompareExchange(ref this.readerWait, null, SignedSharedWaiter);
        if (object.ReferenceEquals(waiter, SignedSharedWaiter) is false)
            waiter?.Dispose();
        waiter = Interlocked.CompareExchange(ref this.ownerWait, null, SignedSharedWaiter);
        if (object.ReferenceEquals(waiter, SignedSharedWaiter) is false)
            waiter?.Dispose();

        this.readerContinuationUnion = null;
        this.ownerContinuationUnion = null;
        var next = Interlocked.Exchange(ref pool, this);
        Volatile.Write(ref this.freeNode, next);
    }

    // union(Action | Task | TaskCompletionSource<bool>)
    // null == non signaled
    private ManualResetEventSlim? readerWait;
    private object? readerContinuationUnion;

    private ManualResetEventSlim? ownerWait;
    private object? ownerContinuationUnion;

    public static bool IsReaderCompleted(scoped ref ReaderOwnerAwaitTool? location)
        => IsCompleted(ref location, SignalSwitch.Reader);
    public static bool IsOwnerCompleted(scoped ref ReaderOwnerAwaitTool? location)
        => IsCompleted(ref location, SignalSwitch.Owner);
    private static bool IsCompleted(scoped ref ReaderOwnerAwaitTool? location, SignalSwitch s)
        => object.ReferenceEquals(Volatile.Read(ref location), SignedShared) 
        || Volatile.Read(ref location) is ReaderOwnerAwaitTool tool 
            && s switch
            {
                SignalSwitch.Reader => 
                    (tool.readerWait?.IsSet ?? false)
                    || object.ReferenceEquals(tool.readerContinuationUnion, SignedSharedTask),
                SignalSwitch.Owner => 
                    (tool.ownerWait?.IsSet ?? false)
                    || object.ReferenceEquals(tool.ownerContinuationUnion, SignedSharedTask),
                _ => Throw() is not null and not null,
            };
    public static void SignalToReader(scoped ref ReaderOwnerAwaitTool? location)
        => SetSignal(ref location, SignalSwitch.Reader);

    public static void SignalToOwner(scoped ref ReaderOwnerAwaitTool? location)
        => SetSignal(ref location, SignalSwitch.Owner);
    private static void SetSignal(scoped ref ReaderOwnerAwaitTool? location, SignalSwitch s)
    {
        var tool = Interlocked.CompareExchange(ref location, SignedShared, null);
#if DEBUG
        System.Diagnostics.Debug.Assert(object.ReferenceEquals(tool, SignedShared) is false);
#endif
        if (tool is null)
            return;
        (s switch
        {
            SignalSwitch.Reader => Interlocked.CompareExchange(ref tool.readerWait, SignedSharedWaiter, null),
            SignalSwitch.Owner => Interlocked.CompareExchange(ref tool.ownerWait, SignedSharedWaiter, null),
            _ => Throw()
        })?.Set();

        var continuationUnion = s switch
        {
            SignalSwitch.Reader => Interlocked.Exchange(ref tool.readerContinuationUnion, Task.CompletedTask),
            SignalSwitch.Owner => Interlocked.Exchange(ref tool.ownerContinuationUnion, Task.CompletedTask),
            _ => Throw()
        };
        
        switch (continuationUnion)
        {
            case null:
                break;
            case Action act:
                ThreadPool.UnsafeQueueUserWorkItem(callback, act);
                break;
            case TaskCompletionSource<bool> readerSource:
                readerSource.SetResult(true);
                break;
#if DEBUG
            default: 
                System.Diagnostics.Debug.Fail(nameof(continuationUnion));
                break;
#endif
        }
    }

    public static void ReaderOnCompleted(scoped ref ReaderOwnerAwaitTool? location, Action continuation)
        => OnCompleted(ref location, continuation, SignalSwitch.Reader, false);
    public static void UnsafeReaderOnCompleted(scoped ref ReaderOwnerAwaitTool? location, Action continuation)
        => OnCompleted(ref location, continuation, SignalSwitch.Reader, true);
    public static void OwnerOnCompleted(scoped ref ReaderOwnerAwaitTool? location, Action continuation)
        => OnCompleted(ref location, continuation, SignalSwitch.Owner, false);
    public static void UnsafeOwnerOnCompleted(scoped ref ReaderOwnerAwaitTool? location, Action continuation)
        => OnCompleted(ref location, continuation, SignalSwitch.Owner, true);
            
    private static void OnCompleted(scoped ref ReaderOwnerAwaitTool? location, Action continuation, SignalSwitch s, bool isUnsafe)
    {
        while (true)
        {
            var tool = GetOrRent(ref location);
            if (object.ReferenceEquals(tool, SignedShared))
                break;
            scoped ref object? unionRef = ref Unsafe.NullRef<object?>();
            var continuationUnion = s switch
            {
                SignalSwitch.Reader => unionRef = ref tool.readerContinuationUnion,
                SignalSwitch.Owner => unionRef = ref tool.ownerContinuationUnion,
                _ => Throw()
            };

            if (object.ReferenceEquals(continuationUnion, SignedSharedTask))
                break;

            Task task;
            if (isUnsafe
            && (continuationUnion = Interlocked.CompareExchange(ref unionRef, continuation, null)) is null)
                return;
            if (continuationUnion is TaskCompletionSource<bool> taskSrc)
                task = taskSrc.Task;
            else if (object.ReferenceEquals(continuationUnion, SignedSharedTask))// double check
                break;
            else
            {
                var newTaskSrc = new TaskCompletionSource<bool>();
                task = newTaskSrc.Task;
                if (continuationUnion is Action prevUnsafeContinuationAction)
                    task.GetAwaiter().UnsafeOnCompleted(prevUnsafeContinuationAction);
                if (object.ReferenceEquals(
                    continuationUnion,
                    Interlocked.CompareExchange(ref unionRef, newTaskSrc, continuationUnion)
                ) is false)
                    continue;
            }

            if (isUnsafe)
                task.GetAwaiter().UnsafeOnCompleted(continuation);
            else
                task.GetAwaiter().OnCompleted(continuation);

            return;
        }

        if (isUnsafe)
            ThreadPool.UnsafeQueueUserWorkItem(callback, continuation);
        else
            ThreadPool.QueueUserWorkItem(callback, continuation);
        return;
    }
    public static void Reset(scoped ref ReaderOwnerAwaitTool? location)
    {
        var tool = Interlocked.Exchange(ref location, null);
        if (tool is null 
        || object.ReferenceEquals(tool, SignedShared))
            return;
        tool.Return();
    }

    public static void ReaderWait(scoped ref ReaderOwnerAwaitTool? location, CancellationToken cancellationToken = default)
        => Wait(ref location, SignalSwitch.Reader, cancellationToken);
    public static void OwnerWait(scoped ref ReaderOwnerAwaitTool? location, CancellationToken cancellationToken = default)
        => Wait(ref location, SignalSwitch.Owner, cancellationToken);
    private static void Wait(scoped ref ReaderOwnerAwaitTool? location, SignalSwitch s, CancellationToken cancellationToken = default)
    {
        var tool = GetOrRent(ref location);

        ref var waiterRef = ref Unsafe.NullRef<ManualResetEventSlim>();
        var waiter = s switch
        {
            SignalSwitch.Reader => waiterRef = ref tool.readerWait,
            SignalSwitch.Owner => waiterRef = ref tool.ownerWait,
            _ => Throw()
        };
        if (waiter is null)
        {
            var newWaiter = new ManualResetEventSlim(false);
            if ((waiter = Interlocked.CompareExchange(ref waiterRef, newWaiter, null)) is null)
                waiter = newWaiter;
        }

        waiter.Wait(cancellationToken);
    }

    private static ReaderOwnerAwaitTool GetOrRent(scoped ref ReaderOwnerAwaitTool? location)
    {
        var ret = location;
        if (ret is not null)
            return ret;
        else
            return RentLoop(ref location);
        static ReaderOwnerAwaitTool RentLoop(scoped ref ReaderOwnerAwaitTool? location)
        {
            var rented = Rent();

            var ret = Interlocked.CompareExchange(ref location, rented, null);
            if (ret is null)
                return rented;
            else
            {
                rented.Return();
                return ret;
            }
        }
    }
    private static ManualResetEventSlim? Throw()
            => throw new ArgumentException(nameof(SignalSwitch));

}
