#define EXCESSIVE_TAG_SAFE
using BroadcastLocal.Attributes;
using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace ReadableLocal;


public readonly struct OwnCloseResult<T>
{
    private readonly BroadcastLocal<T>? own;

}
public ref struct ReadableLocalOwner<T>
{

    private BroadcastLocal<T>? own;
    public readonly bool IsValid
        => this.own is not null;

    public readonly ref readonly T Value
        => ref this.own!.Value;
    private ReadableLocalOwner(BroadcastLocal<T>? allocated)
        => this.own = allocated;
    public static ReadableLocalOwner<T> Allocate(scoped in T value, bool exchangeable = false)
    {
        var allocated = GlobalStaticPool<T>.Allocate(exchangeable);
        allocated.Publish(value);
        return new(allocated);
    }

    public readonly bool TryGetValue([System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] scoped out T value)
    {
        var own = this.own;
        if (own is null)
        {
            value = default;
            return false;
        }

        value = own.Value;
        return true;
    }

    public void FreeOnce()
    {
        this.own = this.own?.OwnerExitReturn();
    }
    public void Dispose()
    {
        var current = this;
        while (current.IsValid)
            current.FreeOnce();
        this = default;
    }
}

public ref struct ReadableLocalReader<T>
{
    private BroadcastLocal<T>? read;

    public readonly T Value
        => this.read!.Value;
    public readonly bool IsValid
    {
        get
        {
            var read = this.read;
            return read is not null && read.IsValid;
        }
    }
    internal ReadableLocalReader(BroadcastLocal<T> read)
        => this.read = read;

    public readonly bool TryGetValue([System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] scoped out T value)
    {
        var read = this.read;
        if (read is null)
        {
            value = default;
            return false;
        }

        value = read.Value;
        return true;
    }
    public bool TryExchange(scoped in T value)
    {
        var read = this.read;
        if (read is null)
            return false;
        var ret = read.TryExchange(value);
        if (ret is null)
            return false;

        ret.ValueReaderExitReturn();
        return true;
    }

    public void Dispose()
    {
        this.read?.ValueReaderExitReturn();
        this = default;
    }

}

public ref struct ReadableLocalEnumerator<T>
{
    private BroadcastLocal<T>.PoolSegment? currentPool;
    private BroadcastLocal<T>? current;
    private int currentIndex;

    public readonly ReadableLocalReader<T> Current
        => new(current!);

    internal ReadableLocalEnumerator(BroadcastLocal<T>.PoolSegment pool, int index)
    {
        this.currentPool = pool;
        this.currentIndex = index;
        this.current = null;
    }

    public ReadableLocalEnumerator<T> GetEnumerator()
        => new(this.currentPool!, this.currentIndex);
    public bool MoveNext()
        => BroadcastLocal<T>.PoolSegment.TryMoveNext(ref this.currentPool, ref this.currentIndex, ref this.current);

    public void Dispose()
    {
        
        this = default;
    }
}

[StructLayout(LayoutKind.Sequential)]
internal sealed partial class BroadcastLocal<T>
{
#if DEBUG

    private static int idCount = 0;
    private int ID { get; } = Interlocked.Increment(ref idCount);
#endif

    
    #region ManagedByPoolSegment
    private PoolSegment pool = null!;
    private int freeNode;
    private int fromIndex;

    private BroadcastLocal<T>? exchangerSlot;

    #endregion
    //#region padding1
    //private long padding0;
    //private (long, long, long, long) padding1;
    //#endregion
    private ReadPublishMutual syncState;// 8
    private ReaderOwnerAwaitTool? awaitTool;
    private BroadcastLocal<T>? nextContinuation;
    private Action SelfInvokeDisposerAction { get; }

    //#region padding2
    //private long padding2;
    //private (long, long, long, long) padding3;
    //#endregion
    private SyncFieldModification<T>? disposer;
    private (BroadcastLocal<T>? closer, SyncFieldModification<T>? beforeModify, SyncFieldModification<T>? afterModify) closerModification;
    //private ContinuationContext[] continuationContexts = [];
    private Continuations continuations = new();
    //private int continuationContextCount = 0;// 4
    private long version;// 4


    private T value = default!;

    internal ref readonly T Value
        => ref this.value;

    private BroadcastLocal()
    {
        this.SelfInvokeDisposerAction = this.InvokeModifiers;
    }

    [PhaseFlow(
        PhaseFlowKind.Clean,
        Transfer = PhaseFlowKind.ValueReader)]
    internal void Publish(scoped in T value)
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().IsNobody);
        System.Diagnostics.Debug.Assert(PoolSegment.GetExchanger(this) is null);
#endif
        ReadPublishMutual.PublishEnter(ref this.syncState);
    }
    internal void Standby()
    {
        ReadPublishMutual.Standby(ref this.syncState);
    }

    internal void CancelStandby()
    {
        ReadPublishMutual.CancelStandby(ref this.syncState);
    }


    [PhaseFlow(
        PhaseFlowKind.Clean,
        Transfer = PhaseFlowKind.ValueReader)]
    internal void PublishOnStandby(scoped in T value)
    {
#if DEBUG
        var state = this.syncState.VolatileRead();
        System.Diagnostics.Debug.Assert(state.IsPublishing is false);
        System.Diagnostics.Debug.Assert(state.SumCount == 0);
        System.Diagnostics.Debug.Assert(state.IsStandby);
#endif
        this.Publish(value);
    }
    [PhaseFlow(
        PhaseFlowKind.None,
        Transfer = PhaseFlowKind.ValueReader,
        WhenReturnFailure = PhaseFlowKind.None)]
    internal bool TryValueReadEnter()
    {
        return ReadPublishMutual.TryValueReaderEnter(ref this.syncState);
    }

    [PhaseFlow(
        PhaseFlowKind.None,
        Transfer = PhaseFlowKind.ContainerReader,
        WhenReturnFailure = PhaseFlowKind.None)]
    internal bool TryContainerReadEnter()
    {
        return ReadPublishMutual.TryContainerReaderEnter(ref this.syncState);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [PhaseFlow(
        PhaseFlowKind.ContainerReader,
        Transfer = PhaseFlowKind.Disposed,
        WhenReturnFailure = PhaseFlowKind.ContainerReader)]
    internal bool TryFastClose()
        => ReadPublishMutual.TryFastClose(ref this.syncState);

    [PhaseFlow(PhaseFlowKind.AnyReader)]
    [return: PhaseFlow(
        require: PhaseFlowKind.None,
        Transfer = PhaseFlowKind.CloseOwner | PhaseFlowKind.ValueReader | PhaseFlowKind.AnotherHandle,
        WhenReturnFailure = PhaseFlowKind.None
    )]
    internal BroadcastLocal<T>? TryCloserEnter()
    {
        var current = this;
        do
        {
#if DEBUG
            System.Diagnostics.Debug.Assert(current.syncState.VolatileRead().IsNobody is false);
#endif
            var exchanger = PoolSegment.CloseExchanger(current);
            if (exchanger is null)
                return ReadPublishMutual.TryPublishExitAndValueReaderEnter(ref current.syncState)
                    ? current
                    : null;
            current = exchanger;
        } while (true);
    }

   
    [PhaseFlow(
        PhaseFlowKind.ValueReader,
        Transfer = PhaseFlowKind.Disposed)]
    internal void ValueReaderExitReturn()
    {
        this.MoveValueReaderToContainerReader();
        this.ContainerReaderExitReturn();
    }
    
    [PhaseFlow(
        PhaseFlowKind.ContainerReader,
        Transfer = PhaseFlowKind.Disposed)]
    internal void ContainerReaderExitReturn()
    {
        var res = ReadPublishMutual.ContainerReaderExit(ref this.syncState);
        if (res.IsNobody)
        {
            this.Reset();
            this.Return();
        }
    }
    [PhaseFlow(
        PhaseFlowKind.ValueReader,
        Transfer = PhaseFlowKind.ContainerReader)]
    private void MoveValueReaderToContainerReader()
    {
        var res = ReadPublishMutual.MoveValueReaderToContainerReader(ref this.syncState);
#if DEBUG
        System.Diagnostics.Debug.Assert(res.ValueReaderCount < int.MaxValue);
#endif
        if (res.ValueReaderCount == 0 && res.IsPublishing is false)
            ReaderOwnerAwaitTool.SignalToOwner(ref this.awaitTool);
    }

    [PhaseFlow(PhaseFlowKind.CloseOwner)]
    private void Reset()
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().IsNobody);
#endif
        this.version++;
        this.continuationContextCount = 0;
        PoolSegment.GetExchanger(this)?.ValueReaderExitReturn();
        ReaderOwnerAwaitTool.Reset(ref this.awaitTool);
        this.nextContinuation = null;
    }
    // 3
    [PhaseFlow(
        PhaseFlowKind.CloseOwner,
        Transfer = PhaseFlowKind.Clean)]
    private void Return()
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().IsNobody);
        System.Diagnostics.Debug.Assert(Volatile.Read(ref this.awaitTool) is null);
#endif
        PoolSegment.Free(this);
    }

    [PhaseFlow(PhaseFlowKind.AnyReader)]
    [return: PhaseFlow(
        PhaseFlowKind.None,
        Transfer = PhaseFlowKind.ValueReader,
        WhenReturnFailure = PhaseFlowKind.None)]
    internal BroadcastLocal<T>? TryExchange(scoped in T value)
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().ContainerReaderCount >= 1);
#endif
        if (this.TryValueReadEnter() is false)
            return null;
        var ret = PoolSegment.TryGetExchangerLocal(this);
        if (ret is not null)
        {
            var debug_ = ReadPublishMutual.PublishExit(ref this.syncState);
#if DEBUG
            System.Diagnostics.Debug.Assert(debug_);
#endif
            ret.PublishOnStandby(value);
        }
        this.ValueReaderExitReturn();
        return ret;
    }


    [PhaseFlow(PhaseFlowKind.AnyReader)]
    internal bool TryRegisterSyncModification(SyncFieldModification<T> modification)
    {
        if (ReadPublishMutual.TryForcibleValueReaderEnter(ref this.syncState) is false)
            return false;
#if DEBUG
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().IsNobody is false);
#endif
        Continuations.AddCommon(ref this.continuations, modification);
        this.ValueReaderExitReturn();
        return true;
    }

    [PhaseFlow(PhaseFlowKind.AnyReader)]
    internal bool TryRegisterCloseChain(
        [PhaseFlow(PhaseFlowKind.AnyReader)] BroadcastLocal<T> target,
        SyncFieldModification<T> beforeModify,
        SyncFieldModification<T> afterModify)
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().IsNobody is false);
#endif
        if (ReadPublishMutual.TryForcibleValueReaderEnter(ref this.syncState) is false)
            return false;
        if (ReadPublishMutual.TryForcibleValueReaderEnter(ref target.syncState) is false)
        {
            this.ValueReaderExitReturn();
            return false;
        }

        Continuations.AddCloseChain(ref this.continuations, target, beforeModify, afterModify);

        target.ValueReaderExitReturn();
        this.ValueReaderExitReturn();
        return true;
    }

    private void InvokeModifiers()
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().IsPublishing is false);
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().IsNobody is false);
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().ValueReaderCount == 0);
#endif
        var continuations = this.continuations;
        var chain = continuations.CloseChain;
        foreach(var (iTarget, iBefore, iAfter) in chain)
        {
            var closed = iTarget.TryCloserEnter();
            if (closed is null)
                continue;
            
        }
        var count = this.continuationContextCount;
        var arr = this.continuationContexts;
        if ((uint)count > (uint)arr.Length)
        {
#if DEBUG
            System.Diagnostics.Debug.Fail(nameof(InvokeModifiers));
#endif

            return;
        }
        var span = arr.AsSpan(0, count);
        var (closerTemp, before, after) = this.closerModification;
#if DEBUG
        System.Diagnostics.Debug.Assert(closerTemp is not null);
#endif
        if (closerTemp is null)
            closerTemp = this;
        var closer = new CloserInfo<T>(closerTemp);
        before?.Invoke(ref this.value, closer);
        for(var i = 0; i < span.Length; ++i)
        {
            var ctx = span[i];
            if (ctx.Target is null)
                ctx.BeforeModificationORCommon?.Invoke(ref this.value, closer);
            else
            {
                var closed = ctx.Target.TryCloserEnter();
                if (closed is not null)
                {
                    closed.SetCloserModifications(this, ctx.BeforeModificationORCommon, ctx.AfterModification);
                    closed.ValueReaderExitReturn();
                }
            }
        }
        after?.Invoke(ref this.value, closer);
        
        this.ContainerReaderExitReturn();
    }

    [PhaseFlow(PhaseFlowKind.CloseOwner | PhaseFlowKind.ValueReader)]
    private void SetCloserModifications(BroadcastLocal<T> target, SyncFieldModification<T>? beforeModify, SyncFieldModification<T>? afterModify)
    {
        this.closerModification = (target, beforeModify, afterModify);
#if DEBUG
        System.Diagnostics.Debug.Assert(this.syncState.VolatileRead().ValueReaderCount != 0);
#endif
    }

  
    public override string ToString()
        =>
#if DEBUG
        $"id: {this.ID,5}, " +
#endif
        $"reader: {this.syncState.ContainerReaderCount,3}, hasOwner: {this.syncState.IsPublishing,5}, value: {this.value}"
#if DEBUG
        + $", root: {this.pool}";
#endif
    internal readonly struct ContinuationContext
    {

        internal readonly BroadcastLocal<T>? Target { get; }
        internal readonly SyncFieldModification<T>? BeforeModificationORCommon { get; }
        
        internal readonly SyncFieldModification<T>? AfterModification { get; }
        private ContinuationContext(
            BroadcastLocal<T>? target,
            SyncFieldModification<T>? beforeModificationORCommon,
            SyncFieldModification<T>? afterModification)
        {
            this.Target = target;
            this.BeforeModificationORCommon = beforeModificationORCommon;
            this.AfterModification = afterModification;
        }

        internal static ContinuationContext CreateCommon(SyncFieldModification<T> commonModification)
            => new(default, beforeModificationORCommon: commonModification, default);
        internal static ContinuationContext CreateChain(
            BroadcastLocal<T> target,
            SyncFieldModification<T>? beforeModify,
            SyncFieldModification<T>? afterModify)
            => new(default, beforeModificationORCommon: beforeModify, afterModify);
    }

    private struct Continuations()
    {
        private const int InitialContinuationsArraySize = 1 << 4;
        private const int closeChainOffset = 0;
        private const long closeChainIncrement = 1L << closeChainOffset;
        private const int commonOffset = sizeof(int) * 8;
        private const long commonIncrement = 1L << commonOffset;
        private (BroadcastLocal<T> target, SyncFieldModification<T>? beforeModify, SyncFieldModification<T>? afterModify)[] closeChains = [];
        private (BroadcastLocal<T>? argument, SyncFieldModification<T> modify)[] commonModifications = [];
        private long doubleCount = 0;
        public readonly bool IsEmpty
            => this.doubleCount == default;
        public readonly Span<(BroadcastLocal<T>? argument, SyncFieldModification<T> modify)> Common
            => this.commonModifications.AsSpan(0, (int)(this.doubleCount >>> commonOffset));
        public readonly Span<(BroadcastLocal<T> target, SyncFieldModification<T>? beforeModify, SyncFieldModification<T>? afterModify)> CloseChain
            => this.closeChains.AsSpan(0, (int)(this.doubleCount >>> closeChainOffset));
        public static void AddCloseChain(
            scoped ref Continuations self,
            BroadcastLocal<T> target,
            SyncFieldModification<T>? beforeModify,
            SyncFieldModification<T>? afterModify)
            => Add(ref self, (target, beforeModify, afterModify));
        public static void AddCommon(
            scoped ref Continuations self,
            SyncFieldModification<T> modify,
            BroadcastLocal<T>? argument = null)
            => Add(ref self, (argument, modify));
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ref TItem[] SwitchArrayRef<TItem>(ref Continuations self)
            where TItem : struct
        {
            if (typeof(TItem) == typeof((BroadcastLocal<T>?, SyncFieldModification<T>)))
                return ref Unsafe.As<(BroadcastLocal<T>?, SyncFieldModification<T>)[], TItem[]>(ref self.commonModifications);
#if DEBUG
            else if (typeof(TItem) != typeof((BroadcastLocal<T>, SyncFieldModification<T>?, SyncFieldModification<T>?)))
                throw new InvalidCastException();
#endif
            else
                return ref Unsafe.As<(BroadcastLocal<T>, SyncFieldModification<T>?, SyncFieldModification<T>?)[], TItem[]>(ref self.closeChains);
        }
        private  static void Add<TItem>(
            scoped ref Continuations self,
            TItem item)
            where TItem : struct
        {
            var increment = typeof(TItem) == typeof((BroadcastLocal<T>?, SyncFieldModification<T>))
                ? commonIncrement
                : closeChainIncrement;

            var idx = Interlocked.Add(ref self.doubleCount, increment);
            do
            {
                scoped ref var arrLocation = ref SwitchArrayRef<TItem>(ref self);
                var arr = Volatile.Read(ref arrLocation);
                scoped ref var slot = ref (uint)idx < (uint)arr.Length
                    ? ref arr[idx]
                    : ref ExpandGetSlot(ref arrLocation, idx, out arr);
                slot = item;
                if (object.ReferenceEquals(arr, Volatile.Read(ref arrLocation)))
                    break;
            } while (true);
            return;
            static ref TItem ExpandGetSlot(
                scoped ref TItem[] arrayLocation,
                long index,
                scoped out TItem[] array)
            {
                var arr = Volatile.Read(ref arrayLocation);
                while (true)
                {
                    if ((uint)index < (uint)arr.Length)
                    {
                        array = arr;
                        return ref arr[index];
                    }

                    TItem[] newArr;
                    var lockObj = arr;
                    lock (lockObj)
                    {
                        if (object.ReferenceEquals(arr, arr = arrayLocation) is false)
                            continue;

                        newArr = ArrayPool<TItem>.Shared.Rent(Math.Max(InitialContinuationsArraySize, arr.Length * 2));
                        arr.AsSpan().CopyTo(newArr);
                        arrayLocation = newArr;
                    }
                    ReturnRentedArray(arr);
                    arr = newArr;
                    continue;
                }
            }
        }

        /// <summary>
        /// not thread safe
        /// </summary>
        /// <param name="self"></param>
        public static void Reset(scoped ref Continuations self)
        {
            ReturnRentedArray(self.commonModifications);
            ReturnRentedArray(self.closeChains);
            self = new();
        }
        private static void ReturnRentedArray<TItem>(TItem[] array)
            where TItem : struct
            => ArrayPool<TItem>.Shared.Return(array);
    }
}
public readonly struct CloserInfo<T>
{
    private readonly BroadcastLocal<T> closer;
    internal CloserInfo(BroadcastLocal<T> closer)
        => this.closer = closer;
    public readonly ref readonly T Field
        => ref this.closer.Value;

    
}

public delegate void SyncFieldModification<T>(scoped ref T field, CloserInfo<T> closer);