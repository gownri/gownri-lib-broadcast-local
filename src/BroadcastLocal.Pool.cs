#define EXCESSIVE_TAG_SAFE
using BroadcastLocal.Attributes;
using System;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using BroadcastLocal;
using LentCount = byte;
using TopIndex = byte;
using UPoolCount = byte;
using UTopIndex = byte;
namespace Gownri;


internal class GlobalTLSReadableLocal<T>
{
    private static readonly BroadcastLocal<T>.PoolSegment.Manager manager = new();
    [ThreadStatic]
    private static GlobalTLSReadableLocal<T>? tlsSelf;

    private BroadcastLocal<T> rent;

    private GlobalTLSReadableLocal()
    {
        this.rent = manager.Allocate();
    }
    internal static GlobalTLSReadableLocal<T> Allocate()
    {
        var tls = tlsSelf ?? new();
        tlsSelf = null;
        return tls;
    }

    internal void Return()
    {
        if (this.rent.TryFastClose())
        {
            var pushingOut = tlsSelf;
            tlsSelf = this;
            pushingOut?.rent.ValueReaderExitReturn();
        }

        this.rent.ValueReaderExitReturn();
        tlsSelf = null;
        GC.SuppressFinalize(this);
    }

    ~GlobalTLSReadableLocal()
    {
        this.rent.ValueReaderExitReturn();
#if DEBUG
        this.rent = null!;
#endif
    }
}
internal sealed partial class BroadcastLocal<T>
{
    internal sealed partial class PoolSegment
    {
        internal sealed class Manager
        {
            private static int initialSize { get; } = Environment.ProcessorCount;
            private PoolSegment primary;
            private PoolSegment[] array;
            private int cutout = -1;
            internal Manager(int initialSize = 0, int initialManageCapacity = 1)
            {
                this.primary = new(initialSize, this, 0);
                initialManageCapacity = Math.Max(initialManageCapacity, 1);
                var arr = new PoolSegment[initialManageCapacity];
                arr[0] = this.primary;
                this.array = arr;
            }
            internal PoolSegment Primary
                => this.primary;

            internal BroadcastLocal<T> Allocate()
                => PoolSegment.AllocateCore(this.primary);
            internal static PoolSegment? TryGetNext(PoolSegment prev)
            {
                var self = prev.poolManager;
                var index = prev.selfIndex - 1;
                var arr = self.array;
                return (uint)index < (uint)arr.Length
                    ? arr[index]
                    : null;
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static PoolSegment GetNext(PoolSegment prev)
            {
                var self = prev.poolManager;
#if DEBUG
                System.Diagnostics.Debug.Assert(object.ReferenceEquals(self, prev.poolManager));
#endif
                var index = prev.selfIndex - 1;
                var arr = self.array;
                return (uint)index < (uint)arr.Length
                    ? arr[index]
                    : ExpandGet(self, arr);
                static PoolSegment ExpandGet(Manager self, PoolSegment[] arr)
                {
                    var primary = self.primary;
                    var pIndex = primary.selfIndex + 1;
                    if (primary.Capacity > 0)
                        return primary;
                    var newSegment = new PoolSegment(pIndex * initialSize, self, pIndex);
                    if ((uint)pIndex < (uint)arr.Length)
                    {
                        if (Interlocked.CompareExchange(ref arr[pIndex], newSegment, null) is not null)
                            return Volatile.Read(ref self.primary);
                    }
                    else
                    {
                        var newArr = new PoolSegment[arr.Length * 2];
                        arr.AsSpan().CopyTo(newArr);
                        newArr[arr.Length] = newSegment;
                        if (object.ReferenceEquals(arr, Interlocked.CompareExchange(ref self.array, newArr, arr)) is false)
                            return Volatile.Read(ref self.primary);
                    }

                    do
                    {
                        if (object.ReferenceEquals(
                            primary,
                            primary = Interlocked.CompareExchange(ref self.primary, newSegment, primary)
                        ))
                            return newSegment;
#if DEBUG
                        System.Diagnostics.Debug.WriteLine(self);
#endif
                        if (primary.selfIndex > pIndex)
                            return primary;
                    } while (true);
                }
            }
            
            internal static PoolSegment CompareExchangedOrPrimary(PoolSegment newPool, PoolSegment comparand)
            {
                var self = comparand.poolManager;
                var index = comparand.selfIndex;
#if DEBUG
                System.Diagnostics.Debug.Assert(object.ReferenceEquals(self, newPool.poolManager));
#endif
                var arr = self.array;
                if ((uint)index >= (uint)arr.Length)
                    return self.primary;
                PoolSegment ret;
                if (object.ReferenceEquals(
                    comparand,
                    ret = Interlocked.CompareExchange(
                        ref arr[index],
                        newPool,
                        comparand)
                    ) is false
                )
                    return ret;

                Interlocked.CompareExchange(ref self.primary, newPool, comparand);
                return newPool;
            }
        }
    }
}


internal sealed partial class BroadcastLocal<T>
{
    internal sealed partial class PoolSegment
    {
        private delegate ref PoolSegment RootGetter();
        private const int MaxCapacity = LentCount.MaxValue;

        // for TopCountTag
        private const int IndexOffset = 0;
        private const long IndexMask = (long)(uint)UTopIndex.MaxValue;
        private const int CountOffset = sizeof(TopIndex) * 8;
        private const long CountMask = (long)(uint)UPoolCount.MaxValue << CountOffset;
        private const long CountIncrement = 1L << CountOffset;
        private const int TagOffset = (sizeof(TopIndex) + sizeof(LentCount)) * 8;
        private const long TagMask = ~0L ^ (CountMask | IndexMask);
        private const long TagIncrement = 1L << TagOffset;
        private const long ValidFlag = long.MinValue;

        private const long StartTag = (1L << TagOffset) | ValidFlag;
        private const long DisposedTag = (~0L & ~(CountMask | IndexMask)) & ~ValidFlag;
        //

        private static Manager exchangerPoolManager = new(Environment.ProcessorCount);
        
        private readonly BroadcastLocal<T>[] array;
        private long topCountTag;

        internal int selfIndex;
        internal readonly Manager poolManager;
        internal int Count
            => (int)((Volatile.Read(ref topCountTag) & CountMask) >>> CountOffset);
        internal int Capacity
            => this.array.Length - this.Count;
        private PoolSegment(int size, Manager manager, int selfIndex)
        {
            size = Math.Clamp(size, Environment.ProcessorCount, MaxCapacity);
            var arr = new BroadcastLocal<T>[size];
            for (var i = 0; i < arr.Length; ++i)
                arr[i] = new() 
                {
                    pool = this,
                    fromIndex = i,
                    freeNode = i + 1,
                };
            this.array = arr;
            this.topCountTag = StartTag | (0 << IndexOffset);
            this.poolManager = manager;
            this.selfIndex = selfIndex;
        }


        private static ref BroadcastLocal<T> AllocateCore(PoolSegment root, bool returnNullIfFull = false)
        {
            var self = root;

        retry:;
            var arr = self.array;
            var top = Volatile.Read(ref self.topCountTag);
            var idx = (int)((top & IndexMask) >>> IndexOffset);
#if DEBUG
            System.Diagnostics.Debug.Assert(idx >= 0);
#endif
            if ((uint)idx >= (uint)arr.Length)
                if (returnNullIfFull)
                    return ref Unsafe.NullRef<BroadcastLocal<T>>();
                else
                {
                    self = Manager.GetNext(self);
                    goto retry;
                }


            ref var ret = ref arr[idx];
#if DEBUG
#endif
            var nextFree = Volatile.Read(ref ret.freeNode);
#if DEBUG
            System.Diagnostics.Debug.Assert(arr[idx].fromIndex == idx);
            System.Diagnostics.Debug.Assert((uint)nextFree < (uint)arr.Length || nextFree == arr.Length);
#endif
            top |= ValidFlag;
            if (top == (top = Interlocked.CompareExchange(
                ref self.topCountTag,
                (top & TagMask)
                    | ((top + CountIncrement) & CountMask)
                    | (long)(uint)nextFree,
                top
            )))
                return ref ret!;

            if ((top & ValidFlag) == default)
                self = self.TryComebackOverflowedTag() ?? root;
            goto retry;
        }
       
        internal static int Free(BroadcastLocal<T> allocated)
        {
#if DEBUG
            var retryCount = 0;
#endif
            allocated.exchangerSlot = default;
            var pool = allocated.pool;
            var top = Volatile.Read(ref pool.topCountTag);
            var from = allocated.fromIndex;

#if DEBUG
            System.Diagnostics.Debug.Assert(from < TopIndex.MaxValue);
#endif
        retry:
            allocated.freeNode = (int)((top & IndexMask) >>> IndexOffset);
            top |= ValidFlag;
            if (top == (top = Interlocked.CompareExchange(
                ref pool.topCountTag,
                ((top + TagIncrement) & TagMask)
                    | ((top - CountIncrement) & CountMask)
                    | (long)(uint)from,
                top
            )))
                return (int)((top & CountMask) >>> CountOffset);
            else
            {
                if ((top & ValidFlag) == default)
                {
                    pool = pool.TryComebackOverflowedTag() ?? Throw(); 
                    allocated.pool = pool;
                    top = Volatile.Read(ref pool.topCountTag);
                }
                static PoolSegment Throw()
                    => throw new ObjectDisposedException(nameof(pool));
            }
#if DEBUG
            System.Diagnostics.Debug.Assert(retryCount++ < 100000);
#endif
            goto retry;
        }
        private PoolSegment? TryComebackOverflowedTag()
        {
#if EXCESSIVE_TAG_SAFE
            if (this.topCountTag == DisposedTag)
                return null;
            var newPool = (PoolSegment)this.MemberwiseClone();
            newPool.topCountTag |= ValidFlag;
#if DEBUG
            System.Diagnostics.Debug.Assert((this.topCountTag & ValidFlag) == default);
            System.Diagnostics.Debug.Assert((this.topCountTag | ValidFlag) == newPool.topCountTag);
#endif
            return Manager.CompareExchangedOrPrimary(newPool, this);
#else
            var comparand = this.topCountTag;
            while ((comparand & ValidFlag) == default)
                if (comparand == DisposedTag)
                    return null;
                else if (comparand == (comparand = Interlocked.CompareExchange(ref this.topCountTag, comparand | ValidFlag, comparand)))
                     break;
            return this;
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static BroadcastLocal<T>? CloseExchanger(BroadcastLocal<T> allocated)
        {
            var next = Interlocked.CompareExchange(ref allocated.exchangerSlot, allocated, null);
            return object.ReferenceEquals(next, allocated) ? null : next;
        } 


        internal static BroadcastLocal<T>? TryGetExchangerLocal(BroadcastLocal<T> old)
        {
#if DEBUG
            System.Diagnostics.Debug.Assert(object.ReferenceEquals(old.pool.poolManager, exchangerPoolManager) is false);
            System.Diagnostics.Debug.Assert(old.syncState.VolatileRead().IsNobody is false);
#endif
            
            ref var pooledRef = ref AllocateCore(exchangerPoolManager.Primary);
            var newItem = pooledRef;
            var newIdx = newItem.fromIndex;
            var newPool = newItem.pool;

            var dstArr = (newItem.pool = old.pool).array;
            var dstIdx = (newItem.fromIndex = old.fromIndex);
#if DEBUG
            System.Diagnostics.Debug.Assert(old.syncState.VolatileRead().ContainerReaderCount > 0);
#endif
            newItem.Standby();
            if (Interlocked.CompareExchange(ref old.exchangerSlot, newItem, null) is null)
            {
                old.pool = newPool;
                old.fromIndex = newIdx;
                pooledRef = old;
#if DEBUG
                System.Diagnostics.Debug.Assert(old.syncState.VolatileRead().IsNobody is false);
                System.Diagnostics.Debug.Assert(object.ReferenceEquals(
                    Interlocked.CompareExchange(ref dstArr[dstIdx], newItem, old),
                    old
                ));
#else
                Volatile.Write(ref dstArr[dstIdx], newItem);
#endif
                return newItem;
            }
            newItem.CancelStandby();
            Free(newItem);
            return null;
        }
        internal static bool TryMoveNext(
            [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] scoped ref PoolSegment? currentPool,
            scoped ref int index, 
            [System.Diagnostics.CodeAnalysis.MaybeNullWhen(false)] scoped ref BroadcastLocal<T>? current)
        {
            var idx = index;
            var cPool = currentPool;
            current?.ContainerReaderExitReturn();
            if (cPool is null)
            {
                current = null;
                return false; 
            }
#if DEBUG
            System.Diagnostics.Debug.Assert(object.ReferenceEquals(cPool.poolManager, exchangerPoolManager) is false);
#endif

            var arr = cPool.array;
            do
            {
                if (cPool.Count > 0 
                && (uint)idx < (uint)arr.Length)
                {
                    var read = arr[idx];
                    if (read.TryContainerReaderEnter())
                    {
                        if (object.ReferenceEquals(Volatile.Read(ref read.pool), cPool))
                        {
                            current = read;
                            index = idx + 1;
                            return true;
                        }
                        else
                        {
                            read.ContainerReaderExitReturn();
                            continue;
                        }
                    }

                    idx++;
                    continue;
                }
                cPool = Manager.TryGetNext(cPool);
                if (cPool is null)
                {
                    currentPool = null;
                    current = null;
                    return false;
                }

                //index = 0 | flag;
                idx = 0;
                currentPool = cPool;
                arr = cPool.array;
                continue;
            } while (true);
        }
        internal static BroadcastLocal<T>? GetExchanger(BroadcastLocal<T> allocated)
        {
#if DEBUG
            System.Diagnostics.Debug.Assert(allocated.syncState.VolatileRead().IsPublishing is false);
#endif
            var next = Volatile.Read(ref allocated.exchangerSlot);
            return object.ReferenceEquals(next, allocated) ? null : next;
        }
        internal static void InitializeExchanger(BroadcastLocal<T> allocated, bool allowExchange)
            => allocated.exchangerSlot = allowExchange ? null : allocated;
        public override string ToString()
            => $"count: {this.Count,5}";
    }
}

