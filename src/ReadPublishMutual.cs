using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace BroadcastLocal;

internal readonly struct ReadPublishMutual
{
    private const long PublishFlag = 1L;
    private const long StandbyFlag = 1L << 1;

    private const long Handlers = PublishFlag | StandbyFlag;
    private const int ValueReaderCountOffset = 2;
    private const int ContainerReaderCountOffset = ValueReaderCountOffset + (sizeof(int) * 8) - 1;
    private const int Mask = int.MaxValue;
    private const long ValueReaderCountMask = Mask << ValueReaderCountOffset;
    private const long ContainerReaderCountMask = Mask << ContainerReaderCountOffset;

    private const long ValueReaderIncrement = 1L << ValueReaderCountOffset;
    private const long ContainerReaderIncrement = 1L << ContainerReaderCountOffset;
    private const long Initial = ValueReaderIncrement | PublishFlag | StandbyFlag;


    private readonly long value;

    internal readonly int SumCount
    {
        get
        {
            var value = this.value;
            return ((int)(value >>> ValueReaderCountOffset) + (int)(value >>> ContainerReaderCountOffset)) & Mask;
        }
    }
    internal readonly bool IsStandby
        => this.value == StandbyFlag;
    internal readonly int ValueReaderCount
        => (int)(this.value >>> ValueReaderCountOffset) & Mask;
    internal readonly bool IsZeroValueReaderCount
        => (this.value & ValueReaderCountMask) == default;
    internal readonly int ContainerReaderCount
        => (int)(this.value >>> ContainerReaderCountOffset) & Mask;
    internal readonly bool IsPublishing
        => (this.value & PublishFlag) != default;
    private readonly bool HasHandler
        => (this.value & Handlers) != default;

    internal readonly bool IsNobody
        => this.value == default;
    private ReadPublishMutual(long value)
        => this.value = value;
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadPublishMutual CompareExchange(scoped ref ReadPublishMutual location, ReadPublishMutual value, ReadPublishMutual comparand)
        => new(Interlocked.CompareExchange(ref Unsafe.As<ReadPublishMutual, long>(ref location), value.value, comparand.value));

    internal readonly ReadPublishMutual VolatileRead()
        => new(Volatile.Read(ref Unsafe.As<ReadPublishMutual, long>(ref Unsafe.AsRef(in this))));
    
    internal static void PublishEnter(scoped ref ReadPublishMutual location)
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(CompareExchange(
            ref location,
            new(
                Initial
            ),
            default
        ).value == default);
#else
        Volatile.Write(
            ref Unsafe.As<ReaderConsistency, long>(ref location), 
            new ReaderConsistency(
                Initial
            ).value
        );
#endif
    }

    internal static void Standby(scoped ref ReadPublishMutual location)
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(
            CompareExchange(ref location, new(StandbyFlag), default)
            == default
        );
#else
        Volatile.Write(
            ref Unsafe.As<ReadPublishMutual, long>(ref location),
            new ReadPublishMutual(
                new(StandbyFlag)
            ).value
        );
#endif
    }
    internal static void CancelStandby(scoped ref ReadPublishMutual location)
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(
            CompareExchange(ref location, default, new(StandbyFlag))
            == new ReadPublishMutual(StandbyFlag)
        );
#else
        Volatile.Write(
            ref Unsafe.As<ReadPublishMutual, long>(ref location),
            default
        );
#endif
    }
    internal static bool TryContainerReaderEnter(scoped ref ReadPublishMutual location)
    {
        var comparand = new ReadPublishMutual(location.value);
        while (true)
        {
            if (comparand.IsNobody)
                return false;
            if (comparand == (comparand = CompareExchange(ref location, new(comparand.value + ContainerReaderIncrement), comparand)))
                return true;
        }
    }

    internal static bool TryValueReaderEnter(scoped ref ReadPublishMutual location)
    {
        var comparand = new ReadPublishMutual(location.value | PublishFlag);
        while (true)
        {
            if (comparand == (comparand = CompareExchange(ref location, new ((comparand.value + ValueReaderIncrement) & ~StandbyFlag), comparand)))
                return true;
            else if (comparand.HasHandler)
                continue;

            return false;
        }
    }

    internal static ReadPublishMutual ContainerReaderExit(scoped ref ReadPublishMutual location)
        => new(Interlocked.Add(ref Unsafe.As<ReadPublishMutual, long>(ref location), -ContainerReaderIncrement));
    internal static ReadPublishMutual ValueReaderExit(scoped ref ReadPublishMutual location)
        => new(Interlocked.Add(ref Unsafe.As<ReadPublishMutual, long>(ref location), -ValueReaderIncrement));
    internal static bool TryFastClose(scoped ref ReadPublishMutual location, ReadPublishMutual after = default)
        => CompareExchange(ref location, after, new(Initial)).value == Initial;
    internal static bool PublishExit(scoped ref ReadPublishMutual location)
    {
        var comparand = new ReadPublishMutual(location.value | PublishFlag);
        while (true)
            if (comparand == (comparand = CompareExchange(ref location, new(comparand.value & ~PublishFlag), comparand)))
                return true;
            else if (comparand.IsPublishing is false)
                return false;
    }
    internal static bool TryPublishExitAndValueReaderEnter(scoped ref ReadPublishMutual location)
    {
        var comparand = new ReadPublishMutual(location.value | PublishFlag);
        while (true)
            if (comparand == (comparand = CompareExchange(ref location, new((comparand.value & ~PublishFlag) + ValueReaderIncrement), comparand)))
                return true;
            else if (comparand.IsPublishing is false)
                return false;
    }
    internal static ReadPublishMutual MoveValueReaderToContainerReader(scoped ref ReadPublishMutual location)
    {
#if DEBUG
        System.Diagnostics.Debug.Assert(location.VolatileRead().ContainerReaderCount > 0);
#endif
        return new(Interlocked.Add(ref Unsafe.As<ReadPublishMutual, long>(ref location), -ContainerReaderIncrement + ValueReaderIncrement));
    }

    internal static bool TryMoveContainerReaderToValueReader(scoped ref ReadPublishMutual location)
    {
        var comparand = new ReadPublishMutual(location.value | PublishFlag);
#if DEBUG
        System.Diagnostics.Debug.Assert(comparand.ValueReaderCount > 0);
#endif
        while (true)
        {
            if (comparand == (comparand = CompareExchange(
                ref location,
                new((comparand.value - ValueReaderIncrement + ContainerReaderIncrement) & ~StandbyFlag), 
                comparand)))
                return true;
            else if (comparand.IsPublishing is false)
                return false;
        }
    }

    internal static bool TryForcibleValueReaderEnter(ref ReadPublishMutual location)
    {
        var comparand = location;
        while (true)
        {
            if (comparand.IsZeroValueReaderCount && comparand.IsPublishing is false)
                return false;
            if (comparand == (comparand = CompareExchange(ref location, new((comparand.value + ValueReaderIncrement) & ~StandbyFlag), comparand)))
                return true;
        }
    }
    public readonly override bool Equals(object obj)
        => obj is ReadPublishMutual self && this == self;
    public readonly override int GetHashCode()
        => this.value.GetHashCode();

    public static bool operator ==(ReadPublishMutual lhs, ReadPublishMutual rhs)
        => lhs.value == rhs.value;
    public static bool operator !=(ReadPublishMutual lhs, ReadPublishMutual rhs)
        => lhs.value != rhs.value;

}
