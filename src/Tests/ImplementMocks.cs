using Gownri;
using System;
using System.Collections.Generic;
using System.Text;

namespace BroadcastLocal.Tests;


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
