using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace BroadcastLocal;

internal sealed class WaitState : IDisposable
{
    private sealed class WaitAsyncNode
    {
        private WaitAsyncNode? next;
        private CancellationToken token;
    }

    internal ManualResetEvent ReaderWait = new(false);
    internal ManualResetEvent OwnerWait = new(true);
    private WaitAsyncNode? nodeHead;

    public void Dispose()
    {
        this.ReaderWait.Dispose();
        this.OwnerWait.Dispose();
    }
}
