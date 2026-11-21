using System;
using System.Collections.Generic;
using System.Text;

namespace BroadcastLocal.Module;

internal class ConcurrentAddableDelegate<TDelegate>
    where TDelegate : Delegate
{
    private TDelegate[] list = [];
    private int index;
}
