using System;
using System.Collections.Generic;
using System.Text;

namespace BroadcastLocal.Attributes;

[AttributeUsage(AttributeTargets.Parameter | AttributeTargets.ReturnValue | AttributeTargets.Method)]
internal sealed class PhaseFlowAttribute : Attribute
{
    public PhaseFlowKind Require { get; }
    public PhaseFlowKind WhenReturnFailure { get; set; } = default;
    public PhaseFlowKind Transfer { get; set; } = PhaseFlowKind.Relay;
    public PhaseFlowAttribute(PhaseFlowKind require)
    {
        this.Require = require;
    }
}

[Flags]
internal enum PhaseFlowKind : int
{
    None = 0,
    ValueReader = 1 << 0,
    ContainerReader = 1 << 1,
    AnyReader = 1 << 2,
    CloseOwner = 1 << 3,
    Disposed = 1 << 4,

    Relay = 1 << 10,
    AnotherHandle = 1 << 11,
    Clean = 1 << 12,
}
