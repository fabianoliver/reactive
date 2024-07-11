// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT License.
// See the LICENSE file in the project root for more information.

using System.Runtime.CompilerServices;

namespace System.Threading.Tasks
{
    internal static class TaskExt
    {
        public static readonly Task<bool> Never = new TaskCompletionSource<bool>().Task;
    }
}
