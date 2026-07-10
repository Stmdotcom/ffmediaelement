namespace Unosquare.FFME.Primitives
{
    using System.Runtime.InteropServices;

    /// <summary>
    /// Process-wide, reference-counted holder of the Windows 1 ms multimedia
    /// timer resolution (winmm timeBeginPeriod). At the default ~15.6 ms
    /// timer resolution every sleep and timed wait in the engine quantizes
    /// up: the 15 ms worker pacing becomes ~15.6-31 ms and the non-vsync
    /// frame wait alternates 31/47 ms at 30 fps, which shows as judder.
    /// The request is held only while at least one engine has media open,
    /// and reference-counted so overlapping engines acquire it once.
    /// </summary>
    internal static class HighResolutionTimePeriod
    {
        private const uint PeriodMs = 1;
        private static readonly object SyncLock = new object();
        private static int RefCount;

        /// <summary>
        /// Acquires the high-resolution period on behalf of a holder.
        /// Idempotent per holder: the referenced token tracks whether this
        /// holder already owns an acquisition.
        /// </summary>
        /// <param name="isHeld">The holder's ownership token.</param>
        public static void Acquire(ref bool isHeld)
        {
            lock (SyncLock)
            {
                if (isHeld) return;

                if (RefCount == 0)
                    _ = NativeMethods.TimeBeginPeriod(PeriodMs);

                RefCount++;
                isHeld = true;
            }
        }

        /// <summary>
        /// Releases a holder's acquisition; restores the system default
        /// timer resolution when the last holder releases. No-op if the
        /// holder does not own an acquisition.
        /// </summary>
        /// <param name="isHeld">The holder's ownership token.</param>
        public static void Release(ref bool isHeld)
        {
            lock (SyncLock)
            {
                if (!isHeld) return;

                RefCount--;
                if (RefCount == 0)
                    _ = NativeMethods.TimeEndPeriod(PeriodMs);

                isHeld = false;
            }
        }

        private static class NativeMethods
        {
            private const string WinMM = "winmm.dll";

            [DllImport(WinMM, EntryPoint = "timeBeginPeriod")]
            public static extern uint TimeBeginPeriod(uint period);

            [DllImport(WinMM, EntryPoint = "timeEndPeriod")]
            public static extern uint TimeEndPeriod(uint period);
        }
    }
}
