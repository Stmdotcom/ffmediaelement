namespace Unosquare.FFME.Primitives
{
    using System;
    using System.Diagnostics;
    using System.Threading;

    /// <summary>
    /// A base class for implementing interval workers. Each worker runs its
    /// cycles on its own dedicated AboveNormal-priority thread, paced by a
    /// wait of <see cref="Constants.DefaultTimingPeriod"/> per cycle that a
    /// derived class can cut short via <see cref="SignalCycle"/> for
    /// immediate, event-driven dispatch.
    /// This replaced the former shared <c>StepTimer</c> design which queued
    /// every cycle of every worker to the ThreadPool: under pool saturation
    /// (heavy WPF work, many concurrent media elements) cycle dispatch was
    /// observed to lag 500+ ms, and each 15 ms tick paid one no-op pool
    /// dispatch per registered worker even when completely idle.
    /// </summary>
    internal abstract class IntervalWorkerBase : WorkerBase
    {
        private readonly AutoResetEvent CycleWakeup = new(false);
        private readonly Thread CycleThread;

        /// <summary>
        /// Initializes a new instance of the <see cref="IntervalWorkerBase"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        protected IntervalWorkerBase(string name)
            : base(name)
        {
            CycleThread = new Thread(CycleLoop)
            {
                Name = name + ".cycle",
                Priority = ThreadPriority.AboveNormal,
                IsBackground = true,
            };
            CycleThread.Start();
        }

        /// <summary>
        /// Wakes the cycle thread immediately instead of letting it sleep out
        /// the remainder of its current timing period. Call this after
        /// enqueueing work for <see cref="WorkerBase.ExecuteCycleLogic"/> so
        /// the work is picked up with sub-millisecond latency.
        /// </summary>
        protected void SignalCycle()
        {
            // The wakeup handle is disposed after the cycle thread has been
            // joined; a concurrent signaler racing disposal may still touch
            // the dead handle — the worker is gone, so there is nothing to
            // wake and the race is benign.
            try { CycleWakeup.Set(); }
            catch (ObjectDisposedException) { /* Ignore */ }
        }

        /// <inheritdoc />
        protected override void Dispose(bool alsoManaged)
        {
            base.Dispose(alsoManaged);

            // Join the dedicated cycle thread so derived-class state cannot
            // be torn down while a cycle is still executing on this thread.
            if (alsoManaged && CycleThread != Thread.CurrentThread && CycleThread.IsAlive)
                CycleThread.Join(TimeSpan.FromSeconds(5));

            CycleWakeup.Dispose();
        }

        /// <summary>
        /// Runs <see cref="WorkerBase.ExecuteCycleLogic"/> on the dedicated
        /// cycle thread. Loop exits when <see cref="WorkerBase.TryBeginCycle"/>
        /// returns false (worker stopped or disposed).
        /// </summary>
        private void CycleLoop()
        {
            // Wait for StartAsync to transition the state out of Created.
            // Until then TryBeginCycle returns false and the loop would exit
            // immediately. This gate also keeps the thread from dispatching
            // into a derived class whose constructor has not finished.
            while (WorkerState == WorkerState.Created && !IsDisposed)
                Thread.Sleep(10);

            try
            {
                while (TryBeginCycle())
                {
                    ExecuteCyle();
                    CycleWakeup.WaitOne(Constants.DefaultTimingPeriod);
                }
            }
            catch (Exception ex)
            {
                // An exception escaping a dedicated thread takes the process
                // down. Cycle-logic exceptions already route to
                // OnCycleException, so anything landing here is a teardown
                // straggler touching a disposed primitive.
                try { Debug.WriteLine($"{Name}.CycleLoop terminated by exception. {ex.GetType().Name}: {ex.Message}"); }
                catch { /* Ignore */ }
            }
        }
    }
}
