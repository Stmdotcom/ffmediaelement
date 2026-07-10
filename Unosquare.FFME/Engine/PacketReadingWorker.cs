namespace Unosquare.FFME.Engine
{
    using Common;
    using Container;
    using Diagnostics;
    using Primitives;
    using Rendering.Wave;
    using System;
    using System.Threading;

    /// <summary>
    /// Implement packet reading worker logic.
    /// </summary>
    /// <seealso cref="IMediaWorker" />
    internal sealed class PacketReadingWorker : WorkerBase, IMediaWorker, ILoggingSource
    {
        private static long NextReaderInstanceId;

        private readonly long DiagId = Interlocked.Increment(ref NextReaderInstanceId);
        private readonly Thread CycleThread;
        private long LastCycleTicks;

        public PacketReadingWorker(MediaEngine mediaCore)
            : base(nameof(PacketReadingWorker))
        {
            MediaCore = mediaCore;
            Container = mediaCore.Container;

            // Enable data frame processing as a connector callback (i.e. hanlde non-media frames)
            Container.Data.OnDataPacketReceived = (dataPacket, stream) =>
            {
                try
                {
                    var dataFrame = new DataFrame(dataPacket, stream, MediaCore);
                    MediaCore.Connector?.OnDataFrameReceived(dataFrame, stream);
                }
                catch
                {
                    // ignore
                }
            };

            // Packet Buffer Notification Callbacks
            Container.Components.OnPacketQueueChanged = (op, packet, mediaType, state) =>
            {
                MediaCore.State.UpdateBufferingStats(state.Length, state.Count, state.CountThreshold, state.Duration);

                if (op != PacketQueueOp.Queued)
                    return;

                unsafe
                {
                    MediaCore.Connector?.OnPacketRead(packet.Pointer, Container.InputContext);
                }
            };

            // Run the read cycle on a dedicated AboveNormal-priority thread
            // instead of the shared-timer + ThreadPool path. See comment in
            // FrameDecodingWorker — same fix, same reasoning.
            CycleThread = new Thread(CycleLoop)
            {
                Name = nameof(PacketReadingWorker) + ".cycle",
                Priority = ThreadPriority.AboveNormal,
                IsBackground = true,
            };
            CycleThread.Start();
        }

        /// <inheritdoc />
        public MediaEngine MediaCore { get; }

        /// <inheritdoc />
        ILoggingHandler ILoggingSource.LoggingHandler => MediaCore;

        /// <summary>
        /// Gets the Media Engine's container.
        /// </summary>
        private MediaContainer Container { get; }

        /// <inheritdoc />
        protected override void ExecuteCycleLogic(CancellationToken ct)
        {
            // Cycle gap detection — see comment in FrameDecodingWorker.
            // PacketReadingWorker also runs on a dedicated AboveNormal
            // thread, so a gap here is CPU starvation or slow I/O, not
            // ThreadPool scheduling; combined with decoder.gap and
            // renderer.silence reason=buffer_empty events, this gives us
            // full visibility into upstream pipeline stalls.
            var nowTicks = Environment.TickCount64;
            if (LastCycleTicks != 0)
            {
                var gap = nowTicks - LastCycleTicks;
                if (gap > 150)
                {
                    var t = Thread.CurrentThread;
                    ThreadPool.GetAvailableThreads(out var poolWorkers, out var poolIO);
                    ThreadPool.GetMaxThreads(out var poolWorkersMax, out var poolIOMax);
                    DirectSoundDiagnostics.Log(
                        DiagId,
                        "reader.gap",
                        "ms=" + gap
                        + " pool=" + (t.IsThreadPoolThread ? "y" : "n")
                        + " prio=" + t.Priority
                        + " workers=" + (poolWorkersMax - poolWorkers) + "/" + poolWorkersMax
                        + " io=" + (poolIOMax - poolIO) + "/" + poolIOMax
                        + " gen0=" + GC.CollectionCount(0)
                        + " gen1=" + GC.CollectionCount(1)
                        + " gen2=" + GC.CollectionCount(2));
                }
            }

            LastCycleTicks = nowTicks;

            while (MediaCore.ShouldReadMorePackets)
            {
                if (Container.IsReadAborted || Container.IsAtEndOfStream || ct.IsCancellationRequested ||
                    WorkerState != WantedWorkerState)
                {
                    break;
                }

                try { Container.Read(); }
                catch (MediaContainerException) { /* ignore */ }
            }
        }

        /// <inheritdoc />
        protected override void OnCycleException(Exception ex) =>
            this.LogError(Aspects.ReadingWorker, "Worker Cycle exception thrown", ex);

        /// <inheritdoc />
        protected override void Dispose(bool alsoManaged)
        {
            base.Dispose(alsoManaged);

            // Join the dedicated cycle thread so the container cannot be
            // disposed while a packet read is still executing on this thread.
            if (alsoManaged && CycleThread != Thread.CurrentThread && CycleThread.IsAlive)
                CycleThread.Join(TimeSpan.FromSeconds(5));
        }

        /// <summary>
        /// Runs <see cref="WorkerBase.ExecuteCycleLogic"/> on a dedicated
        /// AboveNormal-priority thread. Pacing is a fixed 15 ms sleep per
        /// cycle (the engine's default timing period).
        /// </summary>
        private void CycleLoop()
        {
            // Wait for the framework to call StartAsync, which transitions
            // WorkerState from Created to Running.
            while (WorkerState == WorkerState.Created && !IsDisposed)
                Thread.Sleep(10);

            try
            {
                while (TryBeginCycle())
                {
                    ExecuteCyle();
                    Thread.Sleep(15);
                }
            }
            catch (Exception ex)
            {
                // An exception escaping a dedicated thread takes the process
                // down. Cycle-logic exceptions already route to
                // OnCycleException, so anything landing here is a teardown
                // straggler touching a disposed primitive.
                try { this.LogError(Aspects.ReadingWorker, "Worker cycle loop terminated by exception.", ex); }
                catch { /* ignore */ }
            }
        }
    }
}
