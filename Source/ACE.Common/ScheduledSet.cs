using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace ACE.Common
{
    /// <summary>
    /// A ScheduledSet provides a min-heap where ordering is based on scheduled
    /// time, with the ability to update or remove entries.
    ///
    /// This implementation uses a log-structured approach with lazy compaction.
    /// Rather than performing expensive O(n) in-place updates to the PriorityQueue,
    /// we treat the queue as a log of scheduled events. We use the EntryHandle as
    /// a source of truth to version-gate these events; stale events (where the
    /// queued time != the handle's current time) are treated as ghosts and discarded
    /// during consumption.
    ///
    /// Thread safe for concurrent producers (insert/update/remove).
    /// Consumers must be serialized (only one consumer waiting at a time).
    /// </summary>
    public class ScheduledSet<T>(TimeProvider? timeProvider = null, long maxGhostsCleanedPerLock = 25) where T : class
    {
        private readonly long _maxGhostsCleanedPerLock = maxGhostsCleanedPerLock;
        private readonly object _lock = new();
        private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;

        // The lightweight nobject that lives in the queue to support ghost
        // entries without pinning the underlying T until the ghost is cleaned up.
        private class EntryHandle(T item, long scheduledUtcTicks)
        {
            public T? Item = item; // Null when an entry is a ghost.
            public long queuedAtUtcTicks = scheduledUtcTicks;
            public long scheduledUtcTicks = scheduledUtcTicks;
        }

        // Map: Item to the currently active handle in the queue.
        // The handles in this map should ALWAYS be "valid".
        private readonly Dictionary<T, EntryHandle> _map = [];

        // Min-Heap: Holds handles ordered by utcTicks, which may contain ghosts (deleted / outdated).
        private readonly PriorityQueue<EntryHandle, long> _queue = new();

        // Signal: raised when the consumer should wake and check the queue.
        private readonly SemaphoreSlim _signal = new(0);

        private bool _isStopped = false;

        /// <summary>
        /// Stops and empties the set, releases waiting consumers, and prevents
        /// future items from being added.
        /// </summary>
        public void Stop()
        {
            lock (_lock)
            {
                _isStopped = true;
                _map.Clear();
                _queue.Clear();

                // Release to wake up the waiter so it sees _isStopped.
                if (_signal.CurrentCount == 0) _signal.Release();
            }
        }

        /// <summary>
        /// Inserts or updates an entry with an immediate scheduled time.
        /// </summary>
        public void Upsert(T entry)
        {
            Upsert(entry, _timeProvider.GetUtcNow());
        }

        /// <summary>
        /// Inserts or updates an entry.
        /// </summary>
        public void Upsert(T entry, DateTimeOffset scheduledTime)
        {
            long scheduledUtcTicks = scheduledTime.UtcTicks;
            lock (_lock)
            {
                if (_isStopped) return;

                // Get a reference to the value slot.  If the entry didn't exist, 'exists' is false and the ref points to null.
                ref EntryHandle? handle = ref CollectionsMarshal.GetValueRefOrAddDefault(_map, entry, out bool exists);

                bool needsEnqueue = false;
                if (exists && handle != null)
                {
                    // UPDATE: Change the time on the existing handle
                    // If the new time is earlier than the current queue position, we must re-enqueue.
                    if (scheduledUtcTicks < handle.queuedAtUtcTicks)
                    {
                        handle.queuedAtUtcTicks = scheduledUtcTicks;
                        needsEnqueue = true;
                    }
                    handle.scheduledUtcTicks = scheduledUtcTicks;
                }
                else
                {
                    // INSERT: Create the handle and assign it to the reference slot
                    needsEnqueue = true;
                    handle = new EntryHandle(entry, scheduledUtcTicks);
                }

                // An entirely new item requires an enqueue.
                // A modified item only needs an enqueue if it has moved forward in scheduling time.
                if (needsEnqueue) { 
                    // Signal if the new item will represent an earlier time.
                    // Since we're holding the lock, it's ok to signal before the enqueue.
                    if (_signal.CurrentCount == 0)
                    {
                        bool entryExists = _queue.TryPeek(out _, out long minUtcTicks);
                        if (!entryExists || scheduledUtcTicks < minUtcTicks) _signal.Release();
                    }

                    // Add to PriorityQueue (Lazy, we don't remove old entries).
                    _queue.Enqueue(handle, scheduledUtcTicks);
                }
            }
        }

        /// <summary>
        /// Removes and unschedules an entry.
        /// </summary>
        public void Remove(T entry)
        {
            lock (_lock)
            {
                if (_map.TryGetValue(entry, out EntryHandle? handle))
                {
                    // Release the entry, turning it into a ghost.
                    // The item remains in the queue, but we release T so it is not pinned in memory.
                    handle.Item = null;
                    _map.Remove(entry);
                    if (_map.Count == 0) _queue.Clear();
                }
            }
        }

        public bool IsEmpty { get { lock (_lock) return _map.Count == 0; } }
        public int Count { get { lock (_lock) return _map.Count; } }
        public bool Contains(T entry) { lock (_lock) return _map.ContainsKey(entry); }

        /// <summary>
        /// Consumes the next item if ready, otherwise returns false.
        /// </summary>
        /// <param name="entry">Filled with an entry if one was ready.</param>
        /// <returns>A bool representing whether an item was found.</returns>
        /// <exception cref="InvalidOperationException">If set is stopped/empty or no item is ready.</exception>
        public bool TryConsumeNextItem([NotNullWhen(true)] out T? entry)
        {
            long nowUtcTicks = _timeProvider.GetUtcNow().UtcTicks;

            while (true)
            {
                // To ensure we don't starve the lock for long periods of time, we release the lock
                // as soon as we clean _maxGhostsCleanedPerLock and re-enter the lock to continue.
                long ghostsCleanedThisLock = 0;
                lock (_lock)
                {
                    if (_isStopped) throw new OperationCanceledException("ScheduledSet is stopped.");

                    while (_queue.TryPeek(out EntryHandle? handle, out long nextScheduledUtcTicks))
                    {
                        // GHOST CHECK
                        if (handle.Item == null || handle.queuedAtUtcTicks != nextScheduledUtcTicks)
                        {
                            // True ghost.
                            // If handle.Item is null, it was already processed (earlier) or removed.
                            // If queuedAtUtcTicks isn't a match, it isn't the authoritative entry.
                            ++ghostsCleanedThisLock;
                            if (ghostsCleanedThisLock >= _maxGhostsCleanedPerLock) break;
                            _queue.Dequeue();
                            continue;
                        }
                        else if (handle.scheduledUtcTicks != nextScheduledUtcTicks)
                        {
                            // Deferred ite.
                            // The scheduled time is different, but this is the authoritative entry
                            // for this handle (since the queued time of the handle matches). As a
                            // result, the item needs to be re-enqueued at its actual time, which
                            // should be strictly later than now.
                            ++ghostsCleanedThisLock;
                            handle.queuedAtUtcTicks = handle.scheduledUtcTicks;
                            _queue.DequeueEnqueue(handle, handle.scheduledUtcTicks);
                            if (ghostsCleanedThisLock >= _maxGhostsCleanedPerLock) break;
                            continue;
                        }

                        if (nextScheduledUtcTicks <= nowUtcTicks)
                        {
                            entry = handle.Item!; // We know it's non-nullable (see check above).

                            // The entry is ready, so we consume it. We must:
                            // - Remove it from the front of the queue.
                            // - Remove it from the map.
                            // - Mark the handle as a ghost, in case there are stale references deeper in the queue.
                            _queue.Dequeue();
                            RemoveFromMapAndGhostHandleLocked(handle);
                            return true;
                        }
                    }
                }

                // We exited the lock because we didn't find an entry, not because we cleaned too many ghosts.
                if (ghostsCleanedThisLock < _maxGhostsCleanedPerLock) break;
            }

            // There was no valid handle, or the valid handle was in the future.
            entry = null;
            return false;
        }

        /// <summary>
        /// Waits for an element whose schedule has passed.
        /// </summary>
        /// <returns>The item that is ready.</returns>
        /// <exception cref="InvalidOperationException">If cancelled or stopped.</exception>
        /// <exception cref="TimeoutException">If timeout occurs.</exception>
        public async Task<T> WaitForNextItemAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            DateTimeOffset deadline = _timeProvider.GetUtcNow().Add(timeout);
            return await WaitForNextItemAsync(deadline, cancellationToken);
        }

        /// <summary>
        /// Waits until the specific deadline for an item to become ready.
        /// </summary>
        /// <returns>The item that is ready.</returns>
        /// <exception cref="InvalidOperationException">If cancelled or stopped.</exception>
        /// <exception cref="TimeoutException">If deadline passes.</exception>
        public async Task<T> WaitForNextItemAsync(DateTimeOffset deadline, CancellationToken cancellationToken)
        {
            long deadlineUtcTicks = deadline.UtcTicks;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                long nowUtcTicks = _timeProvider.GetUtcNow().UtcTicks;
                TimeSpan timeToDeadline = TimeSpan.FromTicks(deadlineUtcTicks - nowUtcTicks);
                TimeSpan waitTime = timeToDeadline;

                while (true)
                {
                    // To ensure we don't starve the lock for long periods of time, we release the lock
                    // as soon as we clean _maxGhostsCleanedPerLock and re-enter the lock to continue.
                    long ghostsCleanedThisLock = 0;
                    lock (_lock)
                    {
                        if (_isStopped) throw new InvalidOperationException("ScheduledSet is stopped.");

                        // We are now "looking" at the state, so any previous signals are obsolete.
                        while (_signal.CurrentCount > 0) _signal.Wait(0);

                        while (_queue.TryPeek(out EntryHandle? handle, out long nextScheduledUtcTicks))
                        {
                            // GHOST CHECK
                            if (handle.Item == null || handle.queuedAtUtcTicks != nextScheduledUtcTicks)
                            {
                                // True ghost.
                                // If handle.Item is null, it was already processed (earlier) or removed.
                                // If queuedAtUtcTicks isn't a match, it isn't the authoritative entry.
                                ++ghostsCleanedThisLock;
                                if (ghostsCleanedThisLock >= _maxGhostsCleanedPerLock) break;
                                _queue.Dequeue();
                                continue;
                            }
                            else if (handle.scheduledUtcTicks != nextScheduledUtcTicks)
                            {
                                // Deferred ite.
                                // The scheduled time is different, but this is the authoritative entry
                                // for this handle (since the queued time of the handle matches). As a
                                // result, the item needs to be re-enqueued at its actual time, which
                                // should be strictly later than now.
                                ++ghostsCleanedThisLock;
                                handle.queuedAtUtcTicks = handle.scheduledUtcTicks;
                                _queue.DequeueEnqueue(handle, handle.scheduledUtcTicks);
                                if (ghostsCleanedThisLock >= _maxGhostsCleanedPerLock) break;
                                continue;
                            }

                            if (nextScheduledUtcTicks <= nowUtcTicks)
                            {
                                T entry = handle.Item!; // We know it's non-nullable (see check above).

                                // The entry is ready, so we consume it. We must:
                                // - Remove it from the front of the queue.
                                // - Remove it from the map.
                                // - Mark the handle as a ghost, in case there are stale references deeper in the queue.
                                _queue.Dequeue();
                                RemoveFromMapAndGhostHandleLocked(handle);
                                return entry;
                            }

                            // Next item isn't ready, so determine wait time.
                            TimeSpan timeToNext = TimeSpan.FromTicks(nextScheduledUtcTicks - nowUtcTicks);
                            waitTime = timeToNext < timeToDeadline ? timeToNext : timeToDeadline;
                        }

                        // An item wasn't immediately ready, so throw a timeout if applicable.
                        if (timeToDeadline <= TimeSpan.Zero) throw new TimeoutException("Timed out while waiting for item.");

                        // We exited the lock because we didn't find an entry, not because we cleaned too many ghosts.
                        if (ghostsCleanedThisLock < _maxGhostsCleanedPerLock) break;
                    }
                }

                // Unlock and wait (clamping to zero wait minimum).
                // - If Upsert puts an item at the front of the queue, _signal is released and we re-loop to check the start time.
                // - If the wait time passes, we timeout and re-loop (this tells us our deadline passed or the next item is ready).
                // - If cancellation is raised, the waiter requested to stop waiting.
                if (waitTime < TimeSpan.Zero) waitTime = TimeSpan.Zero;
                await _signal.WaitAsync(waitTime, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Removes a handle from the map and turns a handle into a ghost.
        /// Also clears the queue if all remaining enqueued entries are ghosts.
        /// </summary>
        /// <param name="handle"></param>
        private void RemoveFromMapAndGhostHandleLocked(EntryHandle handle)
        {
            T? item = handle.Item;
            if (item == null) return;

            _map.Remove(item);
            handle.Item = null; // Unpin T immediately

            if (_map.Count == 0) _queue.Clear();
        }
    }
}
