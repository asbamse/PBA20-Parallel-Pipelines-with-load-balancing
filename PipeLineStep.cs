﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T">The input Blocking collections objects type</typeparam>
    /// <typeparam name="U">The output Blocking collections objects type</typeparam>
    public class PipeLineStep<T, U> : IPipelineStep where U : ISequenceIdentifiable
    {
        class SuspensableTaskObj
        {
            public Task Task { get; set; }
            public CancellationTokenSource SuspensionToken { get; set; }
        }

        static int BUFFER_SIZE = 10;
        static int MULTIPLEXOR_TIMEOUT = -1; // -1 is infinite

        // Task Queues holds the output queues of each task
        private readonly List<BlockingCollection<U>> taskQueues = new List<BlockingCollection<U>>();
        private readonly List<SuspensableTaskObj> tasks = new List<SuspensableTaskObj>();

        private readonly BlockingCollection<T> inputQueue;
        private readonly BlockingCollection<U>[] outputQueues;
        private readonly Action<BlockingCollection<T>, BlockingCollection<U>, CancellationToken, CancellationTokenSource> action;

        private readonly TaskFactory taskFactory = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

        private readonly CancellationTokenSource cts;


        // TODO Add posibility for no output queue, when there is no output queue the multiplexor is not needed
        // TODO Make someway to wait for the tasks
        /// <summary>
        /// A Loadbalanced Pipeline step, the step can not be dependent on other task before it since earlier tasks might not have finished yet.
        /// </summary>
        /// <param name="inputQueue">Queue for items to process</param>
        /// <param name="action">The Action to process an item takes inputQueue, outputQueue, taskSuspensionToken (used for suspending task when fewer tasks are needed, remeber to call completeAdding when this is received), CancellationTokenSource (Cancellation of complete Pipeline step, shared among tasks)</param>
        /// <param name="cts">Cancellation Token Source</param>
        /// <param name="outputQueues">Queue(s) for processed items</param>
        public PipeLineStep(BlockingCollection<T> inputQueue, Action<BlockingCollection<T>, BlockingCollection<U>, CancellationToken, CancellationTokenSource> action, CancellationTokenSource cts, params BlockingCollection<U>[] outputQueues)
        {
            this.inputQueue = inputQueue;
            this.outputQueues = outputQueues;
            this.action = action;
            this.cts = cts;
        }

        public void Start()
        {
            //Add initial task 
            AddTask();

            //Start Multiplexor
            Multiplexer(cts);
        }

        public static PipeLineStep<T, U> StartNew(BlockingCollection<T> inputQueue, Action<BlockingCollection<T>, BlockingCollection<U>, CancellationToken, CancellationTokenSource> action, CancellationTokenSource cts, params BlockingCollection<U>[] outputQueues)
        {
            var instance = new PipeLineStep<T, U>(inputQueue, action, cts, outputQueues);
            instance.Start();
            return instance;
        }


        // DISCUSS Might not be relevant to return boolean since there is no max and an exception might be more useful
        public bool AddTask()
        {
            try
            {
                var taskOutputQueue = new BlockingCollection<U>(BUFFER_SIZE);
                taskQueues.Add(taskOutputQueue);

                CancellationTokenSource suspensionTokenSource = new CancellationTokenSource();
                CancellationToken suspensionToken = suspensionTokenSource.Token;

                var task = taskFactory.StartNew(() => action(inputQueue, taskOutputQueue, suspensionToken, cts));

                tasks.Add(new SuspensableTaskObj
                {
                    Task = task,
                    SuspensionToken = suspensionTokenSource,
                });

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        // DISCUSS is read from the outputQueue which is given via the constructer so might not be the resposebility of this class
        public int QueueFillLevel()
        {
            // SHOULD NOT BE USED I THINK
            return outputQueues[0].Count;
        }

        public int TaskAmount()
        {
            return tasks.Count;
        }

        public bool RemoveTask()
        {
            if (tasks.Count > 1)
            {
                var toSuspend = tasks.Last();
                tasks.Remove(toSuspend);
                toSuspend.SuspensionToken.Cancel();

                // TODO Could have an exception
                // TODO Could not check suspension token maybe a timeout
                toSuspend.Task.Wait();
                // TODO only return if cancellation was proper (that it has run to finish, to ensure a valid state and that no item is lost)
                return true;
            }
            return false;
        }

        private void Multiplexer(CancellationTokenSource cts)
        {
            CancellationToken token = cts.Token;
            int nextIndex = 1;
            var foundItems = new List<U>();

            try
            {
                while (!taskQueues.All(q => q.IsCompleted))
                {
                    if (token.IsCancellationRequested)
                    {
                        break;
                    }
                    // TODO Check if the ToArray() gives problems
                    // TODO TryTake from any might not be the best if it is not waiting for an item
                    BlockingCollection<U>.TryTakeFromAny(taskQueues.ToArray(), out var item, MULTIPLEXOR_TIMEOUT, token);
                    if (item != null)
                    {
                        if (item.SeqId == nextIndex)
                        {
                            AddToOutpuQueues(item, token);

                            U nextFound = default(U);
                            do
                            {
                                if (token.IsCancellationRequested)
                                {
                                    break;
                                }

                                nextIndex++;
                                nextFound = foundItems.Find(i => i.SeqId == nextIndex);

                                if (!EqualityComparer<U>.Default.Equals(nextFound, default(U)))
                                {
                                    foundItems.Remove(nextFound);
                                    AddToOutpuQueues(nextFound, token);
                                }
                            } while (!EqualityComparer<U>.Default.Equals(nextFound, default(U)));
                        }
                        else
                        {
                            foundItems.Add(item);
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                if (!(ex is OperationCanceledException))
                {
                    cts.Cancel();
                    throw;
                }
            }
            finally
            {
                foreach (var queue in outputQueues)
                {
                    queue.CompleteAdding();
                }
            }
        }

        private void AddToOutpuQueues(U item, CancellationToken token)
        {
            foreach (var queue in outputQueues)
            {
                queue.Add(item, token);
            }
        }
    }
}
