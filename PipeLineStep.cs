using System;
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
        private readonly BlockingCollection<U> outputQueue;
        private readonly Action<BlockingCollection<T>, BlockingCollection<U>, CancellationToken, CancellationTokenSource> action;

        private readonly TaskFactory taskFactory = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

        private readonly CancellationTokenSource cts;

        /// <summary>
        /// A Loadbalanced Pipeline step, the step can not be dependent on other task before it since earlier tasks might not have finished yet.
        /// </summary>
        /// <param name="inputQueue">Queue for items to process</param>
        /// <param name="outputQueue">Queue for processed items</param>
        /// <param name="action">The Action to process an item takes inputQueue, outputQueue, taskSuspensionToken (used for suspending task when fewer tasks are needed), CancellationTokenSource (Cancellation of complete Pipeline step, shared among tasks)</param>
        /// <param name="token">Cancellation Token</param>
        public PipeLineStep(BlockingCollection<T> inputQueue, BlockingCollection<U> outputQueue, Action<BlockingCollection<T>, BlockingCollection<U>, CancellationToken, CancellationTokenSource> action, CancellationToken token)
        {
            this.inputQueue = inputQueue;
            this.outputQueue = outputQueue;
            this.action = action;

            cts = CancellationTokenSource.CreateLinkedTokenSource(token);

            //Add initial task 
            AddTask();

            //Start Multiplexor
            Multiplexer(cts);

        }


        // DISCUSS Might not be relevant to return boolean since there is no max and an exception might be more useful
        public bool AddTask()
        {
            try
            {
                var taskOutputQueue = new BlockingCollection<U>(BUFFER_SIZE);
                // TODO MAYBE Add an id to keep count of who is who
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
            return outputQueue.Count;
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

                // TODO Could have exception
                // TODO Could not check suspension token maybe a timeout
                toSuspend.Task.Wait();
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
                    BlockingCollection<U>.TryTakeFromAny(taskQueues.ToArray(), out var item, MULTIPLEXOR_TIMEOUT, token);
                    if (item != null)
                    {
                        if (item.SeqId == nextIndex)
                        {
                            outputQueue.Add(item, token);

                            U nextFound = default(U);
                            do
                            {
                                if (token.IsCancellationRequested)
                                {
                                    break;
                                }

                                nextIndex++;
                                nextFound = foundItems.Find(i => i.SeqId == nextIndex);

                                if (nextFound.Equals(default(U)))
                                {
                                    //foundItems.Remove(nextFound);
                                    outputQueue.Add(nextFound, token);
                                }
                            } while (nextFound.Equals(default(U)));
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
                outputQueue.CompleteAdding();
            }
        }
    }
}
