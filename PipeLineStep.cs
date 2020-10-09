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
        static int BUFFER_SIZE = 10;
        static int MULTIPLEXOR_TIMEOUT = -1; // -1 is infinite

        // Task Queues holds the output queues of each task
        private readonly List<BlockingCollection<U>> taskQueues = new List<BlockingCollection<U>>();
        private readonly List<Task> tasks = new List<Task>();
        private readonly List<bool> shouldCloseStates = new List<bool>();

        private readonly BlockingCollection<T> inputQueue;
        private readonly BlockingCollection<U> outputQueue;
        private readonly Action<BlockingCollection<T>, BlockingCollection<U>> action;

        private readonly TaskFactory taskFactory = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

        public PipeLineStep(BlockingCollection<T> inputQueue, BlockingCollection<U> outputQueue, Action<BlockingCollection<T>, BlockingCollection<U>> action, CancellationToken token)
        {
            this.inputQueue = inputQueue;
            this.outputQueue = outputQueue;
            this.action = action;
            using (CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                //Add initial task 
                AddTask();

                //TODO START MULTIPLEXOR
                Multiplexer();
            }
        }


        // DISCUSS Might not be relevant to return boolean since there is no max and an exception might be more useful
        public bool AddTask()
        {
            try
            {
                var taskOutputQueue = new BlockingCollection<U>(BUFFER_SIZE);
                // TODO MAYBE Add an id to keep count of who is who
                taskQueues.Add(taskOutputQueue);

                taskFactory.StartNew(() => action(inputQueue, taskOutputQueue));
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
            throw new NotImplementedException();
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
