using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using System.Threading;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    public class AutomatedLoadBalancingPipeline
    {
        private class TaskPipelineStep<T>
        {
            public string Name { get; set; }
            public List<Task> Tasks { get; set; }
            public IPipelineStep PipelineStep { get; set; }
            public BlockingCollection<T> Queue { get; set; }
        }

        static readonly int BUFFER_SIZE = 10;
        static readonly int TASK_DISTRIBUTION_SLEEP = 100;

        public static void ExecuteTestPipelineStepOperation(string inputDirectory, string BackgroundFilePath, string outputdir, CancellationToken token)
        {
            var buffer1 = new BlockingCollection<BitmapWithFilePathAndSeq>(BUFFER_SIZE);

            var buffer2ForNormalTask1 = new BlockingCollection<BitmapWithFilePathAndSeq>(BUFFER_SIZE);
            var buffer2ForThumbnailTask1 = new BlockingCollection<BitmapWithFilePathAndSeq>(BUFFER_SIZE);
            var buffer2ForNormalTask2 = new BlockingCollection<BitmapWithFilePathAndSeq>(BUFFER_SIZE);
            var buffer2ForThumbnailTask2 = new BlockingCollection<BitmapWithFilePathAndSeq>(BUFFER_SIZE);

            var buffer3ForNormal = new BlockingCollection<BitmapWithFilePathAndSeq>(BUFFER_SIZE);
            var buffer3ForThumbnail = new BlockingCollection<BitmapWithFilePathAndSeq>(BUFFER_SIZE);

            var buffer4 = new BlockingCollection<BitmapWithFilePathAndSeq>(BUFFER_SIZE);

            Bitmap background_bm = ImageProcessor.LoadFileAsImage(BackgroundFilePath);

            using (CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                var f = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);
                List<TaskPipelineStep<BitmapWithFilePathAndSeq>> pipelineSteps = new List<TaskPipelineStep<BitmapWithFilePathAndSeq>>();

                // FIRST TASK
                var pipelineStep1Task = new PipeLineStep<BitmapWithFilePathAndSeq, BitmapWithFilePathAndSeq>(
                    null,
                    (
                        BlockingCollection<BitmapWithFilePathAndSeq> inputQ,
                        BlockingCollection<BitmapWithFilePathAndSeq> outputQ,
                        CancellationToken suspend,
                        CancellationTokenSource cancel
                    ) => LoadImages(inputDirectory, outputQ, cancel),
                    cts,
                    buffer1
                ).Start();

                // SECOND TASK
                var pipelineStep2 = new PipeLineStep<BitmapWithFilePathAndSeq, BitmapWithFilePathAndSeq>(
                    buffer1,
                    (
                        BlockingCollection<BitmapWithFilePathAndSeq> inputQ,
                        BlockingCollection<BitmapWithFilePathAndSeq> outputQ,
                        CancellationToken suspend,
                        CancellationTokenSource cancel
                    ) => RemoveBackground(inputQ, background_bm, cts, suspend, outputQ),
                    cts,
                    buffer3ForThumbnail,
                    buffer3ForNormal
                );
                Task pipelineStep2Task = pipelineStep2.Start();
                pipelineSteps.Add(new TaskPipelineStep<BitmapWithFilePathAndSeq>()
                {
                    Name = "Step 2",
                    PipelineStep = pipelineStep2,
                    Tasks = new List<Task>() { pipelineStep2Task, pipelineStep2.MultiplexorTask },
                    Queue = buffer1,
                });

                // THIRD TASKs
                var pipelineStep3 = new PipeLineStep<BitmapWithFilePathAndSeq, BitmapWithFilePathAndSeq>(
                    buffer3ForThumbnail,
                    (
                        BlockingCollection<BitmapWithFilePathAndSeq> inputQ,
                        BlockingCollection<BitmapWithFilePathAndSeq> outputQ,
                        CancellationToken suspend,
                        CancellationTokenSource cancel
                    ) => CreateThumbnail(inputQ, outputQ, cancel),
                    cts,
                    buffer4
                );
                Task pipelineStep3Task = pipelineStep3.Start();
                pipelineSteps.Add(new TaskPipelineStep<BitmapWithFilePathAndSeq>()
                {
                    Name = "Step 3",
                    PipelineStep = pipelineStep3,
                    Tasks = new List<Task>() { pipelineStep3Task, pipelineStep3.MultiplexorTask },
                    Queue = buffer3ForThumbnail,
                });

                var stage3Normal = f.StartNew(() => SaveBitmap(buffer3ForNormal, outputdir, cts));

                // FOURTH TASK
                var stage4 = f.StartNew(() => SaveThumbnailBitmap(buffer4, outputdir, cts));

                try
                {
                    DistributeAvailableTasksAndAwaitPipelineCompletion(new List<Task>() { pipelineStep1Task, stage3Normal, stage4 }, pipelineSteps);
                }
                catch (Exception ex)
                {
                    if (ex is AggregateException ae) // Unwrap aggregate exception.
                    {
                        ae.Handle((ie) =>
                        {
                            throw ie;
                        });
                    }
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Distributes available tasks to pipeline steps. Awaits tasks to complete.
        /// </summary>
        /// <param name="nonParallelTasks">Tasks which should be awaited.</param>
        /// <param name="pipelineSteps">The pipeline steps which should be distributed tasks.</param>
        private static void DistributeAvailableTasksAndAwaitPipelineCompletion(List<Task> nonParallelTasks, List<TaskPipelineStep<BitmapWithFilePathAndSeq>> pipelineSteps)
        {
            if (nonParallelTasks is null)
            {
                throw new ArgumentNullException(nameof(nonParallelTasks));
            }

            if (pipelineSteps is null)
            {
                throw new ArgumentNullException(nameof(pipelineSteps));
            }

            // Calculates max amount of tasks to prevent processor oversubscription.
            int max_task_count = (int)Math.Max(Environment.ProcessorCount * 4, pipelineSteps.Count * 2);

            // While tasks are not completed.
            while ((nonParallelTasks.Count > 0 && nonParallelTasks.All(x => !x.IsCompleted)) || (!pipelineSteps.Where(x => x.Tasks.Count > 0).SelectMany(x => x.Tasks).Where(x => !(x is null)).All(x => x.IsCompleted)))
            {
                // only manage steps is available.
                if (pipelineSteps.Count > 0)
                {
                    // Calculate tasks not in use.
                    int tasksAvailable = max_task_count - pipelineSteps.Sum(x => x.Queue.IsCompleted ? 0 : x.PipelineStep.TaskAmount());

                    // If no available tasks.
                    if (tasksAvailable == 0 && pipelineSteps.Count(x => !x.Queue.IsCompleted) > 1)
                    {
                        // Find step which is the least in need of tasks.
                        var stepToAffect = pipelineSteps
                            .Where(x => !x.Queue.IsCompleted && x.PipelineStep.TaskAmount() > 1)
                            .OrderBy(x => x.Queue.Count / x.PipelineStep.TaskAmount())
                            .FirstOrDefault();
                        // Remove task from step.
                        if (!(stepToAffect is null))
                        {
                            stepToAffect.PipelineStep.RemoveTask();
                            //Console.WriteLine($"Removed task from {stepToAffect.Name} which has {stepToAffect.PipelineStep.TaskAmount()} tasks");
                            tasksAvailable = max_task_count - pipelineSteps.Sum(x => x.Queue.IsCompleted ? 0 : x.PipelineStep.TaskAmount());
                        }
                    }

                    // If task is available.
                    if (tasksAvailable > 0)
                    {
                        // Get the step in most need of a task.
                        var stepToAffect = pipelineSteps
                            .Where(x => !x.Queue.IsCompleted && x.PipelineStep.TaskAmount() > 0)
                            .OrderByDescending(x => x.Queue.Count / x.PipelineStep.TaskAmount())
                            .FirstOrDefault();
                        // Add task.
                        if (!(stepToAffect is null))
                        {
                            stepToAffect.Tasks.Add(stepToAffect.PipelineStep.AddTask());
                            //Console.WriteLine($"Added task to {stepToAffect.Name} which has {stepToAffect.PipelineStep.TaskAmount()} tasks");
                        }
                    }

                    // Sleep if all tasks are distributed.
                    if (tasksAvailable == 0)
                    {
                        Thread.Sleep(TASK_DISTRIBUTION_SLEEP);
                    }
                }
            }

            if (nonParallelTasks.Any(x => !(x.Exception is null)))
            {
                throw nonParallelTasks.FirstOrDefault(x => !(x.Exception is null)).Exception;
            }
            if (pipelineSteps.Where(x => x.Tasks.Count > 0).SelectMany(x => x.Tasks).Where(x => !(x is null)).Any(x => !(x.Exception is null)))
            {
                throw pipelineSteps.Where(x => x.Tasks.Count > 0).SelectMany(x => x.Tasks).FirstOrDefault(x => !(x.Exception is null)).Exception;
            }
        }

        private static void LoadImages(string InputDirectory, BlockingCollection<BitmapWithFilePathAndSeq> outputQueue, CancellationTokenSource cts)
        {
            int SeqIdNext = 1;
            CancellationToken token = cts.Token;
            try
            {
                foreach (string filePath in Directory.GetFiles(InputDirectory))
                {
                    if (token.IsCancellationRequested)
                    {
                        break;
                    }

                    if (Path.GetExtension(filePath) == ".bmp")
                    {
                        Bitmap bm = ImageProcessor.LoadFileAsImage(filePath);

                        var outputObj = new BitmapWithFilePathAndSeq()
                        {
                            FilePath = filePath,
                            Image = bm,
                            SeqId = SeqIdNext++
                        };
                        outputQueue.Add(outputObj, token);
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

        private static void RemoveBackground(BlockingCollection<BitmapWithFilePathAndSeq> inputQueue, Bitmap background_bm, CancellationTokenSource cts, CancellationToken suspensionToken, params BlockingCollection<BitmapWithFilePathAndSeq>[] outputQueues)
        {
            CancellationToken token = cts.Token;
            try
            {
                foreach (var input in inputQueue.GetConsumingEnumerable())
                {
                    if (token.IsCancellationRequested)
                    {
                        break;
                    }

                    lock (input.Image)
                    {
                        lock (background_bm)
                        {
                            var result = ImageProcessor.RemoveBackground(input.Image, background_bm);

                            for (int i = 0; i < outputQueues.Length; i++)
                            {
                                if (token.IsCancellationRequested)
                                {
                                    break;
                                }

                                outputQueues[i].Add(new BitmapWithFilePathAndSeq()
                                {
                                    FilePath = input.FilePath,
                                    Image = (Bitmap)result.Clone(),
                                    SeqId = input.SeqId,
                                }, token);
                            }
                        }
                    }

                    if (suspensionToken.IsCancellationRequested)
                    {
                        break;
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
                foreach (var outputQueue in outputQueues)
                {
                    outputQueue.CompleteAdding();
                }
            }
        }

        private static void CreateThumbnail(BlockingCollection<BitmapWithFilePathAndSeq> inputQueue, BlockingCollection<BitmapWithFilePathAndSeq> outputQueue, CancellationTokenSource cts)
        {
            CancellationToken token = cts.Token;
            try
            {
                foreach (var input in inputQueue.GetConsumingEnumerable())
                {
                    if (token.IsCancellationRequested)
                    {
                        break;
                    }

                    lock (input.Image)
                    {
                        var result = ImageProcessor.ResizeToThumbnail(input.Image);

                        outputQueue.Add(new BitmapWithFilePathAndSeq()
                        {
                            FilePath = input.FilePath,
                            Image = (Bitmap)result.Clone(),
                            SeqId = input.SeqId,
                        }, token);
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

        private static void SaveThumbnailBitmap(BlockingCollection<BitmapWithFilePathAndSeq> inputQueue, string outputdir, CancellationTokenSource cts)
        {
            CancellationToken token = cts.Token;
            try
            {
                foreach (var input in inputQueue.GetConsumingEnumerable(token))
                {
                    if (token.IsCancellationRequested)
                    {
                        break;
                    }

                    lock (input.Image)
                    {
                        string output_thumb = outputdir + Path.DirectorySeparatorChar + Path.GetFileNameWithoutExtension(input.FilePath) + "_thumbnail" + Path.GetExtension(input.FilePath);
                        ImageProcessor.SaveBitmapToFile(input.Image, output_thumb);
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
        }

        private static void SaveBitmap(BlockingCollection<BitmapWithFilePathAndSeq> inputQueue, string outputdir, CancellationTokenSource cts)
        {
            CancellationToken token = cts.Token;
            try
            {
                foreach (var input in inputQueue.GetConsumingEnumerable(token))
                {
                    if (token.IsCancellationRequested)
                    {
                        break;
                    }

                    lock (input.Image)
                    {
                        string output = outputdir + Path.DirectorySeparatorChar + Path.GetFileName(input.FilePath);
                        ImageProcessor.SaveBitmapToFile(input.Image, output);
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
        }
    }
}
