using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using System.Linq;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    public class SimplePipelineAutomaticLoadBalancing
    {
        static int BUFFER_SIZE = 10000;
        static int TASK_DISTRIBUTION_SLEEP = 100;

        public static void ExecuteSimpleLoadBalencedPipelineOperation(CancellationToken token)
        {
            int max_task_count = (int)Math.Log(Environment.ProcessorCount, 2) + 4;

            // Task to handle task balancing
            // - While loop
            // - 

            var buffer1 = new BlockingCollection<SeqObject<int>>(BUFFER_SIZE);

            var bufferForTimesTwo = new BlockingCollection<SeqObject<int>>(BUFFER_SIZE);

            using (CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                var f = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);
                List<TaskPipelineStep<SeqObject<int>>> taskPipelineSteps = new List<TaskPipelineStep<SeqObject<int>>>();

                // FIRST STEP
                PipelineStep_GenerateNumbers ps_gn = new PipelineStep_GenerateNumbers(buffer1, cts);
                Task step1 = ps_gn.Start();

                // SECOND STEP
                PipelineStep_TimesTwo ps_tt = new PipelineStep_TimesTwo(buffer1, cts, bufferForTimesTwo);
                Task step2 = ps_tt.Start();
                taskPipelineSteps.Add(new TaskPipelineStep<SeqObject<int>>()
                {
                    PipelineStep = ps_tt,
                    Tasks = new List<Task>() { step2 },
                    Queue = buffer1,
                });

                // THIRD STEP
                //PipelineStep_DisplayAll ps_da = new PipelineStep_DisplayAll(bufferForTimesTwo, cts);
                //Task step3 = f.StartNew(() => ps_da.Start());

                try
                {
                    while (!(taskPipelineSteps is null) && taskPipelineSteps.Count > 0 && !taskPipelineSteps.SelectMany(x => x.Tasks).All(x => x.IsCompleted))
                    {
                        int tasksAvailable = max_task_count - taskPipelineSteps.Sum(x => x.Queue.IsCompleted ? 0 : x.PipelineStep.TaskAmount());


                        if (tasksAvailable == 0 && taskPipelineSteps.Count(x => !x.Queue.IsCompleted) > 1)
                        {
                            var stepToAffect = taskPipelineSteps
                                .Where(x => !x.Queue.IsCompleted && x.PipelineStep.TaskAmount() > 1)
                                .OrderBy(x => x.Queue.Count / x.PipelineStep.TaskAmount())
                                .FirstOrDefault();
                            if (!(stepToAffect is null))
                            {
                                stepToAffect.PipelineStep.RemoveTask();
                            }
                        }
                        if (tasksAvailable > 0)
                        {
                            var stepToAffect = taskPipelineSteps
                                .Where(x => !x.Queue.IsCompleted && x.PipelineStep.TaskAmount() > 0)
                                .OrderByDescending(x => x.Queue.Count / x.PipelineStep.TaskAmount())
                                .FirstOrDefault();
                            if(!(stepToAffect is null))
                            {
                                stepToAffect.PipelineStep.AddTask();
                            }
                        } 

                        if(tasksAvailable == 0)
                        {
                            Thread.Sleep(TASK_DISTRIBUTION_SLEEP);
                        }
                    }
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

        private class TaskPipelineStep<T>
        {
            public List<Task> Tasks { get; set; }
            public IPipelineStep PipelineStep { get; set; }
            public BlockingCollection<T> Queue { get; set; }
        }

        private class PipelineStep_GenerateNumbers : IPipelineStep
        {
            public BlockingCollection<SeqObject<int>> Output { get; set; }
            public CancellationTokenSource Cts { get; set; }
            public int Tasks { get; set; } = 0;

            public PipelineStep_GenerateNumbers(BlockingCollection<SeqObject<int>> output, CancellationTokenSource cts)
            {
                Output = output;
                Cts = cts;
            }

            public Task Start()
            {
                if (Tasks == 0)
                {
                    return (new TaskFactory()).StartNew(() => { GenerateNumbers(); });
                }
                Tasks = 1;
                return null;
            }

            public Task AddTask()
            {
                return null;
            }

            public bool RemoveTask()
            {
                return true;
            }

            public int TaskAmount()
            {
                return Tasks;
            }

            private void GenerateNumbers()
            {
                CancellationToken token = Cts.Token;
                try
                {
                    Console.WriteLine("Start generating numbers");
                    for (int i = 0; i < BUFFER_SIZE; i++)
                    {
                        if (token.IsCancellationRequested)
                        {
                            break;
                        }

                        decimal percent = ((i / (decimal)BUFFER_SIZE) * 100);
                        if (percent % 1 == 0)
                        {
                            Console.WriteLine("    Generating " + (int)percent + "%");
                        }

                        int i2 = i;
                        Output.Add(new SeqObject<int>() { SeqId = i2, Value = i2 }, token);
                    }
                    Console.WriteLine("Finished generating numbers");
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Generating numbers failed");
                    if (!(ex is OperationCanceledException))
                    {
                        Cts.Cancel();
                        throw;
                    }
                }
                finally
                {
                    Output.CompleteAdding();
                }
            }
        }

        private class PipelineStep_TimesTwo : IPipelineStep
        {
            public BlockingCollection<SeqObject<int>> Input { get; set; }
            public BlockingCollection<SeqObject<int>> Output { get; set; }
            public CancellationTokenSource Cts { get; set; }
            public int Tasks { get; set; } = 0;

            public PipelineStep_TimesTwo(BlockingCollection<SeqObject<int>> input, CancellationTokenSource cts, BlockingCollection<SeqObject<int>> output)
            {
                Input = input;
                Cts = cts;
                Output = output;
            }

            public Task Start()
            {
                if (Tasks == 0)
                {
                    Tasks = 1;
                    return (new TaskFactory()).StartNew(() => { TimesTwo(); });
                }
                return null;
            }

            public Task AddTask()
            {
                Tasks += 1;
                return (new TaskFactory()).StartNew(() => { TimesTwo(); });
            }

            public bool RemoveTask()
            {
                Tasks -= 1;
                return true;
            }

            public int TaskAmount()
            {
                return Tasks;
            }

            private void TimesTwo()
            {
                CancellationToken token = Cts.Token;
                try
                {
                    Console.WriteLine("Calculating...");

                    int i = 0;
                    foreach (var input in Input.GetConsumingEnumerable())
                    {
                        if (token.IsCancellationRequested)
                        {
                            break;
                        }

                        decimal percent = ((i / (decimal)BUFFER_SIZE) * 100);
                        if (percent % 10 == 0)
                        {
                            Console.WriteLine($"    {input.Value}*2={input.Value * 2}");
                        }
                        if (percent % 1 == 0)
                        {
                            Thread.Sleep(1);
                        }
                        Output.Add(new SeqObject<int>() { SeqId = input.SeqId, Value = input.Value * 2 }, token);
                        i += 1;
                    }
                    Console.WriteLine("Finished calculating");
                }
                catch (Exception ex)
                {
                    if (!(ex is OperationCanceledException))
                    {
                        Cts.Cancel();
                        throw;
                    }
                }
                finally
                {
                    Output.CompleteAdding();
                }
            }
        }

        private class PipelineStep_DisplayAll : IPipelineStep
        {
            public BlockingCollection<SeqObject<int>> Input { get; set; }
            public CancellationTokenSource Cts { get; set; }
            public int Tasks { get; set; } = 0;

            public PipelineStep_DisplayAll(BlockingCollection<SeqObject<int>> input, CancellationTokenSource cts)
            {
                Input = input;
                Cts = cts;
            }

            public Task Start()
            {
                if (Tasks == 0)
                {
                    return (new TaskFactory()).StartNew(() => { DisplayAll(); });
                }
                Tasks = 1;
                return null;
            }

            public Task AddTask()
            {
                return null;
            }

            public bool RemoveTask()
            {
                return true;
            }

            public int TaskAmount()
            {
                return Tasks;
            }

            private void DisplayAll()
            {
                CancellationToken token = Cts.Token;
                try
                {
                    foreach (var input in Input.GetConsumingEnumerable())
                    {
                        if (token.IsCancellationRequested)
                        {
                            break;
                        }

                        Console.WriteLine(input.SeqId + ": " + input.Value + " & ");
                    }
                }
                catch (Exception ex)
                {
                    if (!(ex is OperationCanceledException))
                    {
                        Cts.Cancel();
                        throw;
                    }
                }
            }
        }

        private class SeqObject<T>
        {
            public int SeqId { get; set; }
            public T Value { get; set; }
        }
    }
}
