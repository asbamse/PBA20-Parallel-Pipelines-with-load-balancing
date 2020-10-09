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
    public class SimplePipelineLoadBalenced
    {
        static int BUFFER_SIZE = 10;

        public static void ExecuteSimpleLoadBalencedPipelineOperation(string inputDirectory, string BackgroundFilePath, string outputdir, CancellationToken token)
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

                // FIRST TASK
                var stage1 = f.StartNew(() => LoadImages(inputDirectory, buffer1, cts));

                // SECOND TASK
                var stage2Task1 = f.StartNew(() => RemoveBackground(buffer1, background_bm, cts, buffer2ForNormalTask1, buffer2ForThumbnailTask1));
                var stage2Task2 = f.StartNew(() => RemoveBackground(buffer1, background_bm, cts, buffer2ForNormalTask2, buffer2ForThumbnailTask2));

                //MULTIPLEXER
                var multiplexerNormal = f.StartNew(() => Multiplexer(buffer3ForNormal, cts, buffer2ForNormalTask1, buffer2ForNormalTask2));
                var multiplexerThumbnail = f.StartNew(() => Multiplexer(buffer3ForThumbnail, cts, buffer2ForThumbnailTask1, buffer2ForThumbnailTask2));

                // THIRD TASKs
                var stage3Normal = f.StartNew(() => SaveBitmap(buffer3ForNormal, outputdir, cts));
                var stage3Thumbnail = f.StartNew(() => CreateThumbnail(buffer3ForThumbnail, buffer4, cts));

                // FOURTH TASK
                var stage4 = f.StartNew(() => SaveThumbnailBitmap(buffer4, outputdir, cts));

                try
                {
                    Task.WaitAll(stage1, stage2Task1, stage2Task2, multiplexerNormal, multiplexerThumbnail, stage3Normal, stage3Thumbnail, stage4);
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

        private static void Multiplexer(BlockingCollection<BitmapWithFilePathAndSeq> outputQueue, CancellationTokenSource cts, params BlockingCollection<BitmapWithFilePathAndSeq>[] inputQueues)
        {
            CancellationToken token = cts.Token;
            int nextIndex = 1;
            var foundItems = new List<BitmapWithFilePathAndSeq>();

            try
            {
                while (!inputQueues.All(q => q.IsCompleted))
                {
                    if (token.IsCancellationRequested)
                    {
                        break;
                    }

                    BlockingCollection<BitmapWithFilePathAndSeq>.TakeFromAny(inputQueues, out var item, token);
                    if (item != null)
                    {
                        if (item.SeqId == nextIndex)
                        {
                            outputQueue.Add(item, token);

                            BitmapWithFilePathAndSeq nextFound = null;
                            do
                            {
                                if (token.IsCancellationRequested)
                                {
                                    break;
                                }

                                nextIndex++;
                                nextFound = foundItems.Find(i => i.SeqId == nextIndex);

                                if (nextFound != null)
                                {
                                    foundItems.Remove(nextFound);
                                    outputQueue.Add(nextFound, token);
                                }
                            } while (nextFound != null);
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

        private static void RemoveBackground(BlockingCollection<BitmapWithFilePathAndSeq> inputQueue, Bitmap background_bm, CancellationTokenSource cts, params BlockingCollection<BitmapWithFilePathAndSeq>[] outputQueues)
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

                    var result = ImageProcessor.RemoveBackground(input.Image, background_bm);

                    for (int i = 0; i < outputQueues.Length; i++)
                    {
                        if (token.IsCancellationRequested)
                        {
                            break;
                        }

                        var outputObj = input;
                        outputObj.Image = i == 0 ? result : (Bitmap)result.Clone();
                        outputQueues[i].Add(outputObj, token);
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
                    var result = ImageProcessor.ResizeToThumbnail(input.Image);

                    var outputObj = input;
                    outputObj.Image = result;
                    outputQueue.Add(outputObj, token);
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

                    string output_thumb = outputdir + Path.DirectorySeparatorChar + Path.GetFileNameWithoutExtension(input.FilePath) + "_thumbnail" + Path.GetExtension(input.FilePath);
                    ImageProcessor.SaveBitmapToFile(input.Image, output_thumb);
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

                    string output = outputdir + Path.DirectorySeparatorChar + Path.GetFileName(input.FilePath);
                    ImageProcessor.SaveBitmapToFile(input.Image, output);
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
