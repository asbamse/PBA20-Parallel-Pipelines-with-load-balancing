using System;
using System.Collections.Concurrent;
using System.Drawing;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    public class SimplePipeline
    {
        static int BUFFER_SIZE = 10;

        public static void ExecuteSimplePipelineOperation(string inputDirectory, string BackgroundFilePath, string outputdir, CancellationToken token)
        {
            var buffer1 = new BlockingCollection<BitmapWithFilePath>(BUFFER_SIZE);
            var buffer2ForNormal = new BlockingCollection<BitmapWithFilePath>(BUFFER_SIZE);
            var buffer2ForThumbnail = new BlockingCollection<BitmapWithFilePath>(BUFFER_SIZE);
            var buffer3 = new BlockingCollection<BitmapWithFilePath>(BUFFER_SIZE);

            Bitmap background_bm = ImageProcessor.LoadFileAsImage(BackgroundFilePath);

            using (CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                var f = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

                // FIRST TASK
                var stage1 = f.StartNew(() => LoadImages(inputDirectory, buffer1, cts));

                // SECOND TASK
                var stage2 = f.StartNew(() => RemoveBackground(buffer1, background_bm, new []{ buffer2ForNormal, buffer2ForThumbnail }, cts));

                // THIRD TASKs
                var stage3Normal = f.StartNew(() => SaveBitmap(buffer2ForNormal, outputdir, cts));
                var stage3Thumbnail = f.StartNew(() => CreateThumbnail(buffer2ForThumbnail, buffer3, cts));

                // FOURTH TASK
                var stage4 = f.StartNew(() => SaveThumbnailBitmap(buffer3, outputdir, cts));

                try
                {
                    Task.WaitAll(stage1, stage2, stage3Normal, stage3Thumbnail, stage4);
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


        private static void LoadImages(string InputDirectory, BlockingCollection<BitmapWithFilePath> outputQueue, CancellationTokenSource cts)
        {
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

                        var outputObj = new BitmapWithFilePath()
                        {
                            FilePath = filePath,
                            Image = bm
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

        private static void RemoveBackground(BlockingCollection<BitmapWithFilePath> inputQueue, Bitmap background_bm, BlockingCollection<BitmapWithFilePath>[] outputQueues, CancellationTokenSource cts)
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

                    var result = ImageProcessor.RemoveBackground(input.Image, background_bm);
                    for (int i = 0; i < outputQueues.Length; i++)
                    {
                        if (token.IsCancellationRequested)
                        {
                            break;
                        }

                        var outputObj = new BitmapWithFilePath()
                        {
                            FilePath = input.FilePath,
                            Image = i == 0 ? result : (Bitmap)result.Clone()
                        };
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

        private static void CreateThumbnail(BlockingCollection<BitmapWithFilePath> inputQueue, BlockingCollection<BitmapWithFilePath> outputQueue, CancellationTokenSource cts)
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

                    var result = ImageProcessor.ResizeToThumbnail(input.Image);
                    var outputObj = new BitmapWithFilePath()
                    {
                        FilePath = input.FilePath,
                        Image = result
                    };
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

        private static void SaveThumbnailBitmap(BlockingCollection<BitmapWithFilePath> inputQueue, string outputdir, CancellationTokenSource cts)
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

        private static void SaveBitmap(BlockingCollection<BitmapWithFilePath> inputQueue, string outputdir, CancellationTokenSource cts)
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
                if(!(ex is OperationCanceledException))
                {
                    cts.Cancel();
                    throw;
                }
            }
        }
    }
}
