using System;
using System.Collections.Concurrent;
using System.Drawing;
using System.IO;
using System.Threading.Tasks;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    public class SimplePipeline
    {
        public SimplePipeline()
        {
        }

        static int BUFFER_SIZE = 10;

        public static void ExecuteSimplePipelineOperation(string inputDirectory, string BackgroundFilePath, string outputdir)
        {
            var buffer1 = new BlockingCollection<BitmapWithFilePath>(BUFFER_SIZE);
            var buffer2ForNormal = new BlockingCollection<BitmapWithFilePath>(BUFFER_SIZE);
            var buffer2ForThumbnail = new BlockingCollection<BitmapWithFilePath>(BUFFER_SIZE);
            var buffer3 = new BlockingCollection<BitmapWithFilePath>(BUFFER_SIZE);

            Bitmap background_bm = ImageProcessor.LoadFileAsImage(BackgroundFilePath);

            //TODO Add Cancelation Token
            var f = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

            // FIRST TASK
            var stage1 = f.StartNew(() => LoadImages(inputDirectory, buffer1));

            // SECOND TASK
            var stage2 = f.StartNew(() => RemoveBackground(buffer1, background_bm, buffer2ForNormal, buffer2ForThumbnail));

            // THIRD TASKs
            var stage3Normal = f.StartNew(() => SaveBitmap(buffer2ForNormal, outputdir));
            var stage3Thumbnail = f.StartNew(() => CreateThumbnail(buffer2ForThumbnail, buffer3));

            // FOURTH TASK
            var stage4 = f.StartNew(() => SaveThumbnailBitmap(buffer3, outputdir));

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
            }
        }


        private static void LoadImages(string InputDirectory, BlockingCollection<BitmapWithFilePath> outputQueue)
        {
            try
            {
                foreach (string filePath in Directory.GetFiles(InputDirectory))
                {
                    if (Path.GetExtension(filePath) == ".bmp")
                    {
                        Bitmap bm = ImageProcessor.LoadFileAsImage(filePath);

                        var outputObj = new BitmapWithFilePath()
                        {
                            FilePath = filePath,
                            Image = bm
                        };
                        outputQueue.Add(outputObj);
                    }
                }
            }
            finally
            {
                outputQueue.CompleteAdding();
            }
        }

        private static void RemoveBackground(BlockingCollection<BitmapWithFilePath> inputQueue, Bitmap background_bm, params BlockingCollection<BitmapWithFilePath>[] outputQueues)
        {
            try
            {
                foreach (var input in inputQueue.GetConsumingEnumerable())
                {
                    var result = ImageProcessor.RemoveBackground(input.Image, background_bm);
                    foreach (var outputQueue in outputQueues)
                    {
                        var outputObj = new BitmapWithFilePath()
                        {
                            FilePath = input.FilePath,
                            Image = result
                        };
                        outputQueue.Add(outputObj);
                    }
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

        private static void CreateThumbnail(BlockingCollection<BitmapWithFilePath> inputQueue, BlockingCollection<BitmapWithFilePath> outputQueue)
        {
            try
            {
                foreach (var input in inputQueue.GetConsumingEnumerable())
                {
                    var result = ImageProcessor.ResizeToThumbnail(input.Image);
                    var outputObj = new BitmapWithFilePath()
                    {
                        FilePath = input.FilePath,
                        Image = result
                    };
                    outputQueue.Add(outputObj);
                }
            }
            finally
            {
                outputQueue.CompleteAdding();
            }
        }

        private static void SaveThumbnailBitmap(BlockingCollection<BitmapWithFilePath> inputQueue, string outputdir)
        {
            foreach (var input in inputQueue.GetConsumingEnumerable())
            {
                string output_thumb = outputdir + Path.DirectorySeparatorChar + Path.GetFileNameWithoutExtension(input.FilePath) + "_thumbnail" + Path.GetExtension(input.FilePath);
                ImageProcessor.SaveBitmapToFile(input.Image, output_thumb);
            }
        }

        private static void SaveBitmap(BlockingCollection<BitmapWithFilePath> inputQueue, string outputdir)
        {
            foreach (var input in inputQueue.GetConsumingEnumerable())
            {
                string output = outputdir + Path.DirectorySeparatorChar + Path.GetFileName(input.FilePath);
                ImageProcessor.SaveBitmapToFile(input.Image, output);
            }
        }
    }
}
