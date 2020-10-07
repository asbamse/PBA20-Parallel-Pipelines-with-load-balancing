using System;
using System.Collections.Concurrent;
using System.Drawing;
using System.IO;
using System.Threading.Tasks;

namespace PBA20_Parallel_Pipelines_with_load_balancing.input
{
    public class SimplePipeline
    {
        public SimplePipeline()
        {
        }

        static int BUFFER_SIZE = 10;

        public static void ExecuteSimplePipelineOperation(string inputDirectory, string BackgroundFilePath, string outputdir, string filePath)
        {
            var buffer1 = new BlockingCollection<Bitmap>(BUFFER_SIZE);
            var buffer2ForNormal = new BlockingCollection<Bitmap>(BUFFER_SIZE);
            var buffer2ForThumbnail = new BlockingCollection<Bitmap>(BUFFER_SIZE);
            var buffer3 = new BlockingCollection<Bitmap>(BUFFER_SIZE);

            Bitmap background_bm = ImageProcessor.LoadFileAsImage(BackgroundFilePath);

            //TODO Add Cancelation Token
            var f = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

            // FIRST TASK
            var stage1 = f.StartNew(() => LoadImages(inputDirectory, buffer1));

            // SECOND TASK
            var stage2 = f.StartNew(() => RemoveBackground(buffer1, background_bm, buffer2ForNormal, buffer2ForThumbnail));

            // THIRD TASKs
            var stage3Normal = f.StartNew(() => SaveBitmap(buffer2ForNormal, outputdir, filePath));
            var stage3Thumbnail = f.StartNew(() => CreateThumbnail(buffer2ForThumbnail, buffer3));

            // FOURTH TASK
            var stage4 = f.StartNew(() => SaveThumbnailBitmap(buffer3, outputdir, filePath));
        }


        private static void LoadImages(string InputDirectory, BlockingCollection<Bitmap> outputQueue)
        {
            try
            {
                foreach (string filePath in Directory.GetFiles(InputDirectory))
                {
                    Bitmap bm = ImageProcessor.LoadFileAsImage(filePath);
                    outputQueue.Add(bm);
                }
            }
            finally
            {
                outputQueue.CompleteAdding();
            }
        }

        private static void RemoveBackground(BlockingCollection<Bitmap> inputQueue, Bitmap background_bm, params BlockingCollection<Bitmap>[] outputQueues)
        {
            try
            {
                foreach (var target_bm in inputQueue.GetConsumingEnumerable())
                {
                    var result = ImageProcessor.RemoveBackground(target_bm, background_bm);
                    foreach (var outputQueue in outputQueues)
                    {
                        outputQueue.Add(result);
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

        private static void CreateThumbnail(BlockingCollection<Bitmap> inputQueue, BlockingCollection<Bitmap> outputQueue)
        {
            try
            {
                foreach (var target_bm in inputQueue.GetConsumingEnumerable())
                {
                    var result = ImageProcessor.ResizeToThumbnail(target_bm);
                    outputQueue.Add(result);
                }
            }
            finally
            {
                outputQueue.CompleteAdding();
            }
        }

        private static void SaveThumbnailBitmap(BlockingCollection<Bitmap> inputQueue, string outputdir, string filePath)
        {
            string output_thumb = outputdir + Path.DirectorySeparatorChar + Path.GetFileNameWithoutExtension(filePath) + "_thumbnail" + Path.GetExtension(filePath);

            foreach (var target_thumb_bm in inputQueue.GetConsumingEnumerable())
            {
                ImageProcessor.SaveBitmapToFile(target_thumb_bm, output_thumb);
            }
        }

        private static void SaveBitmap(BlockingCollection<Bitmap> inputQueue, string outputdir, string filePath)
        {
            string output = outputdir + Path.DirectorySeparatorChar + Path.GetFileName(filePath);

            foreach (var target_bm in inputQueue.GetConsumingEnumerable())
            {
                ImageProcessor.SaveBitmapToFile(target_bm, output);
            }
        }
    }
}
