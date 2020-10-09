using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Threading.Tasks;
using System.Linq;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    public class SimplePipelineLoadBalenced
    {
        static int BUFFER_SIZE = 10;

        public static void ExecuteSimpleLoadBalencedPipelineOperation(string inputDirectory, string BackgroundFilePath, string outputdir)
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

            //TODO Add Cancelation Token
            var f = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);

            // FIRST TASK
            var stage1 = f.StartNew(() => LoadImages(inputDirectory, buffer1));

            // SECOND TASK
            var stage2Task1 = f.StartNew(() => RemoveBackground(buffer1, background_bm, buffer2ForNormalTask1, buffer2ForThumbnailTask1));
            var stage2Task2 = f.StartNew(() => RemoveBackground(buffer1, background_bm, buffer2ForNormalTask2, buffer2ForThumbnailTask2));

            //MULTIPLEXER
            var multiplexerNormal = f.StartNew(() => Multiplexer(buffer3ForNormal, buffer2ForNormalTask1, buffer2ForNormalTask2));
            var multiplexerThumbnail = f.StartNew(() => Multiplexer(buffer3ForThumbnail, buffer2ForThumbnailTask1, buffer2ForThumbnailTask2));

            // THIRD TASKs
            var stage3Normal = f.StartNew(() => SaveBitmap(buffer3ForNormal, outputdir));
            var stage3Thumbnail = f.StartNew(() => CreateThumbnail(buffer3ForThumbnail, buffer4));

            // FOURTH TASK
            var stage4 = f.StartNew(() => SaveThumbnailBitmap(buffer4, outputdir));

            Task.WaitAll(stage1, stage2Task1, stage2Task2, multiplexerNormal, multiplexerThumbnail, stage3Normal, stage3Thumbnail, stage4);
        }

        private static void Multiplexer(BlockingCollection<BitmapWithFilePathAndSeq> outputQueue, params BlockingCollection<BitmapWithFilePathAndSeq>[] inputQueues)
        {
            int nextIndex = 1;
            var foundItems = new List<BitmapWithFilePathAndSeq>();

            while (!inputQueues.All(q => q.IsCompleted))
            {
                BlockingCollection<BitmapWithFilePathAndSeq>.TakeFromAny(inputQueues, out var item);
                if (item != null)
                {
                    if (item.SeqId == nextIndex)
                    {
                        outputQueue.Add(item);

                        BitmapWithFilePathAndSeq nextFound = null;
                        do
                        {
                            nextIndex++;
                            nextFound = foundItems.Find(i => i.SeqId == nextIndex);

                            if (nextFound != null)
                            {
                                outputQueue.Add(nextFound);
                            }
                        } while (nextFound != null);
                    }
                    else
                    {
                        foundItems.Add(item);
                    }
                }
            }
            outputQueue.CompleteAdding();
        }


        private static void LoadImages(string InputDirectory, BlockingCollection<BitmapWithFilePathAndSeq> outputQueue)
        {
            int SeqIdNext = 1;
            try
            {
                foreach (string filePath in Directory.GetFiles(InputDirectory))
                {
                    if (Path.GetExtension(filePath) == ".bmp")
                    {
                        Bitmap bm = ImageProcessor.LoadFileAsImage(filePath);

                        var outputObj = new BitmapWithFilePathAndSeq()
                        {
                            FilePath = filePath,
                            Image = bm,
                            SeqId = SeqIdNext++
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

        private static void RemoveBackground(BlockingCollection<BitmapWithFilePathAndSeq> inputQueue, Bitmap background_bm, params BlockingCollection<BitmapWithFilePathAndSeq>[] outputQueues)
        {
            try
            {
                foreach (var input in inputQueue.GetConsumingEnumerable())
                {
                    var result = ImageProcessor.RemoveBackground(input.Image, background_bm);
                    foreach (var outputQueue in outputQueues)
                    {
                        var outputObj = input;
                        outputObj.Image = result;
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

        private static void CreateThumbnail(BlockingCollection<BitmapWithFilePathAndSeq> inputQueue, BlockingCollection<BitmapWithFilePathAndSeq> outputQueue)
        {
            try
            {
                foreach (var input in inputQueue.GetConsumingEnumerable())
                {
                    var result = ImageProcessor.ResizeToThumbnail(input.Image);

                    var outputObj = input;
                    outputObj.Image = result;
                    outputQueue.Add(outputObj);
                }
            }
            finally
            {
                outputQueue.CompleteAdding();
            }
        }

        private static void SaveThumbnailBitmap(BlockingCollection<BitmapWithFilePathAndSeq> inputQueue, string outputdir)
        {
            foreach (var input in inputQueue.GetConsumingEnumerable())
            {
                string output_thumb = outputdir + Path.DirectorySeparatorChar + Path.GetFileNameWithoutExtension(input.FilePath) + "_thumbnail" + Path.GetExtension(input.FilePath);
                ImageProcessor.SaveBitmapToFile(input.Image, output_thumb);
            }
        }

        private static void SaveBitmap(BlockingCollection<BitmapWithFilePathAndSeq> inputQueue, string outputdir)
        {
            foreach (var input in inputQueue.GetConsumingEnumerable())
            {
                string output = outputdir + Path.DirectorySeparatorChar + Path.GetFileName(input.FilePath);
                ImageProcessor.SaveBitmapToFile(input.Image, output);
            }
        }
    }
}
