using System;
using System.Diagnostics;
using System.Drawing;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Executing all operations sequentially.");
            MeasureTime(ExecuteSequentialOperation);
            Console.WriteLine("Finished executing all operations sequentially.");
        }

        /// <summary>
        /// Executes all operations sequentially.
        /// </summary>
        private static void ExecuteSequentialOperation()
        {
            Bitmap target_bm = ImageProcessor.LoadFileAsImage(new Uri("C:\\image.bmp"));
            Bitmap background_bm = ImageProcessor.LoadFileAsImage(new Uri("C:\\backgroundFilter.bmp"));
            target_bm = ImageProcessor.RemoveBackground(target_bm, background_bm);
            target_bm = ImageProcessor.ResizeToThumbnail(target_bm);
            ImageProcessor.SaveBitmapToFile(target_bm, new Uri("C:\\processed_image.bmp"));
        }

        /// <summary>
        /// Measures action and prints the result to console.
        /// </summary>
        /// <param name="p">Action to measure.</param>
        private static void MeasureTime(Action p)
        {
            Stopwatch sw = Stopwatch.StartNew();
            p.Invoke();
            sw.Stop();
            Console.WriteLine("Time = {0:F5} sec.", sw.ElapsedMilliseconds / 1000d);
        }
    }
}
