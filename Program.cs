using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Diagnostics;
using System.Drawing;
using System.IO;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    class Program
    {
        private static string InputDirectory { get; set; }
        private static string OutputDirectory { get; set; }

        static void Main(string[] args)
        {
            LoadAppSettings();

            Console.WriteLine("Executing all operations sequentially.");
            MeasureTime(ExecuteSequentialOperation);
            Console.WriteLine("Finished executing all operations sequentially.");
        }

        /// <summary>
        /// Loads application settings from the appsettings.json
        /// </summary>
        private static void LoadAppSettings()
        {
            string path = $"{Directory.GetParent(AppContext.BaseDirectory).FullName}{Path.DirectorySeparatorChar}appsettings.json";
            JObject o1 = JObject.Parse(File.ReadAllText(path));
            InputDirectory = o1.Value<string>("InputDirectory");
            Console.WriteLine("Input from: " + InputDirectory);
            OutputDirectory = o1.Value<string>("OutputDirectory");
            Console.WriteLine("Output from: " + OutputDirectory);
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
