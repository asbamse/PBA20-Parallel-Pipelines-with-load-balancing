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
        private static string BackgroundFilePath { get; set; }

        static void Main(string[] args)
        {
            LoadAppSettings();
            Console.WriteLine(""); // Seperator

            Console.WriteLine("Executing all operations sequentially.");
            MeasureTime(ExecuteSequentialAllOperation);
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
            Console.WriteLine("Output to: " + OutputDirectory);
            BackgroundFilePath = o1.Value<string>("BackgroundFilePath");
            Console.WriteLine("Background bitmap from: " + BackgroundFilePath);
        }

        /// <summary>
        /// Executes all operations sequentially.
        /// </summary>
        private static void ExecuteSequentialAllOperation()
        {
            foreach (string filePath in Directory.GetFiles(InputDirectory))
            {
                Bitmap background_bm = ImageProcessor.LoadFileAsImage(BackgroundFilePath);
                ExecuteSequentialOperation(filePath, background_bm);
                Console.WriteLine(""); // Seperator
            }
        }

        /// <summary>
        /// Executes a image processing operation.
        /// </summary>
        /// <param name="filePath">Path to the file image to process.</param>
        /// <param name="background_bm">The background image.</param>
        private static void ExecuteSequentialOperation(string filePath, Bitmap background_bm)
        {
            Console.WriteLine($"Processing: {Path.GetFileName(filePath)}");
            Bitmap target_bm = ImageProcessor.LoadFileAsImage(filePath);

            target_bm = ImageProcessor.RemoveBackground(target_bm, background_bm);
            Bitmap target_thumb_bm = ImageProcessor.ResizeToThumbnail(target_bm);

            string output = OutputDirectory + Path.DirectorySeparatorChar + Path.GetFileName(filePath);
            string output_thumb = OutputDirectory + Path.DirectorySeparatorChar + Path.GetFileNameWithoutExtension(filePath) + "_thumbnail" + Path.GetExtension(filePath);
            ImageProcessor.SaveBitmapToFile(target_bm, output);
            ImageProcessor.SaveBitmapToFile(target_thumb_bm, output_thumb);

            Console.WriteLine($"Finished processing: {filePath}");
            Console.WriteLine($"Processed image saved to: {output}");
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
