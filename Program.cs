using Newtonsoft.Json.Linq;
using System;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Threading;

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

            Console.WriteLine("Executing Simple Pipeline");

            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                MeasureTime(() => SimplePipeline.ExecuteSimplePipelineOperation(InputDirectory, BackgroundFilePath, OutputDirectory, cts.Token));
                Console.WriteLine("Finished executing Simple Pipeline");
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine(ex.GetType());
                Console.WriteLine($"    {ex.Message}");
            }

            Console.WriteLine("Executing Simple Load Balanced Pipeline");
            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                MeasureTime(() => SimplePipelineLoadBalenced.ExecuteSimpleLoadBalencedPipelineOperation(InputDirectory, BackgroundFilePath, OutputDirectory, cts.Token));
                Console.WriteLine("Finsihed executing Simple Load Balanced Pipeline");
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine(ex.GetType());
                Console.WriteLine($"    {ex.Message}");
            }

            Console.WriteLine("Executing Test PipelineStep");
            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                MeasureTime(() => TestPipeLineStep.ExecuteTestPipelineStepOperation(InputDirectory, BackgroundFilePath, OutputDirectory, cts.Token));
                Console.WriteLine("Finsihed executing Test pipeline step");
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine(ex.GetType());
                Console.WriteLine($"    {ex.Message}");
            }
        }

        /// <summary>
        /// Loads application settings from the appsettings.json
        /// </summary>
        private static void LoadAppSettings()
        {
            string path = $"{Directory.GetParent(AppContext.BaseDirectory).FullName}{Path.DirectorySeparatorChar}appsettings.json";
            JObject o1 = JObject.Parse(File.ReadAllText(path));
            InputDirectory = o1.Value<string>("InputDirectory");
            OutputDirectory = o1.Value<string>("OutputDirectory");
            BackgroundFilePath = o1.Value<string>("BackgroundFilePath");
        }

        /// <summary>
        /// Executes all operations sequentially.
        /// </summary>
        private static void ExecuteSequentialAllOperation()
        {
            foreach (string filePath in Directory.GetFiles(InputDirectory))
            {
                if (Path.GetExtension(filePath) == ".bmp")
                {
                    Bitmap background_bm = ImageProcessor.LoadFileAsImage(BackgroundFilePath);
                    ExecuteSequentialOperation(filePath, background_bm);
                }
            }
        }



        /// <summary>
        /// Executes a image processing operation.
        /// </summary>
        /// <param name="filePath">Path to the file image to process.</param>
        /// <param name="background_bm">The background image.</param>
        private static void ExecuteSequentialOperation(string filePath, Bitmap background_bm)
        {
            Bitmap target_bm = ImageProcessor.LoadFileAsImage(filePath);

            target_bm = ImageProcessor.RemoveBackground(target_bm, background_bm);
            Bitmap target_thumb_bm = ImageProcessor.ResizeToThumbnail(target_bm);

            string output = OutputDirectory + Path.DirectorySeparatorChar + Path.GetFileName(filePath);
            string output_thumb = OutputDirectory + Path.DirectorySeparatorChar + Path.GetFileNameWithoutExtension(filePath) + "_thumbnail" + Path.GetExtension(filePath);
            ImageProcessor.SaveBitmapToFile(target_bm, output);
            ImageProcessor.SaveBitmapToFile(target_thumb_bm, output_thumb);
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
