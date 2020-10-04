using System;
using System.Diagnostics;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
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
