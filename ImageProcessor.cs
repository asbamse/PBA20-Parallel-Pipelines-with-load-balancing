using System;
using System.Drawing;
using System.Threading;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    /// <summary>
    /// Class for processing System.Image objects.
    /// </summary>
    public static class ImageProcessor
    {
        /// <summary>
        /// Loads a file as bitmap.
        /// </summary>
        /// <param name="fileLocation">The files location.</param>
        /// <returns>Image.</returns>
        public static Bitmap LoadFileAsImage(Uri fileLocation)
        {
            Thread.Sleep(2000);
            return new Bitmap(256, 256);
        }

        /// <summary>
        /// Save bitmap to file.
        /// </summary>
        /// <param name="bitmap">The image to use.</param>
        /// <param name="fileLocation">The location to save file.</param>
        public static void SaveBitmapToFile(Bitmap bitmap, Uri fileLocation)
        {
            Thread.Sleep(2000);
        }

        /// <summary>
        /// Resizes/compress bitmap to thumbnail size.
        /// </summary>
        /// <param name="bitmap">The image to use.</param>
        /// <returns>Processed image.</returns>
        public static Bitmap ResizeToThumbnail(Bitmap bitmap)
        {
            Thread.Sleep(2000);
            return new Bitmap(256, 256);
        }

        /// <summary>
        /// Removes the background from image.
        /// </summary>
        /// <param name="bitmap">The image to use.</param>
        /// <param name="background">Bitmap of background</param>
        /// <returns>Processed image.</returns>
        public static Bitmap RemoveBackground(Bitmap bitmap, Bitmap background)
        {
            Thread.Sleep(2000);
            return new Bitmap(256, 256);
        }
    }
}
