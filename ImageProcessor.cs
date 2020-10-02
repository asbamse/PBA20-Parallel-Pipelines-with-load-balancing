using System;
using System.Collections.Generic;
using System.Drawing;
using System.Net;
using System.Text;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    /// <summary>
    /// Class for processing System.Image objects.
    /// </summary>
    public static class ImageProcessor
    {
        /// <summary>
        /// Resizes/compress bitmap to thumbnail size.
        /// </summary>
        /// <param name="bitmap">The image to use.</param>
        /// <returns>Processed image.</returns>
        public static Bitmap ResizeToThumbnail(Bitmap bitmap)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Removes the background from image.
        /// </summary>
        /// <param name="bitmap">The image to use.</param>
        /// <param name="background">Bitmap of background</param>
        /// <returns>Processed image.</returns>
        public static Bitmap RemoveBackground(Bitmap bitmap, Bitmap background)
        {
            throw new NotImplementedException();
        }
    }
}
