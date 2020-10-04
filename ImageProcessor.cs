using System;
using System.Drawing;
using System.Drawing.Imaging;
using System.Runtime.InteropServices;
using System.Threading;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    /// <summary>
    /// Class for processing System.Image objects.
    /// </summary>
    public static class ImageProcessor
    {
        private static readonly float THUMBNAIL_MAX_WIDTH = 128;
        private static readonly float THUMBNAIL_MAX_HEIGHT = 128;

        /// <summary>
        /// Loads a file as bitmap.
        /// </summary>
        /// <param name="fileLocation">The files location.</param>
        /// <returns>Image.</returns>
        public static Bitmap LoadFileAsImage(string fileLocation)
        {
            return new Bitmap(fileLocation);
        }

        /// <summary>
        /// Save bitmap to file.
        /// </summary>
        /// <param name="bitmap">The image to use.</param>
        /// <param name="fileLocation">The location to save file.</param>
        public static void SaveBitmapToFile(Bitmap bitmap, string fileLocation)
        {
            bitmap.Save(fileLocation);
        }

        /// <summary>
        /// Resizes/compress bitmap to thumbnail size.
        /// </summary>
        /// <param name="bitmap">The image to use.</param>
        /// <returns>Processed image.</returns>
        public static Bitmap ResizeToThumbnail(Bitmap bitmap)
        {
            float scale = Math.Min(THUMBNAIL_MAX_WIDTH / bitmap.Width, THUMBNAIL_MAX_HEIGHT / bitmap.Height);

            return new Bitmap(bitmap, new Size((int)(bitmap.Width * scale), (int)(bitmap.Height * scale)));
        }

        /// <summary>
        /// Removes the background from image.
        /// </summary>
        /// <param name="bitmap">The image to use.</param>
        /// <param name="background">Bitmap of background</param>
        /// <returns>Processed image.</returns>
        public static Bitmap RemoveBackground(Bitmap bitmap, Bitmap background)
        {
            BitmapData sourceData = bitmap.LockBits(new Rectangle(0, 0,
                                bitmap.Width, bitmap.Height),
                                ImageLockMode.ReadOnly, PixelFormat.Format32bppArgb);

            byte[] pixelBuffer = new byte[sourceData.Stride * sourceData.Height];
            Marshal.Copy(sourceData.Scan0, pixelBuffer, 0, pixelBuffer.Length);
            bitmap.UnlockBits(sourceData);

            BitmapData blendData = background.LockBits(new Rectangle(0, 0,
                                    background.Width, background.Height),
                                    ImageLockMode.ReadOnly, PixelFormat.Format32bppArgb);

            byte[] blendBuffer = new byte[blendData.Stride * blendData.Height];
            Marshal.Copy(blendData.Scan0, blendBuffer, 0, blendBuffer.Length);
            background.UnlockBits(blendData);

            int blue, green, red;
            for (int k = 0; (k + 4 < pixelBuffer.Length) &&
                            (k + 4 < blendBuffer.Length); k += 4)
            {
                blue = pixelBuffer[k] == blendBuffer[k] ? 255 : pixelBuffer[k];
                green = pixelBuffer[k + 1] == blendBuffer[k + 1] ? 255 : pixelBuffer[k + 1];
                red = pixelBuffer[k + 2] == blendBuffer[k + 2] ? 255 : pixelBuffer[k + 2];

                if (blue < 0) { blue = 0; }
                else if (blue > 255) { blue = 255; }

                if (green < 0) { green = 0; }
                else if (green > 255) { green = 255; }

                if (red < 0) { red = 0; }
                else if (red > 255) { red = 255; }

                pixelBuffer[k] = (byte)blue;
                pixelBuffer[k + 1] = (byte)green;
                pixelBuffer[k + 2] = (byte)red;
            }

            Bitmap resultBitmap = new Bitmap(bitmap.Width, bitmap.Height);

            BitmapData resultData = resultBitmap.LockBits(new Rectangle(0, 0,
                                    resultBitmap.Width, resultBitmap.Height),
                                    ImageLockMode.WriteOnly, PixelFormat.Format32bppArgb);

            Marshal.Copy(pixelBuffer, 0, resultData.Scan0, pixelBuffer.Length);
            resultBitmap.UnlockBits(resultData);

            return resultBitmap;
        }
    }
}
