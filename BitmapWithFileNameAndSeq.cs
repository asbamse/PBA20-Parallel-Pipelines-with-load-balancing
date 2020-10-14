using System;
using System.Drawing;

namespace PBA20_Parallel_Pipelines_with_load_balancing
{
    public class BitmapWithFilePathAndSeq : ISequenceIdentifiable
    {
        public Bitmap Image { get; set; }
        public string FilePath { get; set; }
        public int SeqId { get; set; }
    }
}
