using System;
using System.Threading.Tasks;

namespace Aardvark.Geometry
{
    /// <summary>
    /// LSD radix sort for packed edge keys ((a &lt;&lt; 32) | b with both ids
    /// &lt; 2^22): four 11-bit passes at shifts {0, 11, 32, 43} — the dead bit
    /// ranges of the packing are skipped. ~3-4x faster than comparison sorting
    /// for the pipeline's edge-key arrays.
    /// </summary>
    internal static class RadixSorter
    {
        private static readonly int[] s_shifts = { 0, 11, 32, 43 };
        private const int Mask = (1 << 11) - 1;

        /// <summary>Sorts keys (with parallel values) ascending. Requires both packed ids &lt; 2^22.</summary>
        public static void SortEdgeKeys(long[] keys, int[] values, int count, int maxThreads = 1)
        {
            var blocks = maxThreads <= 1 || count < 1 << 16
                ? 1
                : Math.Max(1, Math.Min(Math.Min(maxThreads, Environment.ProcessorCount), count / (1 << 14)));
            var tk = new long[count];
            var tv = new int[count];
            var srcK = keys; var srcV = values;
            var dstK = tk; var dstV = tv;
            // blocked stable LSD: per-block histograms, global (digit, block)
            // prefix, then each block scatters to precomputed offsets —
            // deterministic and stable at any thread count
            var histograms = new int[blocks][];
            for (var b = 0; b < blocks; b++) histograms[b] = new int[1 << 11];
            var blockSize = (count + blocks - 1) / blocks;

            foreach (var shift in s_shifts)
            {
                var sk = srcK;
                CsgParallel.For(0, blocks, blocks, blk =>
                {
                    var hist = histograms[blk];
                    Array.Clear(hist, 0, hist.Length);
                    var lo = blk * blockSize;
                    var hi = Math.Min(lo + blockSize, count);
                    for (var i = lo; i < hi; i++) hist[(int)((ulong)sk[i] >> shift) & Mask]++;
                });
                var sum = 0;
                for (var d = 0; d <= Mask; d++)
                    for (var b = 0; b < blocks; b++)
                    {
                        var c = histograms[b][d];
                        histograms[b][d] = sum;
                        sum += c;
                    }
                var sv = srcV; var dk = dstK; var dv = dstV;
                CsgParallel.For(0, blocks, blocks, blk =>
                {
                    var offsets = histograms[blk];
                    var lo = blk * blockSize;
                    var hi = Math.Min(lo + blockSize, count);
                    for (var i = lo; i < hi; i++)
                    {
                        var at = offsets[(int)((ulong)sk[i] >> shift) & Mask]++;
                        dk[at] = sk[i];
                        dv[at] = sv[i];
                    }
                });
                (srcK, dstK) = (dstK, srcK);
                (srcV, dstV) = (dstV, srcV);
            }
            // four passes: result is back in the original arrays
        }
    }
}
