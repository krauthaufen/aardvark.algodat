using System;

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
        public static void SortEdgeKeys(long[] keys, int[] values, int count)
        {
            var tk = new long[count];
            var tv = new int[count];
            var counts = new int[1 << 11];
            var srcK = keys; var srcV = values;
            var dstK = tk; var dstV = tv;
            foreach (var shift in s_shifts)
            {
                Array.Clear(counts, 0, counts.Length);
                for (var i = 0; i < count; i++) counts[(int)((ulong)srcK[i] >> shift) & Mask]++;
                var sum = 0;
                for (var b = 0; b < counts.Length; b++) { var c = counts[b]; counts[b] = sum; sum += c; }
                for (var i = 0; i < count; i++)
                {
                    var at = counts[(int)((ulong)srcK[i] >> shift) & Mask]++;
                    dstK[at] = srcK[i];
                    dstV[at] = srcV[i];
                }
                (srcK, dstK) = (dstK, srcK);
                (srcV, dstV) = (dstV, srcV);
            }
            // four passes: result is back in the original arrays
        }
    }
}
