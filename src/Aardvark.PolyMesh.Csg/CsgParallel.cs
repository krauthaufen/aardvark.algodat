using System;
using System.Threading.Tasks;

namespace Aardvark.Geometry
{
    /// <summary>
    /// Parallel loop helper: honors a per-call thread cap, runs plain
    /// sequential loops for maxThreads == 1 (no scheduler overhead), and
    /// unwraps AggregateException so kernel exceptions (CsgInputException,
    /// CsgVerificationException) surface with their original types.
    /// </summary>
    internal static class CsgParallel
    {
        public static void For(int fromInclusive, int toExclusive, int maxThreads, Action<int> body)
        {
            if (maxThreads <= 1 || toExclusive - fromInclusive <= 1)
            {
                for (var i = fromInclusive; i < toExclusive; i++) body(i);
                return;
            }
            try
            {
                Parallel.For(fromInclusive, toExclusive,
                    new ParallelOptions { MaxDegreeOfParallelism = maxThreads }, body);
            }
            catch (AggregateException e)
            {
                throw e.InnerExceptions[0];
            }
        }
    }
}
