using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    [TestFixture]
    public class CsgScratch
    {
        [Test]
        [Explicit]
        public void Dump()
        {
            var a = CsgM0Tests.QuadBox(Box3d.Unit);
            var b = CsgM0Tests.QuadBox(new Box3d(new V3d(0.5, 0.5, 0.5), new V3d(1.5, 1.5, 1.5)));
            var kernel = new Kernel(new Eps(1e-11));
            kernel.Ingest(a, 0);
            kernel.Ingest(b, 1);
            var pipe = new Pipeline(kernel);
            pipe.Run();

            Console.WriteLine($"kernel tris {kernel.TriangleCount} verts {kernel.Positions.Count}");
            Console.WriteLine($"fragments {pipe.Fragments.Count}");
            for (var f = 0; f < pipe.Fragments.Count; f++)
            {
                var fr = pipe.Fragments[f];
                var mesh = kernel.TriMesh[fr.Parent];
                var c = (kernel.Positions[fr.V0] + kernel.Positions[fr.V1] + kernel.Positions[fr.V2]) / 3;
                Console.WriteLine($"  frag {f}: mesh {mesh} parent {fr.Parent} face {kernel.TriFace[fr.Parent]} " +
                    $"verts ({fr.V0},{fr.V1},{fr.V2}) centroid {c:0.000} inside={pipe.Inside[f]}");
            }
        }
    }
}
