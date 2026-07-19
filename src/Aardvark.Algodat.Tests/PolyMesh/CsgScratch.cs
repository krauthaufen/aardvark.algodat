using Aardvark.Base;
using NUnit.Framework;
using System;
using System.Linq;

namespace Aardvark.Geometry.Tests
{
    /// <summary>Manual tools: big fuzz sweep and perf measurement (Explicit).</summary>
    [TestFixture]
    public class CsgScratch
    {
        [Test]
        [Explicit]
        public void BigFuzz()
        {
            var fails = 0; var total = 0;
            void Check(PolyMesh a, PolyMesh b, string ctx)
            {
                total++;
                try
                {
                    var va = CsgM0Tests.Volume(a); var vb = CsgM0Tests.Volume(b);
                    var vu = Csg.Union(a, b).Sum(CsgM0Tests.Volume);
                    var vi = Csg.Intersection(a, b).Sum(CsgM0Tests.Volume);
                    var vd = Csg.Difference(a, b).Sum(CsgM0Tests.Volume);
                    var scale = va.Abs() + vb.Abs();
                    if ((vu + vi - va - vb).Abs() > 1e-9 * scale) throw new Exception($"volume identity union: {vu}+{vi} != {va}+{vb}");
                    if ((vd + vi - va).Abs() > 1e-9 * scale) throw new Exception($"volume identity diff: {vd}+{vi} != {va}");
                }
                catch (Exception e)
                {
                    fails++;
                    Console.WriteLine($"FAIL {ctx}: {e.Message}");
                }
            }

            var rnd = new RandomSystem(123456);
            for (var i = 0; i < 1500; i++)
            {
                var t = new V3d(rnd.UniformInt(17) * 0.25 - 2, rnd.UniformInt(17) * 0.25 - 2, rnd.UniformInt(17) * 0.25 - 2);
                var rot = Trafo3d.RotationEulerInDegrees(rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90, rnd.UniformInt(4) * 90);
                Check(CsgM0Tests.QuadBox(Box3d.Unit),
                      CsgM0Tests.QuadBox(new Box3d(V3d.Zero, new V3d(1 + rnd.UniformInt(3) * 0.5, 1, 1))).Transformed(rot * Trafo3d.Translation(t)),
                      $"snapped {i}");
            }
            for (var i = 0; i < 1000; i++)
            {
                var t = rnd.UniformV3d() * 2.4 - new V3d(1.2);
                var rot = Trafo3d.RotationEuler(rnd.UniformDouble() * 6.28, rnd.UniformDouble() * 6.28, rnd.UniformDouble() * 6.28);
                Check(CsgM0Tests.QuadBox(Box3d.Unit), CsgM0Tests.QuadBox(Box3d.Unit).Transformed(rot * Trafo3d.Translation(t)), $"generic {i}");
            }
            for (var i = 0; i < 200; i++)
            {
                var c = rnd.UniformV3d() * 2 - new V3d(0.5);
                Check(CsgM5Tests.Icosphere(V3d.Zero, 1.0, 2), CsgM5Tests.Icosphere(c, 0.4 + rnd.UniformDouble(), 2), $"spheres {i}");
            }
            for (var i = 0; i < 300; i++)
            {
                var snap = (i & 1) == 0;
                var t = snap
                    ? new V3d(rnd.UniformInt(13) * 0.5 - 2, rnd.UniformInt(13) * 0.5 - 2, rnd.UniformInt(9) * 0.5 - 1)
                    : rnd.UniformV3d() * 4 - new V3d(1.5);
                var rot = snap ? Trafo3d.RotationZInDegrees(rnd.UniformInt(4) * 90)
                    : Trafo3d.RotationEuler(rnd.UniformDouble() * 6.28, rnd.UniformDouble() * 6.28, rnd.UniformDouble() * 6.28);
                Check(CsgM6Tests.LPrism(), CsgM6Tests.LPrism().Transformed(rot * Trafo3d.Translation(t)), $"lprism {i}");
            }
            Console.WriteLine($"BIGFUZZ done: {fails} failures / {total} configs");
            Assert.That(fails, Is.EqualTo(0));
        }

        [Test]
        [Explicit]
        public void Perf()
        {
            foreach (var sub in new[] { 4, 5, 6 })
            {
                var a = CsgM5Tests.Icosphere(V3d.Zero, 1.0, sub);
                var b = CsgM5Tests.Icosphere(new V3d(0.8, 0.3, 0.2), 1.0, sub);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                var union = Csg.Union(a, b);
                sw.Stop();
                var tris = (a.FirstIndexArray.Length - 1) * 2;
                Console.WriteLine($"PERF union {tris} tris: {sw.ElapsedMilliseconds} ms ({union.Sum(m => m.FirstIndexArray.Length - 1)} out tris)");
            }
        }
    }
}
