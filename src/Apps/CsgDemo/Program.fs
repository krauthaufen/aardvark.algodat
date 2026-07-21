open Aardvark.Base
open Aardvark.Rendering
open Aardvark.SceneGraph
open Aardvark.Application
open FSharp.Data.Adaptive
open Aardvark.Geometry
open System

// A minimal Aardvark.Rendering window that shows the output of our mesh-CSG
// kernel (Aardvark.PolyMesh.Csg). SPACE cycles through the test cases.

[<EntryPoint>]
let main _ =
    Aardvark.Init()

    // hand-built / primitive meshes may carry per-face duplicated vertices and
    // loose winding — run them through the repair pass so the kernel gets clean
    // watertight manifolds.
    let clean (m : PolyMesh) =
        match PolyMeshRepair.Repair m with
        | [||] -> m
        | r -> r.[0]

    let box (b : Box3d) = PolyMeshPrimitives.Box(b, C4b.White) |> clean
    let rbox (b : Box3d) (rot : Trafo3d) =
        PolyMeshPrimitives.Box(b, C4b.White).Transformed(rot) |> clean
    let sphere (c : V3d) (r : float) =
        PolyMeshPrimitives.Sphere(48, r, C4b.White).Transformed(Trafo3d.Translation c) |> clean
    let cyl (c : V3d) (r : float) (h : float) (axis : V3d) =
        let m = PolyMeshPrimitives.Cylinder2(48, h, r, C4b.White, Symbol.Empty)
        m.Transformed(Trafo3d.RotateInto(V3d.OOI, axis.Normalized) * Trafo3d.Translation c) |> clean

    // one solid colour per input shape, stamped as a per-face attribute. The
    // CSG kernel back-maps every output face to the source operand it came
    // from (k.TriMesh/TriFace), so the result keeps each input's colour per
    // face — union/intersection/difference all preserve provenance.
    let csgColor = Symbol.Create "CsgColor"
    let palette =
        [| C4b(230uy, 120uy,  70uy); C4b( 70uy, 140uy, 220uy); C4b(110uy, 190uy, 110uy)
           C4b(180uy, 110uy, 205uy); C4b(230uy, 200uy,  90uy); C4b( 90uy, 190uy, 190uy)
           C4b(220uy,  90uy, 110uy) |]
    let paint (c : C4b) (m : PolyMesh) =
        let n = m.FirstIndexArray.Length - 1
        m.FaceAttributes.[csgColor] <- (Array.create n c :> Array)
        m
    let pal i = palette.[i % palette.Length]

    // concatenate two meshes into one (for the self-intersection demo)
    let mergeTwo (a : PolyMesh) (b : PolyMesh) =
        let voff = a.PositionArray.Length
        let ioff = a.VertexIndexArray.Length
        PolyMesh(
            PositionArray = Array.append a.PositionArray b.PositionArray,
            VertexIndexArray = Array.append a.VertexIndexArray (b.VertexIndexArray |> Array.map (fun v -> v + voff)),
            FirstIndexArray = Array.append a.FirstIndexArray (b.FirstIndexArray.[1..] |> Array.map (fun x -> x + ioff)))

    // ---- minimal ASCII-PLY reader (Stanford bunny: x y z [+ extra floats],
    // faces as "n i0 i1 .. in") -> PolyMesh ------------------------------------
    let loadPlyAscii (path : string) =
        let ci = System.Globalization.CultureInfo.InvariantCulture
        use r = new System.IO.StreamReader(path)
        let mutable nv = 0
        let mutable nf = 0
        let mutable vProps = 0
        let mutable el = ""   // which element we are currently counting props for
        let mutable line = r.ReadLine()
        while line <> null && line.Trim() <> "end_header" do
            let t = line.Trim()
            let parts = t.Split([|' '; '\t'|], StringSplitOptions.RemoveEmptyEntries)
            if parts.Length >= 3 && parts.[0] = "element" then
                el <- parts.[1]
                if parts.[1] = "vertex" then nv <- int parts.[2]
                elif parts.[1] = "face" then nf <- int parts.[2]
            elif parts.Length >= 1 && parts.[0] = "property" && el = "vertex" then
                vProps <- vProps + 1
            line <- r.ReadLine()
        let pos = Array.zeroCreate<V3d> nv
        for i in 0 .. nv - 1 do
            let p = r.ReadLine().Split([|' '; '\t'|], StringSplitOptions.RemoveEmptyEntries)
            pos.[i] <- V3d(Double.Parse(p.[0], ci), Double.Parse(p.[1], ci), Double.Parse(p.[2], ci))
        let fia = System.Collections.Generic.List<int>(nf + 1)
        let via = System.Collections.Generic.List<int>(nf * 3)
        fia.Add 0
        for _ in 0 .. nf - 1 do
            let p = r.ReadLine().Split([|' '; '\t'|], StringSplitOptions.RemoveEmptyEntries)
            let cnt = int p.[0]
            for j in 1 .. cnt do via.Add(int p.[j])
            fia.Add(via.Count)
        PolyMesh(PositionArray = pos, VertexIndexArray = via.ToArray(), FirstIndexArray = fia.ToArray())

    // pick up the bunny from an env override or one of the usual spots
    let bunnyPath =
        [ Environment.GetEnvironmentVariable "CSG_BUNNY"
          System.IO.Path.Combine(AppContext.BaseDirectory, "bun_zipper.ply")
          "/home/schorsch/projects/aardvark.algodat/src/Apps/CsgDemo/bun_zipper.ply" ]
        |> List.tryFind (fun p -> not (String.IsNullOrEmpty p) && System.IO.File.Exists p)

    // largest edge-connected component of a repair result
    let biggest (ms : PolyMesh[]) =
        if ms.Length = 0 then None else Some (ms |> Array.maxBy (fun m -> m.FirstIndexArray.Length))

    // load -> repair (welds seams + closes the range-scan holes in the base) ->
    // drill three cylinders straight through the body. Provenance colouring
    // shows the bunny orange and each drilled tunnel wall in its own colour.
    let bunnyCases =
        match bunnyPath with
        | None -> printfn "[bunny] bun_zipper.ply not found — skipping bunny cases"; []
        | Some path ->
            try
                // model up is +Y; the default camera's up is +Z. Trafo `a * b`
                // applies a FIRST, so stand upright (+Y -> +Z) and THEN spin
                // about the vertical to face the camera.
                let stand = Trafo3d.RotationX(Constant.PiHalf) * Trafo3d.RotationZ(2.2)
                let raw = (loadPlyAscii path).Transformed(stand)
                printfn "[bunny] loaded %d verts / %d faces" raw.PositionArray.Length (raw.FirstIndexArray.Length - 1)
                let fixed_ =
                    match biggest (PolyMeshRepair.Repair raw) with
                    | Some m -> m
                    | None -> raw
                printfn "[bunny] repaired -> %d faces (watertight)" (fixed_.FirstIndexArray.Length - 1)
                let repairedCase = "bunny: repaired (holes closed)", [| paint (pal 0) fixed_ |]
                let bb = Box3d(fixed_.PositionArray)
                let c = bb.Center
                let s = bb.Size.NormMax
                let hh = s * 2.4
                // three PARALLEL, non-intersecting tunnels straight through the
                // body (world X, side to side) at three heights — no
                // cylinder-cylinder coincidences, so the arrangement stays clean.
                let mkDrills radius =
                    [| bb.Min.Z + 0.32 * bb.Size.Z
                       bb.Min.Z + 0.54 * bb.Size.Z
                       bb.Min.Z + 0.74 * bb.Size.Z |]
                    |> Array.mapi (fun i z -> paint (pal (i + 3)) (cyl (V3d(c.X, c.Y, z)) radius hh V3d.IOO))
                // drill fat holes; if the organic surface trips verification,
                // back off the radius before giving up.
                let drilledOpt =
                    [ 0.075; 0.06; 0.045; 0.035 ]
                    |> List.tryPick (fun f ->
                        try
                            let d = Csg.Difference(paint (pal 0) fixed_, mkDrills (s * f))
                            printfn "[bunny] drilled r=%.4f -> %d faces" (s * f) (d |> Array.sumBy (fun m -> m.FirstIndexArray.Length - 1))
                            Some d
                        with e ->
                            printfn "[bunny] drill r=%.4f failed (%s) — smaller" (s * f) (e.Message.Split('\n').[0])
                            None)
                match drilledOpt with
                | Some d -> [ repairedCase; "bunny: drilled", d ]
                | None -> printfn "[bunny] all drill radii failed; showing repaired only"; [ repairedCase ]
            with e ->
                printfn "[bunny] failed: %s" e.Message; []

    let cases : (string * PolyMesh[])[] =
        Array.append
          [|
            "union: two spheres",
                Csg.Union(paint (pal 0) (sphere (V3d(-0.5, 0.0, 0.0)) 1.0),
                          paint (pal 1) (sphere (V3d(0.5, 0.0, 0.0)) 1.0))
            "intersect: box AND sphere",
                Csg.Intersection(paint (pal 0) (box (Box3d(V3d(-0.75), V3d(0.75)))),
                                 paint (pal 1) (sphere V3d.Zero 1.0))
            "difference: sphere MINUS box",
                Csg.Difference(paint (pal 0) (sphere V3d.Zero 1.0),
                               paint (pal 1) (box (Box3d(V3d(0.0, -1.1, -1.1), V3d(1.1, 1.1, 1.1)))))
            "difference: box MINUS 3 cylinders",
                Csg.Difference(paint (pal 0) (box (Box3d(V3d(-1.0), V3d(1.0)))),
                    [| paint (pal 1) (cyl V3d.Zero 0.45 3.0 V3d.IOO)
                       paint (pal 2) (cyl V3d.Zero 0.45 3.0 V3d.OIO)
                       paint (pal 3) (cyl V3d.Zero 0.45 3.0 V3d.OOI) |])
            "n-ary union: sphere cluster",
                Csg.Union([| for i in 0 .. 5 ->
                                paint (pal i) (sphere (V3d(cos (float i), sin (float i * 1.3), sin (float i)) * 0.7) 0.6) |])
            "self-intersection resolved",
                PolyMeshRepair.Sanitize(mergeTwo (paint (pal 0) (box (Box3d(V3d(-0.9), V3d(0.3))))) (paint (pal 1) (box (Box3d(V3d(-0.3), V3d(0.9))))))
            "intersect: 3 cylinders (Steinmetz)",
                Csg.Intersection(
                    [| paint (pal 0) (cyl V3d.Zero 0.75 3.0 V3d.IOO)
                       paint (pal 1) (cyl V3d.Zero 0.75 3.0 V3d.OIO)
                       paint (pal 2) (cyl V3d.Zero 0.75 3.0 V3d.OOI) |])
            "difference: sphere MINUS 3 cylinders",
                Csg.Difference(paint (pal 0) (sphere V3d.Zero 1.0),
                    [| paint (pal 1) (cyl V3d.Zero 0.4 3.0 V3d.IOO)
                       paint (pal 2) (cyl V3d.Zero 0.4 3.0 V3d.OIO)
                       paint (pal 3) (cyl V3d.Zero 0.4 3.0 V3d.OOI) |])
            "union: 3 crossed bars",
                Csg.Union(
                    [| paint (pal 0) (box (Box3d(V3d(-1.0, -0.3, -0.3), V3d(1.0, 0.3, 0.3))))
                       paint (pal 1) (box (Box3d(V3d(-0.3, -1.0, -0.3), V3d(0.3, 1.0, 0.3))))
                       paint (pal 2) (box (Box3d(V3d(-0.3, -0.3, -1.0), V3d(0.3, 0.3, 1.0)))) |])
            "intersect: 2 rotated cubes",
                Csg.Intersection(paint (pal 0) (box (Box3d(V3d(-0.9), V3d(0.9)))),
                    paint (pal 1) (rbox (Box3d(V3d(-0.9), V3d(0.9))) (Trafo3d.Rotation(V3d(1.0, 1.0, 1.0).Normalized, Constant.Pi / 4.0))))
            "difference: box MINUS sphere",
                Csg.Difference(paint (pal 0) (box (Box3d(V3d(-0.8), V3d(0.8)))),
                               paint (pal 1) (sphere (V3d(0.8, 0.8, 0.8)) 0.95))
            "union: box + sphere",
                Csg.Union(paint (pal 0) (box (Box3d(V3d(-0.7), V3d(0.7)))),
                          paint (pal 1) (sphere V3d.Zero 0.95))
          |]
          (List.toArray bunnyCases)

    // flat-shaded triangle soup: per-face normals + the colour of the input
    // shape each face came from (carried through the CSG as a face attribute).
    // All three vertices of a face share the colour, so shading stays flat.
    let fallback = C4b(200uy, 200uy, 200uy)
    let toGeometry (ms : PolyMesh[]) =
        let pos = System.Collections.Generic.List<V3f>()
        let nrm = System.Collections.Generic.List<V3f>()
        let col = System.Collections.Generic.List<C4b>()
        for mi in 0 .. ms.Length - 1 do
            let m = ms.[mi]
            let p = m.PositionArray
            let fia = m.FirstIndexArray
            let via = m.VertexIndexArray
            let faceCols =
                match m.FaceAttributes.TryGetValue csgColor with
                | true, (:? (C4b[]) as a) -> Some a
                | _ -> None
            for fi in 0 .. fia.Length - 2 do
                let c = match faceCols with Some a when fi < a.Length -> a.[fi] | _ -> fallback
                let s = fia.[fi]
                let e = fia.[fi + 1]
                for i in s + 1 .. e - 2 do
                    let a = p.[via.[s]]
                    let b = p.[via.[i]]
                    let d = p.[via.[i + 1]]
                    let n = V3f ((b - a).Cross(d - a).Normalized)
                    pos.Add(V3f a); pos.Add(V3f b); pos.Add(V3f d)
                    nrm.Add n; nrm.Add n; nrm.Add n
                    col.Add c; col.Add c; col.Add c
        let ig = IndexedGeometry(Mode = IndexedGeometryMode.TriangleList)
        ig.IndexedAttributes <- SymbolDict<Array>()
        ig.IndexedAttributes.[DefaultSemantic.Positions] <- (pos.ToArray() :> Array)
        ig.IndexedAttributes.[DefaultSemantic.Normals] <- (nrm.ToArray() :> Array)
        ig.IndexedAttributes.[DefaultSemantic.Colors] <- (col.ToArray() :> Array)
        ig

    // normalise each case into roughly [-1,1] so the camera frames them the same
    let fit (ms : PolyMesh[]) =
        let b = Box3d(ms |> Seq.collect (fun m -> m.PositionArray :> seq<V3d>))
        Trafo3d.Translation(-b.Center) * Trafo3d.Scale(3.4 / max 1e-9 b.Size.NormMax)

    let geoms = cases |> Array.map (snd >> toGeometry)
    let fits = cases |> Array.map (snd >> fit)

    let index = cval 0
    // GL by default (GLSL tolerates the double uniforms in the built-in shaders);
    // set CSG_BACKEND=vulkan to use Vulkan/MoltenVK on macOS if GL is unavailable.
    let chosenBackend =
        match Environment.GetEnvironmentVariable "CSG_BACKEND" with
        | "vulkan" | "Vulkan" | "vk" -> Backend.Vulkan
        | _ -> Backend.GL
    let win = window { backend chosenBackend; debug false }

    win.Keyboard.DownWithRepeats.Values.Add(fun k ->
        if k = Keys.Space then
            let ni = (index.Value + 1) % cases.Length
            transact (fun () -> index.Value <- ni)
            printfn "[%d/%d] %s  (%d faces)" (ni + 1) cases.Length (fst cases.[ni])
                (cases.[ni] |> snd |> Array.sumBy (fun m -> m.FirstIndexArray.Length - 1)))

    let sg =
        index
        |> AVal.map (fun i ->
            geoms.[i]
            |> Sg.ofIndexedGeometry
            |> Sg.transform fits.[i]
            |> Sg.shader {
                do! DefaultSurfaces.trafo
                do! DefaultSurfaces.vertexColor
                do! DefaultSurfaces.simpleLighting
               })
        |> Sg.dynamic

    win.Scene <- sg
    printfn "Aardvark.PolyMesh.Csg demo — press SPACE to cycle cases."
    printfn "[1/%d] %s" cases.Length (fst cases.[0])
    win.Run()
    0
