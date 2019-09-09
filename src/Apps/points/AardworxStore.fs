namespace Aardworx

open System
open Aardvark.Base
open Aardvark.Data.Points
open Aardvark.Geometry.Points
open Aardvark.Data
open System.IO
open System.Collections.Immutable
open System.Collections.Generic

[<RequireQualifiedAccess>]
type PathRoot =
    | Home
    | Root

type Path(root : PathRoot, comp : string[]) =

    static let homePath = Path(PathRoot.Home, [||])
    static let rootPath = Path(PathRoot.Root, [||])

    static member Home = homePath
    static member Root = rootPath

    static member FromString (str : string) =
        let root, str =
            if str.StartsWith "~" then PathRoot.Home, str.Substring(1)
            elif str.StartsWith "/" then PathRoot.Root, str
            else PathRoot.Home, str

        let comp =
            str.Split([|'/'|], StringSplitOptions.RemoveEmptyEntries)

        Path(root, comp)
         
    member x.Owner =
        if root = PathRoot.Root && comp.Length >= 2 && comp.[0] = "home" then
            match Guid.TryParse comp.[1] with
            | (true, g) -> Some g
            | _ -> None
        else
            None

    member x.normalize (userId : Guid) =
        if root = PathRoot.Home then
            Path.Root + "home" + string userId + x
        else    
            x

    member x.isParentOf (other : Path) =
        if x.root = other.root && other.components.Length >= comp.Length then
            let rec check (i : int) =
                if i >= comp.Length then 
                    true
                else
                    comp.[i] = other.components.[i] && 
                    check (i + 1)
            check 0
        else
            false
            
    member x.relative (userId : Guid) =
        if root = PathRoot.Root && comp.Length >= 2 && comp.[0] = "home" && comp.[1] = string userId then
            Path(PathRoot.Home, FSharp.Collections.Array.skip 2 comp)
        else    
            x

    member x.name =
        if comp.Length > 0 then comp.[comp.Length - 1]
        else
            match root with
            | PathRoot.Root -> "/"
            | PathRoot.Home -> "~"
    member x.parent =
        if comp.Length > 0 then Path(root, FSharp.Collections.Array.take (comp.Length - 1) comp)
        elif root = PathRoot.Home then Path(PathRoot.Root, [|"home"|])
        else x

    member x.root : PathRoot = root
    member x.components : string[] = comp
    static member (+) (a : Path, b : Path) = Path (a.root, FSharp.Collections.Array.append a.components b.components)
    static member (+) (a : Path, b : string) = a + Path.FromString b
    static member (+) (a : string, b : Path) = Path.FromString a + b
    
    override x.ToString() =
        if comp.Length = 0 then
            match root with
            | PathRoot.Root -> "/"
            | PathRoot.Home -> "~"
        else
            match root with
            | PathRoot.Root -> "/" + FSharp.Core.String.concat "/" comp
            | PathRoot.Home -> "~/" + FSharp.Core.String.concat "/" comp

    override x.GetHashCode() =
        comp |> FSharp.Collections.Array.fold (fun h c -> Aardvark.Base.HashCode.Combine(h, Unchecked.hash c)) (Unchecked.hash root)

    override x.Equals o =
        match o with
        | :? Path as o -> root = o.root && comp.Length = o.components.Length && FSharp.Collections.Array.forall2 (=) comp o.components
        | _ -> false

    interface IComparable with
        member x.CompareTo o =
            compare (string x) (string o)



module Rpc =
    open System.Net.Http
    open Newtonsoft.Json
    open Newtonsoft.Json.Linq

    let private client = new HttpClient()
    let private apiUrl = "https://aardworxapi-dev2.azurewebsites.net/api/rpc"

    let call (token : string) (method : string) (args : seq<string * obj>) =
        let par = JObject()
        for (k, v) in args do
            par.[k] <- JToken.FromObject v

        let r = JObject()
        r.["method"] <- JToken.op_Implicit method
        r.["jsonrpc"] <- JToken.op_Implicit "2.0"
        r.["params"] <- par
        r.["id"] <- JToken.op_Implicit "0"
        

        async {
            use c = new StringContent(r.ToString(), System.Text.Encoding.UTF8, "application/json")
            let! reply = client.PostAsync(apiUrl + "?token=" + token, c) |> Async.AwaitTask

            
            printfn "%s %A" method (Seq.toList args)
            if isNull reply || isNull reply.Content then
                return null
            else
                let! c = reply.Content.ReadAsStringAsync() |> Async.AwaitTask
                if String.IsNullOrWhiteSpace c then
                    return null
                else
                    printfn "%s" c
                    let a = JObject.Parse c
                    return a.["result"]
        }

type Sas =
    {
        url         : string
        sas         : Option<string>
        validUntil  : DateTime
    }

    member x.get (name : string) =
        let url = 
            if x.url.EndsWith "/" then x.url
            else x.url + "/"
        match x.sas with
        | Some sas -> url + name + sas
        | None -> url + name

module Authentication = 
    open Newtonsoft.Json
    open Newtonsoft.Json.Linq

    let me (token : string) : Async<string> =   
        async {
            let! r = Rpc.call token "accounts.me" []
            return r.["id"] |> JToken.op_Explicit
        }

    let renewToken (token : string) : Async<string> =
        async {
            let! res = Rpc.call token "accounts.renewToken" [] 
            return res.["token"] |> JToken.op_Explicit
        }

    let createBlob (token : string) : Async<string> =
        async {
            let! res = Rpc.call token "files.createBlobContainer" ["type", box "AzureBlobContainer"]
            return res.["path"] |> JToken.op_Explicit
        }

    let getUploadSas (token : string) (path : string) =
        async {
            let! res = Rpc.call token "files.getBlobContainerUploadSas" ["path", box path]
            let url : string = res.["url"] |> JToken.op_Explicit
            let sas : string = res.["sas"] |> JToken.op_Explicit
            let valid : DateTime = res.["validUntil"] |> JToken.op_Explicit
            return { url = url; sas = Some sas; validUntil = valid }
        }

module FileSystem =
    open System.Net.Http
    open Newtonsoft.Json
    open Newtonsoft.Json.Linq

    let copyBlob (token : string) (src : string) (dst : string) : Async<string> =
        async {
            let! res = Rpc.call token "files.copyBlobContainer" ["sourceBlobContainer", box src; "destinationFolder", box dst]
            return res.["path"] |> JToken.op_Explicit
        }
        
    let write (token : string) (path : string) (content : JObject) =
        async {
            let! res = Rpc.call token "files.write" ["path", box path; "type", box "file"; "data", box content]
            ()
        }

module Blob =
    open System.Net.Http
    open Newtonsoft.Json
    open Newtonsoft.Json.Linq
    let private client = new HttpClient()

    let put (sas : Sas) (name : string) (data : byte[]) =
        async { 
            use c = new ByteArrayContent(data)
            c.Headers.Add("x-ms-blob-type", "BlockBlob")
            let! res = client.PutAsync(sas.get name, c) |> Async.AwaitTask
            ()
        }

    let putString (sas : Sas) (name : string) (data : string) =
        async { 
            use c = new StringContent(data)
            c.Headers.Add("x-ms-blob-type", "BlockBlob")
            let! res = client.PutAsync(sas.get name, c) |> Async.AwaitTask
            ()
        }


module Test =
    open Newtonsoft.Json.Linq
    
    let createObj (kv : list<string * obj>) =
        let o = JObject()
        for (k, v) in kv do o.[k] <- JToken.FromObject v
        box o


    let createProject (token : string) (name : string) (info : IPointCloudNode) =
        async {
            let projectPath = Path.Home + "pcv" + "projects" + name

            let! blobPath = Authentication.createBlob token
            let! sas = Authentication.getUploadSas token blobPath


            let rec traverse (info : IPointCloudNode) =
                async {
                    if not (isNull info) then

                        let subnodes =
                            if isNull info.Subnodes then [||]
                            else info.Subnodes |> Array.map (fun n -> if isNull n then Guid.Empty else n.Value.Id)

                        let data =
                            List.map (fun (a,b) -> KeyValuePair(a,b)) [
                                yield Durable.Octree.Cell, box info.Cell
                                yield Durable.Octree.PointCountCell, info.PointCountCell |> int :> obj
                                yield Durable.Octree.PointCountTreeLeafsFloat64, info.PointCountTree |> float :> obj
                                yield Durable.Octree.BoundingBoxExactGlobal, info.BoundingBoxExactGlobal :> obj
                                yield Durable.Octree.PositionsLocal3f, (info.Positions.Value :> obj)
                                if subnodes.Length > 0 then
                                    yield Durable.Octree.SubnodesGuids, subnodes :> obj
                                yield Durable.Octree.Colors3b, (info.Colors.Value |> Array.map C3b :> obj)
                            ]

                        let name = string info.Id
                        use s = new MemoryStream()
                        use w = new BinaryWriter(s)
                        Codec.Encode(w, Durable.Octree.Node, data)
                        let bytes = s.ToArray()
                        do! Blob.put sas name bytes

                        if not (isNull info.Subnodes) then
                            for n in info.Subnodes do
                                if not (isNull n) then
                                    do! traverse n.Value

                }


            do! traverse info

            let rootInfo =
                let cell = info.Cell
                let centroid = cell.GetCenter() + V3d info.CentroidLocal
                createObj [
                    "RootId", string info.Id :> obj
                    "PointCount", info.PointCountTree |> float :> obj
                    "Bounds", createObj [
                        "Min", createObj [
                            "X", info.BoundingBoxExactGlobal.Min.X :> obj
                            "Y", info.BoundingBoxExactGlobal.Min.Y :> obj
                            "Z", info.BoundingBoxExactGlobal.Min.Z :> obj
                        ]
                        "Max", createObj [
                            "X", info.BoundingBoxExactGlobal.Max.X :> obj
                            "Y", info.BoundingBoxExactGlobal.Max.Y :> obj
                            "Z", info.BoundingBoxExactGlobal.Max.Z :> obj
                        ]
                    ]
                    "Centroid", createObj [
                        "X", centroid.X :> obj
                        "Y", centroid.Y :> obj
                        "Z", centroid.Z :> obj
                    ]

                    "CentroidStdDev", info.CentroidLocalStdDev |> float :> obj

                    "Cell", createObj [
                        "X", float cell.X :> obj
                        "Y", float cell.Y :> obj
                        "Z", float cell.Z :> obj
                        "E", cell.Exponent :> obj
                    ]

                    "GZipped", false :> obj
                ]

            do! Blob.putString sas "root.json" (string rootInfo)


            let! copyPath = FileSystem.copyBlob token blobPath (projectPath + "clouds" |> string)
            let copyPath = Path.FromString copyPath

            let cellCenter = info.Cell.GetCenter()
            let bounds = info.BoundingBoxExactGlobal
            let centroid = cellCenter + V3d info.CentroidLocal
            let pointCount = info.PointCountTree
            let radius = 20.0

            let projectInfo =
                createObj [
                    "clouds", box [|
                        createObj [
                            "store",        box copyPath.name
                            "key",          box "cloud.pts"
                            "name",         box "cloud"
                            "pointCount",   box (float pointCount)
                            "bounds", createObj [
                                "Min", createObj [
                                    "X", box bounds.Min.X
                                    "Y", box bounds.Min.Y
                                    "Z", box bounds.Min.Z
                                ]
                                "Max", createObj [
                                    "X", box bounds.Max.X
                                    "Y", box bounds.Max.Y
                                    "Z", box bounds.Max.Z
                                ]
                            ]
                            "centroid", createObj [
                                "X", box centroid.X
                                "Y", box centroid.Y
                                "Z", box centroid.Z
                            ]

                            
                            "camera", createObj [
                                "center", createObj [
                                    "X", box centroid.X
                                    "Y", box centroid.Y
                                    "Z", box centroid.Z
                                ]

                                "radius", box radius
                                "phi", box Constant.PiQuarter
                                "theta", box Constant.PiQuarter
                            ]

                        ]
                    |]

                    "camera", createObj []

                    "annotations", box [||]
                ]

            do! FileSystem.write token (projectPath + "project.json" |> string ) (unbox projectInfo)

            ()
        }


