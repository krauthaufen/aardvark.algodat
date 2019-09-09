open Aardvark.Base.Incremental
open Aardvark.Base
open Aardvark.SceneGraph
open Aardvark.Base.Rendering
open Aardvark.Rendering.PointSet
open Aardvark.Application
open Aardvark.Application.Slim
open Aardvark.Rendering.Text
open Aardvark.Algodat.App.Viewer
open Aardvark.Data.Points.Import


[<EntryPoint>]
let main args =  

    import @"C:\Users\Schorsch\Development\WorkDirectory\JBs_Haus.pts"  @"C:\Users\Schorsch\Development\WorkDirectory\jb" "jb" (Args.parse [||])
    view @"C:\Users\Schorsch\Development\WorkDirectory\jb" ["jb"] (Args.parse [||])

    let args = Args.parse args
    
    match args.command with

    | Some (Info filename) -> info filename args

    | Some (Import (filename, store, key)) -> import filename store key args
      
    | Some (View (store, key)) ->
        view store [key] args
      
    | Some Gui ->
        failwith "not implemented"
      
    | Some (Download (baseurl, targetdir)) -> download baseurl targetdir args
      
    | None ->
        printUsage()

    0
