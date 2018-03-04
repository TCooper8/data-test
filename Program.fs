
open System.Diagnostics
open System.Security.Cryptography
open System.Text

let work (filename:string) =
  let hash =
    let enc = Encoding.UTF8
    let md5 = MD5.Create()
    let bytes = enc.GetBytes filename
    let hash = md5.ComputeHash(bytes)
    let builder = StringBuilder()

    for b in hash do
      builder.AppendFormat("{0:x2}", b) |> ignore
    string builder

  let path = "cache/cache_" + hash
  let storage = FileStorage.init path
  let frame =
    Csv.readFile storage filename
    |> Async.RunSynchronously
    //Binary.load storage

  printfn "%A" frame.Columns

  let stopwatch = Stopwatch()
  stopwatch.Start()
  printfn "!!!Starting stats"

  frame.Columns
  |> Seq.map (fun col -> frame.DescribeColumn col)
  |> Async.Parallel
  |> Async.RunSynchronously
  |> Seq.iter (fun stats ->
    printfn "\n%s" stats.column
    printfn "\tmin = %A" stats.min
    printfn "\tmax = %A" stats.max
    printfn "\tunique = %A" stats.unique
    printfn "\tcount = %A" stats.count
    printfn "\ttop = %A" (defaultArg stats.top "")
    printfn "\tfreq = %A" stats.freq
  )
  stopwatch.Stop()
  printfn "Stats time = %A" stopwatch.Elapsed.TotalSeconds

[<EntryPoint>]
let main argv =
  match Array.tryLast argv with
  | Some filename ->
    let stopwatch = Stopwatch()
    stopwatch.Start()
    do work filename
    stopwatch.Stop()

    printfn "Total time = %A" stopwatch.Elapsed.TotalSeconds

    0

  | None ->
    printfn "Expected filename argument."
    printfn "\tExample > dotnet run test.csv"
    1