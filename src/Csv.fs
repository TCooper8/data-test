module Csv

open System
open System.IO
open System.Security.Cryptography
open System.Text

open Storage
open Data

type private CsvFrame (columns, dtypes, storage:IStorage) =
  inherit IFrame(storage)

  override __.Columns = columns
  override __.DTypes = dtypes
  override __.Column columnName = async {
    let! reader = storage.Open columnName
    return reader.Read()
  }

let private splitFields (str:string) =
  str.Split ','
  |> Seq.map (fun str -> str.Trim [| '"'; ' ' |])

let (|ParseFloat|_|) (str:string) =
  match Double.TryParse str with
  | false, _ -> None
  | true, v -> Some v

let private parseField = function
  | FloatDType ->
    function
    | ParseFloat v -> Float v
    | _ -> Float nan
  | StringDType _ ->
    String

let private parseRecord dtypes (line:string) =
  line
  |> splitFields
  |> Seq.zip dtypes
  |> Seq.map (fun (dtype, field) -> parseField dtype field)

type private FalseWriter () =
  interface IColumnWriter with
    member __.Write (_:Value) = async { return () }
    member __.Write (_:Value seq) = async { return () }
    member __.Dispose() = ()

type private Inference = {
  index: int
  floatCount: int
  stringCount: int
  uniques: string list
} with
  static member Empty index = 
    { index = index
      floatCount = 0
      stringCount = 0
      uniques = List.empty
    }

  static member Init index (str:string) =
    let floatCount =
      match Double.TryParse str with
      | false, _ -> 0
      | true, _ -> 1
    let stringCount = if floatCount <= 0 then 1 else 0
    { index = index
      floatCount = floatCount
      stringCount = stringCount
      uniques = [ str ]
    }

  member x.Merge (y:Inference) =
    { x with
        floatCount = x.floatCount + y.floatCount
        stringCount = x.stringCount + y.stringCount
        uniques = x.uniques @ y.uniques
    }

  member x.DType () =
    let floatToStrRatio = float x.floatCount / float x.stringCount
    let uniques = Set x.uniques
    if floatToStrRatio > 0.9 then FloatDType
    else
      let size =
        x.uniques
        |> Seq.maxBy (fun label -> label.Length)
        |> fun s -> s.Length
        |> uint8
      StringDType (Some size)

let private infer (path:string) = async {
  let lines = seq {
    use reader = new StreamReader(path)
    while true do
      yield reader.ReadLine()
  }
  let header = Seq.tryHead lines

  if Option.isNone header then
    failwithf "Expected header in csv file"
  let columns = header.Value |> splitFields |> Seq.toArray
  let rows = Seq.tryTake 1024 lines |> Seq.map splitFields
  let columnCount = columns.Length

  let inferences =
    rows
    |> Seq.fold (fun inferences row ->
      row
      |> Seq.mapi (fun i value -> i, Inference.Init i value)
      |> Seq.fold (fun (inferences:Inference[]) (i, infer) ->
        inferences.[i] <- inferences.[i].Merge infer
        inferences
      ) inferences
    ) (Array.init columnCount Inference.Empty)
    |> Seq.map (fun infer -> infer.DType())

  return inferences
}

let private computeHash (path:string) = async {
  use stream = File.OpenRead(path)
  let chunkOffset = 1 <<< 20
  let chunkSize = 1 <<< 10
  let md5 = MD5.Create()

  let buffer = Array.zeroCreate chunkSize

  while stream.Position < stream.Length - int64 chunkSize do
    let count = stream.Read(buffer, 0, chunkSize)
    md5.TransformBlock(buffer, 0, count, buffer, 0) |> ignore

  let count = stream.Read(buffer, 0, chunkSize)
  let hashBuffer = md5.TransformFinalBlock(buffer, 0, count)
  let mutable builder = StringBuilder()
  let hash = md5.ComputeHash hashBuffer
  for b in hash do
    builder <- builder.AppendFormat("{0:x2}", b)

  let hash = string builder
  return hash
}

let loadHash (path:string) = async {
  let! hash = computeHash path
  let filepath = hash + "_hash"
  printfn "Hash file = %A" filepath

  if File.Exists filepath then
    return true, filepath
  else return false, filepath
}

let readFile (storage:IStorage) (path:string) = async {
  let! cached, hash = loadHash path
  if cached then
    printfn "Loading binary from hash %A" hash
    return Binary.load storage
  else

  use reader = new StreamReader(path)
  let columnNames =
    reader.ReadLine()
    |> splitFields
    |> Seq.toArray

  let! dtypes = infer path
  let dtypes = Seq.toArray dtypes 
  printfn "DTypes = %A" dtypes

  let writers =
    Seq.zip columnNames dtypes
    |> Seq.map (fun (name, dtype) ->
      if storage.Exists name then new FalseWriter() :> IColumnWriter
      else
        storage.Create name dtype |> Async.RunSynchronously
    )
    |> Seq.toArray

  use _writers =
    { new IDisposable with
        member __.Dispose () =
          writers
          |> Seq.iter (fun writer -> writer.Dispose())
    }

  let mutable total = 0
  while not reader.EndOfStream do
    let count = 1024 * 100
    let mutable i = 0
    let buffers = Array.init (Seq.length writers) (fun _ ->
      Array.zeroCreate<Value> count
    )

    while not reader.EndOfStream && i < count do
      let line = reader.ReadLine()

      parseRecord dtypes line
      |> Seq.iteri (fun j value ->
        buffers.[j].[i] <- value
      )

      i <- i + 1

    Seq.zip writers buffers
    |> Seq.map (fun (writer, buffer) ->
      buffer
      |> writer.Write
    )
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    total <- total + i

    printfn "Wrote %d lines" total

  use writer = File.Create hash
  writer.WriteByte(byte 1)
  printfn "Writing cache %A" hash

  return
    CsvFrame(columnNames, dtypes, storage)
    :> IFrame
}
