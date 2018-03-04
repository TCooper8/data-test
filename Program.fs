
/// <summary> This module is intended to act as a utility for storing columnar data. </summary>
module Storage =
  open System

  /// <summary> Union for values. </summary>

  type Value =
    | String of string
    | Float of float
    | Null

  type DType =
    | StringDType of uint8 option
    | FloatDType
    | RemovedDType

  type ColumnName = string

  type Column =
    | StringColumn of ColumnName * uint8 option * string seq
    | FloatColumn of ColumnName * float seq
    | RemovedColumn of ColumnName

  [<Interface>]
  type IColumnReader =
    abstract Read: unit -> Column
    inherit IDisposable

  [<Interface>]
  type IColumnWriter =
    abstract Write: Value -> unit Async
    abstract Write: Value seq -> unit Async
    inherit IDisposable

  [<Interface>]
  type IStorage =
    abstract Create: ColumnName -> DType -> IColumnWriter Async
    abstract Open: ColumnName -> IColumnReader Async
    abstract Exists: ColumnName -> bool
    abstract Columns: string seq
    abstract DTypes: DType seq

module FileStorage =
  open Storage
  open System.IO
  open System.IO.Compression

  let private getString = function
    | String str -> str
    | _ -> ""

  let private  getFloat = function
    | Float f -> f
    | _ -> nan

  let private write (writer:BinaryWriter) = function
    | StringDType (Some size) ->
      fun value ->
        let buffer = Array.zeroCreate (int size)
        let value = getString value
        let len = min value.Length (int size)

        for i in 0 .. len - 1 do
          buffer.[i] <- value.[i]
        writer.Write buffer

    | StringDType None ->
      getString >> writer.Write

    | FloatDType ->
      getFloat >> writer.Write

    | RemovedDType ->
      ignore

  let private read columnName (reader:BinaryReader) =
    let stream = reader.BaseStream
  
    function
    | StringDType (Some size) ->
      StringColumn (
        columnName,
        Some size,
        seq {
          while stream.Position < stream.Length do
            let value = reader.ReadChars (int size)
            yield System.String value
        }
      )

    | StringDType None ->
      StringColumn (
        columnName,
        None,
        seq {
          while stream.Position < stream.Length do
            yield reader.ReadString()
        }
      )

    | FloatDType ->
      FloatColumn(
        columnName,
        seq {
          let recordSize = 8
          let chunkSize = recordSize * (1 <<< 16)
          while stream.Position < stream.Length - int64 chunkSize do
            let chunk = reader.ReadBytes(chunkSize)
            use mem = new MemoryStream(chunk)
            use reader = new BinaryReader(mem)
            while mem.Position < mem.Length do
              yield reader.ReadDouble()

          while stream.Position < stream.Length do
            yield reader.ReadDouble()
        }
      )

    | RemovedDType ->
      RemovedColumn columnName

  type private ColumnWriter (dtype, writer) =
    interface IColumnWriter with
      member __.Write value = async {
        return
          write writer dtype value
      }

      member __.Write values = async {
        for value in values do
          write writer dtype value
      }

      member __.Dispose () =
        writer.Dispose()

  type private ColumnReader (dtype, columnName, reader) =
    interface IColumnReader with
      member __.Read () = read columnName reader dtype

      member __.Dispose() =
        reader.Dispose()

  type private FileStorage (rootDir:string) =
    let dir = Directory.CreateDirectory rootDir

    let cpath columnName = Path.Combine(rootDir, columnName + ".bin")

    let columns =
      dir.GetFiles()
      |> Seq.map (fun info -> info.Name)
      |> Seq.filter (fun name -> name.StartsWith "." |> not)
      |> Seq.map (fun name -> name.Replace(".bin", ""))

    let readDType (reader:BinaryReader) =
      let head = reader.ReadBytes 6 |> Array.map char |> System.String
      let size = reader.ReadInt32()

      match head, size with
      | "string", -1 -> StringDType None
      | "string", size -> StringDType (Some <| uint8 size)
      | "float ", _ -> FloatDType
      | "remove", _ -> RemovedDType

    let dtypes =
      columns
      |> Seq.map (fun col ->
        let path = cpath col
        use stream = File.OpenRead path
        use gstream = new GZipStream(stream, CompressionMode.Decompress)
        use reader = new BinaryReader(gstream)
        readDType reader
      )

    interface IStorage with
      member __.Create columnName dtype = async {
        printfn "Creating %s" columnName
        let stream = File.Create (cpath columnName)
        let gstream = new GZipStream(stream, CompressionLevel.Optimal)
        let writer = new BinaryWriter(stream)

        match dtype with
        | StringDType None ->
          writer.Write "string"B
          writer.Write (int32 -1l)

        | StringDType (Some size) ->
          writer.Write "string"B
          writer.Write (int32 size)
          
        | FloatDType ->
          writer.Write "float "B
          writer.Write (int32 -1)

        | RemovedDType ->
          writer.Write "remove"B
          writer.Write (int32 -1)

        return
          new ColumnWriter(dtype, writer)
          :> IColumnWriter
      }

      member __.Open columnName = async {
        printfn "Opening %s" columnName
        let stream = File.OpenRead (cpath columnName)
        let gstream = new GZipStream(stream, CompressionMode.Decompress)
        let reader = new BinaryReader(stream)
        let dtype = readDType reader

        printfn "Dtype = %A" dtype

        return
          new ColumnReader(dtype, columnName, reader)
          :> IColumnReader
      }

      member __.Exists columnName =
        File.Exists (cpath columnName)

      member __.Columns = columns
      member __.DTypes = dtypes

  let init rootDir =
    FileStorage rootDir
    :> IStorage

module Data =
  open System
  open Storage
  open CardinalityEstimation

  type ColumnStats = {
    column: string
    min: float
    max: float
    unique: float
    count: int64
    top: string option
    freq: int64
  } with
    static member Empty column = 
      { column = column
        min = nan
        max = nan
        unique = nan
        count = 0L
        top = None
        freq = 0L
      }

  type private UniqueSet (estimator:CardinalityEstimator) =
    member x.Add (value:obj) =
      match value with
      | :? float as v ->
      //UniqueSet(Set.add value uniques)
        estimator.Add v
      | :? string as v ->
        estimator.Add v
      | _ -> estimator.Add (value.GetHashCode())
      x

    member __.Count () =
      estimator.Count()

  type private CategoryStats = {
    column: string
    labelCounts: Map<string, int64>
    uniqueSet: UniqueSet
    count: int64
  } with
    static member Empty column =
      { column = column
        labelCounts = Map.empty
        uniqueSet = new UniqueSet(CardinalityEstimator())
        count = 0L
      }

    member x.Stats () =
      let top, freq =
        if x.labelCounts.IsEmpty then None, 0L
        else
          x.labelCounts
          |> Map.toSeq
          |> Seq.maxBy snd
          |> fun (label, count) ->
            Some label, count

      { column = x.column
        min = nan
        max = nan
        unique = float <| x.uniqueSet.Count()
        count = x.count
        top = top
        freq = freq
      }

  type private ColumnStatsAcc = {
    column: string
    min: float
    max: float
    uniqueSet: UniqueSet
    count: int64
  } with
    static member Empty column =
      { column = column
        min = nan
        max = nan
        uniqueSet = new UniqueSet(CardinalityEstimator())
        count = 0L
      }

    member x.Stats () =
      { column = x.column
        min = x.min
        max = x.max
        unique = float <| x.uniqueSet.Count()
        count = x.count
        top = None
        freq = 0L
      }

  let private getFloat = function
    | Float v -> v
    | _ -> nan

  let private bindNaN binding a b =
    if Double.IsNaN a then b
    else binding a b

  let private foldDescribe = function
    | StringColumn (name, _, values) ->
      values
      |> Seq.fold (fun (acc:CategoryStats) value ->
        { acc with
            uniqueSet = acc.uniqueSet.Add value
            labelCounts =
              match Map.tryFind value acc.labelCounts with
              | None -> Map.add value 1L acc.labelCounts
              | Some count -> Map.add value (count + 1L) acc.labelCounts
            count = acc.count + 1L
        }
      ) (CategoryStats.Empty name)
      |> fun acc -> acc.Stats()
    | FloatColumn (name, ls) ->
      ls
      |> Seq.filter (Double.IsNaN >> not)
      |> Seq.fold (fun acc value ->
        { acc with
            min = bindNaN min acc.min value
            max = bindNaN max acc.max value
            uniqueSet = acc.uniqueSet.Add value
            count = acc.count + 1L
        }
      ) (ColumnStatsAcc.Empty name)
      |> fun acc -> acc.Stats()
    | RemovedColumn name -> ColumnStats.Empty name

  let private bindColumn (storage:IStorage) column binding = async {
    let! column = storage.Open column
    let values = column.Read()
    return binding values
  }

  [<AbstractClass>]
  type IFrame (storage:IStorage) =
    abstract Columns: string seq
    abstract DTypes: DType seq
    abstract Column: ColumnName -> Column Async

    member __.DescribeColumn columnName =
      bindColumn
        storage
        columnName
        foldDescribe

module Seq =
  let tryTake (n:int) (s: _ seq) =
    let e = s.GetEnumerator()
    let i = ref 0
    seq {
      while e.MoveNext() && !i < n do
        i := !i + 1
        yield e.Current
    }

module Binary =
  open Storage
  open Data

  type private Frame (storage:IStorage) =
    inherit IFrame(storage)

    override __.Columns = storage.Columns
    override __.DTypes = storage.DTypes
    override __.Column col = async {
      let! reader = storage.Open col
      return reader.Read()
    }

  let load storage =
    Frame storage
    :> IFrame

module Csv =
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
    | RemovedDType ->
      fun _ -> Null

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