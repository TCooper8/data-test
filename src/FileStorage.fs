module FileStorage

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
