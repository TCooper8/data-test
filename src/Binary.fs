module Binary

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

