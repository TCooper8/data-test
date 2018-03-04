/// <summary> This module is intended to act as a utility for storing columnar data. </summary>
module Storage
open System

/// <summary> Union for values. </summary>
type Value =
  | String of string
  | Float of float
  | Null

/// <summary> Data storage types. </summary>
type DType =
  | StringDType of uint8 option
  | FloatDType

type ColumnName = string

type Column =
  | StringColumn of ColumnName * uint8 option * string seq
  | FloatColumn of ColumnName * float seq

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
