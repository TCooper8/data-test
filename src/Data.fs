module Data

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

