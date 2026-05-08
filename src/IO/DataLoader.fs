namespace IO

open System
open System.IO
open System.Collections.Generic
open System.Text.RegularExpressions
open System.Net.Http

open PureDomain.Types


module DataLoader=
    let dataInicio = DateTime(2025, 7, 1)
    let dataFim = DateTime(2025, 12, 31)
    let apiKeyFmp = "3VNl535JXQwpcOSFURuB3SascaWn9C7t"

    let loadConsolidatedData () : PriceData =
        let consolidatedPathApi = Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "dados", "dados_consolidados_api.csv")
        let consolidatedPathRaw = Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "dados", "dados_consolidados.csv")
        
        let consolidatedPath = 
            if File.Exists(consolidatedPathApi) then consolidatedPathApi
            elif File.Exists(consolidatedPathRaw) then consolidatedPathRaw
            else
                RawLoader.consolidateAdjCloseData()
                consolidatedPathRaw
        
        let lines = File.ReadAllLines(consolidatedPath)
        
        if lines.Length < 2 then
            failwith "CSV consolidado vazio ou inválido"
        
        let headerFields = lines.[0].Split(',')
        let tickers = headerFields.[1..]
        
        let prices =
            lines.[1..]
            |> Array.map (fun line ->
                let fields = RawLoader.parseCsvFields line |> List.toArray
                fields.[1..]
                |> Array.map (fun field ->
                    match System.Decimal.TryParse(field) with
                    | (true, value) -> value
                    | (false, _) -> 0m
                )
            )
        
        {
            Tickers = tickers
            Prices = prices
        }

    let loadData () : PriceData =
        loadConsolidatedData ()
