namespace IO

open System
open System.IO
open System.Collections.Generic
open System.Text.RegularExpressions

open PureDomain.Types


module DataLoader=

    let loadConsolidatedData () : PriceData =
        /// Lê o arquivo CSV consolidado e transforma em matriz
        /// Retorna: { Tickers = [AAPL, MSFT, ...], Prices = [[dia1_preços], [dia2_preços], ...] }
        
        let consolidatedPath = Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "dados", "dados_consolidados.csv")
        
        if not (File.Exists(consolidatedPath)) then
            printfn "Arquivo consolidado não encontrado! Rodando consolidação..."
            RawLoader.consolidateAdjCloseData()
        
        let lines = File.ReadAllLines(consolidatedPath)
        
        // Parse header para obter tickers (ignora "DATE" no índice 0)
        let headerFields = lines.[0].Split(',')
        let tickers = headerFields.[1..] // Pula "DATE"
        
        // Parse dados: cria matriz [dia][ação]
        let prices =
            lines.[1..]  // Pula header
            |> Array.map (fun line ->
                let fields = RawLoader.parseCsvFields line |> List.toArray  // Usa RawLoader
                fields.[1..]  // Pula DATE
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
