namespace IO
open System
open System.Net.Http
open System.IO
open System.Collections.Generic
open System.Text.RegularExpressions

open PureDomain.Types

module APILoader =

    let fetchHistoricalData (simbolo: string) (dataInicio: DateTime) (dataFim: DateTime) (apiKey: string) : Async<string> = async {
        try
            use client = new HttpClient()
            client.Timeout <- TimeSpan.FromSeconds(10.0)
            
            let fromDate = dataInicio.ToString("yyyy-MM-dd")
            let toDate = dataFim.ToString("yyyy-MM-dd")
            let url = sprintf "https://financialmodelingprep.com/api/v4/historical-price-full/%s?from=%s&to=%s&apikey=%s" simbolo fromDate toDate apiKey
            
            let! response = client.GetAsync(url) |> Async.AwaitTask
            
            if response.IsSuccessStatusCode then
                let! content = response.Content.ReadAsStringAsync() |> Async.AwaitTask
                return content
            else
                return ""
        with
        | ex ->
            return ""
    }

    let private parseAdjCloseFromJson (jsonContent: string) (ticker: string) : seq<string * decimal> =
        seq {
            try
                // Simples regex para extrair pares date/adjClose (sem depender de bibliotecas JSON complexas)
                let datePattern = @"""date""\s*:\s*""(\d{4}-\d{2}-\d{2})"""
                let closePattern = @"""adjClose""\s*:\s*([\d.]+)"
                
                let dateMatches = Regex.Matches(jsonContent, datePattern)
                let closeMatches = Regex.Matches(jsonContent, closePattern)
                
                if dateMatches.Count = closeMatches.Count then
                    for i = 0 to dateMatches.Count - 1 do
                        let dateStr = dateMatches.[i].Groups.[1].Value
                        let closeStr = closeMatches.[i].Groups.[1].Value
                        
                        match Decimal.TryParse(closeStr) with
                        | (true, closeValue) -> yield (dateStr, closeValue)
                        | _ -> ()
            with _ -> ()
        }

    let consolidateApiData (jsonList: (string * string) list) : unit =
        let allData = Dictionary<string, Dictionary<string, decimal>>()
        let mutable allTickers = Set.empty
        
        for (ticker, jsonContent) in jsonList do
            let data = parseAdjCloseFromJson jsonContent ticker
            allTickers <- Set.add ticker allTickers
            
            for (date, adjClose) in data do
                if not (allData.ContainsKey(date)) then
                    allData.[date] <- Dictionary<string, decimal>()
                allData.[date].[ticker] <- adjClose
        
        let outputPath = Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "dados", "dados_consolidados_api.csv")
        use writer = new StreamWriter(outputPath)
        
        let sortedTickers = allTickers |> Set.toList |> List.sort
        let header = "DATE," + String.concat "," sortedTickers
        writer.WriteLine(header)
        
        let sortedDates = allData.Keys |> Seq.toList |> List.sort
        
        for date in sortedDates do
            let row = ("\"" + date + "\"") :: (sortedTickers |> List.map (fun ticker ->
                if allData.[date].ContainsKey(ticker) then
                    allData.[date].[ticker].ToString()
                else
                    ""
            ))
            writer.WriteLine(String.concat "," row)

    let loadDataFromApi (dataInicio: DateTime) (dataFim: DateTime) (apiKey: string) : PriceData option =
        let tickers = ["AAPL";"AMGN";"AMZN";"AXP";"CAT";"CRM";"CSCO";"CVX";"GS";"HD";"HON";"JNJ";"JPM";"KO";"MCD";"MRK";"MSFT";"NKE";"NVDA";"PG";"SHW";"TRV";"UNH";"V";"WMT"]
        
        let fetchAllTickers = async {
            let mutable jsonList = []
            
            for ticker in tickers do
                let! jsonContent = fetchHistoricalData ticker dataInicio dataFim apiKey
                if jsonContent <> "" then
                    jsonList <- (ticker, jsonContent) :: jsonList
            
            return jsonList
        }
        
        let jsonList = Async.RunSynchronously fetchAllTickers
        
        if jsonList.Length = 0 then
            None
        else
            consolidateApiData jsonList
            
            let consolidatedPath = Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "dados", "dados_consolidados_api.csv")
            
            if File.Exists(consolidatedPath) then
                let lines = File.ReadAllLines(consolidatedPath)
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
                
                Some { Tickers = tickers; Prices = prices }
            else
                None
        


