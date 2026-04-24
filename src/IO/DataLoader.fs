module IO.DataLoader

open System
open System.IO
open System.Collections.Generic
open System.Text.RegularExpressions

let private parseDateString (dateStr: string) : DateTime option =
    try
        DateTime.TryParse(dateStr) |> function
        | (true, dt) -> Some dt
        | (false, _) -> None
    with _ -> None

let private monthToNumber (month: string) : int =
    match month.ToLower() with
    | "jan" -> 1 | "feb" -> 2 | "mar" -> 3 | "apr" -> 4 | "may" -> 5
    | "jun" -> 6 | "jul" -> 7 | "aug" -> 8 | "sep" -> 9 | "oct" -> 10
    | "nov" -> 11 | "dec" -> 12 | _ -> 0

let private parseMonthDateYear (dateStr: string) : (int * int * int) option =
    /// Esta função converte strings de data no formato "Aug 01, 2025" para uma tupla (ano, mês, dia).
    /// Retorna Some(year, month, day) se conseguir parsear, ou None se falhar.
    /// 
    /// Exemplo de entrada e saída:
    /// - Input: "Aug 01, 2025"
    /// - Output: Some (2025, 8, 1)
    
    // Separa a string por espaços e vírgulas, removendo partes vazias
    // "Aug 01, 2025" vira ["Aug"; "01"; "2025"]
    let parts = dateStr.Split([|' '; ','|], System.StringSplitOptions.RemoveEmptyEntries)
    
    if parts.Length >= 3 then
        // Tenta converter o dia (parts.[1]) e ano (parts.[2]) para inteiros
        match System.Int32.TryParse(parts.[1]), System.Int32.TryParse(parts.[2]) with
        | (true, day), (true, year) ->
            // Conversão bem-sucedida: agora converte o nome do mês (parts.[0]) para número
            let month = monthToNumber parts.[0]
            
            // Se o mês é válido (1-12), retorna Some com a tupla (ano, mês, dia)
            // Isso será usado para ordenar as datas cronologicamente
            if month > 0 then Some (year, month, day)
            else None
        | _ -> 
            // Falha na conversão de dia ou ano
            None
    else
        // String não tem 3 partes (ex: malformada)
        None

let private parseCsvFields (line: string) : string list =
    /// Esta função parseia uma linha CSV respeitando aspas duplas.
    /// Exemplo: "Dec 31, 2025",273.06,273.68 -> ["Dec 31, 2025"; "273.06"; "273.68"]
    /// 
    /// Funciona caractere por caractere:
    /// - Quando encontra uma aspa ("), inverte o estado booleano "inQuotes"
    /// - Quando encontra uma vírgula (,) E NÃO está dentro de aspas, usa como separador
    /// - Caso contrário, adiciona o caractere no campo atual
    
    let mutable fields = []           // Lista para armazenar os campos já processados
    let mutable current = ""          // Campo sendo construído atualmente
    let mutable inQuotes = false      // Flag: estamos dentro de aspas?
    
    for c in line do
        match c with
        | '"' -> 
            // Encontrou aspa: inverte o estado (entra ou sai de modo "entre aspas")
            inQuotes <- not inQuotes
        | ',' when not inQuotes -> 
            // Encontrou vírgula FORA de aspas = é um separador de campo
            fields <- current :: fields
            current <- ""
        | c -> 
            // Qualquer outro caractere: adiciona ao campo atual
            current <- current + string c
    
    // Adiciona o último campo (que não tem vírgula após)
    fields <- current :: fields
    
    // Reverte a lista (pois inserimos ao contrário) e remove aspas desnecessárias
    fields |> List.rev |> List.map (fun f -> f.Trim('"').Trim())

let private loadAdjCloseData (filePath: string) : seq<string * decimal> =
    seq {
        try
            let lines = File.ReadAllLines(filePath)
            // Skip header (line 0)
            for i = 1 to lines.Length - 1 do
                let fields = parseCsvFields lines.[i]
                if fields.Length >= 6 then
                    let date = fields.[0]
                    let adjClose = 
                        try
                            decimal fields.[5]
                        with _ -> 0m
                    yield (date, adjClose)
        with _ -> ()
    }

let consolidateAdjCloseData () : unit =
    let dataDir = Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "dados", "dados_crus")
    
    if not (Directory.Exists(dataDir)) then
        printfn "Nenhum arquivo CSV encontrado em: %s" dataDir
    else
        // Dictionary to store data: date -> (ticker -> adj close)
        let allData = Dictionary<string, Dictionary<string, decimal>>()
        
        // Read all CSV files
        let csvFiles = Directory.GetFiles(dataDir, "*_data.csv")
        
        if csvFiles.Length = 0 then
            printfn "Nenhum arquivo CSV encontrado em: %s" dataDir
        else
            for csvFile in csvFiles do
                let ticker = Path.GetFileNameWithoutExtension(csvFile).Replace("_data", "")
                let data = loadAdjCloseData csvFile
                
                for (date, adjClose) in data do
                    if not (allData.ContainsKey(date)) then
                        allData.[date] <- Dictionary<string, decimal>()
                    allData.[date].[ticker] <- adjClose
        
            // Create consolidated CSV
            let outputPath = Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "dados", "consolidated_adj_close.csv")
            
            use writer = new StreamWriter(outputPath)
            
            // Get all unique tickers sorted
            let allTickers = 
                csvFiles
                |> Array.map (fun f -> Path.GetFileNameWithoutExtension(f).Replace("_data", ""))
                |> Array.sort
            
            // Write header
            let header = "DATE," + String.concat "," allTickers
            writer.WriteLine(header)
            
            // Write data rows
            let sortedDates = 
                allData.Keys 
                |> List.ofSeq 
                |> List.sortBy (fun dateStr -> 
                    match parseMonthDateYear dateStr with
                    | Some (year, month, day) -> (year, month, day)
                    | None -> (9999, 12, 31)
                )
            
            for date in sortedDates do
                let row = ("\"" + date + "\"") :: (allTickers |> Array.map (fun ticker ->
                    if allData.[date].ContainsKey(ticker) then
                        allData.[date].[ticker].ToString()
                    else
                        ""
                ) |> Array.toList)
                writer.WriteLine(String.concat "," row)
            
            writer.Flush()
            writer.Close()
            
            printfn "Arquivo consolidado criado: %s" outputPath
            printfn "Datas: %d | Empresas: %d" sortedDates.Length allTickers.Length

let downloadDados () : Async<unit> = async {
    consolidateAdjCloseData()
}