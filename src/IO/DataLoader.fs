module IO.DataLoader

open System
open System.IO
open System.Net.Http

let toUnixTimestamp (dt: DateTime) : int64 =
    int64 ((dt - DateTime(1970, 1, 1)).TotalSeconds)

let downloadDados () : Async<unit> = async {
    let pastaDestino = "dados"
    
    // Verifica se pasta existe e tem arquivos
    if Directory.Exists(pastaDestino) && Directory.GetFiles(pastaDestino).Length > 0 then
        printfn "Dados já existem! Usando arquivos locais."
    else
        // Cria pasta se não existir
        Directory.CreateDirectory(pastaDestino) |> ignore
        
        let client = new HttpClient()
        let dataInicio = DateTime(2025, 7, 1)
        let dataFim = DateTime(2025, 12, 31)
        let period1 = toUnixTimestamp dataInicio
        let period2 = toUnixTimestamp dataFim
        
        let simbolos = [|"AAPL"; "MSFT"; "JPM"; "JNJ"; "V"; "WMT"|]  // Adicione os 30
        
        for simbolo in simbolos do
            let url = $"https://query1.finance.yahoo.com/v7/finance/download/{simbolo}?period1={period1}&period2={period2}&interval=1d&events=history"
            
            try
                let! response = client.GetAsync(url) |> Async.AwaitTask
                let! content = response.Content.ReadAsStringAsync() |> Async.AwaitTask
                let caminho = Path.Combine(pastaDestino, $"{simbolo}.csv")
                File.WriteAllText(caminho, content)
                printfn "✓ %s" simbolo

                do! Async.Sleep(2000)
            with
            | ex -> printfn "✗ %s: %s" simbolo ex.Message
}