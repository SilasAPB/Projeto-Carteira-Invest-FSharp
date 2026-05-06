module Main.Main

open IO.DataLoader
open IO.RawLoader
open PureDomain.Types

let run () = async {
    // Baixa dados se necessário
    if not (System.IO.File.Exists(System.IO.Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "dados", "dados_consolidados.csv"))) then
        do! downloadDados()
    
    // Carrega dados consolidados
    let priceData = loadConsolidatedData()                                          
    printfn "Dados carregados: %d tickers, %d dias" priceData.Tickers.Length priceData.Prices.Length
    
    // Resto da lógica aqui
    printfn "Iniciando simulação..."
}

[<EntryPoint>]
let main argv =
    run() |> Async.RunSynchronously
    0