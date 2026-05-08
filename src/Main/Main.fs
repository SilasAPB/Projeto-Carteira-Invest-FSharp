module Main.Main

open IO.DataLoader
open IO.RawLoader
open PureDomain.Types

let run () = async {
    // Testa API e carrega dados (com fallback para brutos se API falhar)
    let priceData = loadData()
    
    printfn ""
    printfn " Dados carregados: %d tickers, %d dias" priceData.Tickers.Length priceData.Prices.Length
    printfn "  Tickers: %s" (System.String.Join(", ", priceData.Tickers |> Array.take (System.Math.Min(5, priceData.Tickers.Length))))
    if priceData.Tickers.Length > 5 then printfn "  ... (e %d mais)" (priceData.Tickers.Length - 5)
    
}

[<EntryPoint>]
let main argv =
    run() |> Async.RunSynchronously
    0