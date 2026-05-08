module Main.Main

open IO.DataLoader
open IO.RawLoader
open Core.Types
open Core.Functions

let run () = async {
    // Carrega dados (API ou brutos)
    let priceData = loadData()

    printfn ""
    printfn " Dados carregados: %d tickers, %d dias" priceData.Tickers.Length priceData.Prices.Length
    printfn "  Tickers: %s" (System.String.Join(", ", priceData.Tickers |> Array.take (System.Math.Min(5, priceData.Tickers.Length))))
    if priceData.Tickers.Length > 5 then printfn "  ... (e %d mais)" (priceData.Tickers.Length - 5)

    // Validacoes basicas
    if priceData.Tickers.Length = 0 || priceData.Prices.Length <= 1 then
        printfn "Dados insuficientes para cálculo (nenhum ticker ou poucos dias)."
    else
        let nTickers = priceData.Tickers.Length
        let nDias = priceData.Prices.Length

        // Monta matriz de retornos por período [período][ativo] usando retornos simples
        let retornosMatrix =
            [| for i in 1 .. nDias - 1 ->
                [| for j in 0 .. nTickers - 1 ->
                    let prev = priceData.Prices.[i-1].[j]
                    let curr = priceData.Prices.[i].[j]
                    if prev > 0m then (curr - prev) / prev else 0m
                |]
            |]

        // Calcula retorno médio por ativo (necessário para retorno esperado ponderado)
        let retornosMediosPorAtivo =
            [| for j in 0 .. nTickers - 1 ->
                let series = [| for i in 0 .. retornosMatrix.Length - 1 -> retornosMatrix.[i].[j] |]
                media series
            |]

        // Busca aleatória: 25 carteiras com 20 ativos cada (escolha aleatória) e pesos limitados a 0.2
        let attempts = 15
        let portfolioSize = 10
        let periodicidadeAnual = 252 // dias uteis
        let taxaLivreRisco = 0.04m
        let rnd = System.Random()

        let mutable bestSharpe = System.Decimal.MinValue
        let mutable bestTickers : string[] = [||]
        let mutable bestPesos : decimal[] = [||]
        let mutable bestRetorno = 0m
        let mutable bestVol = 0m

        // Para cada amostra de combinação de ativos, gere muitos vetores de pesos em paralelo
        let samplesPerCombination = 1000000 // reduzido para teste rápido
        let maxWeight = 0.2m
        let globalLock = obj()

        for attempt in 1 .. attempts do
            // escolhe combinação sem reposição (Fisher-Yates parcial)
            let idxs = [| 0 .. nTickers - 1 |]
            for i in 0 .. idxs.Length - 1 do
                let j = rnd.Next(i, idxs.Length)
                let tmp = idxs.[i]
                idxs.[i] <- idxs.[j]
                idxs.[j] <- tmp
            let chosen = idxs |> Array.take (min portfolioSize idxs.Length)

            let subsetTickers = chosen |> Array.map (fun i -> priceData.Tickers.[i])
            let retornosSub = [| for periodo in retornosMatrix -> chosen |> Array.map (fun idx -> periodo.[idx]) |]

            let retornosMediosSubset =
                [| for j in 0 .. (chosen.Length - 1) ->
                    let series = [| for i in 0 .. retornosSub.Length - 1 -> retornosSub.[i].[j] |]
                    media series
                |]

            // Melhor da combinação (local)
            let mutable localBestSharpe = System.Decimal.MinValue
            let mutable localBestPesos : decimal[] = [||]
            let mutable localBestRet = 0m
            let mutable localBestVol = 0m

            // Thread-local random para paralelismo
            let threadRnd = new System.Threading.ThreadLocal<System.Random>(fun () -> System.Random(System.Guid.NewGuid().GetHashCode()))

            System.Threading.Tasks.Parallel.For(0, samplesPerCombination, fun i ->
                let rndt = threadRnd.Value
                // gera vetor de pesos positivos e normaliza para soma 1
                let vals = Array.init chosen.Length (fun _ -> rndt.NextDouble())
                let s = Array.sum vals
                if s > 0.0 then
                    // cria vetor decimal normalizado
                    let weights = vals |> Array.map (fun v -> decimal v / decimal s)
                    // descarta se qualquer peso exceder maxWeight
                    if Array.forall (fun w -> w <= maxWeight) weights then
                        // avalia
                        let retornoExpAnual = retornoEsperadoCarteira retornosMediosSubset weights * decimal periodicidadeAnual
                        let volAnual = let vol = volatilidadeCarteira retornosSub weights in vol * (sqrt (float periodicidadeAnual) |> decimal)
                        let sRatio = sharpeRatio retornoExpAnual taxaLivreRisco volAnual
                        // atualiza local se melhor
                        if sRatio > localBestSharpe then
                            lock globalLock (fun () ->
                                // recheck and update both local and global safely
                                if sRatio > localBestSharpe then
                                    localBestSharpe <- sRatio
                                    localBestPesos <- weights
                                    localBestRet <- retornoExpAnual
                                    localBestVol <- volAnual
                                    if sRatio > bestSharpe then
                                        bestSharpe <- sRatio
                                        bestTickers <- subsetTickers
                                        bestPesos <- weights
                                        bestRetorno <- retornoExpAnual
                                        bestVol <- volAnual
                            )
                ) |> ignore

            // fim amostra

        // Imprime melhor carteira encontrada
        printfn "\nMelhor carteira (entre %d amostras):" attempts
        printfn "  Número ativos: %d" bestTickers.Length
        printfn "  Tickers: %s" (System.String.Join(", ", bestTickers))
        printfn "  Pesos: %s" (System.String.Join(", ", bestPesos |> Array.map (fun p -> System.String.Format("{0:F4}", p))))
        printfn "  Retorno esperado (anual): %M" bestRetorno
        printfn "  Volatilidade (anual): %M" bestVol
        printfn "  Sharpe Ratio (anual): %M" bestSharpe
}

[<EntryPoint>]
let main argv =
    run() |> Async.RunSynchronously
    0