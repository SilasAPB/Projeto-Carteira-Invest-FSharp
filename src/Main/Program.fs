module Main.Program

open IO.DataLoader

let run () = async {
    // Baixa dados se necessário
    if not (System.IO.File.Exists(System.IO.Path.Combine(__SOURCE_DIRECTORY__, "..", "..", "dados", "dados_consolidados.csv"))) then
        do! downloadDados()
    
    // Resto da lógica aqui
    printfn "Iniciando simulação..."
}

[<EntryPoint>]
let main argv =
    run() |> Async.RunSynchronously
    0