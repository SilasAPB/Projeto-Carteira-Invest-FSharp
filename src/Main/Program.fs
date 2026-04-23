module Main.Program

open IO.DataLoader

let run () = async {
    // Baixa dados se necessário
    do! downloadDados()
    
    // Resto da lógica aqui
    printfn "Iniciando simulação..."
}

[<EntryPoint>]
let main argv =
    run() |> Async.RunSynchronously
    0