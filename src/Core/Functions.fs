namespace Core

module Functions =
    
    // ============== FUNÇÕES ESTATÍSTICAS ==============
    
    /// Calcula a média de um array de decimais
    let media (valores: decimal[]) : decimal =
        if valores.Length = 0 then 0m
        else valores |> Array.sum |> fun x -> x / decimal valores.Length
    
    /// Calcula a variância de um array de decimais
    let variancia (valores: decimal[]) : decimal =
        if valores.Length <= 1 then 0m
        else
            let med = media valores
            valores
            |> Array.map (fun x -> (x - med) * (x - med))
            |> Array.sum
            |> fun x -> x / decimal (valores.Length - 1)
    
    /// Calcula o desvio padrão de um array de decimais
    let desviaoPadrao (valores: decimal[]) : decimal =
        let var = variancia valores
        sqrt (float var) |> decimal
    
    // ============== FUNÇÕES DE RETORNO ==============
    
    /// Calcula os retornos logarítmicos para um ativo
    /// Entrada: array de preços
    /// Saída: array de retornos logarítmicos
    let calcularRetornosLogaritmicos (precos: decimal[]) : decimal[] =
        if precos.Length <= 1 then [||]
        else
            [| for i in 1 .. precos.Length - 1 ->
                let preco_anterior = precos.[i-1]
                let preco_atual = precos.[i]
                if preco_anterior > 0m then
                    log (float (preco_atual / preco_anterior)) |> decimal
                else 0m
            |]
    
    /// Calcula os retornos simples para um ativo
    /// Entrada: array de preços
    /// Saída: array de retornos simples (Pt - Pt-1) / Pt-1
    let calcularRetornosSimples (precos: decimal[]) : decimal[] =
        if precos.Length <= 1 then [||]
        else
            [| for i in 1 .. precos.Length - 1 ->
                let preco_anterior = precos.[i-1]
                let preco_atual = precos.[i]
                if preco_anterior > 0m then
                    (preco_atual - preco_anterior) / preco_anterior
                else 0m
            |]
    
    // ============== FUNÇÕES DE PORTFOLIO ==============

    /// Calcula o retorno esperado da carteira
    /// Entrada: array de retornos médios por ativo, array de pesos
    /// Saída: retorno esperado ponderado
    let retornoEsperadoCarteira (retornosMedios: decimal[]) (pesos: decimal[]) : decimal =
        if retornosMedios.Length <> pesos.Length then 0m
        else
            Array.fold2 (fun acc r p -> acc + (r * p)) 0m retornosMedios pesos
    
    /// Calcula a volatilidade (desvio padrão) da carteira
    /// Entrada: matriz de retornos [período][ativo], array de pesos
    /// Saída: volatilidade da carteira
    let volatilidadeCarteira (retornos: decimal[][]) (pesos: decimal[]) : decimal =
        if retornos.Length = 0 || pesos.Length = 0 || pesos.Length <> retornos.[0].Length then 0m
        else
            // Calcula desvio padrão dos retornos da carteira ponderados
            let retornosCarteira = 
                [| for periodo in retornos ->
                    Array.fold2 (fun acc r p -> acc + (r * p)) 0m periodo pesos
                |]
            desviaoPadrao retornosCarteira
    
    // ============== SHARPE RATIO ==============
    
    /// Calcula o Sharpe Ratio
    /// Entrada: retorno esperado da carteira, taxa livre de risco (anualizada), volatilidade da carteira
    /// Saída: Sharpe Ratio
    let sharpeRatio (retornoEsperado: decimal) (taxaLivreRisco: decimal) (volatilidade: decimal) : decimal =
        if volatilidade = 0m then 0m
        else
            (retornoEsperado - taxaLivreRisco) / volatilidade
    
    /// Calcula o Sharpe Ratio para uma carteira completa
    /// Entrada: 
    ///   - retornos: matriz de retornos diários/mensais [período][ativo]
    ///   - pesos: array de pesos dos ativos
    ///   - taxaLivreRisco: taxa anualizada (ex: 0.05 para 5% ao ano)
    ///   - periodicidadeAnual: fator para anualizar (252 para dias úteis, 12 para meses)
    /// Saída: Sharpe Ratio anualizado
    let sharpeRatioCarteira (retornos: decimal[][]) (pesos: decimal[]) (taxaLivreRisco: decimal) (periodicidadeAnual: int) : decimal =
        // Calcula retornos da carteira
        let retornosCarteira = 
            [| for periodo in retornos ->
                Array.fold2 (fun acc r p -> acc + (r * p)) 0m periodo pesos
            |]
        
        // Calcula retorno médio
        let retornoMedio = media retornosCarteira
        let retornoAnualizado = retornoMedio * decimal periodicidadeAnual
        
        // Calcula volatilidade
        let volatilidade = desviaoPadrao retornosCarteira
        let volatilidadeAnualizada = volatilidade * (sqrt (float periodicidadeAnual) |> decimal)
        
        // Calcula Sharpe Ratio
        sharpeRatio retornoAnualizado taxaLivreRisco volatilidadeAnualizada

