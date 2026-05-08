module Tests

open System
open Xunit
open Core.Functions

// ============== TESTES DE ESTATÍSTICAS ==============

[<Fact>]
let ``Media de array simples`` () =
    let valores = [| 1m; 2m; 3m; 4m; 5m |]
    let resultado = media valores
    Assert.Equal(3m, resultado)

[<Fact>]
let ``Media de array vazio`` () =
    let valores: decimal[] = [||]
    let resultado = media valores
    Assert.Equal(0m, resultado)

[<Fact>]
let ``Variancia de array simples`` () =
    let valores = [| 1m; 2m; 3m; 4m; 5m |]
    let resultado = variancia valores
    // Variância amostral: [(1-3)² + (2-3)² + (3-3)² + (4-3)² + (5-3)²] / 4 = 10/4 = 2.5
    Assert.Equal(2.5m, resultado)

[<Fact>]
let ``Variancia de um elemento`` () =
    let valores = [| 5m |]
    let resultado = variancia valores
    Assert.Equal(0m, resultado)

[<Fact>]
let ``Desvio padrao`` () =
    let valores = [| 2m; 4m; 6m; 8m; 10m |]
    let resultado = desviaoPadrao valores
    // Variância: [(2-6)² + (4-6)² + (6-6)² + (8-6)² + (10-6)²] / 4 = 40/4 = 10
    // Desvio padrão: sqrt(10) ≈ 3.162
    Assert.True(resultado > 3.1m && resultado < 3.2m)

// ============== TESTES DE RETORNOS ==============

[<Fact>]
let ``Retornos simples`` () =
    let precos = [| 100m; 105m; 110m |]
    let resultado = calcularRetornosSimples precos
    // Primeira: (105-100)/100 = 0.05
    // Segunda: (110-105)/105 ≈ 0.0476
    Assert.Equal(2, resultado.Length)
    Assert.Equal(0.05m, resultado.[0])
    Assert.True(resultado.[1] > 0.047m && resultado.[1] < 0.048m)

[<Fact>]
let ``Retornos logaritmicos`` () =
    let precos = [| 100m; 105m; 110m |]
    let resultado = calcularRetornosLogaritmicos precos
    Assert.Equal(2, resultado.Length)
    // Verifica se os valores são positivos (crescimento)
    Assert.True(resultado.[0] > 0m)
    Assert.True(resultado.[1] > 0m)

[<Fact>]
let ``Retornos com preço zero`` () =
    let precos = [| 0m; 100m; 110m |]
    let resultado = calcularRetornosSimples precos
    // Quando preço anterior é 0, retorna 0
    Assert.Equal(0m, resultado.[0])

// ============== TESTES DE PORTFOLIO ==============

[<Fact>]
let ``Retorno esperado com pesos iguais`` () =
    let retornos = [| 0.10m; 0.20m; 0.30m |]
    let pesos = [| 1m/3m; 1m/3m; 1m/3m |]
    let resultado = retornoEsperadoCarteira retornos pesos
    // (0.10 + 0.20 + 0.30) / 3 ≈ 0.2
    Assert.True(resultado > 0.1999m && resultado < 0.2001m)

[<Fact>]
let ``Retorno esperado dimensoes incompativeis`` () =
    let retornos = [| 0.10m; 0.20m |]
    let pesos = [| 0.5m; 0.5m; 0.0m |]
    let resultado = retornoEsperadoCarteira retornos pesos
    Assert.Equal(0m, resultado)

[<Fact>]
let ``Volatilidade carteira`` () =
    // Matriz com 3 períodos e 2 ativos
    let retornos = [|
        [| 0.01m; 0.02m |]
        [| 0.02m; 0.01m |]
        [| 0.015m; 0.025m |]
    |]
    let pesos = [| 0.5m; 0.5m |]
    let resultado = volatilidadeCarteira retornos pesos
    // Retornos ponderados: [0.015, 0.015, 0.020]
    // Media: 0.01667
    // Deve ser um valor positivo
    Assert.True(resultado > 0m)

// ============== TESTES DE SHARPE RATIO ==============

[<Fact>]
let ``Sharpe Ratio basico`` () =
    let retornoEsperado = 0.12m
    let taxaLivreRisco = 0.05m
    let volatilidade = 0.10m
    let resultado = sharpeRatio retornoEsperado taxaLivreRisco volatilidade
    // (0.12 - 0.05) / 0.10 = 0.7
    Assert.Equal(0.7m, resultado)

[<Fact>]
let ``Sharpe Ratio com volatilidade zero`` () =
    let retornoEsperado = 0.12m
    let taxaLivreRisco = 0.05m
    let volatilidade = 0m
    let resultado = sharpeRatio retornoEsperado taxaLivreRisco volatilidade
    Assert.Equal(0m, resultado)

[<Fact>]
let ``Sharpe Ratio carteira`` () =
    // Matriz simples com dados consistentes
    let retornos = [|
        [| 0.01m; 0.02m |]
        [| 0.015m; 0.025m |]
        [| 0.02m; 0.015m |]
    |]
    let pesos = [| 0.5m; 0.5m |]
    let taxaLivreRisco = 0.04m
    let periodicidade = 252 // dias úteis ao ano
    let resultado = sharpeRatioCarteira retornos pesos taxaLivreRisco periodicidade
    // Deve ser um número positivo razoável
    Assert.True(resultado > 0m)

[<Fact>]
let ``Sharpe Ratio carteira com arrays vazios`` () =
    let retornos: decimal[][] = [||]
    let pesos: decimal[] = [||]
    let taxaLivreRisco = 0.04m
    let resultado = sharpeRatioCarteira retornos pesos taxaLivreRisco 252
    Assert.Equal(0m, resultado)
