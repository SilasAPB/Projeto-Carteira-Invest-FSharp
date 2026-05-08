# Projeto-Carteira-Invest-FSharp

Projeto para análise e otimização de carteira de investimento em **F#** para a matéria de Programação Funcional - Insper.

## Objetivo

Desenvolver uma aplicação funcional que:
- Carrega dados históricos de preços de 30 ações (índice Dow Jones)
- Gera combinações aleatórias de carteiras com 20 ativos
- Para cada combinação, testa **1 milhão de distribuições de pesos** em **paralelo**
- Descarta pesos que violam restrição: cada ativo ≤ 20% (0.2)
- Calcula **Sharpe Ratio** para cada distribuição válida
- Retorna a **melhor carteira** encontrada

## Estrutura do Projeto

```
projeto02/
├── dados/
│   ├── dados_consolidados.csv        # Dados históricos (consolidado)
│   └── dados_crus/                   # 30 arquivos CSV (um por ação)
│
├── src/
│   ├── Core/
│   │   ├── Types.fs                  # PriceData: tipos de dados
│   │   ├── Functions.fs              # Funções puras: média, variância, desvio padrão, 
│   │   │                             # retornos, Sharpe ratio
│   │   └── Core.fsproj
│   │
│   ├── IO/
│   │   ├── RawLoader.fs              # Carregador de CSV brutos
│   │   ├── APILoader.fs              # Carregador via API (fallback)
│   │   ├── DataLoader.fs             # Orquestração de carregamento
│   │   └── IO.fsproj
│   │
│   └── Main/
│       ├── Main.fs                   # Busca paralela + orquestração
│       └── Main.fsproj
│
├── Sharp.Tests/
│   ├── Tests.fs                      # 15 testes unitários (xUnit)
│   └── Sharp.Tests.fsproj
│
├── projeto02.slnx                    # Solução
└── README.md                         # Este arquivo
```

## Como Executar

### Pré-requisitos
- .NET 10.0+ instalado
- F# compiler (incluído com .NET SDK)

### Compilar
```bash
dotnet build
```

### Executar Busca de Carteira
```bash
dotnet run --project src/Main
```

### Rodar Testes
```bash
dotnet test
```

## Parâmetros Configuráveis

Em `src/Main/Main.fs`, seção `run()`:

```fsharp
let attempts = 25                      # Número de combinações a testar
let portfolioSize = 20                 # Ativos por carteira
let samplesPerCombination = 1_000_000  # Amostras de pesos por combinação
let maxWeight = 0.2m                   # Peso máximo por ativo (20%)
let taxaLivreRisco = 0.04m             # Taxa livre de risco (4% ao ano)
let periodicidadeAnual = 252           # Dias úteis ao ano
```

## 📊 Cálculos Matemáticos

### Retornos Simples
$$R_t = \frac{P_t - P_{t-1}}{P_{t-1}}$$

### Média
$$\mu = \frac{1}{n} \sum_{i=1}^{n} x_i$$

### Variância Amostral
$$\sigma^2 = \frac{1}{n-1} \sum_{i=1}^{n} (x_i - \mu)^2$$

### Desvio Padrão
$$\sigma = \sqrt{\sigma^2}$$

### Retorno Esperado da Carteira (anualizado)
$$R_p = \left(\sum_{i=1}^{n} w_i \cdot r_i\right) \times 252$$

onde $w_i$ = peso do ativo, $r_i$ = retorno médio diário

### Volatilidade Anualizada
$$\sigma_p = \sigma(R_p) \times \sqrt{252}$$

### Sharpe Ratio
$$S = \frac{R_p - R_f}{\sigma_p}$$

onde $R_f$ = taxa livre de risco anualizada

## Exemplo de Saída

```
 Dados carregados: 30 tickers, 128 dias
  Tickers: AAPL, AMGN, AMZN, AXP, BA
  ... (e 25 mais)

Melhor carteira (entre 25 amostras):
  Número ativos: 20
  Tickers: CSCO, AMGN, MSFT, MCD, V, AXP, JNJ, VZ, TRV, BA, WMT, MRK, CVX, DIS, HD, IBM, NVDA, MMM, CAT, GS
  Pesos: 0.0543, 0.0765, 0.0813, 0.0052, 0.0364, 0.0833, 0.0629, 0.0200, 0.0979, 0.0402, ...
  Retorno esperado (anual): 0.2594
  Volatilidade (anual): 0.1045
  Sharpe Ratio (anual): 2.1005
```

## Conceitos de Programação Funcional

### 1. **Funções Puras** (`Core/Functions.fs`)
- Sem efeitos colaterais
- Determinísticas: mesma entrada = mesma saída
- Exemplos: `media`, `variancia`, `sharpeRatio`

### 2. **Funções de Ordem Superior**
- `Array.fold2` para combinar dois arrays
- `Array.map` para transformações
- `Array.filter` para seleção

### 3. **Paralelismo Funcional**
- `System.Threading.Tasks.Parallel.For` com thread-local random
- Sem locks desnecessários (apenas ao atualizar best global)
- Imutabilidade garante thread-safety

### 4. **Type Safety**
- Tipo `PriceData` garante estrutura correta
- `decimal` para precisão financeira
- Tipagem F# detecta erros em compilação

### 5. **Composição de Funções**
```fsharp
let retorno = 
    retornoEsperadoCarteira retornosMedios pesos 
    * decimal periodicidadeAnual
```

### 6. **Pattern Matching**
```fsharp
if s > 0.0 then
    let weights = vals |> Array.map (fun v -> v / s)
    if Array.forall (fun w -> w <= maxWeight) weights then
        // Avaliar...
```

## Testes Unitários (15 testes)

```bash
dotnet test
```

Testes cobrem:
-  Cálculos estatísticos (média, variância, desvio padrão)
-  Retornos simples e logarítmicos
-  Retorno esperado da carteira
-  Volatilidade da carteira
-  Sharpe Ratio (básico e anualizado)
-  Casos extremos (arrays vazios, volatilidade zero)

## Dados

### Arquivo Principal
- `dados/dados_consolidados.csv`: Matriz com 128 dias × 30 ações

### Arquivos Brutos (fallback)
- `dados/dados_crus/`: 30 arquivos CSV individuais (AAPL, AMGN, ..., WMT)
- Formato: data, preço de fechamento

**Tickers disponíveis** (Dow Jones 30):
AAPL, AMGN, AMZN, AXP, BA, CAT, CRM, CSCO, CVX, DIS, GS, HD, HON, IBM, JNJ, JPM, KO, MCD, MMM, MRK, MSFT, NKE, NVDA, PG, SHW, TRV, UNH, V, VZ, WMT

### Conexao com API
- O projeto usa **Financial Modeling Prep (FMP)** em `src/IO/APILoader.fs`
- Fluxo atual: tenta API primeiro, e se falhar usa fallback para CSV consolidado/bruto
- Chave da API: pode ser definida em variável de ambiente `FMP_API_KEY`
- Se `FMP_API_KEY` não estiver definida, usa a chave fallback já presente no projeto

Exemplo para definir chave no Linux/macOS:

```bash
export FMP_API_KEY="sua_chave_aqui"
dotnet run --project src/Main
```

## Extensões Possíveis

1. **Adicionar CLI flags**
   ```fsharp
   dotnet run --attempts 100 --portfolio-size 25 --samples 5000000
   ```

2. **Exportar resultados**
   - CSV com carteiras avaliadas
   - JSON com histórico de Sharpe ratios

3. **Otimização com Restrições**
   - Implementar algoritmo de otimização quadrática (ex: biblioteca open-source)

4. **Análise Comparativa**
   - Comparar com portfólio de pesos iguais
   - Visualizar fronteira eficiente


## Notas

- **Paralelismo**: Uso de `Parallel.For` acelera busca (~3-5x mais rápido que sequencial)
- **Precisão**: `decimal` garante 128 bits (superior a `float`)
- **Aleatoriedade**: `ThreadLocal<Random>` evita sincronização que prejudica performance
- **Restart**: Para parar a execução, pressione `Ctrl+C` no terminal


Projeto para matéria de **Programação Funcional** - Insper, 2026

