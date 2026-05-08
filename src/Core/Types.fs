/// Tipos de dados para a aplicação
namespace Core

module Types =
    type PriceData = {
        Tickers: string[]
        Prices: decimal[][]  // [dia][ação] - matriz de preços
    }
