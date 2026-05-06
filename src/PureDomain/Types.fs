// Define tipos de dados para a aplicação
module PureDomain.Types

type PriceData = {
    Tickers: string[]
    Prices: decimal[][]  // [dia][ação] - matriz de preços
}
