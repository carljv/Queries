using DataFrames
using Datetime
include("query.jl")
using JQ

# A Bunch of stocks
stocks =
[("NASDAQ", "MSFT"),
 ("NASDAQ", "GOOG"),
 ("NASDAQ", "AAPL"),
 ("NASDAQ", "ORCL"),
 ("NASDAQ", "INTC"),
 ("NASDAQ", "YHOO"),
 ("NASDAQ", "CSCO"),
 ("NASDAQ", "ADBE"),
 ("NASDAQ", "AMZN"),
 ("NASDAQ", "QCOM"),
 ("NYSE",   "EMC"),
 ("NYSE",   "HPQ"),
 ("NYSE",   "RHT")]


# Get stock data from Yahoo
function get_stock_data(ticker_info::(String, String))
    exchange, ticker = ticker_info
    println(ticker * " " * exchange)
    link = make_link(ticker, exchange)
    df = link |> download |> open |> readtable
    df[:Ticker] = ticker
    df
end

# Form a Yahoo finance CSV link
function make_link(ticker::String, exchange::String)
   "http://ichart.finance.yahoo.com/table.csv?s=" * ticker *
    "&a=02&b=13&c=2013&d=02&e=20&f=2014&g=d&ignore=.csv"
end

# Get all the data!
goog = get_stock_data(("NASDAQ", "GOOG"))
stock_dfs = map(get_stock_data, stocks)

######
# A query is the piping together of many
# smaller queries
#####
myquery = @update(Date   = map(date, Date),
                  Month  = map(month, Date),
                  Year   = map(year, Date),
                  Spread = High - Low) |>
                  
          @groupby(Ticker, Year, Month) |>                
          @aggregate(TotalVol = sum(Volume),
                     AvgSpread = mean(Spread)) |>                           
          @where(TotalVol .>= max(TotalVol)) |>                
          @select(Ticker, Year, Month, AvgSpread)

goog_result = goog |> myquery


#######
# Queries are composable and delayed, so we can
# break them into meaningful sub-units.
######
format_date = @update(Date = map(date, Date),
                      Month = map(month, Date),
                      Year = map(year, Date))

calc_spread = @update(Spread = High - Low)

summary_stats = @groupby(Ticker, Year, Month) |>
                @aggregate(TotalVol = sum(Volume),
                           AvgSpread = mean(Spread))

highest_vol = @where(TotalVol .>= max(TotalVol)) |>
              @select(Ticker, Year, Month, AvgSpread)


goog |> format_date |> calc_spread |> summary_stats |> highest_vol

# Run the query over a bunch of similar dataframes
stock_results = map(myquery, stock_dfs)
