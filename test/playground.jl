include("query.jl")
using DataFrames

df = DataFrame(x = 1:10,
               y = rand(10),
               z = [fill("B", 5), fill("A", 5)])

timesten = x -> x * 10
newdf = df |>
        JQ.@update(w = timesten(x), q = w.*w) |> 
        JQ.@where(w .> 40)  |>                    
        JQ.@sortby(z, y)    |>
        JQ.@select(y, z, q)

agg = JQ.@aggregate(m = mean(y), sd = std(y))

gdf = df |> JQ.@groupby(z)

gdf |> agg
