include("query.jl")
using DataFrames



df = DataFrame(x = 1:10,
               y = rand(10),
               z = [fill("hello", 5), fill("goodbye", 5)])

timesten = x -> x * 10

df |>
    JQ.@update(w = timesten(x), q = w.*w) |>
    JQ.@where(w .> 40) |>
    JQ.@sortby(z, y)   |>
    JQ.@select(y, z, q) |>
    JQ.@groupby(z)
