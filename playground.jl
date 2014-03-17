include("query.jl")
using JQ
using DataFrames
using Datetime 

# Test DF
dtf1 = DataFrame(x = 1:10, y = map(char, [65:69, 65:69]), z = [fill("2014-03-11", 5), fill("2014-03-12", 5)])
dtf2 = DataFrame(x = [1:5, 21:25], y = dtf1[:y], z = dtf1[:z])

# Combining multiple expressions in a WHERE
subset_rows = where(@? x .> 5 (y .== 'C') | (y .== 'E'))
query(subset_rows, dtf1)

# Queries have access to any functions in the call environment
tenth = x -> x / 10
subset_rows2 = where(@? tenth(x) .> .3 map(date, z) .== date("2014-03-11"))
query(subset_rows2, dtf1)

# SELECT takes a tuple of column names.
# !! DANGER if macro call is @?(x, y)
subset_cols = select(@? x, y)
query(subset_cols, dtf1)

# Piping queries composes them.
subset_xy = select(@? x, y) |>
            where(@? x .> 5 (y .== 'C') | (y .== 'E'))

query(subset_xy, dtf1)

# Mapping predefined query over multiple dataframes.
# WARNING: Can't use [dtf1, dtf2, ...], since the []
# concatenates them (instead of making an array of them)
map(query(subset_xy), (dtf1, dtf2))


# Pipes for everyone.
select(@? x, y) |> where(@? x .> 5 (y .== 'C') | (y .== 'E')) |> query(dtf1)

dtf1 |>
    select(@? x, y) |>
    where(@? x .> 5 (y .== 'C') | (y .== 'E'))
