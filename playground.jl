include("query.jl")
using JQ
using DataFrames
using Datetime 
# Test DF
dtf1 = DataFrame(x = 1:10, y = map(char, [65:69, 65:69]), z = fill("2014-03-12", 10))
dtf2 = DataFrame(x = [1:5, 21:25], y = dtf1[:y], z = dtf1[:z])

# Combining multiple expressions in a WHERE
subset_rows = where(@? x .> 5 (y .== 'C') | (y .== 'E'))
query(subset_rows, dtf1)

# SELECT takes a tuple of column names.
# !! DANGER if macro call is @?(x, y)
subset_cols = select(@? x, y)
query(subset_cols, dtf1)

# Piping queries
subset = select(@? x, y) |>
             where(@? x .> 5 (y .== 'C') | (y .== 'E'))

query(subset, dtf1)

# Mapping predefined query over multiple dataframes.
# WARNING: Can't use [dtf1, dtf2, ...], since the []
# concatenates them (instead of making an array of them)
map(query(subset), (dtf1, dtf2))



    
