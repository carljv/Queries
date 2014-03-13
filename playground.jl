include("query.jl")
using JQ
using DataFrames
using Datetime 
# Test DF
dtf = DataFrame(x = 1:10, y = map(char, [65:69, 65:69]), z = fill("2014-03-12", 10))

# Combining multiple expressions in a WHERE
subset_rows = where(@? x .> 5 (y .== 'C') | (y .== 'E'))
query(subset_rows, dtf)

# SELECT takes a tuple of column names.
# !! DANGER if macro call is @?(x, y)
subset_cols = select(@? x, y)
query(subset_cols, dtf)

# Piping queries
subset = select(@? x, y) |>
             where(@? x .> 5 (y .== 'C') | (y .== 'E'))

query(subset, dtf)






    
