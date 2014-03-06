# -------------------------------------------------------------------------------
# Query tests

include("../query.jl")
using DataFrames
#using Queries
Q = Queries

testdata = readtable("google_stockprices.csv")

# Test 1: Constructors
q1 = Q.select(:High)
@assert isa(q1, Query)
@assert q1.funs == [qselect]
                    
@assert q1.exprs == [:High]

# Test 2: Combination constructor
q2 = Q.where(:(Date == "2004-09-01"))
q3 = q1 |> q2

@assert isa(q3, Query)
@assert length(q3.funs) == length(q3.exprs) == 2
@assert q3.funs[1] == q1.funs[1]
@assert q3.funs[2] == q2.funs[2]
@assert q3.exprs[1] == q1.exprs[1]
@assert q3.exprs[2] == q2.exprs[1]






# Test XX: Select and where
#high_on_date = select(:High) |> where(:Date == "2004-09-01")
#high = @query testdata high_on_date
#@assert high == 102.97


