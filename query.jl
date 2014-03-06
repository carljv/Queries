# --------------------------------------------------------------------------- #
# Composable queries for Julia dataframes.                                    #
#                                                                             #
# C. Vogel       March 2014                                                   #
# --------------------------------------------------------------------------- #

module Queries

using DataFrames

typalias Queryable Union(AbstractDataFrame, GroupedDataFrame, GroupApplied)

type Query
    funs::Vector{Function}
    exprs::Vector{Expr}
end

# New constructors
query(fs::Vector{Fuction}, exs::Vector{Expr}) = Query(fs, exs)
query(f::Function, ex::Expr) = Query([f], [ex])

# Copy constructor
query(q::Query) = query(q.funs, q.exprs)

# ---------------------------------------------------------                                                          
# Query composition. |> connects two queries sequentially.
# ---------------------------------------------------------
function Base.|>(q1::Query, q2::Query)
    query([q1.funs, q2.funs], [q1.exprs, q2.exprs])
end


# ---------------------------------------------------------
# User-facing query functions create instances of Query
# ---------------------------------------------------------
select(ex::Expr) = query(qselect, ex)

where(ex::Expr) = query(qwhere, ex)

DataFrames.groupby(ex::Expr) = query(qgroupby, ex)

aggregate(ex::Expr) = query(qaggregate, ex)

update(ex::Expr) = query(qupdate, ex)


# ---------------------------------------------------------
# Internal qfunctions actually perform queries on resolved
# dataframe expressions
# ---------------------------------------------------------
function qselect(ex::Expr, df::Queryable)
    nothing
end

function qselect(ex::Expr) = df::Queryable -> qselect(ex, df)

function qwhere(ex::Expr, df::Queryable)
    nothing
end

function qwhere(ex::Expr) = df::Queryable -> qwhere(ex, df)
    
function qgroubpy(ex::Expr, df::Queryable)
    nothing
end

qgroubpy(ex::Expr) = df::Queryable -> qgroupby(ex, df)

function qaggregate(ex::Expr, df::Queryable)
    nothing
end

qaggregate(ex::Expr) = 

function qupdate(ex::Expr, df::Queryable)
    nothing
end

# ---------------------------------------------------------
# The @query macro executes a Query on a DataFrame,
# resolving column references in the Query's expressions,
# then executing each query function with the associated
# expression.
# ---------------------------------------------------------

macro query(df, qry)
    qry = eval(qry)
    isa(qry, Query) ? nothing :
        error("Second argument to query must be, or evaluate to, type Query.")

    # Resolve column names in the query's expressions.
    exprs = [ex -> parse_query_expression(ex, df) for ex in qry.exprs]
   
end

function parse_query_expression(ex::Expr, df::Queryable)
    nothing
end


end # module
