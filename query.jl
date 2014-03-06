# --------------------------------------------------------------------------- #
# Composable queries for Julia dataframes.                                    #
#                                                                             #
# C. Vogel       March 2014                                                   #
# --------------------------------------------------------------------------- #

module Queries

using DataFrames

typealias Queryable Union(AbstractDataFrame, GroupedDataFrame, GroupApplied)
typealias QueryExpr Union(Expr, Symbol)

type Query
    funs::Vector{Function}
    exprs::Vector{QueryExpr}
end

# New constructors
query(fs::Vector{Function}, exs::Vector{QueryExpr}) = Query(fs, exs)
query(f::Function, ex::QueryExpr) = Query([f], [ex])

# Copy constructor
query(q::Query) = query(q.funs, q.exprs)
Base.copy(q::Query) = query(q)

# ---------------------------------------------------------
# Query composition. |> connects two queries sequentially.
# ---------------------------------------------------------
function Base.|>(q1::Query, q2::Query)
    query([q1.funs, q2.funs], [q1.exprs, q2.exprs])
end

# ---------------------------------------------------------
# User-facing query functions create instances of Query
# ---------------------------------------------------------
Base.select(ex::Symbol) = query(qselect, ex)

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

qselect(ex::Expr) = df::Queryable -> qselect(ex, df)

function qwhere(ex::Expr, df::Queryable)
    nothing
end

qwhere(ex::Expr) = df::Queryable -> qwhere(ex, df)
    
function qgroubpy(ex::Expr, df::Queryable)
    nothing
end

qgroubpy(ex::Expr) = df::Queryable -> qgroupby(ex, df)

function qaggregate(ex::Expr, df::Queryable)
    nothing
end

function qaggregate(ex::Expr)
    nothing
end

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
    exprs
end

function parse_query_expression(ex::QueryExpr, df::Queryable)
    function expand_nodes(ex, df)
        for (i, arg) in enumerate(ex.args)
            if isa(arg, Symbol)
                if arg in names(df)
                    expanded_expr = Expr(:call, :getindex, df, QuoteNode(:($arg)))
                    setindex!(ex.args, expanded_expr, i)
                end
            elseif isa(arg, Expr)
                expand_nodes(arg, df)
            else
                continue
            end
        end
        ex
    end

    newexpr = copy(ex)
    expand_nodes(newexpr, df)
    newexpr
end


end # module


## # ---------------------------------------------------------
## # PARSING DATAFRAME EXPRESSIONS
## #
## # TODO: This will probably break if the expression contains
## # a getitem call to a same-named column in another DataFrame
## # ---------------------------------------------------------
## function parse_df_expr(dfexpr::Expr, df::AbstractDataFrame)
##     function expand_nodes(ex, df)
##         for (i, arg) in enumerate(ex.args)
##             if isa(arg, Symbol)
##                 if arg in names(df)
##                     expanded_expr = Expr(:call, :getindex, df, QuoteNode(:($arg)))
##                     setindex!(ex.args, expanded_expr, i)
##                 end
##             elseif isa(arg, Expr)
##                 expand_nodes(arg, df)
##             else
##                 continue
##             end
##         end
##         ex
##     end

##     newexpr = copy(dfexpr)
##     expand_nodes(newexpr, df)
##     newexpr
## end 
