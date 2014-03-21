module JQ
export @where, @select, @update, @sortby, @groupby, @aggregate, |>, map
using DataFrames
typealias ColExpr Union(Symbol, Expr, Vector{Symbol}, Vector{Expr})

typealias Queryable Union(AbstractDataFrame, GroupedDataFrame, GroupApplied)


immutable Query
    query_type::Symbol
    parser::Function
    ex::ColExpr
end

function Base.show(io::IO, q::Query)
    print(io, uppercase(string(q.query_type)) * " ")
    print(io, string(q.ex))
end


# NB: Allow multiple where expressions? Currently multiple exprs
# as AND conditions.
macro where(exs...)
    esc(:( [JQ.Query(:where,
                     df -> eval(JQ.parse_where(ex, df)),
                     ex) for ex in $exs] ))
end

macro select(cols...)
    esc(:( JQ.Query(:select,
                    df -> eval(JQ.parse_select(collect($cols), df)),
                    collect($cols)) ))
end

macro update(exs...)
    esc(:( [JQ.Query(:update,
                     df -> eval(JQ.parse_update(ex, df)),
                     ex) for ex in $exs] ))
end

macro sortby(cols...)
    esc(:( JQ.Query(:sortby,
                    df -> eval(JQ.parse_sortby(collect($cols), df)),
                    collect($cols)) ))
end

macro groupby(cols...)
    esc(:( JQ.Query(:groupby,
                     df -> eval(JQ.parse_groupby(collect($cols), df)),
                     collect($cols)) ))
end

macro aggregate(exs...)
    esc(:( JQ.Query(:aggregate,
                    function(dfs)
                        # Get the keys
                        keydf = dfs.parent[dfs.starts, dfs.cols]
                        # Evaluate the aggregator over all the sub-dfs.
                        aggdfs =
                            map(df -> eval(JQ.parse_aggregate(collect($exs), df)), dfs)
                        # Combine
                        hcat(keydf, vcat(aggdfs.vals...))
                     end,
                     collect($exs))) )
end


# Composition of Queries.
Base.|>(q1::Union(Query, Vector{Query}),
        q2::Union(Query, Vector{Query})) = [q1, q2]


# Executing a query
function query(df::Queryable, qs::Vector{Query})
    # Not all queryables implement copy().
    newdf = try copy(df) catch(e) df end
    
    foldl(query, newdf, qs)
end

function query(df::Queryable, q::Query)
    q.parser(df)
end

Base.|>(df::Queryable, qs::Union(Vector{Query}, Query)) = query(df, qs)


#####
# Column resolver. Returns a new expression with any symbols
# that match columns in the Queryable substituted to fully-qualified
# references to the Queryable's column.
#####
function resolve_columns(node::Expr, df::Queryable)
    Expr(node.head, [resolve_columns(a, df) for a in node.args]...)
end

function resolve_columns(s::Symbol, df::Queryable)
    newsym = s in names(df) ? Expr(:ref, df, QuoteNode(s)) : s 
end

# Fall-through; typically for literals: numbers, strings, etc.
resolve_columns(x, df::Queryable) = x

#####
# Query Parsers
# Return a new expression which, when evaluated
# transforms the Queryable according to the
# associated query.
#####
function parse_where(ex::ColExpr, df::Queryable)
    resolved_ex = resolve_columns(ex, df)
    :(getindex($df, $resolved_ex, names($df)))
end

function parse_select(ex::ColExpr, df::Queryable)
    :(getindex($df, $ex))
end

function parse_update(ex::ColExpr, df::Queryable)
    lhs, rhs = ex.args
    lhs = QuoteNode(lhs)
    resolved_rhs = resolve_columns(rhs, df)
    :(begin setindex!($df, $resolved_rhs, $lhs); $df end)
end

function parse_sortby(ex::ColExpr, df::Queryable)
    :(getindex($df, sortperm(getindex($df, $ex)), names($df)))
end

function parse_groupby(ex::ColExpr, df::Queryable)
    :(groupby($df, $ex))
end

function parse_aggregate(exs::ColExpr, df::AbstractDataFrame)
    # Aggregate takes assignment expressions.
    # LHSs will be new variables, RHSs should be
    # resolved against queried DataFrame
    resolved_kwargs = [Expr(:kw, ex.args[1],
                            resolve_columns(ex.args[2], df))
                    for ex in exs]
    Expr(:call, :DataFrame, resolved_kwargs...)            
end


function Base.map(q::Union(Vector{Query}, Query), dfs::Vector{DataFrame})
    map(df -> df |> q, dfs)
end
    
end # module


    
