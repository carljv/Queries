# --------------------------------------------------------------------------- #
# Composable queries for Julia dataframes.                                    #
#                                                                             #
# C. Vogel       March 2014                                                   #
# --------------------------------------------------------------------------- #
module JQ
using DataFrames
export query, where, select, @?, show, parse_col_expression

typealias ColumnExpr Union(Symbol, Expr)
typealias ParserVector Vector{(Function, Expr)}
typealias Queryable Union(AbstractDataFrame, GroupedDataFrame, GroupApplied)

type Query
    parser::Function
    ex::ColumnExpr
    extype::Symbol
    qtype::Symbol
end

function Base.show(io::IO, q::Query; firstrow=true)
    exmargin = length(string(q.qtype)) + 1
    if firstrow
        print("   ", uppercase(string(q.qtype))) 
        print(" $(q.ex)\n")
    else
        println((" " ^ exmargin) * "$(q.ex)")
    end
end

Base.show(io::IO, q::Query) = show(io, q, firstrow=true)

typealias CompositeQuery Vector{Query}

function Base.show(io::IO, qs::CompositeQuery)
    println("Query:")
    prevq = None
    for q in qs
        if q.qtype != prevq
            show(q, firstrow=true)
        else
            show(q, firstrow=false)
        end
        prevq = q.qtype
    end
end

# Construct array of Query-s from a vector of query expressions.
# This is a *CompositeQuery*.
# qtype is one of :select, :where, :groupby, :aggregate, :sortby, etc.
function make_queries(qs::ParserVector, qtype::Symbol)
    [Query(q[1], q[2], classify_col_expression(q[2]), qtype) for q in qs]
end

# Composing queries. |> just becomes a Vector append
|>(cq1::CompositeQuery, cq2::CompositeQuery) = [cq1, cq2]

    
# Query functions are thin wrappers around the Query type constructor.
# TODO: Add checks that expressions are consistent with the query function.
where(qs::ParserVector)              = make_queries(qs, :where)
Base.select(qs::ParserVector)        = make_queries(qs, :select)
update(qs::ParserVector)             = make_queries(qs, :update)
sortby(qs::ParserVector)             = make_queries(qs, :sortby)
DataFrames.groupby(qs::ParserVector) = make_queries(qs, :groupby)
aggregate(qs::ParserVector)          = make_queries(qs, :aggregate)


# Executable query functions (Query, Queryable) -> Queryable

# where returns subset of rows
function xwhere(q::Query, df::Queryable)
    getindex(df, q.parser(df), names(df))
end

# Curried 
xwhere(q::Query) = df::Queryable -> xwhere(q, df)

# select returns subset of columns
function xselect(q::Query, df::Queryable)
    getindex(df, q.parser(df))
end

# Curried
xselect(q::Query) = df::Queryable -> xselect(q, df)    

# update adds columns or modifies existing ones
function xupdate(q::Query, df::Queryable)
    nothing
end

# Curried
xupdate(q::Query) = df::Queryable -> xupdate(q, df)

# Sortby arranges row
function xsortby(q::Query, df::Queryable)
        nothing
end

# Curried
xsortby(q::Query) = df::Queryable -> xsortby(q, df)

# Groupby and aggregate tk.
function xgroupby(q::Query, df::Queryable)
        nothing
end

function xaggregate(q::Query, df::Queryable)
        nothing
end

function query_function(qtype::Symbol)
    qmap = {:where     => xwhere,
            :select    => xselect,
            :update    => xupdate,
            :sortby    => xsortby,
            :groupby   => xgroupby,
            :aggregate => xaggregate}
    qmap[qtype]
end
    
# Executing a query. Pipe through series of queries
function query(cq::CompositeQuery, df)
    qfuncs = [query_function(q.qtype)(q) for q in cq]
    foldl(|>, df, qfuncs)
end

query(cq::CompositeQuery) = df::Queryable -> query(cq, df)
    
# This macro returns a ParserVector type, which is a (Function, ColumnExpr) pair.  
macro ?(exs...)
    esc(:([(df -> eval(parse_col_expression(ex, df)), ex) for ex in $exs]))
end

function classify_col_expression(ex::Expr)
    invalid_expr_error = () -> error("Not a valid query expression: $ex")
    if isa(ex, Symbol)
        :columns
    elseif ex.head == :tuple || ex.head == :vcat
        if all(map(typeof, ex.args) .== Symbol)
            :columns
        else
            invalid_expr_error()
        end
    elseif ex.head == :(=)
        if typeof(ex.args[1]) == Symbol
            :assignment
        else
            invalid_expr_error()
        end
        
    elseif ex.head in (:comparison, :call)
        :conditional
    else
        invalid_expr_error()
    end
end

function parse_col_expression(ex::Expr, df::DataFrame)
    function expand_column_refs(node)
        for (i, arg) in enumerate(node.args)
            if isa(arg, Symbol)
                if arg in names(df)
                    expanded = Expr(:ref, df, QuoteNode(arg))
                    setindex!(node.args, expanded, i)
                end
            elseif isa(arg, Expr)
                expand_column_refs(arg)
            else
                continue
            end
        end
        node
    end
    newex = copy(ex)
    ex_type = classify_col_expression(newex)
    
    if is(ex_type, :columns)
        Expr(:vcat, [QuoteNode(arg) for arg in ex.args]...)
    elseif is(ex_type, :assignment)
        lhs, rhs = newex.args
        isa(lhs, Symbol) ? nothing:
            error("LHS of an assignment must be a valid column name.")
        # df[:newcol] = ... df[:oldcol1] ... df[:oldcol2] ....
        # !!! This mutates the original DF.
        Expr(:(=), Expr(:ref, df, QuoteNode(lhs)), expand_column_refs(newex))
    else
        expand_column_refs(newex)
    end
end

end # module
