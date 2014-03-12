# --------------------------------------------------------------------------- #
# Composable queries for Julia dataframes.                                    #
#                                                                             #
# C. Vogel       March 2014                                                   #
# --------------------------------------------------------------------------- #

module JQ
export @?, update, select, where, groupby, aggregate, sortby, show


using DataFrames

typealias Queryable Union(AbstractDataFrame, GroupedDataFrame, GroupApplied)
typealias ColumnExpression Union(Expr, Symbol)

# A DataFrame expression is an expression that references a dataframe's columns,
# and, when matched with a query term (select, where, groupby, ...) creates
# and executable query that can be matched with a DataFrame.
#
# A DFExpr is created by the @? macro. 
#
# Fields:
# -------
# *parser* an anonymous function that takes a dataframe, and resolves column
# names from that dataframe referred to in the expression. 
# *expr* the unresolved query expression
# *expr_type* one of (:columns, :assignment, :condition).
#    - :columns is a list of columns in a dataframe. Ex: A, X, Col10
#      - used in select and groupby queries
#    - :assignment is an assignment intended to create a new varialbe or replace
#      an existing one. Ex: C = B + f(A, D)).
#    - :condition is an expression that should return a boolean array when evaluated.
#      Ex: X .> Y, isnull(Z)
type DFExpr
    parser::Function
    expr::ColumnExpression
    expr_type::Symbol
end


function DFExpr(e::ColumnExpression)
    DFExpr(() -> nothing, e, classify_query_expression(e))
end

Base.show(q::DFExpr) = println(ucfirst(string(q.expr_type)), " expression: $(q.expr)")

function Base.show(io::IO, qs::Vector{DFExpr})
    println(length(qs), "-element Array{$(eltype(qs)), 1}:")
    for q in qs
        print("    ")
        show(io::IO, q)
    end
end

typealias QueryPair (Symbol, Vector{DFExpr})

type Query
    qps::Vector{QueryPair}
end

Query(s::Symbol, qs::Vector{DFExpr}) = Query([(s, qs)])

function Base.show(io::IO, cq::Query)
    for qp in cq.qps
        println(" ", uppercase(string(qp[1])))
        for q in qp[2]
            println("      $(q.expr)")
        end
    end
end

|>(q1::Query, q2::Query) = Query([q1.qps, q2.qps])

macro ?(ex)
    ex_type = classify_query_expression(ex)
    parser = df -> eval(parse_query_expression(ex, df))
    DFExpr(parser, ex, ex_type) 
end
    
#    if length(exprs) > 1
#       collect(map(make_dfx, exs...))
#   else
#       [make_dfx(exs..)]
#   end
#end

### TEST MACRO
macro cool(dfexpr)
   esc( :($dfexpr.parser = () -> eval($dfexpr.expr)) )
end

macro lame(ex)
    () -> eval(ex)
end

    
function classify_query_expression(ex::ColumnExpression)
    invalid_expr_error = () -> error("Not a valid query expression: $ex")
    if isa(ex, Symbol)
        :columns        
    elseif ex.head == :tuple
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

# Query expression predicates make sure that proper query
# expressions are passed to query functions.
is_columns(q::DFExpr)    = is(q.expr_type, :columns)
is_assignment(q::DFExpr) = is(q.expr_type, :assignment)
is_condition(q::DFExpr)  = is(q.expr_type, :conditional)


# Query functions construct Queries
function update(qs::Vector{DFExpr})
    all(map(is_assignment, qs)) ? nothing :
        error("Update query require assignment expressions.")
    Query(:update, qs)
end

function Base.select(qs::Vector{DFExpr})
    all(map(is_columns, qs)) ? nothing :
        error("Select query requires columns.")
    Query(:select, qs)
end

function where(qs::Vector{DFExpr})
    all(map(is_condition, qs)) ? nothing :
        error("Where query requires condition expressions.")
    Query(:where, qs)
end

function groupby(qs::Vector{DFExpr})
    all(map(is_columns, qs)) ? nothing:
        error("Groupby query requires column expressions.")
    Query(:groupby, qs)
end

function aggregate(qs::Vector{DFExpr})
    all(map(is_assignment, qs)) ? nothing :
        error("Aggregate query requires assignment expressions.")
    Query(:aggregate, qs)
end

function sortby(qs::Vector{DFExpr})
    all(map(is_columns, qs)) ? nothing :
        error("Sortby query requires column expressions.")
    Query(:sortby, qs)
end

#qfmap = {
#    :update    => qryupdate,
#    :select    => qryselect,
#    :where     => qrywhere,
#    :groupby   => qrygroupby,
#    :aggregate => qryaggregate,
#    :sortby    => qrysortby
#}


function parse_query_expression(expr::ColumnExpression, df::Queryable)
    # Resolve all names.
    function expand_column_refs(expr)
        for (i, arg) in enumerate(expr.args)
            if isa(arg, Symbol)
                if arg in names(df)
                    expanded = Expr(:ref, df, QuoteNode(arg))
                    setindex!(expr.args, expanded, i)
                end
            elseif isa(arg, Expr)
                expand_column_refs(arg)
            else
                continue
            end
        end
        expr
    end
    newexpr = copy(expr)
    expr_type = classify_query_expression(newexpr)
    if is(expr_type, :assignment)
        lhs, rhs = newexpr.args
        isa(lhs, Symbol) ? nothing:
            error("LHS of an assignment must be a valid column name.")
        Expr(:(=), Expr(:ref, df, QuoteNode(lhs)), expand_column_refs(newexpr))
    else
        expand_column_refs(newexpr)
    end
end



### TEST MACRO
macro cool(ex)
   esc(:(() -> eval($ex)))
end

end # Module







































        


                # ------------------------------ #
                # Now let's draw a TIE Fighter!  #
                # ------------------------------ #
                #    .             *         .   #
                #       *   /  _  \      *       #
                #   *       |=|_|=|     *     *  #
                # *     *   \     /        .     #
                #    .                *          #
                #        *     .            *    #
                # ------------------------------ #
     
 
