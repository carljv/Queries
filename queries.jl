# -----------------------------------------------------------------------------
# Composable queries for Julia dataframes.
#
# C. Vogel       March 2014
# -----------------------------------------------------------------------------

using DataFrames
import DataFrames.groupby

typealias Queryable Union(AbstractDataFrame, GroupedDataFrame, GroupApplied)
typealias QueryableGroup Union(GroupedDataFrame, GroupApplied)

# ---------------------------------------------------------
# GROUPBY
# Convenience wrappers around DataFrame.groupby
# ---------------------------------------------------------
groupby(cols::Vector{Symbol}, df::AbstractDataFrame) = groupby(df, cols)

groupby(cols::Vector{Symbol}) = df::AbstractDataFrame -> groupby(df, cols)
groupby(cols::Symbol...) = groupby([cols...])


# ---------------------------------------------------------
# SELECT
# Column of Symbols
# ---------------------------------------------------------
Base.select(cols::Vector{Symbol}, df::AbstractDataFrame) = df[:, cols]

Base.select(cols::Vector{Symbol}) = df::Queryable -> select(cols, df)
select(cols::Symbol...) = select([cols...])

oBase.select(cols::Vector{Symbol}, dfs::GroupedDataFrame) = map(select(cols), dfs)


# ---------------------------------------------------------
# PARSING DATAFRAME EXPRESSIONS
#
# TODO: This will probably break if the expression contains
# a getitem call to a same-named column in another DataFrame
# ---------------------------------------------------------
function parse_df_expr(dfexpr::Expr, df::AbstractDataFrame)
    function expand_nodes(ex, df)
        for (i, arg) in enumerate(ex.args)
            if isa(arg, Symbol)
                if arg in names(df)
                    argsym = symbol(arg)
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
    newexpr = copy(dfexpr)
    expand_nodes(newexpr, df)
    newexpr
end 

# ---------------------------------------------------------
# UPDATE
# ---------------------------------------------------------
function update(assignment::Expr, df::AbstractDataFrame)
    assignment.head == :(=) ? nothing : error("Expression must be an assignment.")
    lhs, rhs = assignment.args
    val = parse_df_expr(rhs, df) |> eval
    newdf = copy(df)
    setindex!(newdf, val, lhs)
    newdf
end

function update!(assignment::Expr, df::AbstractDataFrame)
    assignment.head == :(=) ? nothing : error("Expression must be an assignment.")
    lhs, rhs = assignment.args
    val = parse_df_expr(rhs, df) |> eval
    setindex!(df, val, lhs)
end

function update(assignment_vector::Vector{Expr}, df::AbstractDataFrame)
    newdf = copy(df)
    for a in assignment_vector
        update!(a, newdf)
    end
    newdf
end

update(assignment::Expr) = (df::AbstractDataFrame) -> update(assignment, df)
update!(assignment::Expr) = (df::AbstractDataFrame) -> update!(assignment, df)

update(assignment_vector::Vector{Expr}) =
    df::Union(AbstractDataFrame) -> update(assignment_vector, df)
update(assignments::Expr...) = update([assignments...])

# ---------------------------------------------------------
# WHERE
# ---------------------------------------------------------
function where(condition::Expr, df::AbstractDataFrame)
    condition.head == :(=) ? error("Cannot filter on an assignment.") : nothing
    val = parse_df_expr(condition, df) |> eval
    typeof(val) <: AbstractArray{Bool} ? nothing :
        error("Filter condition expression must evaluate to a boolean vector.")
    getindex(df, val, names(df))
end

# Subdataframes don't have a copy method.
Base.copy(sdf::SubDataFrame) = sdf

function where(condition_vector::Vector{Expr}, df::AbstractDataFrame)
    newdf = copy(df)
    for c in condition_vector
       newdf = where(c, newdf)
    end
    newdf
end

where(condition::Expr, dfs::QueryableGroup) = map(where(condition), dfs)
where(conditions::Vector{Expr}, dfs::QueryableGroup) = map(where(conditions), dfs)

where(condition::Expr) = df::Queryable -> where(condition, df)
where(condition_vector::Vector{Expr}) = df::Queryable -> where(condition_vector, df)
where(conditions::Expr...) = where([conditions...])


# ---------------------------------------------------------
# AGGREGATE
# ---------------------------------------------------------
function aggregate(assignment::Expr, df::AbstractDataFrame)
    assignment.head == :(=) ? nothing :
        error("Aggregation must be an assignment expression.")
    lhs, rhs = assignment.args
    aggdf = DataFrame()
    val = parse_df_expr(rhs, df) |> eval
    aggdf[lhs] = val
    aggdf
end

# Each aggregate calculation constitutes a column of the resulting
# dataframe
aggregate(assignment_vector::Vector{Expr}, df::AbstractDataFrame) =
    hcat(map(a -> aggregate(a, df), assignment_vector)...)
    
function aggregate(assignment::Expr, dfs::GroupedDataFrame)
    keydf = dfs.parent[dfs.starts, dfs.cols]
    hcat(keydf, vcat(map(aggregate(assignment), dfs).vals...))
end

function aggregate(assignment_vector::Vector{Expr}, dfs::GroupedDataFrame)
    keydf = dfs.parent[dfs.starts, dfs.cols]
    hcat(keydf, vcat(map(aggregate(assignment_vector), dfs).vals...))
end

function aggregate(assignment::Expr, dfs::GroupApplied)
    aggdfs = vcat(map(aggregate(assignment), dfs.vals)...)
    hcat(vcat(dfs.keys), vcat(aggdfs...))
end

function aggregate(assignment_vector::Vector{Expr}, dfs::GroupApplied)
    hcat(vcat(dfs.keys), vcat(map(aggregate(assignment_vector), dfs.vals)...))
end
 
aggregate(assignment_vector::Vector{Expr}) =
    df::Queryable -> aggregate(assignment_vector, df)

aggregate(assignments::Expr...) = aggregate([assignments...])


# ---------------------------------------------------------
# TEST CASE
# ---------------------------------------------------------

using Datetime

const googurl = "http://ichart.finance.yahoo.com/table.csv?s=GOOG&a=07&b=19&c=2004&d=02&e=5&f=2014&g=d&ignore=.csv"


data =
googurl |>
    download |>
    open |>
    readtable |>
    update(:(Date  = map(date, Date)),
           :(Year  = map(year, Date)),
           :(Month = map(month, Date)),
           :(Spread = High - Low)) |>
    groupby(:Year, :Month) |>
    where(:(Volume .>= quantile(Volume, .75))) |>
    aggregate(:(AvgVolume = mean(Volume)),
              :(AvgSpread = mean(Spread)))
               
