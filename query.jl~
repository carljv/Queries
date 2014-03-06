# -----------------------------------------------------------------------------
# Composable queries for Julia dataframes.
#
# C. Vogel       March 2014
# -----------------------------------------------------------------------------

using DataFrames
import DataFrames.groupby

type Query
    functions::Vector{Function}
    expressions::Vector{Expr}
end



