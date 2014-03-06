Query
=====

Concise, composable queries for Julia DataFrames.


## Principles ##
1. Queries are created by functions, not macros. Avoid using macros whenever possible.
2. Queries have type Query. This makes queries modifiable and extensible via Julia's dispatch mechanisms.
3. Queries are composable. Multiple queries can be combined to create a more complex one.
4. Queries are independent of a specific DataFrame. A query can be created once, then applied to multiple DataFrames.

## API Overview ##
### Query functions ###
Query functions take one or more expressions (type `Expr`) and return instances of type `Query`. The don't perform queries, they just create queries that can be applied to a DataFrame.

- `select`: Expression is a list of columns. The resulting `Query` subsets a DataFrame on those columns.
- `where`: Expressions are conditional statement. The resulting `Query` subsets a DataFrame to rows where the expression is true.
- `update`: Expression are assignments. The resulting `Query` adds or changes columns in a DataFrame.
- `groupby`: Expression is one or more columns. The resulting `Query` creates a Grouped DataFrame with the unique column values as keys.
- `aggregate`: Expression is an assigment. The resulting `Query` creates a DataFrame with the results of the assigments (typically summaries/aggregates).
- `sort`: Expression is one or more columns. The resulting `Query` sorts a DataFrame on the columns.

### Combining queries ###
Queries can be sequentially combined with the `|>` operator. To create a complex query, one can pipe together multiple query functions. E.g.,

```{.julia}
myquery = update(:C = f(A)) |> where(:C .> :B) |> select(:A, :C)
```

### The query macro ###
Queries are evaluated over DataFrames (or similar *queryable* types) with the `@query` macro. The query macro resolves all the references to column in the query by checking for them in the DataFrame and expanding them to fully-qualified references to the DataFrame. It then executes its component query functions and returns a resulting DataFrame (or similary queryable type).

## Syntax Example ##

The following example query some stock price data.

```{.julia}
# A query that computes the monthly volume-weghted avg price.
monthly_vwap = update(:(PV = Volume * Price),
                     :(Year = map(year, Date)),
                     :(Month = map(month, Date))) |>
               groupby(:Year, :Month) |>
               aggregate(:(sumPV = sum(PV)), :(sumV = sum(Volume))) |>
               update(:(VWAP = sumPV / sumV)) |>
               select(:Year, :Month, :VWAP)

# A query that finds the top decile of VWAPS
vwap_topdec = monthly_vwap |> where(VWAP >=. quantile(VWAP, .9))

# Queries can be applied to any DF with matching columns.
goog_topvwap = @query goog_df vwap_topdec
msft_topvwap = @query msft_df vwap_topdec

# Queries can be composed in the query macro
goog_lowvol = @query goog_df begin
    select(:Date, :Volume) |>
    where(:(Volume .< median(Volume))
end

```
