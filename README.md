# Goal
The main goal of this task is to calculate transactions from order updates.

However, attached code contains various business logic and technical errors.
The goal is to track those errors and fix them.

# Introduction

Cryptocurrency exchanges usually provide websocket streams for clients to oversee their orders progress.

Those updates are cumulative per order and follow a global sequence number.
Let's assume that `Update` for an order can be defined as:

````scala
Update(id: String,
  market: String,
  side: Side,
  total: BigDecimal,
  filled: BigDecimal,
  sequence: Int)
````

We define Order status as `Executed` if `total == filled`.
We expect no further updates as soon as we fill our order.

Transaction status is always `Executed` and all transactions must have positive amount.

Typical order sequence goes like this, let's assume a tuple (sequence, status, total, filled) with a fixed order id

```
(1, Pending, 0.8, 0) ->
 -> (2, Pending, 0.8, 0.5) 
 -> (3, Executed, 0.8, 0.8)
```

This results in two transactions: one transaction for amount `0.5` and second one for amount `0.8 - 0.5 = 0.3`.

Transaction can also get completed in a single go:
```
(1, Pending, 0.8, 0) ->
 -> (2, Executed, 0.8, 0.8) 
```
This results in one transaction for `0.8` amount.

The goal of this task is to find and calculate transactions for an order with a given id.

## General requirements
Note: Some of those cases are covered in tests, some are not. Which ones?

1. Test results must not be changed, unless proven wrong.
2. All tests inside `TransactionsStreamSpec` must pass. Particularly:
   1. On application shutdown, we should process all remaining and currently processing records
   2. If sequence number is missed, we should shut down the whole application, but for simplicity we still want to process all remaining updates.
   3. If we receive the update and order is not present in the state, we want to wait until it is present in the state. If it isn't present in the state after 5 seconds pass, we can drop it.
3. Transactions/orders must be inserted and updated only if `performLongRunningOperation` succeeds.
4. All updates should be processed concurrently. Introduce a parameter `maxConcurrent: Int` that dictates the max amount of concurrently processing updates. Write a test.<br> Hint: Orders with the same id must be processed sequentially regardless of the degree of concurrency.

## Given resources

A docker-compose file

```shell
cd development && docker-compose up
```

SQL queries (inside `com.example.persistence` package, migrations for orders and transactions tables, database pool of
connections)

Data model inside `com.example.model` package.

`StateManager` - an object that manages global orders state.

Main logic inside `TransactionStream`.
