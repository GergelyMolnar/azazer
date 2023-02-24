
# Data Engineer Tech Test

This is a simple tech test asking you to write some Python program with a purpose to verify your learning capability and Python skills.
Please note that we do expect you to have sufficient Python skills but not on the specific tech stack required. The expectation
is that if you don't know about something, learn how to use it by reading and trying to solve the problem. There are
plenty of tutorials and examples online, and you can Google as much as you like to complete the task.

Do get prepared on explaining what you have done, especially when third party code or tutorial code have been used.

## Overall requirement
1. Once the solution is finished, please store it in a public Git repository on GitHub (this is free to create) and share the link with us
2. The solution should be working e2e, and ideally we would expect to clone the repo and run a single command to get the output.
3. You do not have to use Cloud Dataflow, Direct Runner is fine

## Task 1
Write an Apache Beam batch job in Python satisfying the following requirements
1. Read the input from   
   `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
2. Find all transactions have a `transaction_amount` greater than `20`
3. Exclude all transactions made before the year `2010`
4. Sum the total by `date`
5. Save the output into `output/results.jsonl.gz` and make sure all files in the `output/` directory is git ignored

If the output is in a CSV file, it would have the following format
```
date, total_amount
2011-01-01, 12345.00
```

## Task 2
Following up on the same Apache Beam batch job, also do the following
1. Group all transform steps into a single `Composite Transform`
2. Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam
