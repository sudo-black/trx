from time import sleep

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import List, Dict, Tuple


class WithdrawProcessor:
    def __init__(self, balance_df: DataFrame, withdraw_df: DataFrame):
        """
        Initialize the WithdrawProcessor with balance and withdrawal dataframes.

        :param balance_df: DataFrame containing balance information.
        :param withdraw_df: DataFrame containing withdrawal information.
        """
        self.balance_df = balance_df
        self.withdraw_df = withdraw_df

    def validate_withdrawal(
        self, total_balance: float, withdraw_amount: float
    ) -> Tuple[bool, str]:
        """
        Validate if a withdrawal can be made given the total balance.

        :param total_balance: Total balance available.
        :param withdraw_amount: Amount to withdraw.
        :return: A tuple containing a boolean indicating if the withdrawal is valid and a validation message.
        """
        if withdraw_amount > total_balance:
            return False, "WITHDRAW FAILED: Insufficient total balance"
        return True, "WITHDRAW SUCCESS"

    def process_single_withdrawal(
        self, account_balances: List[Dict[str, float]], withdraw_amount: float
    ) -> None:
        """
        Process a single withdrawal against a list of account balances.

        :param account_balances: List of account balance dictionaries.
        :param withdraw_amount: Amount to withdraw.
        """
        for balance in account_balances:
            if withdraw_amount == 0:
                break

            if balance["available_balance"] >= withdraw_amount:
                balance["available_balance"] -= withdraw_amount
                withdraw_amount = 0
                if balance["available_balance"] == 0:
                    balance["status"] = "BALANCE WITHDREW"
            else:
                withdraw_amount -= balance["available_balance"]
                balance["available_balance"] = 0
                balance["status"] = "BALANCE WITHDREW"

    def update_results(
        self,
        account_balances: List[Dict[str, float]],
        validation_results: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        """
        Update the results with the latest validation status.

        :param account_balances: List of account balance dictionaries.
        :param validation_results: List of validation result dictionaries.
        :return: List of result dictionaries with updated validation status.
        """
        results = []
        for balance in account_balances:
            last_validation_result = next(
                (
                    v["validation_result"]
                    for v in validation_results
                    if v["account_id"] == balance["account_id"]
                ),
                "NO VALIDATION",
            )
            results.append(
                {
                    "account_id": balance["account_id"],
                    "balance_order": balance["balance_order"],
                    "initial_balance": balance["initial_balance"],
                    "available_balance": balance["available_balance"],
                    "status": balance.get("status", "ACTIVE"),
                    "validation_result": last_validation_result,
                }
            )
        return results

    def process_withdrawals(
        self,
        account_balances: List[Dict[str, float]],
        withdrawals: List[Dict[str, float]],
    ) -> List[Dict[str, str]]:
        """
        Process multiple withdrawals for an account.

        :param account_balances: List of account balance dictionaries.
        :param withdrawals: List of withdrawal dictionaries.
        :return: List of result dictionaries after processing withdrawals.
        """
        validation_results = []
        for withdraw in withdrawals:
            withdraw_amount = withdraw["withdraw_amount"]
            total_balance = sum([b["available_balance"] for b in account_balances])

            is_valid, validation_result = self.validate_withdrawal(
                total_balance, withdraw_amount
            )
            if is_valid:
                self.process_single_withdrawal(account_balances, withdraw_amount)

            validation_results.append(
                {
                    "account_id": withdraw["account_id"],
                    "withdraw_order": withdraw["withdraw_order"],
                    "validation_result": validation_result,
                }
            )

        results = self.update_results(account_balances, validation_results)
        return results

    def execute(self) -> List[Dict[str, str]]:
        """
        Execute the withdrawal processing for all accounts.

        :return: List of result dictionaries after processing all withdrawals.
        """
        balances = (
            self.balance_df.rdd.map(lambda row: row.asDict())
            .groupBy(lambda x: x["account_id"])
            .collectAsMap()
        )
        withdrawals = (
            self.withdraw_df.rdd.map(lambda row: row.asDict())
            .groupBy(lambda x: x["account_id"])
            .collectAsMap()
        )

        results = []

        for account_id in withdrawals:
            account_balances = balances.get(account_id, [])
            account_withdrawals = sorted(
                withdrawals[account_id], key=lambda x: x["withdraw_order"]
            )

            account_result = self.process_withdrawals(
                account_balances, account_withdrawals
            )
            results.extend(account_result)

        return results


def main() -> None:
    """
    Main entry point for the balance withdrawal processing application.
    Initializes Spark session, loads data, processes withdrawals, and displays results.
    """
    spark = (
        SparkSession.builder.appName("BalanceWithdrawProcessing")
        # .master("local[3]")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    # Load data
    balance_df = spark.read.csv("/mnt/data/balance.csv", header=True, inferSchema=True)
    withdraw_df = spark.read.csv(
        "/mnt/data/withdraw.csv", header=True, inferSchema=True
    )

    # Add initial_balance column
    balance_df = balance_df.withColumn("initial_balance", col("available_balance"))

    # Join data on account_id and balance_order to prepare for withdrawal processing
    withdraw_df = withdraw_df.withColumn(
        "withdraw_order", col("withdraw_order").cast("int")
    )
    withdraw_df = withdraw_df.orderBy("account_id", "withdraw_order")

    processor = WithdrawProcessor(balance_df, withdraw_df)
    results = processor.execute()

    # Create DataFrame from the results
    result_df = spark.createDataFrame(results)

    result_df.show(100, False)

    spark.stop()


if __name__ == "__main__":
    main()
