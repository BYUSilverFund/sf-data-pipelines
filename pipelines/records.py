import polars as pl


def check_columns(expected: list[str], actual: list[str]) -> None:
    left_unique = list(set(expected) - set(actual))
    right_unique = list(set(actual) - set(expected))

    if len(left_unique) > 0:
        raise ValueError(f"Columns missing: {left_unique}")

    if len(right_unique) > 0:
        raise ValueError(f"Extra columns found: {right_unique}")


def check_schema(expected: dict[str, pl.DataType], actual: pl.Schema) -> None:
    for col, dtype in expected.items():
        if actual[col] != dtype:
            raise ValueError(f"Column {col} has incorrect type: {actual[col]}, expected: {dtype}")


class CovarianceMatrix(pl.DataFrame):
    """Represents a covariance matrix DataFrame with a specific structure.

    Ensures that the DataFrame contains the expected columns, schema, and order,
    and provides sorting and initialization for further use in financial analysis.

    Args:
        cov_mat (pl.DataFrame): DataFrame containing the covariance matrix data.
        barrids (list[str]): List of 'barrid' values to be included in the covariance matrix.

    Raises:
        ValueError: If the columns or schema do not match the expected structure.
    """

    def __init__(self, cov_mat: pl.DataFrame, barrids: list[str]) -> None:
        expected_order = ["barrid"] + sorted(barrids)

        valid_schema = {barrid: pl.Float64 for barrid in barrids}

        # Check columns
        check_columns(expected_order, cov_mat.columns)

        # Check schema
        check_schema(valid_schema, cov_mat.schema)

        # Reorder columns
        cov_mat = cov_mat.select(expected_order)

        # Sort
        cov_mat = cov_mat.sort("barrid")

        # Initialize
        super().__init__(cov_mat)

    def to_matrix(self):
        """Converts the covariance matrix to a numpy matrix, excluding 'barrid'.

        Returns:
            np.ndarray: The covariance matrix as a numpy array.
        """
        return self.drop("barrid").to_numpy()

