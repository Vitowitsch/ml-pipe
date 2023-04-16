import pandas as pd

import ml_pipe.pipeline as pipe


def test_pipeline():
    """Test get data and apply two models."""
    models = [ModelA(), ModelB()]
    y = pipe.exec(pd.DataFrame(), models)

    assert ["output_A", "output_B"] == [*y]


class ModelA:
    def apply(self, df):
        """Add row."""
        df["output_A"] = "A"
        return df


class ModelB:
    def apply(self, df):
        """Add row."""
        df["output_B"] = "B"
        return df
