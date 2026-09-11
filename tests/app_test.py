from pathlib import Path

from streamlit.testing.v1 import AppTest

# newer streamlit resolves relative paths against THIS file, not the
# cwd; anchor to the repo root so it works in CI and locally alike
APP = str(Path(__file__).resolve().parents[1] / "src" / "app" / "app.py")


def test_app_shows_title():
    at = AppTest.from_file(APP).run(timeout=30)
    assert len(at.title) > 0
    assert at.title[0].value == "Kingfisher County Well Location Variance"


def test_app_populated_dataframe():
    # actually checks for a rendered dataframe, not populated df
    at = AppTest.from_file(APP).run(timeout=30)

    dataframes = list(at.dataframe) + list(at.table)

    assert dataframes, "No DataFrame or Table found in Streamlit output."

    df_element = dataframes[0]
    assert hasattr(df_element, "value"), "df[0] has no 'value' attribute."
    assert not df_element.value.empty, "The displayed DataFrame is empty."
