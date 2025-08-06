from streamlit.testing.v1 import AppTest


def test_app_shows_title():
    at = AppTest.from_file("src/app/app.py").run()
    assert len(at.title) > 0
    assert at.title[0].value == "Kingfisher County Well Location Variance"


def test_app_populated_dataframe():
    # actually checks for a rendered dataframe, not populated df
    at = AppTest.from_file("src/app/app.py").run(timeout=30)

    dataframes = list(at.dataframe) + list(at.table)

    assert dataframes, "No DataFrame or Table found in Streamlit output."

    df_element = dataframes[0]
    assert hasattr(df_element, "value"), "df[0] has no 'value' attribute."
    assert not df_element.value.empty, "The displayed DataFrame is empty."
