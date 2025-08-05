from streamlit.testing.v1 import AppTest


def test_app_shows_title():
    at = AppTest.from_file("src/app/app.py").run()
    assert len(at.title) > 0
    assert at.title[0].value == "Kingfisher County Well Location Variance"
