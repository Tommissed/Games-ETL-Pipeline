from dagster import build_op_context

from analytics.ops.rawg import transform_rawg


def test_transform_rawg():
    # ASSEMBEL
    input_data = [
        {
            "id": 101,
            "slug": "elden-ring",
            "name": "Elden Ring",
            "released": "2022-02-25",
            "tba": False,
            "rating": 4.5,
            "ratings": [{"title": "exceptional"}],
            "rating_top": 5,
            "ratings_count": 2500,
            "reviews_text_count": 500,
            "metacritic": 95,
            "playtime": 80,
            "updated": "2023-01-01T00:00:00",
            "platforms": [{"platform": {"name": "PC"}}],
            "junk_column": "test column to be dropped",
        }
    ]

    expected_data = [
        {
            "game_id": 101,
            "slug": "elden-ring",
            "name": "Elden Ring",
            "released": "2022-02-25",
            "tba": False,
            "rating": 4.5,
            "ratings": [{"title": "exceptional"}],
            "rating_top": 5,
            "ratings_count": 2500,
            "reviews_text_count": 500,
            "metacritic": 95,
            "playtime": 80,
            "updated_at": "2023-01-01T00:00:00",
            "platforms": [{"platform": {"name": "PC"}}],
        }
    ]

    # ACT
    context = build_op_context()
    actual_data = transform_rawg(context, input_data)

    # ASSERT
    assert actual_data == expected_data
