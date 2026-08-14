from qmb.search.fuzzy import fuzzy_score, normalize


def test_substring_match_beats_subsequence_match() -> None:
    query = normalize("orders")
    substring_score = fuzzy_score(query, "raw_orders")
    subsequence_score = fuzzy_score(query, "o_r_d_e_r_s_extra")

    assert substring_score is not None
    assert subsequence_score is not None
    assert substring_score > subsequence_score


def test_consecutive_run_bonus_scores_higher_than_scattered_match() -> None:
    query = normalize("abc")
    consecutive_score = fuzzy_score(query, "xabcx")
    scattered_score = fuzzy_score(query, "xaxbxcx")

    assert consecutive_score is not None
    assert scattered_score is not None
    assert consecutive_score > scattered_score


def test_no_match_returns_none() -> None:
    query = normalize("zzz")
    assert fuzzy_score(query, "orders") is None


def test_query_letters_out_of_order_return_none() -> None:
    query = normalize("cba")
    assert fuzzy_score(query, "abc") is None


def test_empty_query_matches_everything_with_a_score() -> None:
    assert fuzzy_score("", "anything") is not None
    assert fuzzy_score("", "") is not None


def test_normalize_lowercases_strips_and_maps_colon_to_dot() -> None:
    assert normalize("  Project:Dataset.Table  ") == "project.dataset.table"
