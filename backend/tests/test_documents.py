from app.documents import propose_groups, validate_group_partition


def test_pid_grouping_detects_questionnaire_boundaries():
    groups = propose_groups([
        "Participant CSA001 page 1",
        "CSA001 continuation",
        "Participant CSA002 page 1",
        "CSA002 continuation",
    ])
    assert [(group.start_page, group.end_page, group.participant_id) for group in groups] == [
        (1, 2, "CSA001"),
        (3, 4, "CSA002"),
    ]


def test_group_partition_requires_complete_coverage():
    validate_group_partition([(1, 2), (3, 5)], 5)
    try:
        validate_group_partition([(1, 2), (4, 5)], 5)
    except ValueError as exc:
        assert "gaps" in str(exc)
    else:
        raise AssertionError("Expected an invalid partition")
