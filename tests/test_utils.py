import pytest

_full_error = pytest.param(
    """
E           NOTICE:  drop cascades to 8 other objects
E           DETAIL:  drop cascades to view general_fra.entities_view
E           drop cascades to view general_fra.bookings_sourcing_view
E           drop cascades to view general_fra.entity_files_view
E           drop cascades to view general_fra.bookings_view
E           drop cascades to view billing_fra.bills_lite_view
E           drop cascades to view general_fra.entity_users_view
E           drop cascades to view meta_fra.emails_to_send_view
E           drop cascades to view general_fra.users_view
E           NOTICE:  Recreating general_fra.entities_view
E           NOTICE:  Done
E           NOTICE:  Recreating general_fra.bookings_sourcing_view
E           NOTICE:  Done
E           NOTICE:  Recreating general_fra.entity_files_view
E           NOTICE:  Done
E           NOTICE:  Recreating general_fra.bookings_view
E           NOTICE:  Done
E           NOTICE:  Recreating billing_fra_fra.bills_lite_view
E           ERROR:  query string argument of EXECUTE is null
E           CONTEXT:  PL/pgSQL function utils.recreate_ordered_views(text,text,text[]) line 30 at EXECUTE
E           ERROR:  cannot drop column duns_number of table general_fra.companies because other objects depend on it
E           DETAIL:  view general_fra.companies_view depends on column duns_number of table general_fra.companies
E           view general_fra.entities_view depends on view general_fra.companies_view
E           view general_fra.bookings_sourcing_view depends on view general_fra.entities_view
E           view general_fra.entity_files_view depends on view general_fra.entities_view
E           view general_fra.bookings_view depends on view general_fra.entities_view
E           view billing_fra.bills_lite_view depends on view general_fra.companies_view
E           view general_fra.entity_users_view depends on view general_fra.companies_view
E           view meta_fra.emails_to_send_view depends on view general_fra.entity_users_view
E           view general_fra.users_view depends on view general_fra.companies_view
E           HINT:  Use DROP ... CASCADE to drop the dependent objects too.
""",
    [
        "\n".join(
            [
                "ERROR: query string argument of EXECUTE is null",
                "CONTEXT: PL/pgSQL function utils.recreate_ordered_views(text,text,text[]) line 30 at EXECUTE",
            ]
        ),
        "\n".join(
            [
                "ERROR: cannot drop column duns_number of table general_fra.companies because other objects depend on it",
                "DETAIL: view general_fra.companies_view depends on column duns_number of table general_fra.companies",
                "view general_fra.entities_view depends on view general_fra.companies_view",
                "view general_fra.bookings_sourcing_view depends on view general_fra.entities_view",
                "view general_fra.entity_files_view depends on view general_fra.entities_view",
                "view general_fra.bookings_view depends on view general_fra.entities_view",
                "view billing_fra.bills_lite_view depends on view general_fra.companies_view",
                "view general_fra.entity_users_view depends on view general_fra.companies_view",
                "view meta_fra.emails_to_send_view depends on view general_fra.entity_users_view",
                "view general_fra.users_view depends on view general_fra.companies_view",
                "HINT: Use DROP ... CASCADE to drop the dependent objects too.",
            ]
        ),
    ],
    id="full_error",
)


@pytest.mark.parametrize("data,expected", [_full_error])
def test_extract_error_message(data, expected):
    from padmy.utils import extract_pg_error

    assert extract_pg_error(data) == expected
