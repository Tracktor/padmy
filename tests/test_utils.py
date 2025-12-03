import os
from contextlib import nullcontext
from pathlib import Path

import pytest

from padmy.utils import PGConnectionInfo


class TestPGConnectionInfo:
    @pytest.mark.parametrize(
        "uri,expected,expectation,expected_env_vars",
        [
            pytest.param(
                "postgresql://myuser:mypass@dbhost:5432/mydb",
                {
                    "pg_user": "myuser",
                    "pg_password": "mypass",
                    "pg_host": "dbhost",
                    "pg_port": 5432,
                    "database": "mydb",
                    "ssl_mode": None,
                    "ssl_ca": None,
                    "ssl_cert": None,
                    "ssl_key": None,
                },
                nullcontext(),
                {
                    "PGHOST": "dbhost",
                    "PGPORT": "5432",
                    "PGUSER": "myuser",
                    "PGPASSWORD": "mypass",
                    "PGDATABASE": "mydb",
                },
                id="basic_uri",
            ),
            pytest.param(
                "postgresql://user:pass@localhost:5432",
                {
                    "pg_user": "user",
                    "pg_password": "pass",
                    "pg_host": "localhost",
                    "pg_port": 5432,
                    "database": None,
                    "ssl_mode": None,
                    "ssl_ca": None,
                    "ssl_cert": None,
                    "ssl_key": None,
                },
                nullcontext(),
                {
                    "PGHOST": "localhost",
                    "PGPORT": "5432",
                    "PGUSER": "user",
                    "PGPASSWORD": "pass",
                },
                id="without_database",
            ),
            pytest.param(
                "postgresql://user:pass@localhost:5432/db?sslmode=require",
                {
                    "pg_user": "user",
                    "pg_password": "pass",
                    "pg_host": "localhost",
                    "pg_port": 5432,
                    "database": "db",
                    "ssl_mode": "require",
                    "ssl_ca": None,
                    "ssl_cert": None,
                    "ssl_key": None,
                },
                nullcontext(),
                {
                    "PGHOST": "localhost",
                    "PGPORT": "5432",
                    "PGUSER": "user",
                    "PGPASSWORD": "pass",
                    "PGDATABASE": "db",
                    "PGSSLMODE": "require",
                },
                id="with_ssl_mode",
            ),
            pytest.param(
                "postgresql://user:pass@localhost:5432/db?"
                "sslmode=verify-full&"
                "sslrootcert=/path/to/ca.crt&"
                "sslcert=/path/to/client.crt&"
                "sslkey=/path/to/client.key",
                {
                    "pg_user": "user",
                    "pg_password": "pass",
                    "pg_host": "localhost",
                    "pg_port": 5432,
                    "database": "db",
                    "ssl_mode": "verify-full",
                    "ssl_ca": Path("/path/to/ca.crt"),
                    "ssl_cert": Path("/path/to/client.crt"),
                    "ssl_key": Path("/path/to/client.key"),
                },
                nullcontext(),
                {
                    "PGHOST": "localhost",
                    "PGPORT": "5432",
                    "PGUSER": "user",
                    "PGPASSWORD": "pass",
                    "PGDATABASE": "db",
                    "PGSSLMODE": "verify-full",
                    "PGSSLROOTCERT": "/path/to/ca.crt",
                    "PGSSLCERT": "/path/to/client.crt",
                    "PGSSLKEY": "/path/to/client.key",
                },
                id="with_all_ssl_params",
            ),
            pytest.param(
                "postgresql://user:p%40ss%3Aword@localhost:5432/db",
                {
                    "pg_user": "user",
                    "pg_password": "p%40ss%3Aword",
                    "pg_host": "localhost",
                    "pg_port": 5432,
                    "database": "db",
                    "ssl_mode": None,
                    "ssl_ca": None,
                    "ssl_cert": None,
                    "ssl_key": None,
                },
                nullcontext(),
                {
                    "PGHOST": "localhost",
                    "PGPORT": "5432",
                    "PGUSER": "user",
                    "PGPASSWORD": "p%40ss%3Aword",
                    "PGDATABASE": "db",
                },
                id="url_encoded_password",
            ),
            pytest.param(
                "invalid://uri",
                None,
                pytest.raises(ValueError, match="Invalid PG uri"),
                None,
                id="invalid_uri",
            ),
            pytest.param(
                "http://user:pass@localhost:5432/db",
                None,
                pytest.raises(ValueError, match="Invalid PG uri"),
                None,
                id="wrong_scheme",
            ),
            pytest.param(
                "postgresql://localhost",
                None,
                pytest.raises(ValueError, match="Invalid PG uri"),
                None,
                id="missing_credentials",
            ),
        ],
    )
    def test_from_uri(self, uri, expected, expectation, expected_env_vars):
        """Test URI parsing with various configurations and temp_env context manager."""
        with expectation:
            info = PGConnectionInfo.from_uri(uri)

            if expected is not None:
                assert info.pg_user == expected["pg_user"]
                assert info.pg_password == expected["pg_password"]
                assert info.pg_host == expected["pg_host"]
                assert info.pg_port == expected["pg_port"]
                assert info.database == expected["database"]
                assert info.ssl_mode == expected["ssl_mode"]
                assert info.ssl_ca == expected["ssl_ca"]
                assert info.ssl_cert == expected["ssl_cert"]
                assert info.ssl_key == expected["ssl_key"]

                # Test temp_env context manager
                if expected_env_vars is not None:
                    # Save original env
                    orig_env = {key: os.environ.get(key) for key in expected_env_vars.keys()}

                    # Use temp_env
                    with info.temp_env(include_database=True):
                        for key, expected_value in expected_env_vars.items():
                            assert os.environ.get(key) == expected_value, f"{key} should be {expected_value}"

                    # Verify restoration
                    for key, orig_value in orig_env.items():
                        assert os.environ.get(key) == orig_value, f"{key} should be restored to {orig_value}"

    @pytest.mark.parametrize(
        "ssl_config,expected_env_vars",
        [
            pytest.param(
                {},
                {},
                id="no_ssl",
            ),
            pytest.param(
                {
                    "ssl_mode": "require",
                    "ssl_ca": Path("/ca.crt"),
                    "ssl_cert": Path("/client.crt"),
                    "ssl_key": Path("/client.key"),
                },
                {
                    "PGSSLMODE": "require",
                    "PGSSLROOTCERT": "/ca.crt",
                    "PGSSLCERT": "/client.crt",
                    "PGSSLKEY": "/client.key",
                },
                id="with_all_ssl",
            ),
            pytest.param(
                {"ssl_mode": "require"},
                {"PGSSLMODE": "require"},
                id="ssl_mode_only",
            ),
            pytest.param(
                {"ssl_ca": Path("/ca.crt")},
                {"PGSSLROOTCERT": "/ca.crt"},
                id="ssl_ca_only",
            ),
            pytest.param(
                {
                    "ssl_cert": Path("/client.crt"),
                    "ssl_key": Path("/client.key"),
                },
                {
                    "PGSSLCERT": "/client.crt",
                    "PGSSLKEY": "/client.key",
                },
                id="ssl_cert_and_key",
            ),
        ],
    )
    def test_get_env_vars(self, ssl_config, expected_env_vars):
        """Test get_env_vars() with various SSL configurations."""
        info = PGConnectionInfo(
            pg_user="user",
            pg_password="pass",
            pg_host="localhost",
            pg_port=5432,
            **ssl_config,
        )
        env_vars = info.get_env_vars()

        assert env_vars == expected_env_vars

    @pytest.mark.parametrize(
        "user,password,host,port,expected_dsn,expected_obfuscated",
        [
            pytest.param(
                "user",
                "pass",
                "localhost",
                5432,
                "postgresql://user:pass@localhost:5432",
                "postgresql://user:****@localhost:5432",
                id="basic",
            ),
            pytest.param(
                "admin",
                "secretpass",
                "db.example.com",
                5433,
                "postgresql://admin:secretpass@db.example.com:5433",
                "postgresql://admin:****@db.example.com:5433",
                id="custom_host_port",
            ),
        ],
    )
    def test_dsn_properties(self, user, password, host, port, expected_dsn, expected_obfuscated):
        """Test DSN and obfuscated DSN property generation."""
        info = PGConnectionInfo(
            pg_user=user,
            pg_password=password,
            pg_host=host,
            pg_port=port,
        )
        assert info.dsn == expected_dsn
        assert info.obfuscated_dsn == expected_obfuscated
        assert password not in info.obfuscated_dsn


class TestErrorMessageExtraction:
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
    def test_extract_error_message(self, data, expected):
        from padmy.utils import extract_pg_error

        assert extract_pg_error(data) == expected
