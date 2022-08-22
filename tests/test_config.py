import tempfile
from dataclasses import asdict
from pathlib import Path

import pytest

from padmy.config import Config, ConfigSchema, ConfigTable, AnoFields
from .utils import pprint_dataclass_diff

CONFIG_FULL = """
sample: 20
schemas:
  - schema_1
  - name: schema_2
    sample: 30

tables:
  - schema: public
    table: table_1
    sample: 10
    fields:
      bar: EMAIL
  - schema: public
    table: table_2
"""


def get_config_full_expected():
    schemas = [
        ConfigSchema(schema='schema_1'),
        ConfigSchema(schema='schema_2', sample=30),
    ]
    tables = [
        ConfigTable(schema='public', table='table_1',
                    sample=10,
                    fields=[AnoFields.load({'bar': 'EMAIL'})]),
        ConfigTable(schema='public', table='table_2')
    ]
    expected = Config(
        sample=20,
        schemas=schemas,
        tables=tables,
    )
    return expected


CONFIG_CUSTOM_TABLE_FIELDS = """
tables:
  - schema: public
    table: table_1
    sample: 10
    fields:
      - column: bar
        type: EMAIL
        # Extra args
        domain_name: tracktor.fr
"""


def get_custom_fields_expected():
    expected = Config(
        tables=[
            ConfigTable(schema='public', table='table_1',
                        sample=10,
                        fields=[AnoFields(column='bar', type='EMAIL',
                                          extra_args={'domain_name': 'tracktor.fr'})]),
        ],
    )
    return expected


@pytest.mark.parametrize('config, expected', [
    (CONFIG_FULL, get_config_full_expected()),
    (CONFIG_CUSTOM_TABLE_FIELDS, get_custom_fields_expected())
])
def test_load_config_file(config, expected):
    from padmy.config import Config

    with tempfile.TemporaryDirectory() as dir:
        _file = Path(dir) / 'test.yml'
        _file.write_text(config)
        config = Config.load_from_file(_file)

    assert asdict(config) == asdict(expected), pprint_dataclass_diff(config, expected)


def test_load_config():
    from padmy.config import Config, ConfigSchema

    schemas = [
        ConfigSchema(schema='schema_1'),
        ConfigSchema(schema='schema_2'),
    ]

    expected = Config(
        sample=20,
        schemas=schemas,
    )
    config = Config.load(sample=20, schemas=['schema_1', 'schema_2'])
    assert asdict(config) == asdict(expected), pprint_dataclass_diff(config, expected)
