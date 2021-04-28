import json
import importlib
import logging
import os
from collections import defaultdict

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Any, Iterator  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata
from databuilder.models.table_source import TableSource
from databuilder.models.table_owner import TableOwner


LOGGER = logging.getLogger(__name__)


class DBTExtractor(Extractor):
    # CONFIG KEYS
    CLUSTER_KEY = "cluster_key"
    DBT_MANIFEST_FILE_LOCATION = "manifest_file_location"
    DBT_CATALOG_FILE_LOCATION = "catalog_file_location"
    DBT_COMPILED_LOCATION = "dbt_compiled_location"
    SOURCE_URL_PREFIX = "source_url_prefix"
    SCHEMA_FILTER = "schema_filter"

    # Default values
    DEFAULT_CLUSTER_NAME = "master"
    DEFAULT_MANIFEST_FILE_LOCATION = "manifest.json"
    DEFAULT_CATALOG_FILE_LOCATION = "catalog.json"
    DEFAULT_COMPILED_LOCATION = "/var/tmp/amundsen/tables/compiled/"
    DEFAULT_SOURCE_URL_PREFIX = ""
    DEFAULT_SCHEMA_FILTER = ""

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            CLUSTER_KEY: DEFAULT_CLUSTER_NAME,
            DBT_MANIFEST_FILE_LOCATION: DEFAULT_MANIFEST_FILE_LOCATION,
            DBT_CATALOG_FILE_LOCATION: DEFAULT_CATALOG_FILE_LOCATION,
            DBT_COMPILED_LOCATION: DEFAULT_COMPILED_LOCATION,
            SOURCE_URL_PREFIX: DEFAULT_SOURCE_URL_PREFIX,
            SCHEMA_FILTER: DEFAULT_SCHEMA_FILTER,
        }
    )

    def init(self, conf):
        # type: (ConfigTree) -> None
        """
        :param conf:
        """
        conf = conf.with_fallback(DBTExtractor.DEFAULT_CONFIG)
        self._cluster = "{}".format(conf.get_string(DBTExtractor.CLUSTER_KEY))
        self._dbt_manifest_file_location = conf.get_string(
            DBTExtractor.DBT_MANIFEST_FILE_LOCATION
        )
        self._dbt_catalog_file_location = conf.get_string(
            DBTExtractor.DBT_CATALOG_FILE_LOCATION
        )
        self._source_url_prefix = "{}".format(
            conf.get_string(DBTExtractor.SOURCE_URL_PREFIX)
        )
        self._dbt_compiled_file_location = conf.get_string(
            DBTExtractor.DBT_COMPILED_LOCATION
        )
        self._schema_filter = "{}".format(conf.get_string(DBTExtractor.SCHEMA_FILTER))
        self._load_dbt_files()

    def _get_key(self, db, cluster, schema, tbl):
        return TableMetadata.TABLE_KEY_FORMAT.format(
            db=db, cluster=cluster, schema=schema, tbl=tbl
        )

    def _load_dbt_files(self):
        # type: () -> None
        """
        Parse manifest and catalog files for table/column info
        """
        # Read in manifest.json
        with open(self._dbt_manifest_file_location, "r") as f:
            self.manifest_data = json.load(f)

        # Read in catalog.json
        with open(self._dbt_catalog_file_location, "r") as f:
            self.catalog_data = json.load(f)

        # Load compiled sql into dictionary
        compiled_dict = {}
        if os.path.isdir(self._dbt_compiled_file_location):
            for file in os.listdir(self._dbt_compiled_file_location):
                filename = os.fsdecode(file)
                if filename.endswith('.sql'):
                    f = open(os.path.join(self._dbt_compiled_file_location,filename), "r")
                    contents = f.read()
                    filename = os.path.splitext(filename)[0]
                    compiled_dict[filename] = contents


        # Loop over and extract models
        results = []
        for key, manifest in self.manifest_data["nodes"].items():
            if (
                manifest["resource_type"] == "model"
                and key in self.catalog_data["nodes"]
                and (
                    not self._schema_filter
                    or str(self._schema_filter).lower() == manifest["schema"].lower()
                )
            ):
                LOGGER.info(
                    "Extracting dbt {}.{}".format(manifest["schema"], manifest["alias"])
                )

                # Get corresponding catalog data
                catalog = self.catalog_data["nodes"][key]

                # Loop over columns
                parsed_columns = []
                for name, metadata in manifest["columns"].items():
                    if name.upper() in catalog["columns"]:
                        column = ColumnMetadata(
                            name=metadata["name"],
                            description=metadata["description"].replace("\ ", " "),
                            col_type=catalog["columns"][name.upper()]["type"],
                            sort_order=int(catalog["columns"][name.upper()]["index"]),
                        )
                        parsed_columns.append(column)

                # Extract table info
                dbt_owner = manifest["config"].get("owner")
                schema = manifest["schema"];
                dbt_tags = []
                raw_sql = manifest["raw_sql"].replace("\ ", " ")
                model_name = schema.lower() + '_' + manifest["alias"].lower()
                if model_name in compiled_dict:
                    compiled_sql = compiled_dict[model_name]
                else:
                    compiled_sql = "Compiled SQL file not found"
                if isinstance(schema, str):
                    dbt_tags.append(schema)
                if isinstance(schema, list):
                    dbt_tags.extend(schema)
                if dbt_owner:
                    if isinstance(dbt_owner, str):
                        dbt_tags.append(dbt_owner)
                    if isinstance(dbt_owner, list):
                        dbt_tags.extend(dbt_owner)
                table = TableMetadata(
                    database=manifest["database"].lower(),
                    cluster=self._cluster.lower(),
                    schema=schema,
                    name=manifest["alias"],
                    owners=dbt_owner,
                    description=manifest["description"].replace("\ ", " "),
                    columns=parsed_columns,
                    is_view=(catalog["metadata"]["type"] == "VIEW"),
                    tags=dbt_tags,
                )
                results.append(table)

                programmatic_description = TableMetadata(
                    database=manifest["database"].lower(),
                    cluster=self._cluster.lower(),
                    schema=schema,
                    name=manifest["alias"],
                    description_source="compiled_sql",
                    description=compiled_sql.replace("\\", "\\\\"),
                )
                results.append(programmatic_description)

                # Extract table source info
                table_source_url = "/".join(
                    [self._source_url_prefix, manifest["original_file_path"]]
                )
                table_source = TableSource(
                    db_name=manifest["database"].lower(),
                    schema=schema,
                    table_name=manifest["alias"],
                    cluster=self._cluster.lower(),
                    source=table_source_url,
                    source_type="github",
                )
                results.append(table_source)

                # Extract table owner info
                if dbt_owner:
                    table_owner = TableOwner(
                        db_name=manifest["database"].lower(),
                        schema=schema,
                        table_name=manifest["alias"],
                        cluster=self._cluster.lower(),
                        owners=dbt_owner,
                    )
                    results.append(table_owner)

        self._iter = iter(results)

    def extract(self):
        # type: () -> Any
        """
        Yield the csv result one at a time.
        convert the result to model if a model_class is provided
        """
        try:
            return next(self._iter)
        except StopIteration:
            return None
        except Exception as e:
            raise e

    def get_scope(self):
        # type: () -> str
        return "extractor.dbt"
